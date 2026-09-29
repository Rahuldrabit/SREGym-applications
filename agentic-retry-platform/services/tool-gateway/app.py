#!/usr/bin/env python3
"""Tool Gateway Service for Agentic Retry Platform.

Executes autonomous agent tool calls by delegating to Data API.
Enforces timeout hierarchy, tool-level retries, HTTP transport retries,
cancellation propagation (POST /tools/cancel), and end-to-end retry budget tracking.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"tool-gateway","msg":"%(message)s"}',
)
logger = logging.getLogger("tool-gateway")

PORT = int(os.environ.get("PORT", "8001"))
DATA_API_URL = os.environ.get("DATA_API_URL", "http://data-api:8002/data/query")
DATA_API_CANCEL_URL = os.environ.get("DATA_API_CANCEL_URL", "http://data-api:8002/data/cancel")
POLICY_PATH = os.environ.get("POLICY_PATH", "/etc/agent-policy/policy.json")


def load_policy() -> dict:
    if os.path.exists(POLICY_PATH):
        try:
            with open(POLICY_PATH, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read policy from {POLICY_PATH}: {e}")
    return {
        "tool": {"timeout_ms": 1400, "max_attempts": 2},
        "transport": {"timeout_ms": 1200, "max_attempts": 2},
        "retry_budget": {"enabled": False, "max_physical_attempts_per_workflow": 4},
    }


class ToolGatewayMetrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.operations_started = 0
        self.operations_completed = 0
        self.unique_operations: set[str] = set()
        self.tool_retries = 0
        self.http_attempts = 0
        self.http_timeouts = 0
        self.cancellations_forwarded = 0
        self.budget_exhausted_total = 0


metrics = ToolGatewayMetrics()


def forward_cancellation(op_id: str):
    """Sends cancellation request downstream to Data API."""
    try:
        payload = json.dumps({"operation_id": op_id}).encode("utf-8")
        req = urllib.request.Request(DATA_API_CANCEL_URL, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Operation-ID", op_id)
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            logger.info(f"Forwarded cancellation for op_id={op_id}: {resp.status}")
        with metrics.lock:
            metrics.cancellations_forwarded += 1
    except Exception as e:
        logger.warning(f"Failed to forward cancellation for {op_id}: {e}")


def execute_http_call(req_id: str, wf_id: str, op_id: str, gen: int, attempt: int, layer: str, budget: int) -> dict:
    policy = load_policy()
    transport_timeout = policy.get("transport", {}).get("timeout_ms", 1200) / 1000.0

    payload = json.dumps({
        "logical_request_id": req_id,
        "workflow_id": wf_id,
        "operation_id": op_id,
        "generation": gen,
        "attempt": attempt,
        "retry_layer": layer,
        "retry_budget": budget,
    }).encode("utf-8")

    req = urllib.request.Request(DATA_API_URL, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-Logical-Request-ID", req_id)
    req.add_header("X-Workflow-ID", wf_id)
    req.add_header("X-Operation-ID", op_id)
    req.add_header("X-Generation", str(gen))
    req.add_header("X-Attempt", str(attempt))
    req.add_header("X-Retry-Layer", layer)
    req.add_header("X-Retry-Budget", str(budget))

    start = time.time()
    with metrics.lock:
        metrics.http_attempts += 1

    try:
        with urllib.request.urlopen(req, timeout=transport_timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return {"status": "ok", "latency": time.time() - start, "data": data}
    except urllib.error.HTTPError as e:
        return {"status": "error", "error": f"HTTP {e.code}", "latency": time.time() - start}
    except Exception as e:
        is_to = "timed out" in str(e).lower()
        if is_to:
            with metrics.lock:
                metrics.http_timeouts += 1
        return {"status": "error", "error": str(e), "is_timeout": is_to, "latency": time.time() - start}


def execute_tool_operation(req_id: str, wf_id: str, op_id: str, gen: int, budget: int) -> dict:
    policy = load_policy()
    tool_retries = policy.get("tool", {}).get("max_attempts", 2)
    http_retries = policy.get("transport", {}).get("max_attempts", 2)
    budget_enabled = policy.get("retry_budget", {}).get("enabled", False)

    with metrics.lock:
        metrics.operations_started += 1
        metrics.unique_operations.add(op_id)

    curr_budget = budget
    last_error = None

    for tool_try in range(tool_retries + 1):
        if tool_try > 0:
            with metrics.lock:
                metrics.tool_retries += 1

        layer = "tool" if tool_try > 0 else "initial"

        for http_try in range(http_retries + 1):
            if budget_enabled and curr_budget <= 0:
                with metrics.lock:
                    metrics.budget_exhausted_total += 1
                logger.warning(f"Retry budget exhausted ({budget}) for wf={wf_id} op={op_id}")
                return {
                    "status": "error",
                    "error": "Global retry budget exhausted",
                    "budget_exhausted": True,
                }

            curr_budget -= 1
            http_layer = "transport" if http_try > 0 else layer
            res = execute_http_call(req_id, wf_id, op_id, gen, tool_try * 3 + http_try + 1, http_layer, curr_budget)

            if res["status"] == "ok":
                with metrics.lock:
                    metrics.operations_completed += 1
                return {
                    "status": "success",
                    "operation_id": op_id,
                    "generation": gen,
                    "tool_retries": tool_try,
                    "http_retries": http_try,
                    "remaining_budget": curr_budget,
                    "result": res["data"],
                }
            last_error = res.get("error", "timeout")

    return {
        "status": "error",
        "operation_id": op_id,
        "generation": gen,
        "error": f"Tool execution failed: {last_error}",
        "remaining_budget": curr_budget,
    }


class ToolGatewayHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, data: dict):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, status_code: int, text: str, content_type: str = "text/plain"):
        body = text.encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/healthz", "/"):
            self._send_json(200, {"status": "ok", "service": "tool-gateway"})
            return

        if self.path == "/metrics":
            with metrics.lock:
                metrics_text = (
                    f"# HELP tool_operations_started_total Total tool operations started\n"
                    f"# TYPE tool_operations_started_total counter\n"
                    f"tool_operations_started_total {metrics.operations_started}\n"
                    f"# HELP tool_operations_completed_total Successfully completed tool operations\n"
                    f"# TYPE tool_operations_completed_total counter\n"
                    f"tool_operations_completed_total {metrics.operations_completed}\n"
                    f"# HELP unique_operation_ids_total Unique operation IDs handled\n"
                    f"# TYPE unique_operation_ids_total counter\n"
                    f"unique_operation_ids_total {len(metrics.unique_operations)}\n"
                    f"# HELP tool_retries_total Retries executed at tool level\n"
                    f"# TYPE tool_retries_total counter\n"
                    f"tool_retries_total {metrics.tool_retries}\n"
                    f"# HELP tool_http_attempts_total Total HTTP calls to Data API\n"
                    f"# TYPE tool_http_attempts_total counter\n"
                    f"tool_http_attempts_total {metrics.http_attempts}\n"
                    f"# HELP tool_cancellations_forwarded Cancellations forwarded to Data API\n"
                    f"# TYPE tool_cancellations_forwarded counter\n"
                    f"tool_cancellations_forwarded {metrics.cancellations_forwarded}\n"
                    f"# HELP retry_budget_exhausted_total Invocations blocked by retry budget\n"
                    f"# TYPE retry_budget_exhausted_total counter\n"
                    f"retry_budget_exhausted_total {metrics.budget_exhausted_total}\n"
                )
            self._send_text(200, metrics_text, "text/plain; version=0.0.4")
            return

        if self.path == "/config":
            self._send_json(200, load_policy())
            return

        self._send_json(404, {"error": "Not Found"})

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = {}
        if content_length > 0:
            try:
                body = json.loads(self.rfile.read(content_length).decode("utf-8"))
            except Exception:
                body = {}

        if self.path in ("/tools/cancel", "/cancel"):
            op_id = self.headers.get("X-Operation-ID", body.get("operation_id", ""))
            threading.Thread(target=forward_cancellation, args=(op_id,), daemon=True).start()
            self._send_json(200, {"status": "cancellation_dispatched", "operation_id": op_id})
            return

        if self.path in ("/tools/execute", "/execute"):
            req_id = self.headers.get("X-Logical-Request-ID", body.get("logical_request_id", "req-unknown"))
            wf_id = self.headers.get("X-Workflow-ID", body.get("workflow_id", "wf-unknown"))
            op_id = self.headers.get("X-Operation-ID", body.get("operation_id", f"{wf_id}-op"))
            gen = int(self.headers.get("X-Generation", body.get("generation", 0)))
            budget = int(self.headers.get("X-Retry-Budget", body.get("retry_budget", 4)))

            result = execute_tool_operation(req_id, wf_id, op_id, gen, budget)
            if result["status"] == "success":
                self._send_json(200, result)
            else:
                self._send_json(504, result)
            return

        self._send_json(404, {"error": "Not Found"})

    def log_message(self, format, *args):
        pass


def run_server():
    server_address = ("0.0.0.0", PORT)
    httpd = ThreadingHTTPServer(server_address, ToolGatewayHandler)
    logger.info(f"Starting Tool Gateway on 0.0.0.0:{PORT} (data_api={DATA_API_URL})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Tool Gateway...")
        httpd.server_close()


if __name__ == "__main__":
    run_server()
