#!/usr/bin/env python3
"""Tool Gateway Service for Agentic Retry Platform.

Executes autonomous agent tool calls by delegating to Data API.
Enforces timeouts (600ms), HTTP transport retries (R_http = 2),
tool-level retries (R_tool = 2), circuit breaking, and trace header propagation.
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
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "0.60"))         # 600ms per attempt
HTTP_MAX_RETRIES = int(os.environ.get("HTTP_MAX_RETRIES", "2"))     # R_http = 2
TOOL_MAX_RETRIES = int(os.environ.get("TOOL_MAX_RETRIES", "2"))     # R_tool = 2


class ToolGatewayMetrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.tool_requests = 0
        self.tool_successes = 0
        self.tool_failures = 0
        self.tool_retries = 0
        self.http_attempts = 0
        self.http_timeouts = 0
        self.circuit_breaker_state = "closed"  # closed, open, half_open
        self.consecutive_failures = 0
        self.cb_failure_threshold = 15
        self.cb_reset_timeout = 5.0
        self.cb_opened_at = 0.0

    def record_attempt(self, success: bool, is_timeout: bool = False):
        with self.lock:
            self.http_attempts += 1
            if is_timeout:
                self.http_timeouts += 1
            if success:
                self.consecutive_failures = 0
                if self.circuit_breaker_state == "half_open":
                    self.circuit_breaker_state = "closed"
            else:
                self.consecutive_failures += 1
                if self.consecutive_failures >= self.cb_failure_threshold and self.circuit_breaker_state == "closed":
                    self.circuit_breaker_state = "open"
                    self.cb_opened_at = time.time()
                    logger.warning(f"Circuit breaker tripped OPEN after {self.consecutive_failures} consecutive failures")

    def check_circuit_breaker(self) -> bool:
        """Returns True if request is allowed, False if blocked by circuit breaker."""
        with self.lock:
            if self.circuit_breaker_state == "closed":
                return True
            if self.circuit_breaker_state == "open":
                if time.time() - self.cb_opened_at > self.cb_reset_timeout:
                    self.circuit_breaker_state = "half_open"
                    logger.info("Circuit breaker entering HALF_OPEN probe state")
                    return True
                return False
            # half_open allows probe
            return True


metrics = ToolGatewayMetrics()


def execute_http_call(req_id: str, wf_id: str, attempt: int, layer: str) -> dict:
    """Executes single HTTP call to Data API with timeout."""
    payload = json.dumps({
        "logical_request_id": req_id,
        "workflow_id": wf_id,
        "attempt": attempt,
        "retry_layer": layer,
    }).encode("utf-8")

    req = urllib.request.Request(DATA_API_URL, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-Logical-Request-ID", req_id)
    req.add_header("X-Workflow-ID", wf_id)
    req.add_header("X-Attempt", str(attempt))
    req.add_header("X-Retry-Layer", layer)

    start = time.time()
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            metrics.record_attempt(success=True)
            return {"status": "ok", "latency": time.time() - start, "data": data}
    except urllib.error.HTTPError as e:
        metrics.record_attempt(success=False)
        return {"status": "error", "error": f"HTTP {e.code}", "latency": time.time() - start}
    except Exception as e:
        is_to = "timed out" in str(e).lower()
        metrics.record_attempt(success=False, is_timeout=is_to)
        return {"status": "error", "error": str(e), "is_timeout": is_to, "latency": time.time() - start}


def execute_tool_with_retries(req_id: str, wf_id: str, planner_attempt: int) -> dict:
    """Executes tool with nested R_tool retries and R_http transport retries."""
    if not metrics.check_circuit_breaker():
        return {
            "status": "error",
            "error": "Circuit breaker OPEN: shedding load to allow downstream recovery",
            "circuit_breaker": "open",
        }

    with metrics.lock:
        metrics.tool_requests += 1

    last_error = None
    for tool_try in range(TOOL_MAX_RETRIES + 1):
        if tool_try > 0:
            with metrics.lock:
                metrics.tool_retries += 1

        layer = "tool" if tool_try > 0 else "initial"

        # Inner transport retry loop (R_http)
        for http_try in range(HTTP_MAX_RETRIES + 1):
            http_layer = "transport" if http_try > 0 else layer
            res = execute_http_call(req_id, wf_id, tool_try * (HTTP_MAX_RETRIES + 1) + http_try + 1, http_layer)
            if res["status"] == "ok":
                with metrics.lock:
                    metrics.tool_successes += 1
                return {
                    "status": "success",
                    "tool_retries_used": tool_try,
                    "http_retries_used": http_try,
                    "result": res["data"],
                }
            last_error = res.get("error", "unknown error")

    with metrics.lock:
        metrics.tool_failures += 1

    return {
        "status": "error",
        "error": f"Tool execution failed after {TOOL_MAX_RETRIES} tool retries: {last_error}",
        "last_error": last_error,
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
                    f"# HELP tool_requests_total Total logical tool requests received\n"
                    f"# TYPE tool_requests_total counter\n"
                    f"tool_requests_total {metrics.tool_requests}\n"
                    f"# HELP tool_retries_total Retries executed at tool level\n"
                    f"# TYPE tool_retries_total counter\n"
                    f"tool_retries_total {metrics.tool_retries}\n"
                    f"# HELP tool_timeouts_total Downstream HTTP timeouts experienced by tool gateway\n"
                    f"# TYPE tool_timeouts_total counter\n"
                    f"tool_timeouts_total {metrics.http_timeouts}\n"
                    f"# HELP tool_http_attempts_total Total HTTP calls dispatched to data API\n"
                    f"# TYPE tool_http_attempts_total counter\n"
                    f"tool_http_attempts_total {metrics.http_attempts}\n"
                    f"# HELP tool_circuit_breaker_tripped Whether circuit breaker is open (1) or closed (0)\n"
                    f"# TYPE tool_circuit_breaker_tripped gauge\n"
                    f"tool_circuit_breaker_tripped {1 if metrics.circuit_breaker_state == 'open' else 0}\n"
                )
            self._send_text(200, metrics_text, "text/plain; version=0.0.4")
            return

        if self.path == "/config":
            self._send_json(200, {
                "data_api_url": DATA_API_URL,
                "http_timeout": HTTP_TIMEOUT,
                "http_max_retries": HTTP_MAX_RETRIES,
                "tool_max_retries": TOOL_MAX_RETRIES,
            })
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

        if self.path in ("/tools/execute", "/execute"):
            req_id = self.headers.get("X-Logical-Request-ID", body.get("logical_request_id", "req-unknown"))
            wf_id = self.headers.get("X-Workflow-ID", body.get("workflow_id", "wf-unknown"))
            attempt = int(self.headers.get("X-Attempt", body.get("attempt", 1)))

            result = execute_tool_with_retries(req_id, wf_id, attempt)
            if result["status"] == "success":
                self._send_json(200, result)
            elif result.get("circuit_breaker") == "open":
                self._send_json(503, result)
            else:
                self._send_json(504, result)
            return

        self._send_json(404, {"error": "Not Found"})

    def log_message(self, format, *args):
        pass


def run_server():
    server_address = ("0.0.0.0", PORT)
    httpd = ThreadingHTTPServer(server_address, ToolGatewayHandler)
    logger.info(f"Starting Tool Gateway on 0.0.0.0:{PORT} (data_api={DATA_API_URL}, timeout={HTTP_TIMEOUT}s, retries_tool={TOOL_MAX_RETRIES}, retries_http={HTTP_MAX_RETRIES})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Tool Gateway...")
        httpd.server_close()


if __name__ == "__main__":
    run_server()
