#!/usr/bin/env python3
"""Agent Orchestrator Service for Agentic Retry Platform.

Acts as autonomous agent supervisor. Coordinates multi-stage tool execution,
manages workflow generations, handles speculative replanning upon planner timeout,
implements uncancelled orphaned work mechanics vs cancellation propagation,
enforces global retry budgets, and tracks durable queue leases.
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
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"agent-orchestrator","msg":"%(message)s"}',
)
logger = logging.getLogger("agent-orchestrator")

PORT = int(os.environ.get("PORT", "8000"))
TOOL_GATEWAY_URL = os.environ.get("TOOL_GATEWAY_URL", "http://tool-gateway:8001/tools/execute")
TOOL_GATEWAY_CANCEL_URL = os.environ.get("TOOL_GATEWAY_CANCEL_URL", "http://tool-gateway:8001/tools/cancel")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
POLICY_PATH = os.environ.get("POLICY_PATH", "/etc/agent-policy/policy.json")


def load_policy() -> dict:
    if os.path.exists(POLICY_PATH):
        try:
            with open(POLICY_PATH, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read policy from {POLICY_PATH}: {e}")
    return {
        "workflow": {
            "timeout_ms": 1500,
            "max_attempts": 3,
            "max_inflight": 100,
            "cancel_children_on_timeout": False,
        },
        "queue": {
            "visibility_timeout_ms": 2000,
        },
        "retry_budget": {
            "enabled": False,
            "max_physical_attempts_per_workflow": 4,
        },
    }


class OrchestratorState:
    def __init__(self):
        self.lock = threading.Lock()
        self.logical_requests = 0
        self.goodput_requests = 0
        self.failed_workflows = 0
        self.workflow_generations = 0
        self.active_workflows = 0
        self.total_duration_seconds = 0.0
        self.redis_backlog_count = 0
        self.queue_redeliveries = 0
        self.orphaned_operations = 0
        self.in_memory_queue = []

    def inc_active(self):
        with self.lock:
            self.active_workflows += 1

    def dec_active(self, success: bool, duration: float):
        with self.lock:
            self.active_workflows = max(0, self.active_workflows - 1)
            if success:
                self.goodput_requests += 1
            else:
                self.failed_workflows += 1
            self.total_duration_seconds += duration


state = OrchestratorState()


def dispatch_tool_cancellation(op_id: str):
    """Sends cancellation signal to Tool Gateway to kill child operation."""
    try:
        payload = json.dumps({"operation_id": op_id}).encode("utf-8")
        req = urllib.request.Request(TOOL_GATEWAY_CANCEL_URL, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Operation-ID", op_id)
        with urllib.request.urlopen(req, timeout=1.0) as resp:
            logger.info(f"Dispatched cancellation for op_id={op_id}: {resp.status}")
    except Exception as e:
        logger.warning(f"Failed to dispatch cancellation for op_id={op_id}: {e}")


def execute_workflow(req_id: str, wf_id: str) -> dict:
    """Executes workflow with speculative replanning and generation tracking."""
    policy = load_policy()
    wf_cfg = policy.get("workflow", {})
    planner_timeout = wf_cfg.get("timeout_ms", 1500) / 1000.0
    max_generations = wf_cfg.get("max_attempts", 3)
    cancel_children = wf_cfg.get("cancel_children_on_timeout", False)
    max_inflight = wf_cfg.get("max_inflight", 100)

    queue_cfg = policy.get("queue", {})
    visibility_timeout = queue_cfg.get("visibility_timeout_ms", 2000) / 1000.0

    budget_cfg = policy.get("retry_budget", {})
    initial_budget = budget_cfg.get("max_physical_attempts_per_workflow", 4)

    # Admission control check
    with state.lock:
        if state.active_workflows >= max_inflight:
            logger.warning(f"Admission control rejecting workflow {wf_id}: inflight={state.active_workflows}")
            return {"status": "error", "error": "Admission control: system saturated", "code": 503}
        state.logical_requests += 1

    state.inc_active()
    start_time = time.time()
    last_error = None
    remaining_budget = initial_budget

    active_ops: list[str] = []

    for gen in range(max_generations):
        with state.lock:
            state.workflow_generations += 1

        op_id = f"{wf_id}-op-gen{gen}"
        active_ops.append(op_id)

        payload = json.dumps({
            "logical_request_id": req_id,
            "workflow_id": wf_id,
            "operation_id": op_id,
            "generation": gen,
            "retry_budget": remaining_budget,
        }).encode("utf-8")

        req = urllib.request.Request(TOOL_GATEWAY_URL, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Logical-Request-ID", req_id)
        req.add_header("X-Workflow-ID", wf_id)
        req.add_header("X-Operation-ID", op_id)
        req.add_header("X-Generation", str(gen))
        req.add_header("X-Retry-Budget", str(remaining_budget))

        gen_start = time.time()
        try:
            with urllib.request.urlopen(req, timeout=planner_timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                duration = time.time() - start_time
                state.dec_active(success=True, duration=duration)
                logger.info(f"Workflow {wf_id} succeeded at generation {gen} in {duration:.3f}s")
                return {
                    "status": "success",
                    "logical_request_id": req_id,
                    "workflow_id": wf_id,
                    "completed_generation": gen,
                    "duration_seconds": duration,
                    "result": data,
                }
        except Exception as e:
            gen_elapsed = time.time() - gen_start
            last_error = str(e)
            is_timeout = "timed out" in str(e).lower()

            logger.warning(
                f"Generation {gen} (op={op_id}) failed for wf={wf_id} after {gen_elapsed:.3f}s: {last_error}"
            )

            # Check queue lease expiration
            if gen_elapsed > visibility_timeout:
                with state.lock:
                    state.queue_redeliveries += 1
                logger.info(f"Queue lease expired for wf={wf_id} ({gen_elapsed:.3f}s > {visibility_timeout}s)")

            # Speculative replanning: launch next generation
            # If cancellation is enabled, cancel the timed-out operation
            if cancel_children:
                logger.info(f"Cancellation enabled: killing orphaned operation op={op_id}")
                threading.Thread(target=dispatch_tool_cancellation, args=(op_id,), daemon=True).start()
            else:
                # Decoupled lifetime: operation op_id is left running in the backend as ORPHANED WORK
                with state.lock:
                    state.orphaned_operations += 1
                logger.warning(f"Uncancelled operation op={op_id} continues running in background (ORPHANED WORK)")

            remaining_budget = max(0, remaining_budget - 2)

    duration = time.time() - start_time
    state.dec_active(success=False, duration=duration)
    return {
        "status": "error",
        "logical_request_id": req_id,
        "workflow_id": wf_id,
        "generations_attempted": max_generations,
        "duration_seconds": duration,
        "error": f"Planner exhausted all {max_generations} speculative replans: {last_error}",
    }


class OrchestratorHandler(BaseHTTPRequestHandler):
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
            self._send_json(200, {"status": "ok", "service": "agent-orchestrator"})
            return

        if self.path == "/metrics":
            with state.lock:
                avg_dur = state.total_duration_seconds / max(1, state.goodput_requests)
                metrics_text = (
                    f"# HELP logical_workflows_total Total unique logical workflows submitted\n"
                    f"# TYPE logical_workflows_total counter\n"
                    f"logical_workflows_total {state.logical_requests}\n"
                    f"# HELP goodput_requests_total Successful unique logical requests completed\n"
                    f"# TYPE goodput_requests_total counter\n"
                    f"goodput_requests_total {state.goodput_requests}\n"
                    f"# HELP workflow_generation_total Total speculative replanning branches created\n"
                    f"# TYPE workflow_generation_total counter\n"
                    f"workflow_generation_total {state.workflow_generations}\n"
                    f"# HELP queue_redeliveries_total Tasks redelivered due to lease expiration\n"
                    f"# TYPE queue_redeliveries_total counter\n"
                    f"queue_redeliveries_total {state.queue_redeliveries}\n"
                    f"# HELP orphaned_operations_created Operations left running after planner timeout\n"
                    f"# TYPE orphaned_operations_created counter\n"
                    f"orphaned_operations_created {state.orphaned_operations}\n"
                    f"# HELP workflow_active_count Currently active workflows\n"
                    f"# TYPE workflow_active_count gauge\n"
                    f"workflow_active_count {state.active_workflows}\n"
                    f"# HELP workflow_duration_seconds Average workflow completion duration\n"
                    f"# TYPE workflow_duration_seconds gauge\n"
                    f"workflow_duration_seconds {avg_dur:.4f}\n"
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

        if self.path in ("/agent/workflow", "/query", "/workflow"):
            req_id = self.headers.get("X-Logical-Request-ID", body.get("logical_request_id", f"req-{time.time()}"))
            wf_id = self.headers.get("X-Workflow-ID", body.get("workflow_id", f"wf-{time.time()}"))

            result = execute_workflow(req_id, wf_id)
            code = result.get("code", 200 if result["status"] == "success" else 504)
            self._send_json(code, result)
            return

        self._send_json(404, {"error": "Not Found"})

    def log_message(self, format, *args):
        pass


def run_server():
    server_address = ("0.0.0.0", PORT)
    httpd = ThreadingHTTPServer(server_address, OrchestratorHandler)
    logger.info(f"Starting Agent Orchestrator on 0.0.0.0:{PORT} (tools={TOOL_GATEWAY_URL})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Agent Orchestrator...")
        httpd.server_close()


if __name__ == "__main__":
    run_server()
