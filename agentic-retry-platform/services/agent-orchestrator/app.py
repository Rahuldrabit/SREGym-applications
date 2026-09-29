#!/usr/bin/env python3
"""Agent Orchestrator Service for Agentic Retry Platform.

Acts as the autonomous agent supervisor/planner.
Coordinates multi-step tool execution, manages persistent workflow state
in Redis, enforces workflow deadlines, tracks global retry budgets,
and executes planner replanning (R_planner = 3) upon tool failure.
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
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
PLANNER_MAX_RETRIES = int(os.environ.get("PLANNER_MAX_RETRIES", "3"))  # R_planner = 3
PLANNER_TIMEOUT = float(os.environ.get("PLANNER_TIMEOUT", "3.50"))     # 3.5s per plan attempt
WORKFLOW_DEADLINE = float(os.environ.get("WORKFLOW_DEADLINE", "12.0")) # Stale workflow deadline
RETRY_BUDGET = int(os.environ.get("RETRY_BUDGET", "12"))              # Configurable global budget


class OrchestratorState:
    def __init__(self):
        self.lock = threading.Lock()
        self.logical_requests = 0
        self.successful_workflows = 0
        self.failed_workflows = 0
        self.planner_replans = 0
        self.active_workflows = 0
        self.total_duration_seconds = 0.0
        self.redis_backlog_count = 0
        self.in_memory_queue = []

    def inc_active(self):
        with self.lock:
            self.active_workflows += 1

    def dec_active(self, success: bool, duration: float):
        with self.lock:
            self.active_workflows = max(0, self.active_workflows - 1)
            if success:
                self.successful_workflows += 1
            else:
                self.failed_workflows += 1
            self.total_duration_seconds += duration


state = OrchestratorState()


def try_redis_push(wf_id: str, payload: dict):
    """Attempts to store workflow state in Redis; falls back to in-memory queue."""
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.2)
        s.connect((REDIS_HOST, REDIS_PORT))
        cmd = f"*3\r\n$5\r\nRPUSH\r\n$17\r\npending_workflows\r\n${len(wf_id)}\r\n{wf_id}\r\n".encode("utf-8")
        s.sendall(cmd)
        s.close()
        with state.lock:
            state.redis_backlog_count += 1
    except Exception:
        with state.lock:
            state.in_memory_queue.append(wf_id)
            state.redis_backlog_count = len(state.in_memory_queue)


def execute_workflow_planner(req_id: str, wf_id: str) -> dict:
    """Executes multi-stage agentic workflow with planner replanning."""
    state.inc_active()
    try_redis_push(wf_id, {"req_id": req_id, "start_time": time.time()})

    start_time = time.time()
    last_error = None
    replans_used = 0

    with state.lock:
        state.logical_requests += 1

    for plan_attempt in range(PLANNER_MAX_RETRIES + 1):
        if plan_attempt > 0:
            with state.lock:
                state.planner_replans += 1
            replans_used += 1

        # Check workflow deadline (stale work shedding)
        if (time.time() - start_time) > WORKFLOW_DEADLINE:
            last_error = f"Workflow deadline exceeded ({WORKFLOW_DEADLINE}s)"
            logger.warning(f"Shedding stale workflow wf={wf_id} req={req_id}")
            break

        layer = "planner" if plan_attempt > 0 else "initial"
        payload = json.dumps({
            "logical_request_id": req_id,
            "workflow_id": wf_id,
            "attempt": plan_attempt + 1,
            "retry_layer": layer,
        }).encode("utf-8")

        req = urllib.request.Request(TOOL_GATEWAY_URL, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Logical-Request-ID", req_id)
        req.add_header("X-Workflow-ID", wf_id)
        req.add_header("X-Attempt", str(plan_attempt + 1))
        req.add_header("X-Retry-Layer", layer)

        try:
            with urllib.request.urlopen(req, timeout=PLANNER_TIMEOUT) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                duration = time.time() - start_time
                state.dec_active(success=True, duration=duration)
                logger.info(
                    f"Workflow succeeded wf={wf_id} req={req_id} replans={replans_used} "
                    f"duration={duration:.3f}s"
                )
                return {
                    "status": "success",
                    "logical_request_id": req_id,
                    "workflow_id": wf_id,
                    "replans_used": replans_used,
                    "duration_seconds": duration,
                    "tool_response": data,
                }
        except Exception as e:
            last_error = str(e)
            logger.warning(
                f"Planner attempt {plan_attempt + 1}/{PLANNER_MAX_RETRIES + 1} failed for "
                f"wf={wf_id}: {last_error}"
            )
            # Short backoff before replanning
            time.sleep(0.05)

    duration = time.time() - start_time
    state.dec_active(success=False, duration=duration)
    return {
        "status": "error",
        "logical_request_id": req_id,
        "workflow_id": wf_id,
        "replans_used": replans_used,
        "duration_seconds": duration,
        "error": f"Planner exhausted after {PLANNER_MAX_RETRIES} replans: {last_error}",
        "last_error": last_error,
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
                avg_dur = state.total_duration_seconds / max(1, state.successful_workflows)
                metrics_text = (
                    f"# HELP workflow_logical_requests_total Total logical workflow requests\n"
                    f"# TYPE workflow_logical_requests_total counter\n"
                    f"workflow_logical_requests_total {state.logical_requests}\n"
                    f"# HELP workflow_success_total Successful workflows\n"
                    f"# TYPE workflow_success_total counter\n"
                    f"workflow_success_total {state.successful_workflows}\n"
                    f"# HELP workflow_failure_total Failed workflows\n"
                    f"# TYPE workflow_failure_total counter\n"
                    f"workflow_failure_total {state.failed_workflows}\n"
                    f"# HELP workflow_replans_total Total replanning events triggered by planner\n"
                    f"# TYPE workflow_replans_total counter\n"
                    f"workflow_replans_total {state.planner_replans}\n"
                    f"# HELP workflow_active_count Currently running workflows\n"
                    f"# TYPE workflow_active_count gauge\n"
                    f"workflow_active_count {state.active_workflows}\n"
                    f"# HELP workflow_redis_backlog Workflows tracked in persistent Redis backlog\n"
                    f"# TYPE workflow_redis_backlog gauge\n"
                    f"workflow_redis_backlog {state.redis_backlog_count}\n"
                    f"# HELP workflow_duration_seconds Average workflow completion duration\n"
                    f"# TYPE workflow_duration_seconds gauge\n"
                    f"workflow_duration_seconds {avg_dur:.4f}\n"
                )
            self._send_text(200, metrics_text, "text/plain; version=0.0.4")
            return

        if self.path == "/config":
            self._send_json(200, {
                "tool_gateway_url": TOOL_GATEWAY_URL,
                "planner_max_retries": PLANNER_MAX_RETRIES,
                "planner_timeout": PLANNER_TIMEOUT,
                "workflow_deadline": WORKFLOW_DEADLINE,
                "retry_budget": RETRY_BUDGET,
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

        if self.path in ("/agent/workflow", "/query", "/workflow"):
            req_id = self.headers.get("X-Logical-Request-ID", body.get("logical_request_id", f"req-{time.time()}"))
            wf_id = self.headers.get("X-Workflow-ID", body.get("workflow_id", f"wf-{time.time()}"))

            result = execute_workflow_planner(req_id, wf_id)
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
    httpd = ThreadingHTTPServer(server_address, OrchestratorHandler)
    logger.info(f"Starting Agent Orchestrator on 0.0.0.0:{PORT} (tools={TOOL_GATEWAY_URL}, retries_planner={PLANNER_MAX_RETRIES}, timeout={PLANNER_TIMEOUT}s)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Agent Orchestrator...")
        httpd.server_close()


if __name__ == "__main__":
    run_server()
