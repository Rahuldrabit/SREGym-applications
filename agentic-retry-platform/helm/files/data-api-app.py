#!/usr/bin/env python3
"""Data API Service for Agentic Retry Platform.

Executes database queries through PgBouncer connection pool against PostgreSQL.
Models a finite concurrency bottleneck (pool=25), queue saturation,
self-reverting administrative fault injection, and Prometheus metrics.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"data-api","msg":"%(message)s"}',
)
logger = logging.getLogger("data-api")

PORT = int(os.environ.get("PORT", "8002"))
CONCURRENCY_LIMIT = int(os.environ.get("CONCURRENCY_LIMIT", "25"))
QUEUE_CAPACITY = int(os.environ.get("QUEUE_CAPACITY", "200"))
NORMAL_LATENCY = float(os.environ.get("NORMAL_LATENCY", "0.10"))  # 100ms
FAULT_LATENCY = float(os.environ.get("FAULT_LATENCY", "1.50"))    # 1500ms


class DataAPIState:
    def __init__(self):
        self.lock = threading.Lock()
        self.concurrency_limit = CONCURRENCY_LIMIT
        self.queue_capacity = QUEUE_CAPACITY
        self.normal_latency = NORMAL_LATENCY
        self.fault_latency = FAULT_LATENCY
        self.fault_active = False
        self.fault_timer: threading.Timer | None = None

        self.semaphore = threading.BoundedSemaphore(self.concurrency_limit)
        self.active_requests = 0
        self.queued_requests = 0
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_latency_seconds = 0.0

    def current_latency(self) -> float:
        with self.lock:
            return self.fault_latency if self.fault_active else self.normal_latency

    def inject_fault(self, latency_ms: float = 1500.0, duration_seconds: float = 10.0):
        with self.lock:
            if self.fault_timer is not None:
                self.fault_timer.cancel()
            self.fault_latency = latency_ms / 1000.0
            self.fault_active = True
            logger.warning(f"Injected transient fault: latency={self.fault_latency}s for {duration_seconds}s")

            def _revert():
                with self.lock:
                    self.fault_active = False
                    self.fault_timer = None
                logger.info(f"Transient fault duration elapsed ({duration_seconds}s); restored normal latency ({self.normal_latency}s)")

            self.fault_timer = threading.Timer(duration_seconds, _revert)
            self.fault_timer.daemon = True
            self.fault_timer.start()

    def recover_fault(self):
        with self.lock:
            if self.fault_timer is not None:
                self.fault_timer.cancel()
                self.fault_timer = None
            self.fault_active = False
        logger.info(f"Explicit fault recovery; restored normal latency ({self.normal_latency}s)")


state = DataAPIState()


class DataAPIHandler(BaseHTTPRequestHandler):
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
            self._send_json(200, {"status": "ok", "service": "data-api"})
            return

        if self.path == "/metrics":
            with state.lock:
                avg_lat = (state.total_latency_seconds / max(1, state.successful_requests))
                metrics_text = (
                    f"# HELP backend_requests_total Total number of backend query attempts\n"
                    f"# TYPE backend_requests_total counter\n"
                    f"backend_requests_total {state.total_requests}\n"
                    f"# HELP backend_requests_success_total Successful backend queries\n"
                    f"# TYPE backend_requests_success_total counter\n"
                    f"backend_requests_success_total {state.successful_requests}\n"
                    f"# HELP backend_requests_failed_total Failed backend queries\n"
                    f"# TYPE backend_requests_failed_total counter\n"
                    f"backend_requests_failed_total {state.failed_requests}\n"
                    f"# HELP backend_active_requests Currently executing requests\n"
                    f"# TYPE backend_active_requests gauge\n"
                    f"backend_active_requests {state.active_requests}\n"
                    f"# HELP backend_waiting_requests Currently queued requests awaiting connection pool\n"
                    f"# TYPE backend_waiting_requests gauge\n"
                    f"backend_waiting_requests {state.queued_requests}\n"
                    f"# HELP pgbouncer_used_connections Active PgBouncer connections\n"
                    f"# TYPE pgbouncer_used_connections gauge\n"
                    f"pgbouncer_used_connections {state.active_requests}\n"
                    f"# HELP pgbouncer_waiting_clients Clients waiting for a PgBouncer connection slot\n"
                    f"# TYPE pgbouncer_waiting_clients gauge\n"
                    f"pgbouncer_waiting_clients {state.queued_requests}\n"
                    f"# HELP backend_request_duration_seconds Average latency in seconds\n"
                    f"# TYPE backend_request_duration_seconds gauge\n"
                    f"backend_request_duration_seconds {avg_lat:.4f}\n"
                )
            self._send_text(200, metrics_text, "text/plain; version=0.0.4")
            return

        if self.path == "/status":
            with state.lock:
                self._send_json(200, {
                    "active_requests": state.active_requests,
                    "queued_requests": state.queued_requests,
                    "concurrency_limit": state.concurrency_limit,
                    "total_requests": state.total_requests,
                    "successful_requests": state.successful_requests,
                    "failed_requests": state.failed_requests,
                    "fault_active": state.fault_active,
                    "current_latency": state.current_latency(),
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

        if self.path == "/admin/fault":
            latency_ms = float(body.get("latency_ms", 1500.0))
            duration_seconds = float(body.get("duration_seconds", body.get("duration", 10.0)))
            state.inject_fault(latency_ms, duration_seconds)
            self._send_json(200, {
                "status": "fault_injected",
                "latency_ms": latency_ms,
                "duration_seconds": duration_seconds,
            })
            return

        if self.path == "/admin/recover":
            state.recover_fault()
            self._send_json(200, {"status": "fault_recovered"})
            return

        if self.path in ("/data/query", "/query"):
            req_id = self.headers.get("X-Logical-Request-ID", body.get("logical_request_id", "unknown"))
            wf_id = self.headers.get("X-Workflow-ID", body.get("workflow_id", "unknown"))
            attempt = self.headers.get("X-Attempt", str(body.get("attempt", 1)))
            retry_layer = self.headers.get("X-Retry-Layer", body.get("retry_layer", "initial"))

            with state.lock:
                state.total_requests += 1
                if state.active_requests >= state.concurrency_limit:
                    if state.queued_requests >= state.queue_capacity:
                        state.failed_requests += 1
                        self._send_json(503, {
                            "error": "PgBouncer pool and queue saturated (503 Service Unavailable)",
                            "active": state.active_requests,
                            "queued": state.queued_requests,
                        })
                        return
                    state.queued_requests += 1
                    in_queue = True
                else:
                    state.active_requests += 1
                    in_queue = False

            acquired = False
            start_time = time.time()
            try:
                if in_queue:
                    # Wait for a connection pool slot (up to 3.0s timeout)
                    acquired = state.semaphore.acquire(timeout=3.0)
                    if not acquired:
                        with state.lock:
                            state.failed_requests += 1
                            state.queued_requests = max(0, state.queued_requests - 1)
                        self._send_json(504, {"error": "Timed out waiting for PgBouncer connection slot"})
                        return
                    with state.lock:
                        state.queued_requests = max(0, state.queued_requests - 1)
                        state.active_requests += 1
                else:
                    acquired = state.semaphore.acquire(blocking=False)
                    if not acquired:
                        # Fallback if state raced
                        state.semaphore.acquire()

                # Process database query with current latency
                latency = state.current_latency()
                time.sleep(latency)

                elapsed = time.time() - start_time
                with state.lock:
                    state.successful_requests += 1
                    state.total_latency_seconds += elapsed

                logger.info(
                    f"Processed DB query req={req_id} wf={wf_id} attempt={attempt} "
                    f"layer={retry_layer} latency={elapsed:.3f}s"
                )

                self._send_json(200, {
                    "status": "success",
                    "logical_request_id": req_id,
                    "workflow_id": wf_id,
                    "attempt": attempt,
                    "retry_layer": retry_layer,
                    "execution_time_seconds": elapsed,
                    "records": [
                        {"id": 101, "key": "agent_state", "value": "persisted"},
                        {"id": 102, "key": "retrieval_doc", "value": "context_vector_77"},
                    ],
                })
            finally:
                if acquired:
                    state.semaphore.release()
                    with state.lock:
                        state.active_requests = max(0, state.active_requests - 1)
            return

        self._send_json(404, {"error": "Not Found"})

    def log_message(self, format, *args):
        pass


def run_server():
    server_address = ("0.0.0.0", PORT)
    httpd = ThreadingHTTPServer(server_address, DataAPIHandler)
    logger.info(f"Starting Data API on 0.0.0.0:{PORT} (concurrency={CONCURRENCY_LIMIT}, normal_lat={NORMAL_LATENCY}s, fault_lat={FAULT_LATENCY}s)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Data API...")
        httpd.server_close()


if __name__ == "__main__":
    run_server()
