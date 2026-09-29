#!/usr/bin/env python3
"""Backend Service for Agentic RAG Platform.

Acts as the concurrency-bound knowledge base / vector database / retrieval store.
Supports concurrency limits, queue depth tracking, latency perturbation injection,
and metrics reporting.
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
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("backend-service")

PORT = int(os.environ.get("PORT", "8002"))
CONCURRENCY_LIMIT = int(os.environ.get("CONCURRENCY_LIMIT", "25"))
NORMAL_LATENCY = float(os.environ.get("NORMAL_LATENCY", "0.25"))
FAULT_LATENCY = float(os.environ.get("FAULT_LATENCY", "1.5"))
QUEUE_CAPACITY = int(os.environ.get("QUEUE_CAPACITY", "50"))


class BackendState:
    def __init__(self):
        self.lock = threading.Lock()
        self.concurrency_limit = CONCURRENCY_LIMIT
        self.normal_latency = NORMAL_LATENCY
        self.fault_latency = FAULT_LATENCY
        self.fault_active = False

        self.semaphore = threading.BoundedSemaphore(self.concurrency_limit)
        self.active_requests = 0
        self.queued_requests = 0
        self.total_backend_attempts = 0
        self.successful_requests = 0
        self.failed_requests = 0

    def current_latency(self) -> float:
        with self.lock:
            return self.fault_latency if self.fault_active else self.normal_latency

    def inject_fault(self, fault_latency: float | None = None):
        with self.lock:
            self.fault_active = True
            if fault_latency is not None:
                self.fault_latency = fault_latency
        logger.warning(f"Latency fault injected: service time is now {self.fault_latency}s")

    def recover_fault(self):
        with self.lock:
            self.fault_active = False
        logger.info(f"Latency fault recovered: service time restored to {self.normal_latency}s")

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "active_requests": self.active_requests,
                "queued_requests": self.queued_requests,
                "concurrency_limit": self.concurrency_limit,
                "fault_active": self.fault_active,
                "current_latency": self.fault_latency if self.fault_active else self.normal_latency,
                "normal_latency": self.normal_latency,
                "fault_latency": self.fault_latency,
                "total_backend_attempts": self.total_backend_attempts,
                "successful_requests": self.successful_requests,
                "failed_requests": self.failed_requests,
            }


state = BackendState()


class BackendHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, data: dict):
        response_bytes = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

    def do_GET(self):
        if self.path == "/healthz" or self.path == "/":
            self._send_json(200, {"status": "ok", "service": "backend"})
        elif self.path == "/metrics":
            self._send_json(200, state.snapshot())
        elif self.path == "/config":
            self._send_json(200, {
                "concurrency_limit": state.concurrency_limit,
                "normal_latency": state.normal_latency,
                "fault_latency": state.fault_latency,
                "fault_active": state.fault_active,
            })
        else:
            self._send_json(404, {"error": "Not Found", "path": self.path})

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = {}
        if content_length > 0:
            try:
                body = json.loads(self.rfile.read(content_length).decode("utf-8"))
            except Exception:
                body = {}

        if self.path == "/fault/inject":
            fault_latency = body.get("fault_latency", None)
            state.inject_fault(fault_latency)
            self._send_json(200, {"status": "fault_injected", "metrics": state.snapshot()})
            return

        if self.path == "/fault/recover":
            state.recover_fault()
            self._send_json(200, {"status": "fault_recovered", "metrics": state.snapshot()})
            return

        if self.path == "/query":
            with state.lock:
                state.total_backend_attempts += 1
                if state.active_requests >= state.concurrency_limit:
                    if state.queued_requests >= QUEUE_CAPACITY:
                        state.failed_requests += 1
                        self._send_json(503, {
                            "error": "Backend queue capacity exceeded (503 Service Unavailable)",
                            "active_requests": state.active_requests,
                            "queued_requests": state.queued_requests,
                        })
                        return
                    state.queued_requests += 1
                else:
                    state.active_requests += 1

            in_queue = state.active_requests > state.concurrency_limit
            acquired = False
            try:
                # Wait for concurrency slot
                acquired = state.semaphore.acquire(timeout=5.0)
                if not acquired:
                    with state.lock:
                        state.failed_requests += 1
                        if in_queue:
                            state.queued_requests = max(0, state.queued_requests - 1)
                    self._send_json(504, {"error": "Timed out waiting for backend concurrency slot"})
                    return

                if in_queue:
                    with state.lock:
                        state.queued_requests = max(0, state.queued_requests - 1)
                        state.active_requests += 1

                # Emulate service processing time
                latency = state.current_latency()
                time.sleep(latency)

                with state.lock:
                    state.successful_requests += 1

                req_id = body.get("request_id", "unknown")
                attempt = body.get("attempt", 1)
                retry_layer = body.get("retry_layer", "unknown")

                self._send_json(200, {
                    "status": "success",
                    "request_id": req_id,
                    "attempt": attempt,
                    "retry_layer": retry_layer,
                    "latency": latency,
                    "documents": [
                        {"id": "doc-1", "score": 0.94, "content": "Knowledge base retrieved response"},
                        {"id": "doc-2", "score": 0.88, "content": "Autonomous agent context metadata"},
                    ],
                })

            finally:
                if acquired:
                    state.semaphore.release()
                    with state.lock:
                        state.active_requests = max(0, state.active_requests - 1)
            return

        self._send_json(404, {"error": "Not Found", "path": self.path})

    def log_message(self, format, *args):
        pass


def run_server():
    server_address = ("0.0.0.0", PORT)
    httpd = ThreadingHTTPServer(server_address, BackendHandler)
    logger.info(f"Starting Backend Service on 0.0.0.0:{PORT} (concurrency_limit={CONCURRENCY_LIMIT}, normal_lat={NORMAL_LATENCY}s, fault_lat={FAULT_LATENCY}s)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Backend Service...")
        httpd.server_close()


if __name__ == "__main__":
    run_server()
