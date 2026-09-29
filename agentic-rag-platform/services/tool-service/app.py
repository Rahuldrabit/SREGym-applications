#!/usr/bin/env python3
"""Tool Service for Agentic RAG Platform.

Handles tool execution and vector retrieval. Connects to the backend knowledge base.
Implements tool-level retries and metrics tracking.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("tool-service")

PORT = int(os.environ.get("PORT", "8001"))
BACKEND_URL = os.environ.get("BACKEND_URL", "http://backend:8002/query")
TOOL_MAX_RETRIES = int(os.environ.get("TOOL_MAX_RETRIES", "2"))
TOOL_TIMEOUT = float(os.environ.get("TOOL_TIMEOUT", "1.4"))

metrics = {
    "tool_calls": 0,
    "tool_retries": 0,
    "tool_successes": 0,
    "tool_failures": 0,
}


def call_backend_with_retries(req_id: str, workflow_id: str, attempt: int) -> dict:
    retries = 0
    last_error = None
    
    while retries <= TOOL_MAX_RETRIES:
        metrics["tool_calls" if retries == 0 else "tool_retries"] += 1
        
        req_body = json.dumps({
            "request_id": req_id,
            "workflow_id": workflow_id,
            "attempt": attempt,
            "retry_layer": "tool",
            "tool_retry_attempt": retries
        }).encode("utf-8")
        
        req = urllib.request.Request(
            BACKEND_URL,
            data=req_body,
            headers={"Content-Type": "application/json"}
        )
        
        try:
            start_time = time.time()
            with urllib.request.urlopen(req, timeout=TOOL_TIMEOUT) as response:
                res_body = response.read()
                metrics["tool_successes"] += 1
                return {
                    "status": "success",
                    "retries_used": retries,
                    "backend_response": json.loads(res_body.decode("utf-8")),
                    "latency": time.time() - start_time
                }
        except Exception as e:
            last_error = str(e)
            logger.warning(f"Backend call failed on try {retries}/{TOOL_MAX_RETRIES}: {last_error}")
            retries += 1
            if retries <= TOOL_MAX_RETRIES:
                time.sleep(0.05) # Small backoff
    
    metrics["tool_failures"] += 1
    return {
        "status": "error",
        "error": "Backend calls exhausted",
        "last_error": last_error,
        "retries_used": retries - 1
    }


class ToolHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, data: dict):
        response_bytes = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

    def do_GET(self):
        if self.path == "/healthz" or self.path == "/":
            self._send_json(200, {"status": "ok", "service": "tool-service"})
        elif self.path == "/metrics":
            self._send_json(200, metrics)
        elif self.path == "/config":
            self._send_json(200, {
                "backend_url": BACKEND_URL,
                "tool_max_retries": TOOL_MAX_RETRIES,
                "tool_timeout": TOOL_TIMEOUT,
            })
        else:
            self._send_json(404, {"error": "Not Found"})

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = {}
        if content_length > 0:
            try:
                body = json.loads(self.rfile.read(content_length).decode("utf-8"))
            except Exception:
                body = {}

        if self.path in ("/tools/retrieve", "/tools/execute"):
            req_id = body.get("request_id", "unknown")
            workflow_id = body.get("workflow_id", "unknown")
            attempt = body.get("attempt", 1)
            
            result = call_backend_with_retries(req_id, workflow_id, attempt)
            
            if result["status"] == "success":
                self._send_json(200, result)
            else:
                self._send_json(504, result)
            return

        self._send_json(404, {"error": "Not Found"})

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    server_address = ("0.0.0.0", PORT)
    httpd = ThreadingHTTPServer(server_address, ToolHandler)
    logger.info(f"Starting Tool Service on 0.0.0.0:{PORT} (backend={BACKEND_URL}, retries={TOOL_MAX_RETRIES}, timeout={TOOL_TIMEOUT}s)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        httpd.server_close()
