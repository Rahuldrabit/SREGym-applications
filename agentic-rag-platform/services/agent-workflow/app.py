#!/usr/bin/env python3
"""Agent Workflow Service for Agentic RAG Platform.

Acts as the autonomous agent supervisor/planner. Plans steps and invokes tools.
Implements planner-level retries.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("agent-workflow")

PORT = int(os.environ.get("PORT", "8000"))
TOOL_SERVICE_URL = os.environ.get("TOOL_SERVICE_URL", "http://tool-service:8001/tools/retrieve")
PLANNER_MAX_RETRIES = int(os.environ.get("PLANNER_MAX_RETRIES", "3"))
PLANNER_TIMEOUT = float(os.environ.get("PLANNER_TIMEOUT", "3.5"))

metrics = {
    "logical_queries": 0,
    "planner_retries": 0,
    "workflow_successes": 0,
    "workflow_failures": 0,
}


def execute_workflow(req_id: str, workflow_id: str) -> dict:
    retries = 0
    last_error = None
    
    while retries <= PLANNER_MAX_RETRIES:
        metrics["logical_queries" if retries == 0 else "planner_retries"] += 1
        
        req_body = json.dumps({
            "request_id": req_id,
            "workflow_id": workflow_id,
            "attempt": retries + 1,
            "retry_layer": "planner"
        }).encode("utf-8")
        
        req = urllib.request.Request(
            TOOL_SERVICE_URL,
            data=req_body,
            headers={"Content-Type": "application/json"}
        )
        
        try:
            start_time = time.time()
            with urllib.request.urlopen(req, timeout=PLANNER_TIMEOUT) as response:
                res_body = response.read()
                metrics["workflow_successes"] += 1
                return {
                    "status": "success",
                    "planner_retries_used": retries,
                    "tool_response": json.loads(res_body.decode("utf-8")),
                    "workflow_latency": time.time() - start_time
                }
        except Exception as e:
            last_error = str(e)
            logger.warning(f"Workflow planner failed on try {retries}/{PLANNER_MAX_RETRIES}: {last_error}")
            retries += 1
            if retries <= PLANNER_MAX_RETRIES:
                time.sleep(0.1) # Planner backoff
    
    metrics["workflow_failures"] += 1
    return {
        "status": "error",
        "error": "Planner replanning exhausted",
        "last_error": last_error,
        "planner_retries_used": retries - 1
    }


class WorkflowHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, data: dict):
        response_bytes = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

    def do_GET(self):
        if self.path == "/healthz" or self.path == "/":
            self._send_json(200, {"status": "ok", "service": "agent-workflow"})
        elif self.path == "/metrics":
            self._send_json(200, metrics)
        elif self.path == "/config":
            self._send_json(200, {
                "tool_service_url": TOOL_SERVICE_URL,
                "planner_max_retries": PLANNER_MAX_RETRIES,
                "planner_timeout": PLANNER_TIMEOUT,
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

        if self.path in ("/query", "/workflow/run"):
            req_id = body.get("request_id", f"req-{time.time()}")
            workflow_id = body.get("workflow_id", f"wf-{time.time()}")
            
            result = execute_workflow(req_id, workflow_id)
            
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
    httpd = ThreadingHTTPServer(server_address, WorkflowHandler)
    logger.info(f"Starting Agent Workflow Service on 0.0.0.0:{PORT} (tools={TOOL_SERVICE_URL}, retries={PLANNER_MAX_RETRIES}, timeout={PLANNER_TIMEOUT}s)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        httpd.server_close()
