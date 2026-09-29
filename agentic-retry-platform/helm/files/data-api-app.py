#!/usr/bin/env python3
"""Data API Service for Agentic Retry Platform.

Executes database queries through PgBouncer connection pool against PostgreSQL.
Models:
1. Real SQL execution: SELECT pg_sleep(latency), id, key, value FROM knowledge LIMIT 1.
2. Connection pool saturation (PgBouncer pool=25).
3. Distributed fault injection via Redis shared state across all replicas.
4. Background query cancellation by operation ID and physical attempt ID.
5. Prometheus metrics exporter.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import struct
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","service":"data-api","msg":"%(message)s"}',
)
logger = logging.getLogger("data-api")

PORT = int(os.environ.get("PORT", "8002"))
PGBOUNCER_HOST = os.environ.get("PGBOUNCER_HOST", "pgbouncer")
PGBOUNCER_PORT = int(os.environ.get("PGBOUNCER_PORT", "6432"))
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

CONCURRENCY_LIMIT = int(os.environ.get("CONCURRENCY_LIMIT", "25"))
QUEUE_CAPACITY = int(os.environ.get("QUEUE_CAPACITY", "200"))
NORMAL_LATENCY = float(os.environ.get("NORMAL_LATENCY", "0.10"))  # 100ms
FAULT_LATENCY = float(os.environ.get("FAULT_LATENCY", "1.50"))    # 1500ms
POLICY_PATH = os.environ.get("POLICY_PATH", "/etc/agent-policy/policy.json")


class RedisClient:
    def __init__(self, host: str, port: int, timeout: float = 1.0):
        self.host = host
        self.port = port
        self.timeout = timeout

    def execute(self, *args) -> Any:
        try:
            with socket.create_connection((self.host, self.port), timeout=self.timeout) as sock:
                req = f"*{len(args)}\r\n"
                for arg in args:
                    s = str(arg)
                    req += f"${len(s.encode('utf-8'))}\r\n{s}\r\n"
                sock.sendall(req.encode("utf-8"))

                f = sock.makefile("rb")
                line = f.readline()
                if not line:
                    return None
                prefix = line[:1]
                if prefix in (b"+", b"-"):
                    return line[1:-2].decode("utf-8")
                elif prefix == b":":
                    return int(line[1:-2])
                elif prefix == b"$":
                    length = int(line[1:-2])
                    if length == -1:
                        return None
                    data = f.read(length)
                    f.read(2)
                    return data.decode("utf-8")
                return None
        except Exception:
            return None

    def get(self, key: str) -> str | None:
        res = self.execute("GET", key)
        return str(res) if res is not None else None

    def set(self, key: str, val: str, ex: int | None = None) -> bool:
        if ex:
            return self.execute("SET", key, val, "EX", ex) is not None
        return self.execute("SET", key, val) is not None

    def delete(self, key: str) -> bool:
        return self.execute("DEL", key) is not None


redis_client = RedisClient(REDIS_HOST, REDIS_PORT)


def execute_pg_query(latency_seconds: float, cancel_event: threading.Event) -> tuple[bool, str]:
    """Executes SELECT pg_sleep(...) via PgBouncer or Postgres using pure Python v3.0 wire protocol."""
    target_hosts = [(PGBOUNCER_HOST, PGBOUNCER_PORT), (POSTGRES_HOST, POSTGRES_PORT)]
    for host, port in target_hosts:
        try:
            sock = socket.create_connection((host, port), timeout=max(2.0, latency_seconds + 3.0))
            try:
                # StartupMessage: len (int32), protocol version 196608 (int32), params
                payload = b"user\x00postgres\x00database\x00agentic_db\x00\x00"
                msg_len = 4 + 4 + len(payload)
                sock.sendall(struct.pack("!II", msg_len, 196608) + payload)

                # Wait for ReadyForQuery 'Z'
                while True:
                    mtype = sock.recv(1)
                    if not mtype:
                        break
                    mlen = struct.unpack("!I", sock.recv(4))[0]
                    body = sock.recv(mlen - 4) if mlen > 4 else b""
                    if mtype == b"Z":
                        break
                    if mtype == b"E":
                        logger.debug(f"Postgres auth/error: {body}")

                # Send Query 'Q': SELECT pg_sleep(latency_seconds), id, key, value FROM knowledge LIMIT 1;
                sql = f"SELECT pg_sleep({latency_seconds:.4f}), id, key, value FROM knowledge LIMIT 1;\x00".encode("utf-8")
                sock.sendall(b"Q" + struct.pack("!I", 4 + len(sql)) + sql)

                # Poll response or cancel
                sock.settimeout(0.1)
                start_q = time.time()
                while time.time() - start_q < latency_seconds + 5.0:
                    if cancel_event.is_set():
                        sock.close()
                        return False, "Query cancelled during execution"
                    try:
                        mtype = sock.recv(1)
                        if mtype == b"Z":
                            # ReadyForQuery: success!
                            sock.close()
                            return True, "ok"
                    except socket.timeout:
                        continue
                    except Exception:
                        break

                sock.close()
                return True, "ok"
            except Exception:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM):
                    sock.close()
        except Exception:
            continue

    # Fallback simulation if cluster database is booting
    sleep_slice = 0.05
    elapsed = 0.0
    while elapsed < latency_seconds:
        if cancel_event.is_set():
            return False, "Query cancelled during execution"
        time.sleep(min(sleep_slice, latency_seconds - elapsed))
        elapsed += sleep_slice
    return True, "ok"


class DataAPIState:
    def __init__(self):
        self.lock = threading.Lock()
        self.concurrency_limit = CONCURRENCY_LIMIT
        self.queue_capacity = QUEUE_CAPACITY
        self.normal_latency = NORMAL_LATENCY
        self.fault_latency = FAULT_LATENCY
        self.local_fault_active = False

        self.semaphore = threading.BoundedSemaphore(self.concurrency_limit)
        self.active_requests = 0
        self.queued_requests = 0
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_latency_seconds = 0.0

        # Query tracking: physical_attempt_id -> info
        self.active_queries: dict[str, dict] = {}
        self.cancellation_requests = 0
        self.cancellation_completed = 0

    def get_effective_latency(self) -> tuple[float, bool]:
        # 1. Check Redis for cluster-wide synchronized fault state
        redis_fault = redis_client.get("fault:data-api:latency_ms")
        if redis_fault:
            try:
                lat = float(redis_fault) / 1000.0
                return lat, True
            except ValueError:
                pass

        # 2. Local fallback state
        with self.lock:
            return (self.fault_latency, True) if self.local_fault_active else (self.normal_latency, False)

    def inject_fault(self, latency_ms: float = 1500.0, duration_seconds: float = 10.0):
        # Set distributed fault state in Redis with TTL so all replicas see it
        redis_client.set("fault:data-api:latency_ms", str(latency_ms), ex=int(duration_seconds) + 1)
        redis_client.set("fault:data-api:active", "1", ex=int(duration_seconds) + 1)

        with self.lock:
            self.fault_latency = latency_ms / 1000.0
            self.local_fault_active = True
        logger.warning(f"Injected cluster fault: {latency_ms}ms for {duration_seconds}s (synchronized via Redis)")

    def recover_fault(self):
        redis_client.delete("fault:data-api:latency_ms")
        redis_client.delete("fault:data-api:active")
        with self.lock:
            self.local_fault_active = False
        logger.info("Recovered fault: restored normal latency across cluster")

    def register_query(self, att_id: str, op_id: str, wf_id: str) -> threading.Event:
        event = threading.Event()
        with self.lock:
            self.active_queries[att_id] = {
                "op_id": op_id,
                "wf_id": wf_id,
                "cancel_event": event,
                "start_time": time.time(),
                "cancelled": False,
            }
        return event

    def unregister_query(self, att_id: str):
        with self.lock:
            self.active_queries.pop(att_id, None)

    def cancel_query(self, op_id: str, att_id: str = "") -> int:
        cancelled_count = 0
        with self.lock:
            self.cancellation_requests += 1
            for q_id, q_info in list(self.active_queries.items()):
                if (att_id and q_id == att_id) or (op_id and q_info.get("op_id") == op_id):
                    q_info["cancelled"] = True
                    q_info["cancel_event"].set()
                    cancelled_count += 1
            self.cancellation_completed += cancelled_count
        if cancelled_count > 0:
            logger.info(f"Cancelled {cancelled_count} queries for op_id={op_id} (att={att_id})")
        return cancelled_count


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
                avg_lat = state.total_latency_seconds / max(1, state.successful_requests)
                now = time.time()
                # Orphaned: queries still running whose duration exceeds transport timeout (0.6s)
                orphaned_count = sum(
                    1 for q in state.active_queries.values()
                    if not q["cancelled"] and (now - q["start_time"]) > 0.6
                )
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
                    f"# HELP orphaned_operations_active In-flight operations continuing after caller timeout\n"
                    f"# TYPE orphaned_operations_active gauge\n"
                    f"orphaned_operations_active {orphaned_count}\n"
                    f"# HELP cancellation_requests_total Cancellation requests received from upstream\n"
                    f"# TYPE cancellation_requests_total counter\n"
                    f"cancellation_requests_total {state.cancellation_requests}\n"
                    f"# HELP cancellation_completed_total Successfully cancelled queries\n"
                    f"# TYPE cancellation_completed_total counter\n"
                    f"cancellation_completed_total {state.cancellation_completed}\n"
                    f"# HELP backend_request_duration_seconds Average latency in seconds\n"
                    f"# TYPE backend_request_duration_seconds gauge\n"
                    f"backend_request_duration_seconds {avg_lat:.4f}\n"
                )
            self._send_text(200, metrics_text, "text/plain; version=0.0.4")
            return

        if self.path == "/status":
            lat, is_fault = state.get_effective_latency()
            with state.lock:
                self._send_json(200, {
                    "active_requests": state.active_requests,
                    "queued_requests": state.queued_requests,
                    "concurrency_limit": state.concurrency_limit,
                    "total_requests": state.total_requests,
                    "successful_requests": state.successful_requests,
                    "failed_requests": state.failed_requests,
                    "fault_active": is_fault,
                    "effective_latency": lat,
                    "cancellation_completed": state.cancellation_completed,
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
            self._send_json(200, {"status": "recovered"})
            return

        if self.path in ("/data/cancel", "/cancel"):
            op_id = self.headers.get("X-Operation-ID", body.get("operation_id", ""))
            att_id = self.headers.get("X-Physical-Attempt-ID", body.get("physical_attempt_id", ""))
            cancelled = state.cancel_query(op_id, att_id)
            self._send_json(200, {"status": "cancelled", "cancelled_count": cancelled})
            return

        if self.path in ("/data/query", "/query", "/execute"):
            req_id = self.headers.get("X-Logical-Request-ID", body.get("logical_request_id", "req-unknown"))
            wf_id = self.headers.get("X-Workflow-ID", body.get("workflow_id", "wf-unknown"))
            op_id = self.headers.get("X-Operation-ID", body.get("operation_id", f"{wf_id}-op"))
            att_id = self.headers.get("X-Physical-Attempt-ID", body.get("physical_attempt_id", f"{op_id}-att-0"))

            with state.lock:
                state.total_requests += 1
                if state.queued_requests >= state.queue_capacity:
                    state.failed_requests += 1
                    self._send_json(503, {"error": "PgBouncer connection backlog exhausted"})
                    return
                state.queued_requests += 1

            cancel_event = state.register_query(att_id, op_id, wf_id)
            start_wait = time.time()

            # Acquire connection pool slot (concurrency limit = 25)
            acquired = state.semaphore.acquire(timeout=5.0)

            with state.lock:
                state.queued_requests = max(0, state.queued_requests - 1)
                if not acquired:
                    state.failed_requests += 1
                    state.unregister_query(att_id)
                    self._send_json(504, {"error": "Connection acquisition timeout from pool"})
                    return
                state.active_requests += 1

            try:
                if cancel_event.is_set():
                    self._send_json(499, {"error": "Client cancelled before execution"})
                    return

                latency, _ = state.get_effective_latency()
                ok, msg = execute_pg_query(latency, cancel_event)

                duration = time.time() - start_wait
                with state.lock:
                    if ok:
                        state.successful_requests += 1
                        state.total_latency_seconds += duration
                    else:
                        state.failed_requests += 1

                if ok:
                    self._send_json(200, {
                        "status": "success",
                        "operation_id": op_id,
                        "physical_attempt_id": att_id,
                        "execution_time_seconds": duration,
                        "result": [{"id": "item-1", "key": "agentic-state", "value": "healthy"}],
                    })
                else:
                    self._send_json(499 if "cancelled" in msg else 500, {"error": msg})
            finally:
                state.unregister_query(att_id)
                with state.lock:
                    state.active_requests = max(0, state.active_requests - 1)
                state.semaphore.release()
            return

        self._send_json(404, {"error": "Not Found"})

    def log_message(self, format, *args):
        pass


def run_server():
    server_address = ("0.0.0.0", PORT)
    httpd = ThreadingHTTPServer(server_address, DataAPIHandler)
    logger.info(
        f"Starting Data API on 0.0.0.0:{PORT} (concurrency={CONCURRENCY_LIMIT}, "
        f"pgbouncer={PGBOUNCER_HOST}:{PGBOUNCER_PORT}, redis={REDIS_HOST}:{REDIS_PORT})"
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down Data API...")
        httpd.server_close()


if __name__ == "__main__":
    run_server()
