"""
mitmproxy addon for the forward-proxy E2E tests.

Counts the Snowflake calls seen on each listener port, so a test can tell
which proxy tier a connector used:

  8080  connector tier (jvm.proxy.host / jvm.proxy.port)
  8081  worker JVM tier (-Dhttps.proxyHost / -Dhttps.proxyPort)

Control API on :9080:
  GET  /status  {"counts": {"8080": {...}, "8081": {...}}, "connect_hosts": {...}}
  POST /reset   clears all counters
"""

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from mitmproxy import http

LOCK = threading.Lock()
COUNTS = {}
CONNECT_HOSTS = {}


def log(msg: str) -> None:
    print(f"[proxy_counter] {msg}", file=sys.stderr, flush=True)


def _listener(flow: http.HTTPFlow) -> str:
    return str(flow.client_conn.sockname[1])


def _operations(flow: http.HTTPFlow) -> list:
    path = flow.request.path.split("?", 1)[0]
    method = flow.request.method
    host = flow.request.pretty_host.lower()

    # JDBC (connector's Snowflake connection)
    if method == "POST" and path.endswith("/session/v1/login-request"):
        return ["jdbc_login"]
    if method == "POST" and path.endswith("/queries/v1/query-request"):
        return ["jdbc_query"]
    # Streaming SDK
    if method == "POST" and path.endswith("/oauth/token-request"):
        return ["sdk_initial_auth"]
    if method == "GET" and path.endswith("/v2/streaming/hostname"):
        return ["sdk_get_subdomain_name"]
    if method == "POST" and path.endswith("/oauth/token"):
        return ["sdk_token_downscoping"]
    if method == "POST" and path.endswith(":pipe-info"):
        return ["sdk_get_pipe_info"]
    if method == "PUT" and "/channels/" in path:
        return ["sdk_open_channel"]
    if method == "POST" and path.endswith("/rows"):
        return ["sdk_insert_rows"]
    if method == "POST" and path.endswith("/filebuffer"):
        return ["sdk_insert_files"]
    if method == "PUT" and "amazonaws.com" in host:
        return ["sdk_aws_file_upload"]
    if method == "PUT" and ".blob.core." in host:
        return ["sdk_azure_file_upload"]
    return []


def http_connect(flow: http.HTTPFlow) -> None:
    listener = _listener(flow)
    host = flow.request.pretty_host.lower()
    with LOCK:
        hosts = CONNECT_HOSTS.setdefault(listener, {})
        hosts[host] = hosts.get(host, 0) + 1


def request(flow: http.HTTPFlow) -> None:
    operations = _operations(flow)
    if not operations:
        return
    listener = _listener(flow)
    with LOCK:
        counts = COUNTS.setdefault(listener, {})
        for operation in operations:
            counts[operation] = counts.get(operation, 0) + 1


class _ControlHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?", 1)[0] != "/status":
            self._respond(404, {"error": "not found"})
            return
        with LOCK:
            body = {
                "counts": {k: dict(v) for k, v in COUNTS.items()},
                "connect_hosts": {k: dict(v) for k, v in CONNECT_HOSTS.items()},
            }
        self._respond(200, body)

    def do_POST(self):
        if self.path != "/reset":
            self._respond(404, {"error": "not found"})
            return
        with LOCK:
            COUNTS.clear()
            CONNECT_HOSTS.clear()
        log("counters reset")
        self._respond(200, {"status": "reset"})

    def _respond(self, status: int, body: dict):
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        return


def _serve_control():
    HTTPServer(("0.0.0.0", 9080), _ControlHandler).serve_forever()


threading.Thread(target=_serve_control, daemon=True).start()
log("control API listening on :9080")
