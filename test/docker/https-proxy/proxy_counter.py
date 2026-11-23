"""
mitmproxy addon for the forward-proxy E2E tests.

Counts the Snowflake calls seen on each listener port, so a test can tell
which proxy tier a connector used:

  8080  connector tier (snowflake.https.proxy.host / snowflake.https.proxy.port)
  8081  worker JVM tier (-Dhttps.proxyHost / -Dhttps.proxyPort)
  8082  a second connector's proxy, for tests that run two connectors at once

When JVM_TIER_PROXY_USER and JVM_TIER_PROXY_PASSWORD are set, the worker JVM
tier requires those Basic credentials on CONNECT and answers 407 otherwise. Only
HTTPS tunnels are checked: Snowflake and OAuth traffic is all HTTPS, and the
other listeners stay open.

Control API on :9080:
  GET  /status  {"counts": {"8080": {...}, ...}, "connect_hosts": {...}, "pipes": {...},
                 "proxy_auth": {"8081": {"accepted": n, "rejected": n}}}
                pipes: streaming pipe names seen in SDK request paths, per port
                proxy_auth: CONNECT requests the authenticating listener let through or refused
  POST /reset   clears all counters
"""

import base64
import json
import os
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import unquote

from mitmproxy import http

LOCK = threading.Lock()
COUNTS = {}
CONNECT_HOSTS = {}
# Streaming pipe names seen in SDK request paths, per listener port, so a test running several
# connectors can tell whose traffic went through which listener.
PIPES = {}
_PIPE_IN_PATH = re.compile(r"/pipes/([^/:?]+)")
# CONNECT outcomes on the authenticating listener, kept apart from COUNTS so the
# Snowflake call assertions are unaffected.
PROXY_AUTH = {}

JVM_TIER_PORT = "8081"
_JVM_TIER_USER = os.environ.get("JVM_TIER_PROXY_USER", "")
_JVM_TIER_PASSWORD = os.environ.get("JVM_TIER_PROXY_PASSWORD", "")
_JVM_TIER_CREDENTIALS = (
    "Basic "
    + base64.b64encode(f"{_JVM_TIER_USER}:{_JVM_TIER_PASSWORD}".encode()).decode()
    if _JVM_TIER_USER and _JVM_TIER_PASSWORD
    else None
)


def log(msg: str) -> None:
    print(f"[proxy_counter] {msg}", file=sys.stderr, flush=True)


def _listener(flow: http.HTTPFlow) -> str:
    return str(flow.client_conn.sockname[1])


def _operations(flow: http.HTTPFlow) -> list:
    path = flow.request.path.split("?", 1)[0]
    method = flow.request.method
    host = flow.request.pretty_host.lower()

    # The connector's own OAuth token request; nothing else in the worker uses the JDK HTTP client.
    if method == "POST" and flow.request.headers.get("user-agent", "").startswith(
        "Java-http-client"
    ):
        return ["connector_oauth_token"]
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
    if listener == JVM_TIER_PORT and _JVM_TIER_CREDENTIALS is not None:
        _require_proxy_credentials(flow, listener)


def _require_proxy_credentials(flow: http.HTTPFlow, listener: str) -> None:
    accepted = flow.request.headers.get("proxy-authorization") == _JVM_TIER_CREDENTIALS
    with LOCK:
        outcomes = PROXY_AUTH.setdefault(listener, {"accepted": 0, "rejected": 0})
        outcomes["accepted" if accepted else "rejected"] += 1
    if not accepted:
        flow.response = http.Response.make(
            407, b"", {"Proxy-Authenticate": 'Basic realm="https-proxy"'}
        )


def _pipe(flow: http.HTTPFlow) -> str | None:
    match = _PIPE_IN_PATH.search(flow.request.path)
    if match is None:
        return None
    return unquote(match.group(1)).strip('"').lower()


def request(flow: http.HTTPFlow) -> None:
    listener = _listener(flow)
    pipe = _pipe(flow)
    if pipe is not None:
        with LOCK:
            pipes = PIPES.setdefault(listener, {})
            pipes[pipe] = pipes.get(pipe, 0) + 1
    operations = _operations(flow)
    if not operations:
        return
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
                "pipes": {k: dict(v) for k, v in PIPES.items()},
                "proxy_auth": {k: dict(v) for k, v in PROXY_AUTH.items()},
            }
        self._respond(200, body)

    def do_POST(self):
        if self.path != "/reset":
            self._respond(404, {"error": "not found"})
            return
        with LOCK:
            COUNTS.clear()
            CONNECT_HOSTS.clear()
            PIPES.clear()
            PROXY_AUTH.clear()
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
if _JVM_TIER_CREDENTIALS is not None:
    log(f"listener {JVM_TIER_PORT} requires Basic proxy credentials on CONNECT")
