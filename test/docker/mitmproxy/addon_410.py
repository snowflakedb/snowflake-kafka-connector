"""
mitmproxy addon for E2E client recreation testing.

Responsibilities:
1. Return HTTP 410 on streaming API paths when fault injection is enabled
2. Expose control API on port 9080 (enable/disable 410, status, counters)

Usage:
  mitmdump --mode reverse:https://$UPSTREAM_HOST/ --listen-port 443 \
           --set upstream_cert=false -s /addon/addon_410.py
"""

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from mitmproxy import http

# The hostname that the proxy presents to the SDK. The SDK will use this
# for all subsequent API calls after the initial /v2/streaming/hostname lookup.
PROXY_HOSTNAME = os.environ.get("PROXY_HOSTNAME", "proxy.docker.local")


def log(msg: str) -> None:
    """Print to stderr so mitmdump and Docker capture it."""
    print(f"[addon_410] {msg}", file=sys.stderr, flush=True)


class FaultState:
    """Thread-safe toggle for 410 injection with counters."""

    def __init__(self):
        self._enabled = False
        self._lock = threading.Lock()
        self.injected_count = 0
        self.request_count = 0
        self.rewrite_count = 0

    @property
    def enabled(self) -> bool:
        with self._lock:
            return self._enabled

    @enabled.setter
    def enabled(self, value: bool):
        with self._lock:
            self._enabled = value

    def inc_injected(self):
        with self._lock:
            self.injected_count += 1

    def inc_request(self):
        with self._lock:
            self.request_count += 1

    def inc_rewrite(self):
        with self._lock:
            self.rewrite_count += 1

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "enabled": self._enabled,
                "injected_count": self.injected_count,
                "request_count": self.request_count,
                "rewrite_count": self.rewrite_count,
            }

    def reset_counters(self):
        with self._lock:
            self.injected_count = 0
            self.request_count = 0
            self.rewrite_count = 0


fault_state = FaultState()


class Addon410:
    """mitmproxy addon that injects HTTP 410 on streaming API paths."""

    def request(self, flow: http.HTTPFlow) -> None:
        """Inject 410 on streaming API paths when fault mode is active."""
        path = flow.request.path
        fault_state.inc_request()

        # Never intercept the hostname endpoint — must work for client recreation
        if "/v2/streaming/hostname" in path:
            return

        # Only inject 410 on streaming API paths
        if not path.startswith("/v2/streaming/"):
            return

        if fault_state.enabled:
            flow.response = http.Response.make(
                410,
                b"Gone",
                {"Content-Type": "text/plain"},
            )
            fault_state.inc_injected()
            log(f"Injected 410 on {flow.request.method} {path}")


class ControlHandler(BaseHTTPRequestHandler):
    """HTTP handler for the control API on port 9080."""

    def do_POST(self):
        if self.path == "/enable-410":
            fault_state.enabled = True
            log("410 injection ENABLED")
            self._respond(200, {"status": "enabled"})
        elif self.path == "/disable-410":
            fault_state.enabled = False
            log("410 injection DISABLED")
            self._respond(200, {"status": "disabled"})
        elif self.path == "/reset-counters":
            fault_state.reset_counters()
            self._respond(200, {"status": "reset"})
        else:
            self._respond(404, {"error": "not found"})

    def do_GET(self):
        if self.path == "/status":
            self._respond(200, fault_state.to_dict())
        else:
            self._respond(404, {"error": "not found"})

    def _respond(self, status: int, body: dict):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        log(format % args)


def start_control_server():
    server = HTTPServer(("0.0.0.0", 9080), ControlHandler)
    log("Control API listening on :9080")
    server.serve_forever()


addons = [Addon410()]

# Start control server in a daemon thread so it doesn't block mitmproxy
control_thread = threading.Thread(target=start_control_server, daemon=True)
control_thread.start()
