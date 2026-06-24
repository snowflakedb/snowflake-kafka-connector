"""
mitmproxy addon for E2E client recreation testing.

Responsibilities:
1. Return HTTP 410 on streaming API paths when fault injection is enabled
2. Rewrite /v2/streaming/hostname response so the SDK routes subsequent
   streaming API calls to the proxy (via a Docker network alias)
3. Rewrite the OAuth scope in /oauth/token requests to use the real subdomain
   (since Snowflake validates the scope against actual subdomain values)
4. Expose control API on port 9080 (enable/disable 410, status, counters)

Usage:
  mitmdump --mode reverse:https://$UPSTREAM_HOST/ --listen-port 8080 \
           --set keep_host_header=false --set upstream_cert=false \
           -s /addon/addon_410.py
"""

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlencode

from mitmproxy import http

# Docker network alias that resolves to this container. The SDK will use this
# as the "subdomain hostname" for all streaming API calls after discovery.
PROXY_SUBDOMAIN_ALIAS = os.environ.get("PROXY_SUBDOMAIN_ALIAS", "mitmproxy-subdomain")


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
        # Stores the real subdomain returned by Snowflake so we can fix up
        # the OAuth scope in token exchange requests.
        self.real_subdomain = None

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
                "real_subdomain": self.real_subdomain,
            }

    def reset_counters(self):
        with self._lock:
            self.injected_count = 0
            self.request_count = 0
            self.rewrite_count = 0


fault_state = FaultState()

# Separate state for 404-on-bulk-channel-status injection (SNOW-3670537)
fault_state_404_bcs = FaultState()


class Addon410:
    """mitmproxy addon that injects HTTP 410 and manages hostname routing."""

    def request(self, flow: http.HTTPFlow) -> None:
        """Inject 410 on streaming API paths when fault mode is active.

        Also rewrites the OAuth scope in /oauth/token requests to replace the
        Docker alias with the real Snowflake subdomain, and routes streaming
        API requests to the real subdomain (since the scoped token is only
        valid for that specific service instance).
        """
        path = flow.request.path
        fault_state.inc_request()

        # Fix up OAuth scope: the SDK puts the subdomain_hostname (which we
        # rewrote to the Docker alias) as the scope value. Snowflake needs
        # the real subdomain in the scope.
        if "/oauth/token" in path and flow.request.method == "POST":
            self._rewrite_oauth_scope(flow)
            return

        # Never intercept the hostname endpoint — must work for client recreation
        if "/v2/streaming/hostname" in path:
            return

        # Streaming API calls must be routed to the real subdomain (the scoped
        # token is only valid for that specific service instance).
        if path.startswith("/v2/streaming/") and fault_state.real_subdomain:
            flow.request.host = fault_state.real_subdomain
            flow.request.port = 443
            flow.request.scheme = "https"

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

        # Inject 404 on :bulk-channel-status when that fault mode is active.
        if fault_state_404_bcs.enabled and ":bulk-channel-status" in path:
            flow.response = http.Response.make(
                404,
                b"Not Found",
                {"Content-Type": "text/plain"},
            )
            fault_state_404_bcs.inc_injected()
            log(f"Injected 404 (bulk-channel-status) on {flow.request.method} {path}")

    def _rewrite_oauth_scope(self, flow: http.HTTPFlow) -> None:
        """Replace the Docker alias in the OAuth scope with the real subdomain."""
        real_subdomain = fault_state.real_subdomain
        if not real_subdomain:
            return

        content_type = flow.request.headers.get("content-type", "")
        if "application/x-www-form-urlencoded" not in content_type:
            return

        body = flow.request.get_text()
        if PROXY_SUBDOMAIN_ALIAS not in body:
            return

        # Parse form body, replace scope, re-encode
        params = parse_qs(body, keep_blank_values=True)
        if "scope" in params:
            params["scope"] = [
                s.replace(PROXY_SUBDOMAIN_ALIAS, real_subdomain)
                for s in params["scope"]
            ]
            new_body = urlencode(params, doseq=True)
            flow.request.set_text(new_body)
            log(f"Rewrote OAuth scope: {PROXY_SUBDOMAIN_ALIAS} -> {real_subdomain}")

    def response(self, flow: http.HTTPFlow) -> None:
        """Rewrite /v2/streaming/hostname response to the Docker alias.

        The SDK calls this endpoint once at client creation to discover the
        subdomain hostname, then uses it for all subsequent streaming API URLs.
        By rewriting to our Docker alias, DNS resolves to this proxy container
        and all streaming traffic flows through us.

        We store the real subdomain so we can fix up the OAuth scope later.
        The response body must be a bare string (no JSON quotes) because the
        SDK wraps it in quotes before JSON-deserializing.
        """
        if "/v2/streaming/hostname" not in flow.request.path:
            return
        if flow.response is None or flow.response.status_code != 200:
            return

        real_subdomain = flow.response.text.strip()
        fault_state.real_subdomain = real_subdomain
        flow.response.text = PROXY_SUBDOMAIN_ALIAS
        fault_state.inc_rewrite()
        log(f"Rewrote hostname: {real_subdomain} -> {PROXY_SUBDOMAIN_ALIAS}")


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
            fault_state_404_bcs.reset_counters()
            self._respond(200, {"status": "reset"})
        elif self.path == "/enable-404-bcs":
            fault_state_404_bcs.enabled = True
            log("404-bulk-channel-status injection ENABLED")
            self._respond(200, {"status": "enabled"})
        elif self.path == "/disable-404-bcs":
            fault_state_404_bcs.enabled = False
            log("404-bulk-channel-status injection DISABLED")
            self._respond(200, {"status": "disabled"})
        else:
            self._respond(404, {"error": "not found"})

    def do_GET(self):
        if self.path == "/status":
            status = fault_state.to_dict()
            status["injected_404_bcs_count"] = fault_state_404_bcs.injected_count
            self._respond(200, status)
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
