"""
mitmproxy addon for E2E streaming client fault-injection testing.

Responsibilities:
1. Inject faults, each toggled independently:
   - fault_410_streaming_except_hostname: HTTP 410 on every /v2/streaming/
     path except /v2/streaming/hostname
   - fault_404_bulk_channel_status: HTTP 404 on :bulk-channel-status
   - fault_404_hostname: HTTP 404 on /v2/streaming/hostname
   - evolve_schema_fault: a configurable status and error code on :evolve-schema,
     optionally for one pipe and a fixed number of responses
   Also counts :evolve-schema requests per pipe.
2. Rewrite /v2/streaming/hostname response so the SDK routes subsequent
   streaming API calls to the proxy (via a Docker network alias)
3. Rewrite the OAuth scope in /oauth/token requests to use the real subdomain
   (since Snowflake validates the scope against actual subdomain values)
4. Expose control API on port 9080 (enable/disable each fault, status,
   counters)

Usage:
  mitmdump --mode reverse:https://$UPSTREAM_HOST/ --listen-port 8080 \
           --certs /certs/mitmproxy.pem \
           --set keep_host_header=false --set upstream_cert=false \
           -s /addon/addon.py
"""

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, unquote, urlencode

from mitmproxy import http

# Docker network alias that resolves to this container. The SDK will use this
# as the "subdomain hostname" for all streaming API calls after discovery.
PROXY_SUBDOMAIN_ALIAS = os.environ.get("PROXY_SUBDOMAIN_ALIAS", "mitmproxy-subdomain")


def log(msg: str) -> None:
    """Print to stderr so mitmdump and Docker capture it."""
    print(f"[addon] {msg}", file=sys.stderr, flush=True)


class ProxyState:
    """Thread-safe state shared by all requests, independent of any fault."""

    def __init__(self):
        self._lock = threading.Lock()
        self.request_count = 0
        self.rewrite_count = 0
        # Stores the real subdomain returned by Snowflake so we can fix up
        # the OAuth scope in token exchange requests.
        self.real_subdomain = None

    def inc_request(self):
        with self._lock:
            self.request_count += 1

    def inc_rewrite(self):
        with self._lock:
            self.rewrite_count += 1

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "request_count": self.request_count,
                "rewrite_count": self.rewrite_count,
                "real_subdomain": self.real_subdomain,
            }

    def reset_counters(self):
        with self._lock:
            self.request_count = 0
            self.rewrite_count = 0


class FaultState:
    """Thread-safe on/off switch for one injected fault, with an injected count."""

    def __init__(self):
        self._enabled = False
        self._lock = threading.Lock()
        self.injected_count = 0

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

    def reset_counters(self):
        with self._lock:
            self.injected_count = 0


class EvolveSchemaFault:
    """Thread-safe rule for :evolve-schema responses, and per-pipe request counts.

    The rule is a status and an error code, written into the body the way Snowflake writes it.
    It applies to pipes whose name contains `pipe` (case-insensitive; every pipe if empty), and
    to the next `times` matching requests (every one if None).
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._rule = None
        self.injected_count = 0
        self.requests_by_pipe = {}

    def set_rule(self, status: int, error_code: str, pipe: str = "", times=None):
        with self._lock:
            self._rule = {
                "status": int(status),
                "error_code": error_code,
                "pipe": (pipe or "").upper(),
                "remaining": times,
            }

    def clear_rule(self):
        with self._lock:
            self._rule = None

    def on_request(self, pipe: str):
        """Counts the request; returns (status, body) to inject, or None to forward it."""
        with self._lock:
            self.requests_by_pipe[pipe] = self.requests_by_pipe.get(pipe, 0) + 1
            rule = self._rule
            if rule is None or rule["pipe"] not in pipe.upper():
                return None
            if rule["remaining"] is not None:
                if rule["remaining"] <= 0:
                    return None
                rule["remaining"] -= 1
            self.injected_count += 1
            body = {
                "code": str(rule["status"]),
                "error_code": rule["error_code"],
                "message": "injected by the e2e mitmproxy",
            }
            return rule["status"], json.dumps(body).encode()

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "evolve_schema_rule": dict(self._rule) if self._rule else None,
                "evolve_schema_injected_count": self.injected_count,
                "evolve_schema_requests": dict(self.requests_by_pipe),
            }

    def reset_counters(self):
        with self._lock:
            self.injected_count = 0
            self.requests_by_pipe = {}


def evolve_schema_pipe(path: str):
    """The decoded pipe name of an :evolve-schema path, or None for other paths."""
    route = path.split("?", 1)[0]
    if not route.endswith(":evolve-schema"):
        return None
    return unquote(route.rsplit("/pipes/", 1)[-1][: -len(":evolve-schema")]).strip('"')


proxy_state = ProxyState()
evolve_schema_fault = EvolveSchemaFault()
# Skips /v2/streaming/hostname: rebuilding the client after the simulated pipe
# failover starts with hostname discovery, which must succeed.
fault_410_streaming_except_hostname = FaultState()
fault_404_bulk_channel_status = FaultState()
fault_404_hostname = FaultState()


class FaultAddon:
    """mitmproxy addon that injects faults and manages hostname routing."""

    def request(self, flow: http.HTTPFlow) -> None:
        """Inject 410 on streaming API paths when fault mode is active.

        Also rewrites the OAuth scope in /oauth/token requests to replace the
        Docker alias with the real Snowflake subdomain, and routes streaming
        API requests to the real subdomain (since the scoped token is only
        valid for that specific service instance).
        """
        path = flow.request.path
        proxy_state.inc_request()

        # Fix up OAuth scope: the SDK puts the subdomain_hostname (which we
        # rewrote to the Docker alias) as the scope value. Snowflake needs
        # the real subdomain in the scope.
        if "/oauth/token" in path and flow.request.method == "POST":
            self._rewrite_oauth_scope(flow)
            return

        # The hostname endpoint is only intercepted to inject the 404 on client
        # create; otherwise it must pass through so client recreation works.
        if "/v2/streaming/hostname" in path:
            if fault_404_hostname.enabled:
                flow.response = http.Response.make(
                    404, b"Not Found", {"Content-Type": "text/plain"}
                )
                fault_404_hostname.inc_injected()
                log(f"Injected 404 (hostname) on {flow.request.method} {path}")
            return

        # Streaming API calls must be routed to the real subdomain (the scoped
        # token is only valid for that specific service instance).
        if path.startswith("/v2/streaming/") and proxy_state.real_subdomain:
            flow.request.host = proxy_state.real_subdomain
            flow.request.port = 443
            flow.request.scheme = "https"

        # Only inject 410 on streaming API paths
        if not path.startswith("/v2/streaming/"):
            return

        pipe = evolve_schema_pipe(path)
        if pipe is not None:
            injected = evolve_schema_fault.on_request(pipe)
            if injected is not None:
                status, body = injected
                flow.response = http.Response.make(
                    status, body, {"Content-Type": "application/json"}
                )
                log(f"Injected {status} (evolve-schema) on {flow.request.method} {path}")
                return

        if fault_410_streaming_except_hostname.enabled:
            flow.response = http.Response.make(
                410,
                b"Gone",
                {"Content-Type": "text/plain"},
            )
            fault_410_streaming_except_hostname.inc_injected()
            log(f"Injected 410 on {flow.request.method} {path}")

        # Inject 404 on :bulk-channel-status when that fault mode is active.
        if (
            fault_404_bulk_channel_status.enabled
            and ":bulk-channel-status" in path
        ):
            flow.response = http.Response.make(
                404,
                b"Not Found",
                {"Content-Type": "text/plain"},
            )
            fault_404_bulk_channel_status.inc_injected()
            log(f"Injected 404 (bulk-channel-status) on {flow.request.method} {path}")

    def _rewrite_oauth_scope(self, flow: http.HTTPFlow) -> None:
        """Replace the Docker alias in the OAuth scope with the real subdomain."""
        real_subdomain = proxy_state.real_subdomain
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
        proxy_state.real_subdomain = real_subdomain
        flow.response.text = PROXY_SUBDOMAIN_ALIAS
        proxy_state.inc_rewrite()
        log(f"Rewrote hostname: {real_subdomain} -> {PROXY_SUBDOMAIN_ALIAS}")


class ControlHandler(BaseHTTPRequestHandler):
    """HTTP handler for the control API on port 9080."""

    def do_POST(self):
        if self.path == "/enable-410":
            fault_410_streaming_except_hostname.enabled = True
            log("410 injection ENABLED")
            self._respond(200, {"status": "enabled"})
        elif self.path == "/disable-410":
            fault_410_streaming_except_hostname.enabled = False
            log("410 injection DISABLED")
            self._respond(200, {"status": "disabled"})
        elif self.path == "/reset-counters":
            proxy_state.reset_counters()
            fault_410_streaming_except_hostname.reset_counters()
            fault_404_bulk_channel_status.reset_counters()
            fault_404_hostname.reset_counters()
            evolve_schema_fault.reset_counters()
            self._respond(200, {"status": "reset"})
        elif self.path == "/evolve-schema-fault":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                rule = json.loads(self.rfile.read(length) or b"{}")
                evolve_schema_fault.set_rule(
                    rule["status"],
                    rule["error_code"],
                    rule.get("pipe", ""),
                    rule.get("times"),
                )
            except (KeyError, ValueError) as e:
                self._respond(400, {"error": f"bad rule: {e}"})
                return
            log(f"evolve-schema fault ENABLED: {rule}")
            self._respond(200, {"status": "enabled"})
        elif self.path == "/evolve-schema-fault/clear":
            evolve_schema_fault.clear_rule()
            log("evolve-schema fault DISABLED")
            self._respond(200, {"status": "disabled"})
        elif self.path == "/enable-404-bcs":
            fault_404_bulk_channel_status.enabled = True
            log("404-bulk-channel-status injection ENABLED")
            self._respond(200, {"status": "enabled"})
        elif self.path == "/disable-404-bcs":
            fault_404_bulk_channel_status.enabled = False
            log("404-bulk-channel-status injection DISABLED")
            self._respond(200, {"status": "disabled"})
        elif self.path == "/enable-404-hostname":
            fault_404_hostname.enabled = True
            log("404-hostname injection ENABLED")
            self._respond(200, {"status": "enabled"})
        elif self.path == "/disable-404-hostname":
            fault_404_hostname.enabled = False
            log("404-hostname injection DISABLED")
            self._respond(200, {"status": "disabled"})
        else:
            self._respond(404, {"error": "not found"})

    def do_GET(self):
        if self.path == "/status":
            status = proxy_state.to_dict()
            status["enabled"] = fault_410_streaming_except_hostname.enabled
            status["injected_count"] = (
                fault_410_streaming_except_hostname.injected_count
            )
            status["injected_404_bcs_count"] = (
                fault_404_bulk_channel_status.injected_count
            )
            status["injected_404_hostname_count"] = fault_404_hostname.injected_count
            status.update(evolve_schema_fault.to_dict())
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


addons = [FaultAddon()]

# Start control server in a daemon thread so it doesn't block mitmproxy
control_thread = threading.Thread(target=start_control_server, daemon=True)
control_thread.start()
