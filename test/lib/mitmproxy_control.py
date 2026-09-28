"""Client for the mitmproxy addon control API."""

import logging
import os

import requests

logger = logging.getLogger(__name__)

# Default to the Docker Compose service URL
_DEFAULT_URL = "http://mitmproxy:9080"


class MitmproxyControl:
    """Controls the mitmproxy 410 injection addon via its HTTP API."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def enable_410(self):
        """Activate HTTP 410 injection on streaming API paths."""
        resp = requests.post(f"{self.base_url}/enable-410", timeout=5)
        resp.raise_for_status()
        logger.info("410 injection enabled")

    def disable_410(self):
        """Deactivate HTTP 410 injection; resume normal forwarding."""
        resp = requests.post(f"{self.base_url}/disable-410", timeout=5)
        resp.raise_for_status()
        logger.info("410 injection disabled")

    def is_enabled(self) -> bool:
        """Return whether 410 injection is currently active."""
        resp = requests.get(f"{self.base_url}/status", timeout=5)
        resp.raise_for_status()
        return resp.json()["enabled"]

    def get_status(self) -> dict:
        """Return full status including counters."""
        resp = requests.get(f"{self.base_url}/status", timeout=5)
        resp.raise_for_status()
        return resp.json()

    def get_injected_count(self) -> int:
        """Return the number of 410 responses injected."""
        return self.get_status()["injected_count"]

    def reset_counters(self):
        """Reset all counters to zero."""
        resp = requests.post(f"{self.base_url}/reset-counters", timeout=5)
        resp.raise_for_status()
        logger.info("Counters reset")

    def enable_404_bulk_channel_status(self):
        """Activate HTTP 404 injection on :bulk-channel-status paths."""
        resp = requests.post(f"{self.base_url}/enable-404-bcs", timeout=5)
        resp.raise_for_status()
        logger.info("404 bulk-channel-status injection enabled")

    def disable_404_bulk_channel_status(self):
        """Deactivate HTTP 404 injection on :bulk-channel-status paths."""
        resp = requests.post(f"{self.base_url}/disable-404-bcs", timeout=5)
        resp.raise_for_status()
        logger.info("404 bulk-channel-status injection disabled")

    def get_404_bcs_injected_count(self) -> int:
        """Return the number of 404 responses injected on :bulk-channel-status."""
        return self.get_status()["injected_404_bcs_count"]

    def is_reachable(self) -> bool:
        """Check if the control API is reachable."""
        try:
            resp = requests.get(f"{self.base_url}/status", timeout=3)
            return resp.status_code == 200
        except (requests.ConnectionError, requests.Timeout, requests.RequestException):
            return False


def from_env() -> MitmproxyControl | None:
    """Create a MitmproxyControl from environment, or None if not configured."""
    url = os.environ.get("MITMPROXY_CONTROL_URL")
    if not url:
        return None
    return MitmproxyControl(url)
