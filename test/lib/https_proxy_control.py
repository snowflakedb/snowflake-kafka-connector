"""Client for the forward-proxy counter addon (docker/https-proxy)."""

import logging
import os

import requests

logger = logging.getLogger(__name__)

CONNECTOR_TIER_PORT = 8080
JVM_TIER_PORT = 8081
PROXY_HOST = "https-proxy"


class HttpsProxyControl:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def get_status(self) -> dict:
        resp = requests.get(f"{self.base_url}/status", timeout=5)
        resp.raise_for_status()
        return resp.json()

    def counts(self, port: int) -> dict:
        """Snowflake call counts seen on one listener port, e.g. {"jdbc_login": 1}."""
        return self.get_status()["counts"].get(str(port), {})

    def reset(self):
        resp = requests.post(f"{self.base_url}/reset", timeout=5)
        resp.raise_for_status()
        logger.info("https-proxy counters reset")

    def is_reachable(self) -> bool:
        try:
            return requests.get(f"{self.base_url}/status", timeout=3).ok
        except requests.RequestException:
            return False


def from_env() -> HttpsProxyControl | None:
    url = os.environ.get("HTTPS_PROXY_CONTROL_URL")
    if not url:
        return None
    return HttpsProxyControl(url)
