from itertools import islice
import json
import logging
import threading
import time
from typing import Callable

import requests

from lib.driver import KafkaDriver


def wait_for(f: Callable[[], bool], *, timeout: int = 60, interval: int = 5) -> bool:
    deadline = time.monotonic() + timeout
    while True:
        if f():
            return True
        if time.monotonic() > deadline:
            return False
        time.sleep(interval)


class RecordProducer:
    """Produces sequentially numbered JSON records to a Kafka topic."""

    def __init__(self, driver: KafkaDriver, topic: str):
        self._driver = driver
        self._topic = topic
        self.records_produced = 0
        self._generator = self._make_generator()
        self._stop_event = threading.Event()
        self._thread = None

    def _make_generator(self):
        while True:
            self.records_produced += 1
            yield json.dumps({"number": str(self.records_produced)}).encode()

    def send(self, n: int):
        self._driver.sendBytesData(self._topic, islice(self._generator, n), [], 0)

    def start_continuous(self, batch_size: int = 10, interval: float = 0.1):
        """Start a background thread that sends records continuously."""
        self._stop_event.clear()

        def _produce():
            while not self._stop_event.is_set():
                self.send(batch_size)
                self._stop_event.wait(interval)

        self._thread = threading.Thread(target=_produce, daemon=True)
        self._thread.start()
        logging.info(
            f"Started continuous producer (batch_size={batch_size}, interval={interval}s)"
        )

    def stop_continuous(self, timeout: float = 5):
        if self._thread is not None:
            self._stop_event.set()
            self._thread.join(timeout=timeout)
            self._thread = None
            logging.info(
                f"Stopped continuous producer (total: {self.records_produced})"
            )


def steal_channel(
    host: str,
    database: str,
    schema: str,
    pipe_name: str,
    channel_name: str,
    jwt_token: str,
) -> dict:
    """Open an SSV2 streaming channel via HTTP, stealing it from KC.

    Calling this endpoint with a different logical client invalidates the
    existing channel owner (KC). KC's next appendRow will get
    InvalidChannelError.

    Returns the parsed JSON response from the server.
    """
    url = (
        f"https://{host}/v2/streaming/databases/{database}"
        f"/schemas/{schema}/pipes/{pipe_name}/channels/{channel_name}"
    )
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    logging.info(f"Stealing channel via PUT {url}")
    resp = requests.put(url, headers=headers, json={}, timeout=30)
    logging.info(f"Channel steal response: {resp.status_code} {resp.text[:500]}")
    resp.raise_for_status()
    return resp.json()
