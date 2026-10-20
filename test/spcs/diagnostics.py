"""Extract bounded identifiers and status codes, never arbitrary log text."""

import re
import sys
from pathlib import Path

MAX_LOG_BYTES = 2 * 1024 * 1024
UUID = r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}"


def signals(text):
    request_ids = re.findall(
        rf"\b(?:request[_ -]?id|SPCS_REQUEST_ID)[\"']?\s*[:=]\s*[\"']?({UUID})(?![0-9a-z-])",
        text,
        re.IGNORECASE,
    )
    statuses = re.findall(
        r"\b(?:HTTP(?:/\d(?:\.\d)?)?(?: response)?(?: status)?(?: code)?|"
        r"status[_ ]code|SPCS_HTTP_STATUS)[\"']?\s*[:=]?\s*[\"']?([45]\d{2})(?!\d)",
        text,
        re.IGNORECASE,
    )
    return {
        "request_ids": sorted(set(value.lower() for value in request_ids))[:20],
        "http_statuses": sorted(set(int(value) for value in statuses))[:20],
    }


def log_tail(path):
    with Path(path).open("rb") as stream:
        stream.seek(0, 2)
        stream.seek(max(0, stream.tell() - MAX_LOG_BYTES))
        return stream.read(MAX_LOG_BYTES).decode("utf-8", errors="replace")


if __name__ == "__main__":
    for filename in sys.argv[1:]:
        try:
            result = signals(log_tail(filename))
        except OSError:
            continue
        for request_id in result["request_ids"]:
            print(f"SPCS_REQUEST_ID={request_id}")
        for status in result["http_statuses"]:
            print(f"SPCS_HTTP_STATUS={status}")
