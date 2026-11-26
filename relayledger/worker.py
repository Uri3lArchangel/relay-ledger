from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx
from psycopg import OperationalError

from .db import Ledger

LOG = logging.getLogger("relayledger")


def retry_delay(attempt: int, header: str | None = None) -> float:
    # Full jitter with a 0.25s floor. Cap receiver-provided delay to 15 minutes.
    computed = random.uniform(0.25, min(60, 2 ** min(attempt, 6)))
    if not header:
        return computed
    try:
        delay = int(header) if header.isascii() and header.isdecimal() else (
            parsedate_to_datetime(header) - datetime.now(timezone.utc)
        ).total_seconds()
    except (TypeError, ValueError, OverflowError):
        return computed
    return max(computed, min(900, delay))


def deliver_one(ledger, client, destination, secret: bytes, lease_seconds: float):
    item = ledger.claim(lease_seconds)
    if item is None:
        return False
    delivery_id = str(item["id"])
    body = json.dumps(item["payload"], separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False, sort_keys=True).encode()
    timestamp = str(int(time.time()))
    signature = hmac.new(secret, timestamp.encode() + b"." + delivery_id.encode() + b"." + body,
                         hashlib.sha256).hexdigest()
    headers = {"Content-Type": "application/json", "X-Relay-ID": delivery_id,
               "X-Relay-Timestamp": timestamp, "X-Relay-Signature": "sha256=" + signature}
    try:
        # Stream headers only: response bodies can be arbitrarily large.
        with client.stream("POST", destination, content=body, headers=headers) as response:
            status = response.status_code
            delay = retry_delay(item["attempts"], response.headers.get("Retry-After")) if (
                status in (408, 425, 429) or status >= 500
            ) else None
            outcome = "delivered" if 200 <= status < 300 else f"http_{status}"
    except httpx.RequestError:
        outcome, delay = "transport_error", retry_delay(item["attempts"])
    if not ledger.finish(item["id"], item["fence"], outcome, retry_after=delay):
        LOG.warning("stale_completion delivery_id=%s fence=%s", delivery_id, item["fence"])
    else:
        LOG.info("attempt_finished delivery_id=%s fence=%s outcome=%s",
                 delivery_id, item["fence"], outcome)
    return True


def run():
    ledger = Ledger(os.environ["DATABASE_URL"])
    destination = httpx.URL(os.environ["RELAY_DESTINATION"])
    if destination.scheme not in ("http","https") or not destination.host:
        raise ValueError("RELAY_DESTINATION must be a fixed HTTP(S) URL")
    if destination.scheme == "http" and destination.host not in ("localhost", "127.0.0.1", "::1", "receiver"):
        raise ValueError("Non-demo destinations must use HTTPS")
    if destination.username or destination.password:
        raise ValueError("Credentials are not allowed in RELAY_DESTINATION")
    secret = os.environ["RELAY_SIGNING_SECRET"].encode()
    if len(secret) < 32:
        raise ValueError("Use an RELAY_SIGNING_SECRET of at least 32 bytes")
    logging.basicConfig(level=logging.INFO)
    with httpx.Client(timeout=5, follow_redirects=False, trust_env=False) as client:
        while True:
            try:
                if not deliver_one(ledger, client, destination, secret, 30):
                    time.sleep(.25)
            except OperationalError:
                LOG.exception("database_unavailable")
                time.sleep(1)

