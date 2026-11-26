"""Local demo receiver. Commit effect and dedupe key in the same SQLite transaction."""
import hashlib
import hmac
import os
import sqlite3
import time
from uuid import UUID

from fastapi import FastAPI, Request, Response

from .api import BodyLimit

app = FastAPI(title="RelayLedger Demo Receiver")
app.add_middleware(BodyLimit)


@app.post("/events")
async def event(request: Request):
    headers = request.headers
    delivery = headers.get("x-relay-id", "")
    stamp = headers.get("x-relay-timestamp", "")
    try:
        UUID(delivery)
        if abs(time.time() - int(stamp)) > 300:
            return Response(status_code=401)
    except ValueError:
        return Response(status_code=401)
    body = await request.body()
    secret = os.environ["RELAY_SIGNING_SECRET"].encode()
    mac = hmac.new(secret, stamp.encode() + b"." + delivery.encode() + b"." + body,
                   hashlib.sha256).hexdigest()
    if not hmac.compare_digest(("sha256=" + mac).encode(),
                               headers.get("x-relay-signature", "").encode()):
        return Response(status_code=401)
    if headers.get("content-type") != "application/json":
        return Response(status_code=415)
    with sqlite3.connect(os.getenv("RELAY_RECEIPTS", "receipts.sqlite3")) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY, body BLOB NOT NULL)")
        conn.execute("INSERT OR IGNORE INTO receipts VALUES (?,?)", (delivery, body))
    return Response(status_code=204)

