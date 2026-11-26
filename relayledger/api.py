from __future__ import annotations

import hmac
import json
import os
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field

from .db import IdempotencyConflict, Ledger


class BodyLimit:
    """Bound buffered bytes before the request reaches FastAPI's JSON parser."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST":
            return await self.app(scope, receive, send)
        body = bytearray()
        while True:
            msg = await receive()
            if msg["type"] == "http.disconnect":
                return
            chunk = msg.get("body", b"")
            if len(body) + len(chunk) > 65_536:
                return await Response(status_code=413)(scope, receive, send)
            body.extend(chunk)
            if not msg.get("more_body", False):
                break

        async def downstream():
            nonlocal body
            if body is None:
                return await receive()
            full = bytes(body)
            body = None
            return {"type": "http.request", "body": full, "more_body": False}

        return await self.app(scope, downstream, send)


class Event(BaseModel):
    payload: dict
    max_attempts: int = Field(default=5, ge=1, le=20)


def make_app(ledger: Ledger, token: str) -> FastAPI:
    if len(token) < 16:
        raise ValueError("Use an LEDGER_TOKEN of at least 16 characters")
    app = FastAPI(title="RelayLedger")

    def authorized(authorization: str):
        if not hmac.compare_digest(authorization.encode(), ("Bearer " + token).encode()):
            raise HTTPException(401, "Invalid credentials", headers={"WWW-Authenticate": "Bearer"})

    app.add_middleware(BodyLimit)

    @app.post("/deliveries", status_code=202)
    def enqueue(event: Event, response: Response,
                idempotency_key: Annotated[str, Header(min_length=1, max_length=128)],
                authorization: Annotated[str, Header()] = ""):
        authorized(authorization)
        # JSONB rejects NUL and unpaired surrogates; reject them and nonfinite
        # numbers before Psycopg retries or a database error obscures the cause.
        try:
            raw = json.dumps(event.payload, ensure_ascii=False, allow_nan=False)
            raw.encode("utf-8")
            stack = [event.payload]
            while stack:
                value = stack.pop()
                if isinstance(value, dict):
                    stack.extend(value.keys())
                    stack.extend(value.values())
                elif isinstance(value, list):
                    stack.extend(value)
                elif isinstance(value, str) and "\x00" in value:
                    raise ValueError("NUL is not supported")
        except (ValueError, RecursionError) as exc:
            raise HTTPException(422, "Payload contains unsupported JSON values") from exc
        try:
            row, created = ledger.enqueue(idempotency_key, event.payload, event.max_attempts)
        except IdempotencyConflict as exc:
            raise HTTPException(409, "Key is bound to a different request") from exc
        response.status_code = 202 if created else 200
        response.headers["Location"] = "/deliveries/" + str(row["id"])
        return row

    @app.get("/deliveries/{delivery_id}")
    def status(delivery_id: UUID, authorization: Annotated[str, Header()] = ""):
        authorized(authorization)
        row = ledger.get(delivery_id)
        if row is None:
            raise HTTPException(404, "Delivery not found")
        return row

    return app


def create_app():
    return make_app(Ledger(os.environ["DATABASE_URL"]), os.environ["LEDGER_TOKEN"])

