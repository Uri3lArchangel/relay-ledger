from concurrent.futures import ThreadPoolExecutor
import os
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from psycopg import connect, sql
from psycopg.conninfo import make_conninfo

from relayledger.api import make_app
from relayledger.db import Ledger
from relayledger.receiver import app as receiver
from relayledger.worker import deliver_one

TOKEN = "only-used-in-tests"
SECRET = b"local-integration-test-secret-only"


@pytest.fixture
def ledger():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")
    schema = "relay_test_" + uuid4().hex
    conn = connect(dsn, autocommit=True)
    try:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        store = Ledger(make_conninfo(dsn, options=f"-c search_path={schema}"))
        store.migrate()
        yield store
    finally:
        conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        conn.close()


def expire(ledger, delivery_id):
    with ledger.connect() as conn:
        conn.execute("UPDATE deliveries SET available_at=clock_timestamp() - interval '1 second' WHERE id=%s",
                     (delivery_id,))


def outcomes(ledger, delivery_id):
    with ledger.connect() as conn:
        return [r["outcome"] for r in conn.execute(
            "SELECT outcome FROM delivery_attempts WHERE delivery_id=%s ORDER BY fence",
            (delivery_id,),
        )]


def test_http_idempotency_reorders_payload_keys_and_distinguishes_changed_request(ledger):
    client = TestClient(make_app(ledger, TOKEN))
    headers = {"Authorization": f"Bearer {TOKEN}", "Idempotency-Key": "abc"}
    r = client.post("/deliveries", headers=headers, json={"payload": {"z": 1, "a": 2}})
    assert r.status_code == 202
    again = client.post("/deliveries", headers=headers, json={"payload": {"a": 2, "z": 1}})
    assert again.status_code == 200
    assert again.json()["id"] == r.json()["id"]
    for body in ({"payload": {"z": 2, "a": 2}}, {"payload": {"z": 1, "a": 2}, "max_attempts": 3}):
        assert client.post("/deliveries", headers=headers, json=body).status_code == 409
    assert client.get(r.headers["Location"], headers=headers).json()["state"] == "ready"
    assert client.get(r.headers["Location"]).status_code == 401
    assert client.post("/deliveries", headers=headers, content=b"x" * 65_537).status_code == 413


def test_concurrent_ingress_produces_one_record(ledger):
    with ThreadPoolExecutor(12) as pool:
        results = list(pool.map(lambda _: ledger.enqueue("same", {"event": 1}), range(36)))
    assert sum(created for _, created in results) == 1
    assert len({r["id"] for r, _ in results}) == 1


def test_multiple_workers_claim_distinct_events(ledger):
    ids = {ledger.enqueue(str(i), {"i": i})[0]["id"] for i in range(30)}
    with ThreadPoolExecutor(10) as pool:
        claims = list(pool.map(lambda _: ledger.claim(30), range(45)))
    claimed = [row for row in claims if row]
    assert len(claimed) == 30 and {row["id"] for row in claimed} == ids
    assert all(row["fence"] == 1 for row in claimed)


def test_crash_after_effect_receiver_dedupes_and_old_fence_is_rejected(ledger, monkeypatch, tmp_path):
    import sqlite3

    monkeypatch.setenv("RELAY_SIGNING_SECRET", SECRET.decode())
    receipts = tmp_path / "receipts.sqlite3"
    monkeypatch.setenv("RELAY_RECEIPTS", str(receipts))
    r, _ = ledger.enqueue("same-order", {"order": 123})
    receiver_client = TestClient(receiver)

    def into_receiver(request):
        reply = receiver_client.post(request.url.path, headers=request.headers, content=request.content)
        return httpx.Response(reply.status_code)

    with httpx.Client(transport=httpx.MockTransport(into_receiver)) as transport:
        class LostFinish:
            claim = ledger.claim

            def finish(self, *_args, **_kwargs):
                raise ConnectionError("simulated worker death after downstream commit")
        with pytest.raises(ConnectionError):
            deliver_one(LostFinish(), transport, "http://receiver/events", SECRET, 30)
        expire(ledger, r["id"])
        assert not ledger.finish(r["id"], 1, "delivered")  # before another worker reclaims
        assert deliver_one(ledger, transport, "http://receiver/events", SECRET, 30)
    assert ledger.get(r["id"])["state"] == "delivered"
    assert not ledger.finish(r["id"], 1, "delivered")
    assert outcomes(ledger, r["id"]) == ["lease_expired", "delivered"]
    with sqlite3.connect(receipts) as conn:
        assert conn.execute("SELECT count(*) FROM receipts").fetchone()[0] == 1


def test_retryable_response_requeues_and_exhausted_job_is_dead(ledger):
    r, _ = ledger.enqueue("retry", {"i": 1}, max_attempts=2)
    with httpx.Client(transport=httpx.MockTransport(
            lambda _: httpx.Response(503, headers={"Retry-After": "120"}))) as mock:
        assert deliver_one(ledger, mock, "http://receiver/events", SECRET, 15)
        assert ledger.get(r["id"])["state"] == "ready"
        assert not deliver_one(ledger, mock, "http://receiver/events", SECRET, 15)
        expire(ledger, r["id"])
        assert deliver_one(ledger, mock, "http://receiver/events", SECRET, 15)
    assert ledger.get(r["id"])["state"] == "dead"
    assert outcomes(ledger, r["id"]) == ["http_503", "http_503"]


def test_worker_lost_on_last_attempt_enters_dead_letter(ledger):
    r, _ = ledger.enqueue("one", {}, max_attempts=1)
    assert ledger.claim(30)["id"] == r["id"]
    expire(ledger, r["id"])
    assert ledger.claim(30) is None
    assert ledger.get(r["id"])["state"] == "dead"
    assert outcomes(ledger, r["id"]) == ["lease_expired"]


def test_permanent_client_error_dead_letters_without_retry(ledger):
    r, _ = ledger.enqueue("bad", {}, max_attempts=5)
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(400))) as mock:
        assert deliver_one(ledger, mock, "http://receiver/events", SECRET, 15)
    assert ledger.get(r["id"])["state"] == "dead"
    assert outcomes(ledger, r["id"]) == ["http_400"]


def test_invalid_receiver_signature_is_rejected(monkeypatch):
    monkeypatch.setenv("RELAY_SIGNING_SECRET", SECRET.decode())
    client = TestClient(receiver)
    import time
    assert client.post("/events", content=b"{}",
                       headers={"X-Relay-ID": str(uuid4()), "X-Relay-Timestamp": str(int(time.time())),
                                "X-Relay-Signature": "bad"}).status_code == 401

