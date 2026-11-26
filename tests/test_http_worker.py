from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from relayledger.api import make_app
from relayledger.db import IdempotencyConflict
from relayledger.worker import deliver_one, retry_delay


class Stub:
    def __init__(self):
        self.record = None
        self.id = uuid4()
        self.finished = None

    def enqueue(self, key, payload, max_attempts):
        d = key, payload, max_attempts
        if self.record is not None and self.record != d:
            raise IdempotencyConflict(key)
        created = self.record is None
        self.record = d
        return {"id": self.id, "state": "ready"}, created

    def get(self, delivery_id):
        return {"id": self.id} if delivery_id == self.id else None

    def claim(self, _):
        return {"id": self.id, "payload": {"amount": 7}, "attempts": 1, "fence": 7}

    def finish(self, id_, fence, outcome, *, retry_after):
        self.finished = id_, fence, outcome, retry_after
        return True


class PassedThrough(httpx.BaseTransport):
    def handle_request(self, request):
        assert list(request.stream) == [b'{"amount":7}']
        assert request.headers["x-relay-signature"].startswith("sha256=")
        return httpx.Response(status_code=503, headers={"Retry-After": "10"})


def test_http_contract_without_a_database_socket():
    stub = Stub()
    client = TestClient(make_app(stub, "long-local-demo-token"))
    r = client.post("/deliveries", headers={"Idempotency-Key": "x"}, json={"payload": {"hi": "test"}})
    assert r.status_code == 401 and stub.record is None
    headers = {"Idempotency-Key": "x", "Authorization": "Bearer long-local-demo-token"}
    r = client.post("/deliveries", headers=headers, json={"payload": {"hi": "test"}})
    assert r.status_code == 202 and r.json()["id"] == str(stub.id)
    assert client.get(r.headers["location"], headers=headers).status_code == 200
    assert client.get(f"/deliveries/{uuid4()}", headers=headers).status_code == 404
    assert client.post("/deliveries", headers=headers, json={"payload": {"hi": "test"}}).status_code == 200
    assert client.post("/deliveries", headers=headers, json={"payload": {}}).status_code == 409
    oversized = lambda: (b"x" * 32768, b"x" * 32769)
    assert client.post("/deliveries", headers=headers, content=iter(oversized())).status_code == 413
    assert client.post("/deliveries", headers=headers, json={"payload": {}, "max_attempts": 21}).status_code == 422
    for value in (r'{"payload":{"bad":"\u0000"}}', r'{"payload":{"bad":"\ud800"}}',
                  r'{"payload":{"bad":NaN}}'):
        response = client.post("/deliveries", headers={**headers, "Content-Type": "application/json"},
                               content=value)
        assert response.status_code == 422



def test_worker_sends_exact_signed_bytes_and_uses_retry_after():
    stub = Stub()
    with httpx.Client(transport=PassedThrough()) as client:
        assert deliver_one(stub, client, "http://receiver/events", b"local", 30)
    assert stub.finished == (stub.id, 7, "http_503", 10)


def test_network_failure_is_ledger_retry():
    def fail(request):
        raise httpx.ConnectError("local mock", request=request)
    stub = Stub()
    with httpx.Client(transport=httpx.MockTransport(fail)) as client:
        assert deliver_one(stub, client, "http://receiver/events", b"local", 30)
    assert stub.finished[:3] == (stub.id, 7, "transport_error")
    assert 0.25 <= stub.finished[3] <= 2


@pytest.mark.parametrize("value", ["bad", "-1", "0", "Wed, 21 Oct 2015 07:28:00 GMT"])
def test_retry_floor_and_bad_headers(value):
    assert 0.25 <= retry_delay(1, value) <= 2


def test_retry_after_is_bounded():
    assert retry_delay(2, "100000000") == 900

