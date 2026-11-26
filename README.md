# RelayLedger

A crash-aware webhook delivery lab built with FastAPI, Python, and PostgreSQL. The interesting case is not a 200 response. It is a receiver that committed its effect while the worker died before recording the response.

## Run the local demo

Requires Docker with Compose. The API binds to loopback; the demo uses synthetic events, a single fixed receiver, and local-only example secrets.

~~~sh
docker compose up --build --scale worker=3
~~~

In a second terminal:

~~~sh
curl -i localhost:8000/deliveries \
  -H 'Authorization: Bearer local-demo-token-change-me' \
  -H 'Idempotency-Key: order-271-v1' \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"order":271,"state":"paid"}}'
# GET the returned Location with the same Authorization header.
# Same key and JSON => 200, same delivery ID. Different body => 409.
~~~

The SQLite demo receiver uses the stable Relay ID as a primary key. Its one committed effect is stored in that same transaction. This is a teaching receiver, not a general deduplication guarantee for remote clients.

## Failure model

| Situation | Behaviour |
|---|---|
| Producer loses enqueue response | Retry by key; one row survives concurrent same-key inserts. PostgreSQL JSONB equality determines equivalent payloads. |
| Multiple workers see due work | Partial index locates active due rows. A short transaction uses FOR UPDATE SKIP LOCKED, advances the fence, and records the attempt. No lock is held across HTTP. |
| Worker dies before sending | PostgreSQL's clock makes its lease reclaimable. Each claim consumes an attempt even if no remote effect occurred. |
| Worker dies after downstream commit | A retry may send the event again. Stable delivery ID lets the receiver deduplicate its own side effect. |
| Old worker returns after lease loss | Completion requires the current fence and an unexpired lease; the late result cannot overwrite the new attempt. |
| Receiver returns 2xx | Commit delivered state and attempt outcome atomically. |
| Transport error, 408, 425, 429 or 5xx | Reschedule with jitter and capped Retry-After. Exhaustion, other status codes and expired final leases enter dead-letter state. |

Explicitly **not exactly-once delivery**. The worker cannot fence side effects at an independent receiver and does not cancel an HTTP request after PostgreSQL lease loss. Its configured HTTPX timeouts apply to phases, not a hard end-to-end deadline; sender windows can overlap. Stable ID and receiver-owned idempotence are required. Poll ordering is best effort, not strict FIFO. Sender signs timestamp, delivery ID and exact emitted body; the demo receives only a configured destination and follows no redirects. Upstream queue and dedupe retention need a real policy; this demo keeps records. It does not implement tenant isolation, per-destination rate limits, multi-destination dispatch, credential rotation, long-running lease renewal, or a production outbox commit tied to another business database. Shared bearer authorization and demo Docker credentials are local examples. Use TLS and enforce egress restrictions in a real deployment.

## Verify and benchmark

~~~sh
python -m pip install '.[test]'
# Unit/API checks run without PostgreSQL. Integration tests are skipped if absent.
python -m pytest -q

# Point at a local PostgreSQL test database; the role needs CREATE SCHEMA.
TEST_DATABASE_URL='postgresql://relay:test-only@127.0.0.1/relay' python -m pytest -q
TEST_DATABASE_URL='postgresql://relay:test-only@127.0.0.1/relay' python bench.py --events 300 --workers 8
~~~

Integration tests use a fresh schema per case, including competing connections and a simulated crash after the receiver committed. The benchmark preloads rows, then times the drain via local PostgreSQL and an in-process 204 stub; it includes per-step connection overhead. It validates one committed attempt per row. It is not API traffic capacity or internet delivery latency.

## Design review notes

- Claim and completion serialize by row lock; claim and attempt insert commit together, and completion and attempt close commit together. Concurrent producers use the unique key and a fresh READ COMMITTED snapshot after conflict.
- The queue is optimized for a small active working set. If dispatch throughput were the goal, measure connection pooling, claims in batches, write contention, and vacuum pressure before replacing the database with a broker.
- The delivery state is inspectable via a status endpoint. Pending, expired and exhausted attempts remain traceable in the database for a postmortem.
- Portfolio prototype, initially implemented with AI assistance. It is not claimed as previous employment, production adoption, or unassisted work.


### Recorded local run

On 2026-11-26, CPython 3.12.14 and PostgreSQL 16.2 (Linux x86-64): **16 tests passed**. A separate loopback smoke run used two Uvicorn processes, the worker CLI, and the actual database: enqueue returned 202, a duplicate returned 200 with the same ID, the worker received HTTP 204, status became delivered, and SQLite held one receipt. No third-party endpoint was contacted.

One recorded run of the synthetic drain benchmark above: **300 deliveries, 300 completed attempts, 8 worker threads, 0.7915 s (~379/s), p50 20.19 ms, p95 28.84 ms** per worker iteration. This is a single local measurement with a mock 204 transport, not a repeatability guarantee. The Compose container orchestration and hosted CI were not run for this measurement.
