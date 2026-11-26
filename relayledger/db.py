from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


class IdempotencyConflict(Exception):
    pass


class Ledger:
    """Each method owns one transaction. Never hold a DB lock across HTTP."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    def connect(self):
        return connect(self.dsn, row_factory=dict_row, connect_timeout=5)

    def migrate(self):
        schema = Path(__file__).with_name("schema.sql").read_text()
        with self.connect() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(926670391)")
            conn.execute(schema)

    def enqueue(self, key: str, payload: dict, max_attempts=5):
        with self.connect() as conn:
            row = conn.execute(
                """INSERT INTO deliveries (id,idempotency_key,payload,max_attempts)
                   VALUES (%s,%s,%s,%s) ON CONFLICT (idempotency_key) DO NOTHING
                   RETURNING id,state""",
                (uuid4(), key, Jsonb(payload), max_attempts),
            ).fetchone()
            if row:
                return row, True
            # Separate statement: following an ON CONFLICT wait, READ COMMITTED
            # must take a new snapshot to see the competing committed record.
            row = conn.execute(
                """SELECT id,state,payload = %s AS same_payload,
                      max_attempts = %s AS same_limit FROM deliveries
                   WHERE idempotency_key=%s""", (Jsonb(payload), max_attempts, key)
            ).fetchone()
            if row is None or not (row.pop("same_payload") and row.pop("same_limit")):
                raise IdempotencyConflict(key)
            return row, False

    def get(self, delivery_id: UUID):
        with self.connect() as conn:
            return conn.execute(
                """SELECT id,state,attempts,max_attempts,available_at,
                   created_at,finished_at,last_outcome FROM deliveries WHERE id=%s""",
                (delivery_id,),
            ).fetchone()

    def claim(self, lease_seconds: float):
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        with self.connect() as conn:
            # One candidate per transaction keeps expiry cleanup bounded. Crash
            # at the last lease consumes the last attempt, so it finishes dead.
            row = conn.execute(
                """SELECT id,payload,state,fence,attempts,max_attempts
                   FROM deliveries WHERE state IN ('ready','leased')
                       AND available_at <= statement_timestamp()
                   ORDER BY available_at,id LIMIT 1 FOR UPDATE SKIP LOCKED"""
            ).fetchone()
            if row is None:
                return None
            id_ = row["id"]
            if row["state"] == "leased":
                self._close_attempt(conn, id_, row["fence"], "lease_expired")
            if row["attempts"] >= row["max_attempts"]:
                conn.execute(
                    """UPDATE deliveries SET state='dead',finished_at=clock_timestamp(),
                       last_outcome='lease_expired' WHERE id=%s""", (id_,)
                )
                return None
            conn.execute(
                """UPDATE deliveries SET state='leased',attempts=attempts+1,
                   fence=fence+1,available_at=clock_timestamp() + %s * interval '1 second'
                   WHERE id=%s""", (lease_seconds, id_)
            )
            row["attempts"] += 1
            row["fence"] += 1
            conn.execute(
                "INSERT INTO delivery_attempts (delivery_id,fence) VALUES (%s,%s)",
                (id_, row["fence"]),
            )
            return row

    @staticmethod
    def _close_attempt(conn, delivery_id, fence, outcome):
        result = conn.execute(
            """UPDATE delivery_attempts SET finished_at=clock_timestamp(),outcome=%s
               WHERE delivery_id=%s AND fence=%s AND finished_at IS NULL""",
            (outcome, delivery_id, fence),
        )
        if result.rowcount != 1:
            raise RuntimeError("queue invariant: active attempt missing")

    def finish(self, delivery_id, fence, outcome, *, retry_after: float | None = None):
        if (outcome == "delivered" and retry_after is not None) or (
            retry_after is not None and retry_after < 0
        ):
            raise ValueError("invalid delivery outcome")
        with self.connect() as conn:
            # Acquire the row lock before testing clock_timestamp(), so a lock
            # wait cannot make an originally valid finish land after expiry.
            row = conn.execute(
                """SELECT attempts,max_attempts FROM deliveries WHERE id=%s
                   AND state='leased' AND fence=%s FOR UPDATE""",
                (delivery_id, fence),
            ).fetchone()
            if row is None:
                return False
            state = ("delivered" if outcome == "delivered" else
                     "ready" if retry_after is not None and row["attempts"] < row["max_attempts"]
                     else "dead")
            updated = conn.execute(
                """UPDATE deliveries SET state=%s, last_outcome=%s,
                   available_at=clock_timestamp() + %s * interval '1 second',
                   finished_at=CASE WHEN %s='ready' THEN NULL ELSE clock_timestamp() END
                   WHERE id=%s AND available_at > clock_timestamp()""",
                (state, outcome, retry_after or 0, state, delivery_id),
            )
            if updated.rowcount != 1:
                return False
            self._close_attempt(conn, delivery_id, fence, outcome)
            return True

