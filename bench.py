"""Bounded drain microbenchmark; HTTP is a deterministic in-process 204 stub."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from statistics import median, quantiles
from time import perf_counter
from uuid import uuid4

import httpx
from psycopg import connect, sql
from psycopg.conninfo import make_conninfo

from relayledger.db import Ledger
from relayledger.worker import deliver_one


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=300)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    if not (2 <= args.events <= 50_000 and 1 <= args.workers <= 48):
        parser.error("expected 2 to 50000 events and 1 to 48 workers")
    dsn = os.environ["TEST_DATABASE_URL"]
    schema = "relay_bench_" + uuid4().hex
    with connect(dsn, autocommit=True) as control:
        control.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            ledger = Ledger(make_conninfo(dsn, options=f"-c search_path={schema}"))
            ledger.migrate()
            for i in range(args.events):
                ledger.enqueue(str(i), {"seq": i})
            def drain():
                durations = []
                mock = httpx.MockTransport(lambda _: httpx.Response(204))
                with httpx.Client(transport=mock) as client:
                    while True:
                        now = perf_counter()
                        if not deliver_one(ledger, client, "http://receiver/events", b"local-only", 30):
                            return durations
                        durations.append(1_000 * (perf_counter() - now))
            start = perf_counter()
            with ThreadPoolExecutor(args.workers) as pool:
                durations = [n for run in pool.map(lambda _: drain(), range(args.workers)) for n in run]
            elapsed = perf_counter() - start
            with ledger.connect() as conn:
                successful = conn.execute(
                    "SELECT count(*) AS n FROM deliveries WHERE state='delivered'").fetchone()["n"]
                attempts = conn.execute("SELECT count(*) AS n FROM delivery_attempts").fetchone()["n"]
                pg = conn.execute("SHOW server_version").fetchone()["server_version"]
            assert args.events == successful == attempts == len(durations)
            print(json.dumps({"postgresql": pg, "event_count": successful,
                              "delivery_attempts": attempts, "worker_threads": args.workers,
                              "seconds": round(elapsed, 4), "deliveries_per_second": round(successful/elapsed, 1),
                              "p50_worker_step_ms": round(median(durations), 2),
                              "p95_worker_step_ms": round(quantiles(durations, n=100)[94], 2)}, indent=2))
        finally:
            control.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    main()

