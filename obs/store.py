"""ClickHouse storage for load events and check results.

ClickHouse is the store because the load-event table is append-only, high
cardinality and always queried as "latest per source" — a MergeTree ordered by
(source, loaded_at) answers that with a skip index instead of a scan.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from .checks import Finding, LoadEvent

DDL = [
    "CREATE DATABASE IF NOT EXISTS obs",
    """
    CREATE TABLE IF NOT EXISTS obs.load_events (
        source      LowCardinality(String),
        run_id      String,
        loaded_at   DateTime64(3, 'UTC'),
        rows        UInt64,
        status      LowCardinality(String),
        ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
    ) ENGINE = ReplacingMergeTree(ingested_at)
    PARTITION BY toYYYYMM(loaded_at)
    ORDER BY (source, loaded_at, run_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS obs.check_results (
        checked_at      DateTime64(3, 'UTC'),
        source          LowCardinality(String),
        state           LowCardinality(String),
        severity        LowCardinality(String),
        owner           LowCardinality(String),
        detail          String,
        observed_rows   Nullable(UInt64),
        expected_rows   Nullable(Float64),
        minutes_late    Nullable(Int32),
        last_success_at Nullable(DateTime64(3, 'UTC')),
        downstream      Array(String)
    ) ENGINE = MergeTree
    PARTITION BY toYYYYMM(checked_at)
    ORDER BY (source, checked_at)
    """,
    # One row per source per state change: the "how long were we broken" table.
    """
    CREATE TABLE IF NOT EXISTS obs.incidents (
        source      LowCardinality(String),
        state       LowCardinality(String),
        opened_at   DateTime64(3, 'UTC'),
        resolved_at Nullable(DateTime64(3, 'UTC')),
        severity    LowCardinality(String)
    ) ENGINE = ReplacingMergeTree
    ORDER BY (source, opened_at)
    """,
]


def client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


def migrate(ch) -> None:
    for stmt in DDL:
        ch.command(stmt)


def insert_events(ch, events: list[LoadEvent]) -> int:
    if not events:
        return 0
    ch.insert(
        "obs.load_events",
        [[e.source, e.run_id, e.loaded_at, e.rows, e.status] for e in events],
        column_names=["source", "run_id", "loaded_at", "rows", "status"],
    )
    return len(events)


def fetch_events(ch, lookback_days: int = 21) -> dict[str, list[LoadEvent]]:
    rows = ch.query(
        """
        SELECT source, run_id, loaded_at, rows, status
        FROM obs.load_events
        WHERE loaded_at >= now() - INTERVAL %(d)s DAY
        ORDER BY source, loaded_at
        """,
        parameters={"d": lookback_days},
    ).result_rows
    out: dict[str, list[LoadEvent]] = {}
    for source, run_id, loaded_at, nrows, status in rows:
        if loaded_at.tzinfo is None:
            loaded_at = loaded_at.replace(tzinfo=timezone.utc)
        out.setdefault(source, []).append(
            LoadEvent(source, run_id, loaded_at, int(nrows), status)
        )
    return out


def observed_sources(ch) -> set[str]:
    rows = ch.query("SELECT DISTINCT source FROM obs.load_events").result_rows
    return {r[0] for r in rows}


def record_results(ch, findings: list[Finding], checked_at: datetime | None = None) -> None:
    checked_at = checked_at or datetime.now(timezone.utc)
    ch.insert(
        "obs.check_results",
        [
            [checked_at, f.source, f.state, f.severity, f.owner, f.detail,
             f.observed_rows, f.expected_rows, f.minutes_late, f.last_success_at,
             list(f.downstream)]
            for f in findings
        ],
        column_names=["checked_at", "source", "state", "severity", "owner", "detail",
                      "observed_rows", "expected_rows", "minutes_late",
                      "last_success_at", "downstream"],
    )


def latest_results(ch):
    return ch.query(
        """
        SELECT source, argMax(state, checked_at) AS state,
               argMax(severity, checked_at) AS severity,
               argMax(owner, checked_at) AS owner,
               argMax(detail, checked_at) AS detail,
               argMax(last_success_at, checked_at) AS last_success_at,
               argMax(minutes_late, checked_at) AS minutes_late,
               argMax(downstream, checked_at) AS downstream,
               max(checked_at) AS checked_at
        FROM obs.check_results
        GROUP BY source
        """
    ).result_rows
