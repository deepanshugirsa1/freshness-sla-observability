"""File-backed stand-in for ClickHouse, so `--offline` runs with no server.

Same function names as obs.store. Used by the test suite, by CI smoke runs and
by anyone cloning the repo who does not want to start a container first.
ClickHouse remains the real store; this exists to keep the demo one command.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .checks import Finding, LoadEvent

PATH = Path(".obs_offline.json")


def _read() -> dict:
    if PATH.exists():
        return json.loads(PATH.read_text())
    return {"load_events": [], "check_results": []}


def _write(data: dict) -> None:
    PATH.write_text(json.dumps(data, default=str, indent=1))


def client():
    return "offline"


def migrate(_ch=None) -> None:
    if not PATH.exists():
        _write(_read())


def insert_events(_ch, events: list[LoadEvent]) -> int:
    data = _read()
    data["load_events"].extend(
        {"source": e.source, "run_id": e.run_id, "loaded_at": e.loaded_at.isoformat(),
         "rows": e.rows, "status": e.status} for e in events
    )
    _write(data)
    return len(events)


def fetch_events(_ch, lookback_days: int = 21) -> dict[str, list[LoadEvent]]:
    out: dict[str, list[LoadEvent]] = {}
    for r in _read()["load_events"]:
        ts = datetime.fromisoformat(r["loaded_at"])
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        out.setdefault(r["source"], []).append(
            LoadEvent(r["source"], r["run_id"], ts, int(r["rows"]), r["status"])
        )
    for events in out.values():
        events.sort(key=lambda e: e.loaded_at)
    return out


def observed_sources(_ch) -> set[str]:
    return {r["source"] for r in _read()["load_events"]}


def record_results(_ch, findings: list[Finding], checked_at: datetime | None = None) -> None:
    checked_at = checked_at or datetime.now(timezone.utc)
    data = _read()
    for f in findings:
        row = f.to_row()
        row["checked_at"] = checked_at.isoformat()
        data["check_results"].append(row)
    _write(data)
