"""Pure check logic: load history in, findings out. No I/O, so it is testable."""
from __future__ import annotations

import statistics
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone

from .registry import Source

OK = "ok"
LATE = "late"
MISSING = "missing"
PARTIAL = "partial"
ANOMALY = "anomaly"
FAILED = "failed"

# A source with no load at all for this many deadlines is missing, not merely late.
MISSING_DEADLINE_MULTIPLIER = 3


@dataclass
class LoadEvent:
    source: str
    run_id: str
    loaded_at: datetime
    rows: int
    status: str = "success"   # success | failed


@dataclass
class Finding:
    source: str
    state: str
    severity: str
    owner: str
    detail: str
    observed_rows: int | None = None
    expected_rows: float | None = None
    minutes_late: int | None = None
    last_success_at: datetime | None = None
    downstream: tuple[str, ...] = ()

    def to_row(self) -> dict:
        d = asdict(self)
        d["downstream"] = list(self.downstream)
        return d

    @property
    def is_alert(self) -> bool:
        return self.state != OK


def _median(values: list[int]) -> float | None:
    return statistics.median(values) if values else None


def _mad_score(value: int, history: list[int]) -> float | None:
    """Robust z-score. Median absolute deviation survives one bad day; stdev does not."""
    if len(history) < 5:
        return None
    med = statistics.median(history)
    mad = statistics.median([abs(h - med) for h in history])
    if mad == 0:
        return 0.0 if value == med else float("inf")
    return abs(value - med) / (1.4826 * mad)


def evaluate_source(
    source: Source,
    events: list[LoadEvent],
    now: datetime | None = None,
    baseline_window: int = 14,
) -> Finding:
    """Decide one source's state. Order matters: missing > failed > late > partial > anomaly."""
    now = now or datetime.now(timezone.utc)
    base = dict(source=source.name, severity=source.severity, owner=source.owner,
                downstream=source.downstream)

    successes = sorted([e for e in events if e.status == "success"], key=lambda e: e.loaded_at)
    if not successes:
        return Finding(**base, state=MISSING, detail="no successful load on record")

    last = successes[-1]
    age_minutes = int((now - last.loaded_at).total_seconds() // 60)
    minutes_late = age_minutes - source.deadline_minutes

    if age_minutes > source.deadline_minutes * MISSING_DEADLINE_MULTIPLIER:
        return Finding(**base, state=MISSING, minutes_late=minutes_late,
                       last_success_at=last.loaded_at,
                       detail=f"no load for {age_minutes}m against a "
                              f"{source.freshness_target_minutes}m target")

    failed_since = [e for e in events if e.status == "failed" and e.loaded_at > last.loaded_at]
    if failed_since:
        return Finding(**base, state=FAILED, last_success_at=last.loaded_at,
                       detail=f"{len(failed_since)} failed run(s) since the last success")

    if minutes_late > 0:
        return Finding(**base, state=LATE, minutes_late=minutes_late,
                       last_success_at=last.loaded_at,
                       detail=f"{minutes_late}m past its {source.freshness_target_minutes}m "
                              f"freshness target (+{source.grace_minutes}m grace)")

    cutoff = now - timedelta(days=baseline_window)
    history = [e.rows for e in successes[:-1] if e.loaded_at >= cutoff]
    expected = _median(history)

    if expected and last.rows < expected * source.partial_load_ratio:
        return Finding(**base, state=PARTIAL, observed_rows=last.rows, expected_rows=expected,
                       last_success_at=last.loaded_at,
                       detail=f"loaded {last.rows} rows against a {expected:.0f}-row median "
                              f"({last.rows / expected:.0%} of normal)")

    score = _mad_score(last.rows, history)
    if score is not None and score > source.anomaly_mad_threshold:
        return Finding(**base, state=ANOMALY, observed_rows=last.rows, expected_rows=expected,
                       last_success_at=last.loaded_at,
                       detail=f"row count {last.rows} is {score:.1f} MAD from the "
                              f"{baseline_window}-day median")

    return Finding(**base, state=OK, observed_rows=last.rows, expected_rows=expected,
                   last_success_at=last.loaded_at, detail="fresh and within expected volume")


def evaluate_all(sources, events_by_source: dict[str, list[LoadEvent]],
                 now: datetime | None = None) -> list[Finding]:
    now = now or datetime.now(timezone.utc)
    findings = [evaluate_source(s, events_by_source.get(s.name, []), now=now) for s in sources]
    rank = {MISSING: 0, FAILED: 1, LATE: 2, PARTIAL: 3, ANOMALY: 4, OK: 5}
    sev = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    return sorted(findings, key=lambda f: (rank[f.state], sev.get(f.severity, 9), f.source))
