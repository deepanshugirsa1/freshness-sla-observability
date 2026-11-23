"""Generate 21 days of plausible load history, with faults planted on purpose."""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from .checks import LoadEvent
from .registry import Registry

BASE_ROWS = {
    "app_db.users": 2_400, "app_db.devices": 1_900,
    "stripe.charges": 8_600, "stripe.subscriptions": 3_100,
    "shopify.orders": 5_400, "amazon.settlements": 12_000,
    "app_store.sales_reports": 9_800, "ad_platform.spend": 640,
    "ai.transcription_quality": 14_000, "ai.summary_quality": 13_200,
    "ai.speaker_mapping_accuracy": 7_500, "ai.ask_pocket_answers": 4_300,
    # Matched to fixtures/jira_issues.json so a seeded history and a real
    # offline ingest agree; a truncated page then shows as genuinely partial.
    "jira.issues": 18, "jira.changelog": 25, "jira.worklogs": 8,
}

# Planted faults, so a fresh clone shows a status page with real findings.
FAULTS = {
    "app_store.sales_reports": "late",       # report landed well past target
    "shopify.orders": "partial",             # paginated pull truncated
    "ai.transcription_quality": "missing",   # eval job stopped running
    "stripe.charges": "anomaly",             # volume spike, nothing else wrong
    "ad_platform.spend": "failed",           # API 429 after the last success
}


def generate(registry: Registry, now: datetime | None = None, days: int = 21,
             seed: int = 7) -> list[LoadEvent]:
    rng = random.Random(seed)
    now = now or datetime.now(timezone.utc)
    events: list[LoadEvent] = []

    for src in registry:
        base = BASE_ROWS.get(src.name, 1_000)
        step = timedelta(minutes=max(src.cadence_minutes, 60))
        fault = FAULTS.get(src.name)
        t = now - timedelta(days=days)
        i = 0
        while t <= now:
            i += 1
            rows = int(base * rng.uniform(0.92, 1.08))
            events.append(LoadEvent(src.name, f"{src.name}-{i:05d}", t, rows, "success"))
            t += step

        if not events:
            continue
        last = [e for e in events if e.source == src.name][-1]

        if fault == "late":
            # drop the runs that would have arrived on time
            cutoff = now - timedelta(minutes=src.deadline_minutes + 180)
            events = [e for e in events
                      if not (e.source == src.name and e.loaded_at > cutoff)]
        elif fault == "partial":
            last.rows = int(base * 0.31)
        elif fault == "missing":
            events = [e for e in events
                      if not (e.source == src.name
                              and e.loaded_at > now - timedelta(
                                  minutes=src.deadline_minutes * 4))]
        elif fault == "anomaly":
            last.rows = int(base * 2.9)
        elif fault == "failed":
            events.append(LoadEvent(src.name, f"{src.name}-fail",
                                    last.loaded_at + timedelta(minutes=30), 0, "failed"))
    return events
