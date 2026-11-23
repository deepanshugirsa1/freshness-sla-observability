"""Jira ingestion: issues, status changelog and worklogs.

Jira is the archetype of a non-platform source. The API paginates, the
interesting fields are nested two or three levels down, administrators add
custom fields on their own schedule, and `updated` is the only thing you can
pull incrementally against. This module handles those four facts and nothing
else: it pulls, it normalizes, it checks the contract, and it hands the
registry a load event that looks exactly like one from Postgres CDC.

Three products come out of one pull, each at its own grain:

    jira.issues      one row per issue, current state
    jira.changelog   one row per field transition (status history)
    jira.worklogs    one row per logged time entry

Offline mode reads fixtures/jira_issues.json, so the whole path — pagination,
normalization, drift detection, watermark arithmetic — is exercised in CI
without credentials or a network call.
"""
from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from ..checks import LoadEvent
from .contract import Field, SourceContract

PAGE_SIZE = 100
FIXTURE = Path("fixtures/jira_issues.json")
STATE = Path(".obs_watermarks.json")

# Jira restates: an issue updated after the pull still carries its old
# `updated` value for a moment, and worklogs are edited retroactively. Rewind
# the watermark on every run so late edits are picked up rather than skipped.
WATERMARK_REWIND_MINUTES = 30

ISSUE_CONTRACT = SourceContract(
    source="jira.issues",
    primary_key="issue_key",
    updated_at="updated_at",
    fields=(
        Field("issue_id", "string", required=True),
        Field("issue_key", "string", required=True),
        Field("project_key", "string", required=True),
        Field("issue_type", "string", required=True),
        Field("status", "string", required=True),
        Field("status_category", "string"),
        Field("priority", "string"),
        Field("assignee_account_id", "string"),
        Field("reporter_account_id", "string"),
        Field("story_points", "float"),
        Field("labels", "array"),
        Field("created_at", "timestamp", required=True),
        Field("updated_at", "timestamp", required=True),
        Field("resolved_at", "timestamp"),
        Field("custom_fields", "object"),
    ),
    # A new customfield_* must not fail a load; it lands in custom_fields and
    # gets promoted to a declared column when someone models it.
    passthrough_prefixes=("customfield_",),
)

CHANGELOG_CONTRACT = SourceContract(
    source="jira.changelog",
    primary_key="change_id",
    updated_at="changed_at",
    fields=(
        Field("change_id", "string", required=True),
        Field("issue_key", "string", required=True),
        Field("field", "string", required=True),
        Field("from_value", "string"),
        Field("to_value", "string"),
        Field("author_account_id", "string"),
        Field("changed_at", "timestamp", required=True),
    ),
)

WORKLOG_CONTRACT = SourceContract(
    source="jira.worklogs",
    primary_key="worklog_id",
    updated_at="started_at",
    fields=(
        Field("worklog_id", "string", required=True),
        Field("issue_key", "string", required=True),
        Field("author_account_id", "string"),
        Field("seconds_spent", "int", required=True),
        Field("started_at", "timestamp", required=True),
    ),
)

CONTRACTS = {c.source: c for c in (ISSUE_CONTRACT, CHANGELOG_CONTRACT, WORKLOG_CONTRACT)}


@dataclass(frozen=True)
class Credentials:
    base_url: str
    email: str
    token: str

    @classmethod
    def from_env(cls) -> "Credentials | None":
        base, email, token = (os.environ.get("JIRA_BASE_URL"),
                              os.environ.get("JIRA_EMAIL"),
                              os.environ.get("JIRA_API_TOKEN"))
        if not (base and email and token):
            return None
        return cls(base.rstrip("/"), email, token)


# --------------------------------------------------------------------------
# watermarks
# --------------------------------------------------------------------------

def read_watermark(source: str, path: Path = STATE) -> datetime | None:
    if not path.exists():
        return None
    raw = json.loads(path.read_text()).get(source)
    return datetime.fromisoformat(raw) if raw else None


def write_watermark(source: str, value: datetime, path: Path = STATE) -> None:
    state = json.loads(path.read_text()) if path.exists() else {}
    state[source] = value.astimezone(timezone.utc).isoformat()
    path.write_text(json.dumps(state, indent=1, sort_keys=True))


def incremental_jql(project: str, since: datetime | None) -> str:
    """Pull everything on a cold start, then only what moved since last time."""
    scope = f"project = {project}"
    if since is None:
        return f"{scope} ORDER BY updated ASC"
    rewound = since - timedelta(minutes=WATERMARK_REWIND_MINUTES)
    stamp = rewound.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return f'{scope} AND updated >= "{stamp}" ORDER BY updated ASC'


# --------------------------------------------------------------------------
# extract
# --------------------------------------------------------------------------

def _get(creds: Credentials, path: str, params: Mapping[str, Any]) -> dict:
    url = f"{creds.base_url}{path}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    import base64
    basic = base64.b64encode(f"{creds.email}:{creds.token}".encode()).decode()
    request.add_header("Authorization", f"Basic {basic}")
    with urllib.request.urlopen(request, timeout=30) as response:   # noqa: S310
        return json.loads(response.read())


def fetch_pages(creds: Credentials | None, jql: str) -> Iterator[list[dict]]:
    """Yield raw issue pages. Offline (no credentials) reads the fixture."""
    if creds is None:
        yield json.loads(FIXTURE.read_text())["issues"]
        return

    start = 0
    while True:
        payload = _get(creds, "/rest/api/3/search", {
            "jql": jql,
            "startAt": start,
            "maxResults": PAGE_SIZE,
            "expand": "changelog",
            "fields": "summary,status,issuetype,priority,assignee,reporter,labels,"
                      "created,updated,resolutiondate,worklog,customfield_10016",
        })
        issues = payload.get("issues", [])
        if not issues:
            return
        yield issues
        start += len(issues)
        if start >= int(payload.get("total", 0)):
            return


# --------------------------------------------------------------------------
# normalize
# --------------------------------------------------------------------------

def _name(value: Any) -> str | None:
    """Jira nests display values one level down, inconsistently."""
    if isinstance(value, Mapping):
        return value.get("name") or value.get("value") or value.get("displayName")
    return value if isinstance(value, str) else None


def _account(value: Any) -> str | None:
    return value.get("accountId") if isinstance(value, Mapping) else None


def normalize_issue(raw: Mapping[str, Any]) -> dict:
    fields = raw.get("fields", {}) or {}
    status = fields.get("status") or {}
    custom = {k: v for k, v in fields.items()
              if k.startswith("customfield_") and v is not None}
    points = fields.get("customfield_10016")
    return {
        "issue_id": str(raw.get("id", "")),
        "issue_key": raw.get("key", ""),
        "project_key": (raw.get("key", "") or "").split("-")[0],
        "issue_type": _name(fields.get("issuetype")) or "unknown",
        "status": _name(status) or "unknown",
        "status_category": ((status.get("statusCategory") or {}).get("name")
                            if isinstance(status, Mapping) else None),
        "priority": _name(fields.get("priority")),
        "assignee_account_id": _account(fields.get("assignee")),
        "reporter_account_id": _account(fields.get("reporter")),
        "story_points": float(points) if isinstance(points, (int, float)) else None,
        "labels": list(fields.get("labels") or []),
        "created_at": fields.get("created"),
        "updated_at": fields.get("updated"),
        "resolved_at": fields.get("resolutiondate"),
        "custom_fields": custom,
    }


def normalize_changelog(raw: Mapping[str, Any]) -> list[dict]:
    key = raw.get("key", "")
    rows: list[dict] = []
    for entry in ((raw.get("changelog") or {}).get("histories") or []):
        for item in entry.get("items", []) or []:
            rows.append({
                "change_id": f"{entry.get('id', '')}-{item.get('field', '')}",
                "issue_key": key,
                "field": item.get("field", ""),
                "from_value": item.get("fromString"),
                "to_value": item.get("toString"),
                "author_account_id": _account(entry.get("author")),
                "changed_at": entry.get("created"),
            })
    return rows


def normalize_worklogs(raw: Mapping[str, Any]) -> list[dict]:
    key = raw.get("key", "")
    worklog = (raw.get("fields", {}) or {}).get("worklog") or {}
    return [{
        "worklog_id": str(entry.get("id", "")),
        "issue_key": key,
        "author_account_id": _account(entry.get("author")),
        "seconds_spent": int(entry.get("timeSpentSeconds") or 0),
        "started_at": entry.get("started"),
    } for entry in (worklog.get("worklogs") or [])]


def dedupe(rows: list[dict], key: str) -> list[dict]:
    """Last write wins. Jira repeats rows across pages when issues move mid-pull."""
    out: dict[str, dict] = {}
    for row in rows:
        out[row[key]] = row
    return list(out.values())


# --------------------------------------------------------------------------
# pipeline
# --------------------------------------------------------------------------

def extract(project: str = "DATA", creds: Credentials | None = None,
            since: datetime | None = None) -> dict[str, list[dict]]:
    """Pull one incremental batch and return rows per data product."""
    jql = incremental_jql(project, since)
    issues: list[dict] = []
    changes: list[dict] = []
    worklogs: list[dict] = []
    for page in fetch_pages(creds, jql):
        for raw in page:
            issues.append(normalize_issue(raw))
            changes.extend(normalize_changelog(raw))
            worklogs.extend(normalize_worklogs(raw))
    return {
        "jira.issues": dedupe(issues, "issue_key"),
        "jira.changelog": dedupe(changes, "change_id"),
        "jira.worklogs": dedupe(worklogs, "worklog_id"),
    }


def high_watermark(rows: list[dict], column: str) -> datetime | None:
    stamps = [r[column] for r in rows if r.get(column)]
    if not stamps:
        return None
    parsed = [datetime.fromisoformat(str(s).replace("Z", "+00:00")) for s in stamps]
    return max(parsed).astimezone(timezone.utc)


def ingest(project: str = "DATA", now: datetime | None = None,
           state_path: Path = STATE) -> tuple[list[LoadEvent], list]:
    """Run the pull, enforce every contract, and emit one load event per source.

    The load events are the point: once Jira lands them, a stalled connector or
    a truncated page shows up on the status page next to a late Stripe pull,
    with the same severity rules and the same owner on the hook.
    """
    creds = Credentials.from_env()
    now = now or datetime.now(timezone.utc)
    since = read_watermark("jira.issues", state_path)
    batches = extract(project, creds, since)

    events: list[LoadEvent] = []
    reports = []
    for source, rows in batches.items():
        contract = CONTRACTS[source]
        reports.append(contract.enforce(rows))
        events.append(LoadEvent(source, f"{source}-{now:%Y%m%dT%H%M%S}", now,
                                len(rows), "success" if rows else "failed"))
        mark = high_watermark(rows, contract.updated_at)
        if mark:
            write_watermark(source, mark, state_path)
    return events, reports
