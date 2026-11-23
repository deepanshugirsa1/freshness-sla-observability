"""Jira connector: normalization, contract drift, incremental watermarks."""
from datetime import datetime, timedelta, timezone

import pytest

from obs.sources import jira
from obs.sources.contract import ContractViolation, Field, SourceContract

NOW = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)


# --- normalization ---------------------------------------------------------

def test_extract_offline_returns_all_three_products():
    batches = jira.extract(project="DATA")
    assert set(batches) == {"jira.issues", "jira.changelog", "jira.worklogs"}
    assert len(batches["jira.issues"]) == 18
    assert batches["jira.changelog"], "status history should normalize to rows"


def test_issue_flattens_nested_jira_fields():
    issue = next(i for i in jira.extract()["jira.issues"] if i["issue_key"] == "DATA-101")
    assert issue["project_key"] == "DATA"
    assert isinstance(issue["status"], str) and issue["status"]
    assert issue["status_category"] in {"To Do", "In Progress", "Done"}
    assert isinstance(issue["labels"], list)


def test_changelog_row_per_field_transition():
    rows = jira.extract()["jira.changelog"]
    assert all(r["field"] and r["issue_key"] and r["changed_at"] for r in rows)
    assert all(r["change_id"].endswith(r["field"]) for r in rows)


def test_worklog_seconds_are_integers():
    rows = jira.extract()["jira.worklogs"]
    assert all(isinstance(r["seconds_spent"], int) for r in rows)


def test_dedupe_keeps_last_write():
    rows = [{"issue_key": "DATA-1", "status": "To Do"},
            {"issue_key": "DATA-1", "status": "Done"}]
    assert jira.dedupe(rows, "issue_key") == [{"issue_key": "DATA-1", "status": "Done"}]


# --- contracts and drift --------------------------------------------------

def test_fixture_satisfies_every_contract():
    for source, rows in jira.extract().items():
        report = jira.CONTRACTS[source].enforce(rows)
        assert not report.fatal(jira.CONTRACTS[source])


def test_unmodelled_custom_field_is_not_drift():
    """An admin adding customfield_11020 must not fail the load."""
    rows = jira.extract()["jira.issues"]
    report = jira.ISSUE_CONTRACT.inspect(rows)
    assert report.added == [], report.summary()
    assert any("customfield_11020" in r["custom_fields"] for r in rows)


def test_undeclared_plain_field_is_reported_but_not_fatal():
    rows = [dict(r) for r in jira.extract()["jira.issues"]]
    rows[0]["sprint_name"] = "Sprint 14"
    report = jira.ISSUE_CONTRACT.inspect(rows)
    assert report.added == ["sprint_name"]
    assert not report.fatal(jira.ISSUE_CONTRACT)


def test_missing_required_field_is_fatal():
    rows = [{k: v for k, v in r.items() if k != "issue_key"}
            for r in jira.extract()["jira.issues"]]
    with pytest.raises(ContractViolation):
        jira.ISSUE_CONTRACT.enforce(rows)


def test_retyped_field_is_fatal_even_when_optional():
    rows = [dict(r) for r in jira.extract()["jira.issues"]]
    rows[0]["story_points"] = "eight"          # number became a string upstream
    with pytest.raises(ContractViolation):
        jira.ISSUE_CONTRACT.enforce(rows)


def test_bool_does_not_satisfy_an_int_field():
    contract = SourceContract("t", (Field("n", "int", required=True),), "n", "n")
    assert contract.inspect([{"n": True}]).retyped == ["n"]


# --- incremental pulls ----------------------------------------------------

def test_cold_start_pulls_everything():
    assert jira.incremental_jql("DATA", None) == "project = DATA ORDER BY updated ASC"


def test_warm_start_rewinds_the_watermark_for_retroactive_edits():
    jql = jira.incremental_jql("DATA", NOW)
    expected = (NOW - timedelta(minutes=jira.WATERMARK_REWIND_MINUTES)).strftime("%Y-%m-%d %H:%M")
    assert f'updated >= "{expected}"' in jql


def test_high_watermark_is_the_newest_timestamp():
    rows = jira.extract()["jira.issues"]
    mark = jira.high_watermark(rows, "updated_at")
    assert mark == max(
        datetime.fromisoformat(r["updated_at"].replace("Z", "+00:00")).astimezone(timezone.utc)
        for r in rows
    )


def test_ingest_emits_one_load_event_per_source(tmp_path):
    events, reports = jira.ingest(now=NOW, state_path=tmp_path / "wm.json")
    assert {e.source for e in events} == set(jira.CONTRACTS)
    assert all(e.loaded_at == NOW and e.rows > 0 for e in events)
    assert all(r.clean for r in reports)


def test_ingest_advances_the_watermark(tmp_path):
    state = tmp_path / "wm.json"
    jira.ingest(now=NOW, state_path=state)
    first = jira.read_watermark("jira.issues", state)
    assert first is not None
    jira.ingest(now=NOW, state_path=state)
    assert jira.read_watermark("jira.issues", state) == first


def test_empty_pull_lands_a_failed_event(tmp_path, monkeypatch):
    monkeypatch.setattr(jira, "extract", lambda *a, **k: {s: [] for s in jira.CONTRACTS})
    events, _ = jira.ingest(now=NOW, state_path=tmp_path / "wm.json")
    assert all(e.status == "failed" and e.rows == 0 for e in events)
