from datetime import datetime, timedelta, timezone

import pytest

from obs import registry as reg
from obs.checks import (ANOMALY, FAILED, LATE, MISSING, OK, PARTIAL,
                        LoadEvent, evaluate_all, evaluate_source)

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)


def src(**kw):
    base = dict(name="stripe.charges", domain="revenue", owner="data-eng",
                system="stripe_api", cadence="1h", freshness_target_minutes=180,
                severity="critical", downstream=("fct_payment",))
    base.update(kw)
    return reg.Source(**base)


def history(n=20, rows=1000, step_minutes=60, end=NOW, status="success"):
    """Oldest first, newest exactly at `end`."""
    return [LoadEvent("stripe.charges", f"r{i}", end - timedelta(minutes=step_minutes * i),
                      rows, status) for i in range(n - 1, -1, -1)]


def test_fresh_source_is_ok():
    f = evaluate_source(src(), history(), now=NOW)
    assert f.state == OK and not f.is_alert


def test_no_events_is_missing():
    assert evaluate_source(src(), [], now=NOW).state == MISSING


def test_past_target_plus_grace_is_late():
    # 180m target + 15m grace = 195m deadline; last load landed 200m ago
    ev = history(end=NOW - timedelta(minutes=200))
    assert evaluate_source(src(), ev, now=NOW).state == LATE


def test_inside_grace_is_still_ok():
    ev = history(end=NOW - timedelta(minutes=190))
    assert evaluate_source(src(), ev, now=NOW).state == OK


def test_long_silence_is_missing_not_late():
    ev = history(end=NOW - timedelta(minutes=195 * 4))
    assert evaluate_source(src(), ev, now=NOW).state == MISSING


def test_truncated_load_is_partial():
    ev = history(rows=1000)
    ev[-1].rows = 250
    f = evaluate_source(src(), ev, now=NOW)
    assert f.state == PARTIAL and f.expected_rows == 1000


def test_volume_spike_is_anomaly_not_partial():
    ev = history(rows=1000)
    for i, e in enumerate(ev):      # a little natural jitter so MAD is non-zero
        e.rows = 1000 + (i % 5) * 7
    ev[-1].rows = 9000
    assert evaluate_source(src(), ev, now=NOW).state == ANOMALY


def test_failed_run_after_last_success():
    ev = history(end=NOW - timedelta(minutes=30))
    ev.append(LoadEvent("stripe.charges", "boom", NOW - timedelta(minutes=5), 0, "failed"))
    assert evaluate_source(src(), ev, now=NOW).state == FAILED


def test_missing_outranks_partial():
    """A source that stopped loading must not be reported as merely thin."""
    ev = history(rows=1000, end=NOW - timedelta(minutes=195 * 5))
    ev[-1].rows = 10
    assert evaluate_source(src(), ev, now=NOW).state == MISSING


def test_restating_source_tolerates_its_long_target():
    s = src(name="app_store.sales_reports", cadence="1d",
            freshness_target_minutes=2160, severity="high")
    ev = [LoadEvent(s.name, f"r{i}", NOW - timedelta(days=i), 9000, "success")
          for i in range(20, 0, -1)]
    assert evaluate_source(s, ev, now=NOW).state == OK


def test_findings_sorted_worst_first():
    s_ok, s_bad = src(name="a", severity="high"), src(name="b", severity="critical")
    events = {"a": history(), "b": []}
    order = [f.state for f in evaluate_all([s_ok, s_bad], events, now=NOW)]
    assert order[0] == MISSING


def test_registry_rejects_target_tighter_than_cadence():
    with pytest.raises(ValueError, match="never pass"):
        reg.validate([src(cadence="1d", freshness_target_minutes=60)])


def test_registry_requires_owner():
    with pytest.raises(ValueError, match="owner"):
        reg.validate([src(owner="")])


def test_registry_audit_flags_unowned_source():
    r = reg.Registry([src()])
    assert reg.audit(r, {"stripe.charges", "shopify.orders"}) == ["shopify.orders"]


def test_real_registry_loads_and_validates():
    r = reg.load("registry/sources.yml")
    assert len(r) >= 10
    assert all(s.owner and s.downstream for s in r)


# --- constant-volume sources (zero MAD) -----------------------------------

def test_constant_volume_source_tolerates_small_wobble():
    """A low-volume source sitting at the same count must not page on one row."""
    events = history(n=20, rows=8)
    events[-1].rows = 9
    assert evaluate_source(src(cadence="1d", freshness_target_minutes=1440),
                           events, now=NOW).state == OK


def test_constant_volume_source_still_catches_a_collapse():
    events = history(n=20, rows=80)
    events[-1].rows = 8
    assert evaluate_source(src(cadence="1d", freshness_target_minutes=1440),
                           events, now=NOW).state in {ANOMALY, PARTIAL}
