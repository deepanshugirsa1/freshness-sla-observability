"""obs — freshness and SLA observability.

    python -m obs.cli migrate          create the ClickHouse schema

Add --offline to any command to run without a ClickHouse server.

    python -m obs.cli migrate          create the ClickHouse schema
    python -m obs.cli seed             load 21 days of demo history (with faults)
    python -m obs.cli check            evaluate every source, record, alert
    python -m obs.cli ingest-jira      pull Jira issues, changelog and worklogs
    python -m obs.cli registry-audit   fail if a landing source has no owner
    python -m obs.cli report           print the current state table
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from . import alerts, checks, registry as reg, seed as seeder
from .sources import jira
from . import store as ch_store
from . import store_memory


def backend(args):
    """ClickHouse by default; --offline swaps in the file-backed stand-in."""
    return store_memory if getattr(args, "offline", False) else ch_store

EXIT_ALERT = 1
EXIT_UNOWNED = 2


def _load(path):
    return reg.load(path)


def cmd_migrate(args):
    store = backend(args)
    ch = store.client()
    store.migrate(ch)
    print("schema ready: obs.load_events, obs.check_results, obs.incidents")


def cmd_seed(args):
    registry = _load(args.registry)
    store = backend(args)
    ch = store.client()
    store.migrate(ch)
    events = seeder.generate(registry, days=args.days)
    n = store.insert_events(ch, events)
    print(f"inserted {n} load events for {len(registry)} sources")


def cmd_check(args):
    registry = _load(args.registry)
    store = backend(args)
    ch = store.client()
    events = store.fetch_events(ch)
    now = datetime.now(timezone.utc)
    findings = checks.evaluate_all(registry, events, now=now)
    store.record_results(ch, findings, checked_at=now)
    raised = alerts.dispatch(findings)
    ok = sum(1 for f in findings if not f.is_alert)
    print(f"\n{ok}/{len(findings)} sources healthy, {len(raised)} alert(s)")
    if args.fail_on_alert and raised:
        return EXIT_ALERT
    return 0


def cmd_ingest_jira(args):
    """Land one incremental Jira batch, then let the usual checks judge it.

    Credentials come from JIRA_BASE_URL, JIRA_EMAIL and JIRA_API_TOKEN. Without
    them the connector reads fixtures/jira_issues.json, so the path runs in CI.
    """
    store = backend(args)
    ch = store.client()
    store.migrate(ch)
    events, reports = jira.ingest(project=args.project)
    store.insert_events(ch, events)
    for report in reports:
        print(report.summary())
    for event in events:
        print(f"landed {event.rows:>6} rows  {event.source}")
    return 0


def cmd_registry_audit(args):
    registry = _load(args.registry)
    store = backend(args)
    ch = store.client()
    unowned = reg.audit(registry, store.observed_sources(ch))
    if unowned:
        print("sources landing with no owner or freshness target:")
        for name in unowned:
            print(f"  - {name}")
        return EXIT_UNOWNED
    print(f"all {len(registry)} monitored sources have an owner, a target and an alert")
    return 0


def cmd_report(args):
    registry = _load(args.registry)
    store = backend(args)
    ch = store.client()
    findings = checks.evaluate_all(registry, store.fetch_events(ch))
    width = max(len(f.source) for f in findings)
    for f in findings:
        print(f"{f.state.upper():<8} {f.source:<{width}}  {f.severity:<8} "
              f"{f.owner:<11} {f.detail}")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(prog="obs", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--registry", default="registry/sources.yml")
    p.add_argument("--offline", action="store_true",
                   help="use the file-backed store instead of ClickHouse")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate").set_defaults(fn=cmd_migrate)
    s = sub.add_parser("seed"); s.add_argument("--days", type=int, default=21)
    s.set_defaults(fn=cmd_seed)
    c = sub.add_parser("check"); c.add_argument("--fail-on-alert", action="store_true")
    c.set_defaults(fn=cmd_check)
    j = sub.add_parser("ingest-jira"); j.add_argument("--project", default="DATA")
    j.set_defaults(fn=cmd_ingest_jira)
    sub.add_parser("registry-audit").set_defaults(fn=cmd_registry_audit)
    sub.add_parser("report").set_defaults(fn=cmd_report)
    args = p.parse_args(argv)
    return args.fn(args) or 0


if __name__ == "__main__":
    sys.exit(main())
