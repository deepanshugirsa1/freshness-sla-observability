"""Severity-tiered alert routing. Console by default, webhook when one is set."""
from __future__ import annotations

import json
import os
import urllib.request

from .checks import ANOMALY, Finding, LATE, MISSING, PARTIAL, FAILED

# How loud a given state is for a given source severity.
ROUTING = {
    ("critical", MISSING): "page",
    ("critical", FAILED): "page",
    ("critical", LATE): "page",
    ("critical", PARTIAL): "page",
    ("critical", ANOMALY): "notify",
    ("high", MISSING): "page",
    ("high", FAILED): "notify",
    ("high", LATE): "notify",
    ("high", PARTIAL): "notify",
    ("high", ANOMALY): "ticket",
    ("medium", MISSING): "notify",
}
DEFAULT_CHANNEL = "ticket"
ICON = {MISSING: "[MISSING]", FAILED: "[FAILED] ", LATE: "[LATE]   ",
        PARTIAL: "[PARTIAL]", ANOMALY: "[ANOMALY]"}


def channel_for(finding: Finding) -> str:
    return ROUTING.get((finding.severity, finding.state), DEFAULT_CHANNEL)


def format_alert(finding: Finding) -> str:
    down = ", ".join(finding.downstream[:3]) or "no declared dependents"
    return (
        f"{ICON.get(finding.state, '[?]')} {finding.source} ({finding.severity}) "
        f"owner={finding.owner} -> {channel_for(finding)}\n"
        f"    {finding.detail}\n"
        f"    blocks: {down}"
    )


def dispatch(findings: list[Finding]) -> list[str]:
    """Return the alerts raised. Paging alerts also go to the webhook, if configured."""
    raised = [f for f in findings if f.is_alert]
    lines = [format_alert(f) for f in raised]
    for line in lines:
        print(line)

    url = os.getenv("ALERT_WEBHOOK_URL")
    paging = [f for f in raised if channel_for(f) == "page"]
    if url and paging:
        body = json.dumps({"text": "\n".join(format_alert(f) for f in paging)}).encode()
        req = urllib.request.Request(url, data=body,
                                     headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=10)
        except Exception as exc:  # an alerting failure must not mask the finding
            print(f"[warn] webhook delivery failed: {exc}")
    return lines
