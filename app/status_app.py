"""Status page: every source, its owner, its state, and what it blocks."""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from obs import checks, registry as reg

# OBS_BACKEND=offline renders from the file-backed store, so the page runs
# without a ClickHouse server; anything else uses ClickHouse.
if os.getenv("OBS_BACKEND", "clickhouse") == "offline":
    from obs import store_memory as store
else:
    from obs import store

STATE_COLOR = {
    "missing": "#e5484d", "failed": "#e5484d", "late": "#f5a524",
    "partial": "#f5a524", "anomaly": "#a78bfa", "ok": "#30a46c",
}

st.set_page_config(page_title="Source freshness & SLA status", layout="centered")
st.title("Source freshness & SLA status")
st.caption(
    "Every source has an owner, a freshness target and an alert. "
    "States, worst first: missing · failed · late · partial · anomaly."
)


def ago(ts: datetime | None) -> str:
    if ts is None:
        return "—"
    minutes = int((datetime.now(timezone.utc) - ts).total_seconds() // 60)
    if minutes < 60:
        return f"{minutes}m ago"
    if minutes < 1440:
        return f"{minutes // 60}h {minutes % 60}m ago"
    return f"{minutes // 1440}d {(minutes % 1440) // 60}h ago"


@st.cache_data(ttl=60)
def load_findings() -> pd.DataFrame:
    findings = checks.evaluate_all(reg.load(), store.fetch_events(store.client()))
    return pd.DataFrame([
        {
            "source": f.source, "state": f.state, "severity": f.severity,
            "owner": f.owner, "last success": ago(f.last_success_at),
            "past target": f"{f.minutes_late}m" if f.minutes_late and f.minutes_late > 0 else "",
            "rows": "" if f.observed_rows is None else f"{f.observed_rows:,}",
            "expected": "" if f.expected_rows is None else f"{f.expected_rows:,.0f}",
            "detail": f.detail, "blocks": ", ".join(f.downstream),
        }
        for f in findings
    ])


df = load_findings()
breached = df[df.state != "ok"]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Sources monitored", len(df))
c2.metric("Healthy", int((df.state == "ok").sum()))
c3.metric("Breaching SLA", len(breached))
c4.metric("Critical breaching",
          int(((df.state != "ok") & (df.severity == "critical")).sum()))

if len(breached):
    st.subheader("Breaching now")
    for _, r in breached.iterrows():
        color = STATE_COLOR.get(r.state, "#8b8b8b")
        st.markdown(
            f"<div style='border-left:4px solid {color};padding:.45rem .9rem;"
            f"margin:.35rem 0;background:rgba(255,255,255,.025);border-radius:3px;'>"
            f"<b>{r.source}</b>&nbsp;&nbsp;"
            f"<span style='background:{color};color:#0b0b0b;font-weight:600;"
            f"padding:1px 7px;border-radius:3px;font-size:.78em;"
            f"letter-spacing:.03em;'>{r.state.upper()}</span>&nbsp;&nbsp;"
            f"<span style='opacity:.6'>{r.severity} · owner {r.owner} · "
            f"last success {r['last success']}</span><br>"
            f"{r.detail}<br>"
            f"<span style='opacity:.6'>blocks: {r.blocks or '—'}</span></div>",
            unsafe_allow_html=True,
        )
else:
    st.success("Every source is within its freshness target.")

st.subheader("All sources")
st.dataframe(
    df, use_container_width=True, hide_index=True,
    column_config={
        "detail": st.column_config.TextColumn("detail", width="large"),
        "blocks": st.column_config.TextColumn("blocks (downstream)", width="medium"),
    },
)
