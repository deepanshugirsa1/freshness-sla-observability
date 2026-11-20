"""Status page: every source, its owner, its state, and what it blocks."""
from __future__ import annotations

import os

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
    "missing": "#b3261e", "failed": "#b3261e", "late": "#f2a600",
    "partial": "#f2a600", "anomaly": "#6750a4", "ok": "#1b7f3b",
}

st.set_page_config(page_title="Source freshness & SLA status", layout="wide")
st.title("Source freshness & SLA status")


@st.cache_data(ttl=60)
def load_findings():
    registry = reg.load()
    ch = store.client()
    events = store.fetch_events(ch)
    findings = checks.evaluate_all(registry, events)
    rows = []
    for f in findings:
        rows.append({
            "source": f.source, "state": f.state, "severity": f.severity,
            "owner": f.owner, "last success": f.last_success_at,
            "minutes late": f.minutes_late, "rows": f.observed_rows,
            "expected": None if f.expected_rows is None else round(f.expected_rows),
            "detail": f.detail, "blocks": ", ".join(f.downstream),
        })
    return pd.DataFrame(rows)


df = load_findings()
breached = df[df.state != "ok"]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Sources monitored", len(df))
c2.metric("Healthy", int((df.state == "ok").sum()))
c3.metric("Breaching SLA", len(breached))
c4.metric("Critical breaching", int(((df.state != "ok") & (df.severity == "critical")).sum()))

if len(breached):
    st.subheader("Breaching now")
    for _, r in breached.iterrows():
        color = STATE_COLOR.get(r.state, "#444")
        st.markdown(
            f"<div style='border-left:4px solid {color};padding:.4rem .8rem;margin:.3rem 0;'>"
            f"<b>{r.source}</b> &nbsp;<code>{r.state}</code> &nbsp;"
            f"<span style='color:#666'>{r.severity} · owner {r.owner}</span><br>"
            f"{r.detail}<br><span style='color:#666'>blocks: {r.blocks or '—'}</span></div>",
            unsafe_allow_html=True,
        )
else:
    st.success("Every source is within its freshness target.")

st.subheader("All sources")
st.dataframe(df, use_container_width=True, hide_index=True)
