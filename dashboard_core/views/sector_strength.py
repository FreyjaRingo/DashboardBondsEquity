"""Halaman 1 — Sector Strength: strength meter, ranking momentum, rebase chart."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from ..sector_data import MARKET_LABELS, market_frame
from ..sector_metrics import (
    TIMEFRAMES,
    composite_scores,
    multi_timeframe_returns,
    rank_delta,
    rebase,
    relative_returns,
)
from .sector_common import (
    admin_sync_panel,
    as_of_caption,
    load_data_or_stop,
    market_selector,
    strength_meter_chart,
    style_returns_table,
)

TF_LABELS = [t[0] for t in TIMEFRAMES]


def render_sector_strength():
    st.title("Sector Strength")
    st.caption("Momentum sektoral multi-timeframe — US, Indonesia, China Onshore & Offshore/HK")

    data = load_data_or_stop()
    market = market_selector("ss_market")
    tf = st.sidebar.selectbox("Timeframe strength meter", TF_LABELS, index=2, key="ss_tf")
    mode = st.sidebar.radio(
        "Mode return", ["Relatif vs Benchmark", "Absolut"], key="ss_mode",
        help="Mode relatif (sektor − benchmark) menunjukkan kekuatan sektoral sesungguhnya.",
    )
    use_12_1 = st.sidebar.toggle("Momentum 12-1 (skip 1 bulan)", value=False, key="ss_121")
    admin_sync_panel()

    prices, bench_ric, name_map = market_frame(data, market)
    if prices.empty:
        st.info(f"Belum ada data harga untuk {MARKET_LABELS[market]}.")
        return
    as_of_caption(prices, MARKET_LABELS[market])

    rets = multi_timeframe_returns(prices)
    if mode.startswith("Relatif") and bench_ric in rets.index:
        meter = relative_returns(rets, bench_ric)[tf]
        meter_title = f"Kekuatan Sektor ({tf}, relatif vs {name_map.get(bench_ric, bench_ric)})"
    else:
        meter = rets[tf].drop(index=bench_ric, errors="ignore")
        meter_title = f"Return Sektor ({tf}, absolut)"

    # ---------- 1. STRENGTH METER ----------
    fig = strength_meter_chart(meter, name_map, meter_title)
    if fig:
        top = meter.idxmax() if meter.notna().any() else None
        bot = meter.idxmin() if meter.notna().any() else None
        c1, c2 = st.columns(2)
        if top is not None:
            c1.metric("Terkuat ▲", name_map.get(top, top), f"{meter[top] * 100:+.2f}%")
        if bot is not None:
            c2.metric("Terlemah ▼", name_map.get(bot, bot), f"{meter[bot] * 100:+.2f}%")
        st.plotly_chart(fig, use_container_width=True)

    # ---------- 2. TABEL MULTI-TIMEFRAME ----------
    st.subheader("Return Multi-Timeframe")
    table = rets.copy()
    if mode.startswith("Relatif") and bench_ric in rets.index:
        table = relative_returns(rets, bench_ric)
    else:
        table = table.drop(index=bench_ric, errors="ignore")
    table.index = [name_map.get(r, r) for r in table.index]
    table = table.sort_values(tf, ascending=False)
    st.dataframe(style_returns_table(table), use_container_width=True)

    # ---------- 3. RANKING MOMENTUM COMPOSITE ----------
    st.subheader("Ranking Momentum Composite")
    st.caption(
        "Skor = 40%·RS 1M + 30%·RS 3M + 20%·RS 6M + 10%·RS 12M (z-score antar sektor). "
        "Badge SMA: posisi harga vs SMA-50/SMA-200."
    )
    scores = composite_scores(prices, bench_ric, use_12_1=use_12_1)
    if not scores.empty:
        delta = rank_delta(prices, bench_ric)
        scores["ΔRank (1W)"] = delta.reindex(scores.index)
        disp = scores.copy()
        disp.index = [name_map.get(r, r) for r in disp.index]
        rs_cols = [c for c in disp.columns if c.startswith("RS_")]
        st.dataframe(
            disp.style.format({c: "{:+.2%}" for c in rs_cols} |
                              {"Raw_Score": "{:+.2%}", "Score": "{:+.2f}", "VolAdj_Score": "{:+.3f}"},
                              na_rep="-")
                .background_gradient(subset=["Score"], cmap="RdYlGn"),
            use_container_width=True,
        )
    else:
        st.info("Data belum cukup untuk skor komposit (butuh ≥ 1 bulan histori).")

    # ---------- 4. REBASE CHART ----------
    st.subheader("Rebase Chart (Index = 100)")
    opts = [r for r in prices.columns]
    default_sel = list(scores.index[:3]) + [bench_ric] if not scores.empty else opts[:4]
    sel = st.multiselect(
        "Pilih sektor / benchmark", opts, default=[s for s in default_sel if s in opts],
        format_func=lambda r: name_map.get(r, r), key="ss_rebase_sel",
    )
    lookback = st.select_slider(
        "Periode", options=["1M", "3M", "6M", "YTD", "1Y", "3Y", "5Y"], value="1Y", key="ss_rebase_lb"
    )
    if sel:
        days = {"1M": 21, "3M": 63, "6M": 126, "1Y": 252, "3Y": 756, "5Y": 1260}
        px = prices[sel].sort_index().ffill()
        if lookback == "YTD":
            base_date = pd.Timestamp(px.index.max().year, 1, 1)
            px = px[px.index >= base_date]
        else:
            px = px.tail(days[lookback])
        rb = rebase(px)
        fig2 = go.Figure()
        for col in rb.columns:
            fig2.add_trace(go.Scatter(
                x=rb.index, y=rb[col], mode="lines",
                name=name_map.get(col, col),
                line=dict(width=3 if col == bench_ric else 1.7,
                          dash="dash" if col == bench_ric else "solid"),
            ))
        fig2.update_layout(height=450, margin=dict(l=10, r=10, t=30, b=10),
                           yaxis_title="Index (awal = 100)", plot_bgcolor="white")
        st.plotly_chart(fig2, use_container_width=True)
