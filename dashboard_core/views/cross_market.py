"""Halaman 3 — Cross-Market: matriks sektor × market, korelasi, global sector view."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from ..sector_data import (
    MARKET_LABELS,
    SECTOR_KEY_LABELS,
    convert_to_usd,
    market_frame,
)
from ..sector_metrics import align_cross_market, composite_scores, multi_timeframe_returns, relative_returns
from .sector_common import admin_sync_panel, load_data_or_stop

TF_CHOICES = ["1W", "1M", "3M", "6M", "12M"]


def _collect(data, currency):
    """Per market: (prices [kalender lokal, sudah dikonversi bila USD], bench_ric, sub-master)."""
    out = {}
    for mkt in MARKET_LABELS:
        prices, bench, _ = market_frame(data, mkt)
        if prices.empty:
            continue
        if currency == "USD":
            prices = convert_to_usd(prices, data["fx"], mkt)
            if prices.empty:
                continue
        out[mkt] = (prices, bench, data["master"][data["master"]["market"] == mkt])
    return out


def render_cross_market():
    st.title("Cross-Market Sector View")
    st.caption("Perbandingan sektor sejenis lintas US / Indonesia / CN Onshore / CN Offshore-HK")

    data = load_data_or_stop()
    currency = st.sidebar.radio(
        "Mata uang", ["Lokal", "USD"], key="cm_ccy",
        help="USD memakai kurs IDR=/CNY=/HKD= dari fx_daily.",
    )
    tf = st.sidebar.selectbox("Timeframe matriks", TF_CHOICES, index=1, key="cm_tf")
    rel_mode = st.sidebar.toggle("Relatif vs benchmark market", value=True, key="cm_rel")
    admin_sync_panel()

    per_market = _collect(data, currency)
    if not per_market:
        st.info("Belum ada data harga sektor (atau kurs FX belum tersedia untuk mode USD).")
        return

    # ---------- 1. MATRIKS SEKTOR x MARKET ----------
    st.subheader(f"Matriks Return {tf} — sektor × market ({'USD' if currency == 'USD' else 'lokal'})")
    rows = {}
    for mkt, (prices, bench, sub) in per_market.items():
        rets = multi_timeframe_returns(prices)
        if rel_mode and bench in rets.index:
            rets = relative_returns(rets, bench)
        key_map = dict(zip(sub["ric"], sub["sector_key"]))
        for ric, val in rets[tf].items():
            skey = key_map.get(ric)
            if skey:
                rows.setdefault(skey, {})[mkt] = val

    matrix = pd.DataFrame(rows).T
    if matrix.empty:
        st.info("Data belum cukup.")
        return
    matrix = matrix.reindex(columns=[m for m in MARKET_LABELS if m in matrix.columns])
    matrix.index = [SECTOR_KEY_LABELS.get(k, k) for k in matrix.index]
    matrix = matrix.sort_index()

    z = matrix.to_numpy(dtype=float) * 100
    fig = go.Figure(go.Heatmap(
        z=z,
        x=[MARKET_LABELS[m] for m in matrix.columns],
        y=matrix.index.tolist(),
        colorscale="RdYlGn", zmid=0,
        text=np.where(np.isnan(z), "", np.vectorize(lambda v: f"{v:+.1f}%")(np.nan_to_num(z))),
        texttemplate="%{text}", hovertemplate="%{y} | %{x}: %{z:.2f}%<extra></extra>",
        colorbar=dict(title="%"),
    ))
    fig.update_layout(height=60 + 38 * len(matrix), margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

    # ---------- 2. GLOBAL SECTOR VIEW ----------
    st.subheader("Global Sector Momentum")
    st.caption("Rata-rata z-score momentum composite sektor sejenis di seluruh market.")
    zrows = {}
    for mkt, (prices, bench, sub) in per_market.items():
        sc = composite_scores(prices, bench)
        if sc.empty:
            continue
        key_map = dict(zip(sub["ric"], sub["sector_key"]))
        for ric, val in sc["Score"].items():
            skey = key_map.get(ric)
            if skey:
                zrows.setdefault(skey, []).append(val)
    if zrows:
        glob = pd.Series({k: np.nanmean(v) for k, v in zrows.items()}).sort_values()
        glob.index = [SECTOR_KEY_LABELS.get(k, k) for k in glob.index]
        colors = ["#2e7d32" if v >= 0 else "#c62828" for v in glob.values]
        figg = go.Figure(go.Bar(
            x=glob.values, y=glob.index.tolist(), orientation="h", marker_color=colors,
            hovertemplate="%{y}: %{x:.2f}<extra></extra>",
        ))
        figg.update_layout(height=80 + 30 * len(glob), xaxis_title="Rata-rata z-score momentum",
                           margin=dict(l=10, r=10, t=10, b=10), plot_bgcolor="white")
        figg.add_vline(x=0, line_color="gray", line_width=1)
        st.plotly_chart(figg, use_container_width=True)

    # ---------- 3. KORELASI LINTAS MARKET ----------
    st.subheader("Korelasi Lintas Market")
    st.caption("Korelasi return harian (union kalender, forward-fill maks 5 hari).")
    sel_keys = st.multiselect(
        "Sektor", list(SECTOR_KEY_LABELS.keys()),
        default=["financials", "info_tech", "energy"],
        format_func=lambda k: SECTOR_KEY_LABELS[k], key="cm_corr_sel",
    )
    lookback = st.select_slider("Periode korelasi", options=["3M", "6M", "1Y", "3Y"], value="1Y", key="cm_corr_lb")
    if sel_keys:
        frames, labels = [], []
        for mkt, (prices, bench, sub) in per_market.items():
            key_map = dict(zip(sub["ric"], sub["sector_key"]))
            cols = [r for r in prices.columns if key_map.get(r) in sel_keys]
            if cols:
                f = prices[cols].copy()
                f.columns = [f"{SECTOR_KEY_LABELS[key_map[c]]} ({mkt})" for c in cols]
                frames.append(f)
        combined = align_cross_market(frames)
        days = {"3M": 63, "6M": 126, "1Y": 252, "3Y": 756}[lookback]
        combined = combined.tail(days)
        if combined.shape[1] >= 2:
            corr = combined.pct_change().corr()
            figc = go.Figure(go.Heatmap(
                z=corr.values, x=corr.columns.tolist(), y=corr.index.tolist(),
                colorscale="RdBu", zmid=0, zmin=-1, zmax=1,
                text=np.round(corr.values, 2), texttemplate="%{text}",
                hovertemplate="%{y} × %{x}: %{z:.2f}<extra></extra>",
            ))
            figc.update_layout(height=120 + 32 * len(corr), margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(figc, use_container_width=True)
        else:
            st.info("Pilih sektor dengan data di ≥ 2 market.")
