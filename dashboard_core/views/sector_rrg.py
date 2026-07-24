"""Halaman 2 — RRG Analysis per market (plotly, interval mingguan default)."""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from rrg_module import calculate_rrg  # noqa: E402

from ..rrg_plotly import plot_rrg_plotly
from ..sector_data import MARKET_LABELS, market_frame
from .sector_common import admin_sync_panel, as_of_caption, load_data_or_stop, market_selector

QUAD_ICON = {"Leading": "🟢", "Improving": "🔵", "Weakening": "🟡", "Lagging": "🔴"}


def render_sector_rrg():
    st.title("Sector RRG")
    st.caption("Relative Rotation Graph: rotasi sektor vs benchmark market masing-masing")

    data = load_data_or_stop()
    market = market_selector("rrg_market")
    interval = st.sidebar.radio("Interval", ["Mingguan", "Harian"], index=0, key="rrg_interval")
    ratio_window = st.sidebar.slider("Window RS-Ratio", 10, 60, 20, 5, key="rrg_rw")
    momentum_window = st.sidebar.slider("Window RS-Momentum", 5, 30, 10, 1, key="rrg_mw")
    trail = st.sidebar.slider("Panjang tail", 3, 20, 8, 1, key="rrg_trail")
    show_labels = st.sidebar.checkbox("Tampilkan label", value=True, key="rrg_lbl")
    admin_sync_panel()

    prices, bench_ric, name_map = market_frame(data, market)
    if prices.empty or bench_ric not in prices.columns:
        st.info(f"Belum ada data harga/benchmark untuk {MARKET_LABELS[market]}.")
        return
    as_of_caption(prices, MARKET_LABELS[market])

    weekly = interval == "Mingguan"
    sector_rics = [c for c in prices.columns if c != bench_ric]
    df_named = prices.rename(columns=name_map)
    fund_cols = [name_map.get(r, r) for r in sector_rics]

    rrg = calculate_rrg(
        df_named,
        benchmark_col_or_series=df_named[name_map.get(bench_ric, bench_ric)],
        fund_cols=fund_cols,
        ratio_window=ratio_window,
        momentum_window=momentum_window,
        weekly=weekly,
    )
    current = rrg.get("current", pd.DataFrame())
    if current.empty:
        st.warning("Data belum cukup untuk RRG (butuh minimal ratio_window + momentum_window periode).")
        return

    col_chart, col_side = st.columns([3, 1])
    with col_chart:
        fig = plot_rrg_plotly(
            rrg, trail_length=trail, show_labels=show_labels,
            title=f"RRG {MARKET_LABELS[market]} — {interval}",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_side:
        st.subheader("Kuadran")
        for q in ["Leading", "Improving", "Weakening", "Lagging"]:
            members = current[current["Quadrant"] == q]["FundName"].tolist()
            st.markdown(f"**{QUAD_ICON[q]} {q}** ({len(members)})")
            for m in members:
                st.markdown(f"- {m}")

    # -------- Sinyal rotasi: yang baru pindah kuadran --------
    st.subheader("Sinyal Rotasi (pindah kuadran)")
    trailing = rrg.get("trailing", pd.DataFrame())
    moves = []
    if not trailing.empty:
        for name in current["FundName"]:
            t = trailing[trailing["FundName"] == name].sort_values("Date").tail(2)
            if len(t) == 2 and t["Quadrant"].iloc[0] != t["Quadrant"].iloc[1]:
                moves.append({
                    "Sektor": name,
                    "Dari": t["Quadrant"].iloc[0],
                    "Ke": t["Quadrant"].iloc[1],
                    "Tanggal": t["Date"].iloc[1].strftime("%d %b %Y"),
                })
    if moves:
        st.dataframe(pd.DataFrame(moves), hide_index=True, use_container_width=True)
    else:
        st.caption("Tidak ada perpindahan kuadran pada periode terakhir.")

    # -------- Tabel detail --------
    st.subheader("Data RRG")
    disp = current[["FundName", "RS_Ratio", "RS_Momentum", "Quadrant"]].copy()
    disp[["RS_Ratio", "RS_Momentum"]] = disp[["RS_Ratio", "RS_Momentum"]].round(2)
    st.dataframe(disp.sort_values("Quadrant"), hide_index=True, use_container_width=True)
