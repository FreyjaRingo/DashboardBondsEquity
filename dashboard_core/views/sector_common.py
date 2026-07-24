"""Helper bersama untuk halaman sektor."""

import datetime as dt

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from ..sector_data import MARKET_LABELS, load_sector_data


def load_data_or_stop(years=11):
    """Load data sektor; tampilkan instruksi setup jika belum ada."""
    end = dt.datetime.today().date()
    start = end - dt.timedelta(days=365 * years)
    data = load_sector_data(start, end)
    if data["master"].empty:
        st.error(
            "Master data sektor belum ada. Jalankan `sql/sector_schema.sql` di Supabase, "
            "lalu verifikasi RIC & backfill lewat panel Admin (lihat SECTOR_README.md)."
        )
        st.stop()
    if data["prices"].empty:
        st.warning(
            "Tabel `sector_prices_daily` masih kosong. Hubungkan Refinitiv di panel "
            "**Admin Sektor** pada sidebar, lalu jalankan **Backfill**."
        )
    return data


def market_selector(key="sector_market"):
    codes = list(MARKET_LABELS.keys())
    return st.sidebar.selectbox(
        "Market", codes, format_func=lambda c: MARKET_LABELS[c], key=key
    )


def admin_sync_panel():
    """Panel admin di sidebar: koneksi Refinitiv + update/backfill/verifikasi sektor."""
    from ..sector_sync import (
        backfill_sectors,
        get_sector_sync_start_dates,
        has_pending_sector_sync,
        run_sector_sync,
        verify_sector_rics,
    )
    from ..sync import init_refinitiv_session
    from ..sector_data import load_sector_data as _lsd

    with st.sidebar.expander("Admin Sektor (Refinitiv)"):
        pw = st.text_input("Password Refinitiv", type="password", key="sector_pw")
        connected = st.session_state.get("connected", False)

        if st.button("Connect", key="sector_connect"):
            connected = init_refinitiv_session(password=pw)
            st.session_state.connected = connected
            (st.success if connected else st.error)(
                "Terhubung." if connected else "Gagal terhubung."
            )

        col1, col2 = st.columns(2)
        status = st.empty()
        bar = st.progress(0)

        def cb(done, total, msg):
            status.write(msg)
            if total:
                bar.progress(min(1.0, done / total))

        with col1:
            if st.button("Update Harian", disabled=not connected, key="sector_update"):
                starts = get_sector_sync_start_dates()
                end_d = dt.datetime.today().date()
                if not has_pending_sector_sync(starts, end_d):
                    st.info("Sudah mutakhir.")
                else:
                    res = run_sector_sync(starts, end_d, progress_callback=cb)
                    _lsd.clear()
                    st.success(f"{res['uploaded']} baris, {res['failed']} batch gagal.")
        with col2:
            if st.button("Backfill 10 Thn", disabled=not connected, key="sector_backfill"):
                res = backfill_sectors("2015-01-01", progress_callback=cb)
                _lsd.clear()
                st.success(f"Backfill: {res['uploaded']} baris, {res['failed']} batch gagal.")

        if st.button("Verifikasi RIC", disabled=not connected, key="sector_verify"):
            df = verify_sector_rics(progress_callback=cb)
            st.dataframe(df, hide_index=True, use_container_width=True)
            bad = df[df["status"] != "OK"]
            if not bad.empty:
                st.warning(
                    f"{len(bad)} RIC bermasalah — perbaiki di tabel sector_instruments "
                    "(kemungkinan RIC salah atau entitlement ditolak; lihat SECTOR_README.md §fallback ETF)."
                )


def strength_meter_chart(returns_series, name_map, title, height=None):
    """Bar horizontal merah/hijau ala forex strength meter."""
    s = returns_series.dropna().sort_values()
    if s.empty:
        return None
    labels = [name_map.get(r, r) for r in s.index]
    colors = ["#2e7d32" if v >= 0 else "#c62828" for v in s.values]
    fig = go.Figure(go.Bar(
        x=s.values * 100, y=labels, orientation="h",
        marker_color=colors,
        text=[f"{v * 100:+.2f}%" for v in s.values], textposition="outside",
        hovertemplate="%{y}: %{x:.2f}%<extra></extra>",
    ))
    fig.update_layout(
        title=title, height=height or (80 + 32 * len(s)),
        xaxis_title="% perubahan", margin=dict(l=10, r=60, t=50, b=30),
        plot_bgcolor="white",
    )
    fig.add_vline(x=0, line_color="gray", line_width=1)
    return fig


def style_returns_table(df):
    """Styler tabel return multi-TF: bar merah/hijau per sel."""
    fmt = df.mul(100)
    styler = (
        fmt.style.format("{:+.2f}%", na_rep="-")
        .bar(align=0, vmin=-abs(fmt).max().max(), vmax=abs(fmt).max().max(),
             color=["#ef9a9a", "#a5d6a7"])
    )
    return styler


def as_of_caption(prices, label=""):
    if prices is not None and not prices.empty:
        st.caption(f"Data {label} per: **{prices.index.max().strftime('%d %b %Y')}** (trading date lokal)")
