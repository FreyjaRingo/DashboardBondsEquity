"""Data layer untuk Dashboard Kekuatan Sektoral.

Mengikuti pola dashboard_core/data.py: master data + harga harian dari Supabase,
di-cache dengan st.cache_data, dan dikembalikan dalam format wide (Date x RIC).
"""

import pandas as pd
import streamlit as st

from .data import fetch_from_supabase, supabase

FX_PAIRS = ["IDR=", "CNY=", "HKD="]

# Kurs untuk konversi ke USD per market (pair dikuotasi sebagai unit lokal per USD)
MARKET_FX = {"US": None, "ID": "IDR=", "CN": "CNY=", "HK": "HKD="}

MARKET_LABELS = {
    "US": "US (S&P 500)",
    "ID": "Indonesia (IDX-IC)",
    "CN": "CN Onshore (CSI 300)",
    "HK": "CN Offshore / HK (HSCI)",
}

SECTOR_KEY_LABELS = {
    "energy": "Energy",
    "materials": "Materials",
    "industrials": "Industrials",
    "cons_disc": "Consumer Discretionary",
    "cons_staples": "Consumer Staples",
    "health_care": "Health Care",
    "financials": "Financials",
    "info_tech": "Information Technology",
    "comm_services": "Communication Services",
    "utilities": "Utilities",
    "real_estate": "Real Estate",
    "infrastructure": "Infrastructure",
    "transport_logistics": "Transportation & Logistics",
}


@st.cache_data(ttl=3600, show_spinner=False)
def load_sector_instruments():
    """Master data indeks sektor dari tabel sector_instruments (DataFrame)."""
    try:
        rows = supabase.table("sector_instruments").select("*").eq("active", True).execute().data
    except Exception as e:
        st.warning(f"Tabel sector_instruments belum tersedia di Supabase: {e}")
        return pd.DataFrame()
    return pd.DataFrame(rows or [])


@st.cache_data(ttl=1800, show_spinner=False)
def load_sector_data(start_date, end_date):
    """Menarik harga sektor + FX dari Supabase.

    Returns
    -------
    dict dengan kunci:
      'master' : DataFrame master instrumen
      'prices' : DataFrame wide (index=Date, columns=RIC) harga close
      'fx'     : DataFrame wide (index=Date, columns=pair) kurs
    """
    master = load_sector_instruments()
    if master.empty:
        return {"master": master, "prices": pd.DataFrame(), "fx": pd.DataFrame()}

    rics = master["ric"].tolist()
    df_px_raw = fetch_from_supabase(
        "sector_prices_daily", "ric", rics, start_date, end_date, "date,ric,close"
    )
    df_fx_raw = fetch_from_supabase(
        "fx_daily", "pair", FX_PAIRS, start_date, end_date, "date,pair,close"
    )

    def pivot(df, id_col):
        if df.empty:
            return pd.DataFrame()
        df = df.drop_duplicates(subset=[id_col, "Date"], keep="last")
        return df.pivot(index="Date", columns=id_col, values="close").sort_index()

    return {
        "master": master,
        "prices": pivot(df_px_raw, "ric"),
        "fx": pivot(df_fx_raw, "pair"),
    }


def market_frame(data, market, include_benchmark=True):
    """Ambil sub-DataFrame harga untuk satu market (kalender market itu sendiri).

    Returns (prices_df, benchmark_ric, name_map) — prices_df kolom = RIC.
    """
    master, prices = data["master"], data["prices"]
    if master.empty or prices.empty:
        return pd.DataFrame(), None, {}

    sub = master[master["market"] == market]
    bench_rows = sub[sub["is_benchmark"] == True]  # noqa: E712
    bench_ric = bench_rows["ric"].iloc[0] if not bench_rows.empty else None

    wanted = sub["ric"].tolist() if include_benchmark else sub[~sub["is_benchmark"]]["ric"].tolist()
    cols = [r for r in wanted if r in prices.columns]
    if not cols:
        return pd.DataFrame(), bench_ric, {}

    df = prices[cols].dropna(how="all")
    # Hanya tanggal saat market ini benar-benar trading (min. 1 seri update)
    df = df[df.notna().any(axis=1)]
    name_map = dict(zip(sub["ric"], sub["name"]))
    return df, bench_ric, name_map


def convert_to_usd(prices, fx, market):
    """Konversi harga lokal ke USD memakai fx_daily (pair = unit lokal per USD)."""
    pair = MARKET_FX.get(market)
    if pair is None:
        return prices
    if fx.empty or pair not in fx.columns:
        return pd.DataFrame()  # kurs tidak tersedia
    rate = fx[pair].reindex(prices.index.union(fx.index)).ffill().reindex(prices.index)
    return prices.div(rate, axis=0)
