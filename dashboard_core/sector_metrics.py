"""Metrik momentum sektoral: multi-timeframe returns, relative strength,
composite score, badge SMA, dan volatility-adjusted momentum.

Semua fungsi menerima DataFrame harga wide (index=DatetimeIndex, columns=instrumen)
DENGAN KALENDER SATU MARKET (jangan campur kalender US/ID/CN — lihat sector_data.market_frame).
"""

import numpy as np
import pandas as pd

TIMEFRAMES = [("1D", 1), ("1W", 5), ("1M", 21), ("3M", 63), ("6M", 126), ("YTD", None), ("12M", 252)]

DEFAULT_COMPOSITE_WEIGHTS = {"1M": 0.40, "3M": 0.30, "6M": 0.20, "12M": 0.10}
# Momentum 12-1 klasik: skip 1 bulan terakhir
MOMENTUM_12_1_WEIGHTS = {"12M-1M": 1.0}


def _period_return(s, days):
    s = s.dropna()
    if len(s) < 2:
        return np.nan
    if days is None:  # YTD
        last = s.index[-1]
        base = s[s.index < pd.Timestamp(last.year, 1, 1)]
        if base.empty:
            return np.nan
        return s.iloc[-1] / base.iloc[-1] - 1
    if len(s) <= days:
        return s.iloc[-1] / s.iloc[0] - 1
    return s.iloc[-1] / s.iloc[-(days + 1)] - 1


def multi_timeframe_returns(prices):
    """Return per timeframe. Output: DataFrame index=instrumen, columns=TF."""
    if prices.empty:
        return pd.DataFrame()
    px = prices.sort_index().ffill()
    out = {label: px.apply(lambda col: _period_return(col, days)) for label, days in TIMEFRAMES}
    return pd.DataFrame(out)


def relative_returns(returns_df, benchmark_ric):
    """Return relatif (sektor - benchmark) per timeframe."""
    if returns_df.empty or benchmark_ric not in returns_df.index:
        return returns_df
    rel = returns_df.sub(returns_df.loc[benchmark_ric], axis=1)
    return rel.drop(index=benchmark_ric)


def _return_12_1(s):
    """Momentum 12-1: return 12 bulan dengan skip 1 bulan terakhir."""
    s = s.dropna()
    if len(s) <= 252:
        return np.nan
    return s.iloc[-22] / s.iloc[-253] - 1


def sma_badges(prices, windows=(50, 200)):
    """Badge tren: posisi harga terakhir vs SMA. Output Series str, mis. '▲/▲'."""
    if prices.empty:
        return pd.Series(dtype=object)
    px = prices.sort_index().ffill()
    marks = {}
    for col in px.columns:
        s = px[col].dropna()
        parts = []
        for w in windows:
            if len(s) < w:
                parts.append("·")
            else:
                parts.append("▲" if s.iloc[-1] >= s.tail(w).mean() else "▼")
        marks[col] = "/".join(parts)
    return pd.Series(marks)


def composite_scores(prices, benchmark_ric, weights=None, use_12_1=False):
    """Skor momentum komposit per sektor (relatif vs benchmark) + ranking.

    Returns DataFrame index=RIC sektor dengan kolom:
    RS_1M..RS_12M, Score (z-score), VolAdj_Score, SMA, Rank
    """
    if prices.empty or benchmark_ric not in prices.columns:
        return pd.DataFrame()
    weights = weights or DEFAULT_COMPOSITE_WEIGHTS

    rets = multi_timeframe_returns(prices)
    rel = relative_returns(rets, benchmark_ric)
    if rel.empty:
        return pd.DataFrame()

    px = prices.sort_index().ffill()

    if use_12_1:
        mom = px.apply(_return_12_1)
        bench_mom = mom.get(benchmark_ric, np.nan)
        raw = (mom - bench_mom).drop(index=benchmark_ric, errors="ignore")
    else:
        raw = pd.Series(0.0, index=rel.index)
        for tf, w in weights.items():
            if tf in rel.columns:
                raw = raw + w * rel[tf].fillna(0.0)

    std = raw.std(ddof=0)
    z = (raw - raw.mean()) / std if std and not np.isclose(std, 0) else raw * 0.0

    # Volatility-adjust: momentum dibagi vol 3M annualized (menghindari bias sektor volatil)
    vol_3m = px.pct_change().tail(63).std() * np.sqrt(252)
    vol_3m = vol_3m.reindex(raw.index).replace(0, np.nan)
    vol_adj = raw / vol_3m

    out = pd.DataFrame(index=raw.index)
    for tf in ["1M", "3M", "6M", "12M"]:
        if tf in rel.columns:
            out[f"RS_{tf}"] = rel[tf]
    out["Raw_Score"] = raw
    out["Score"] = z
    out["VolAdj_Score"] = vol_adj
    out["SMA"] = sma_badges(px[raw.index.tolist()]).reindex(raw.index)
    out["Rank"] = out["Score"].rank(ascending=False, method="min").astype("Int64")
    return out.sort_values("Rank")


def rank_delta(prices, benchmark_ric, lookback_days=5, weights=None):
    """Perubahan rank composite vs `lookback_days` hari bursa lalu (Δrank mingguan)."""
    if prices.empty or len(prices) <= lookback_days:
        return pd.Series(dtype="Int64")
    now = composite_scores(prices, benchmark_ric, weights)
    prev = composite_scores(prices.iloc[:-lookback_days], benchmark_ric, weights)
    if now.empty or prev.empty:
        return pd.Series(dtype="Int64")
    delta = prev["Rank"].reindex(now.index) - now["Rank"]
    return delta.astype("Int64")


def rebase(prices, base_date=None):
    """Index semua kolom = 100 pada tanggal awal (atau base_date)."""
    if prices.empty:
        return prices
    px = prices.sort_index().ffill()
    if base_date is not None:
        px = px[px.index >= pd.Timestamp(base_date)]
    if px.empty:
        return px
    base = px.apply(lambda col: col.dropna().iloc[0] if col.dropna().size else np.nan)
    return px.div(base, axis=1) * 100


def align_cross_market(frames, max_ffill=5):
    """Gabungkan beberapa DataFrame harga (kalender berbeda) via union tanggal +
    forward-fill terbatas `max_ffill` hari — sesuai keputusan desain #3."""
    valid = [f for f in frames if f is not None and not f.empty]
    if not valid:
        return pd.DataFrame()
    idx = valid[0].index
    for f in valid[1:]:
        idx = idx.union(f.index)
    out = pd.concat([f.reindex(idx) for f in valid], axis=1)
    return out.ffill(limit=max_ffill)
