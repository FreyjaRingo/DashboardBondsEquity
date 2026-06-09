import numpy as np
import pandas as pd
import streamlit as st


@st.cache_data(max_entries=50, show_spinner=False)
def calculate_metrics(price_data, benchmark_series, risk_free_rate, eval_window=None, young_funds_list=None, bench_ticker=""):
    price_data = price_data.dropna(axis=1, how='all')
    price_data = price_data.ffill()

    returns_full = price_data.pct_change().dropna(how='all')
    if returns_full.empty: return None

    inception = (price_data.iloc[-1] / price_data.iloc[0]) - 1

    def get_period_return(df, days):
        if len(df) > days: return (df.iloc[-1] / df.iloc[-(days + 1)]) - 1
        else: return (df.iloc[-1] / df.iloc[0]) - 1

    return_1w = get_period_return(price_data, 5)
    return_1m = get_period_return(price_data, 22)
    return_3m = get_period_return(price_data, 63)

    if eval_window is not None and len(price_data) > eval_window:
        price_data_risk = price_data.tail(eval_window)
        bench_risk = benchmark_series.tail(eval_window)
    else:
        price_data_risk = price_data
        bench_risk = benchmark_series

    returns_risk = price_data_risk.pct_change().dropna(how='all')
    bench_returns_risk = bench_risk.pct_change().dropna()

    days_risk = len(price_data_risk)
    risk_period_return = (price_data_risk.iloc[-1] / price_data_risk.iloc[0]) - 1 if days_risk > 0 else 0

    volatility_1sd = returns_risk.std() * np.sqrt(252)
    mean_return = returns_risk.mean() * 252
    sharpe_ratio = (mean_return - risk_free_rate) / volatility_1sd.replace(0, np.nan)

    beta = pd.Series([np.nan] * len(returns_risk.columns), index=returns_risk.columns)
    alpha = pd.Series([np.nan] * len(returns_risk.columns), index=returns_risk.columns)

    if not bench_returns_risk.empty:
        combined_returns = pd.concat([returns_risk, bench_returns_risk.rename('MARKET')], axis=1).dropna()
        cov_matrix = combined_returns.cov()
        var_market = cov_matrix.loc['MARKET', 'MARKET']

        if var_market != 0 and not np.isnan(var_market):
            for col in returns_risk.columns:
                if col in cov_matrix.columns and 'MARKET' in cov_matrix.index:
                    beta[col] = cov_matrix.loc['MARKET', col] / var_market

        market_mean_return = bench_returns_risk.mean() * 252
        expected_return = risk_free_rate + beta * (market_mean_return - risk_free_rate)
        alpha = mean_return - expected_return

    # --- KARANTINA RISIKO UNTUK PRODUK MUDA ---
    if young_funds_list is not None:
        for col in price_data_risk.columns:
            if col in young_funds_list:
                volatility_1sd[col] = np.nan
                sharpe_ratio[col] = np.nan
                beta[col] = np.nan
                alpha[col] = np.nan

    metrics_df = pd.DataFrame({
        'Inception_Return': inception, 'Interval_Return': risk_period_return,
        'Return_1W': return_1w, 'Return_1M': return_1m, 'Return_3M': return_3m,
        'Volatility': volatility_1sd, 'Sharpe_Ratio': sharpe_ratio, 'Beta': beta, 'Alpha': alpha
    })

    # [Logika Consist & Climb Tetap Sama]
    consist_results = {}
    intervals = {'1d': 1, '7d': 7, '14d': 14, '21d': 21}
    top_ns = [5, 10, 20]
    for label, interval in intervals.items():
        today = price_data.index[-1]
        start_date = price_data.index[0]
        target_dates = []
        current_date = today
        while current_date >= start_date:
            target_dates.append(current_date)
            current_date -= pd.Timedelta(days=interval)
        target_dates.reverse()
        snapshot_indices = []
        for d in target_dates:
            idx = price_data.index.get_indexer([d], method='pad')[0]
            if idx != -1: snapshot_indices.append(price_data.index[idx])
        snapshot_indices = sorted(list(set(snapshot_indices)))
        if len(snapshot_indices) < 2:
            for n in top_ns: consist_results[f'Consist_{label}_Top{n}'] = pd.Series(0, index=price_data.columns)
            continue
        sliced_prices = price_data.loc[snapshot_indices]
        period_returns = sliced_prices.pct_change().dropna(how='all')
        if period_returns.empty:
            for n in top_ns: consist_results[f'Consist_{label}_Top{n}'] = pd.Series(0, index=price_data.columns)
            continue
        period_ranks = period_returns.rank(axis=1, ascending=False, method='min')
        for n in top_ns:
            scores = {}
            for col in period_ranks.columns:
                ranks = period_ranks[col].dropna()
                is_top = (ranks <= n) & (ranks > 0)
                count = is_top.sum()
                streak, max_streak = 0, 0
                for val in is_top:
                    if val:
                        streak += 1
                        if streak > max_streak: max_streak = streak
                    else: streak = 0
                scores[col] = count + max_streak
            consist_results[f'Consist_{label}_Top{n}'] = pd.Series(scores)

    df_consist = pd.DataFrame(consist_results)
    metrics_df = pd.concat([metrics_df, df_consist], axis=1)

    daily_ranks = returns_full.rank(axis=1, ascending=False, method='min')
    if not daily_ranks.empty:
        rank_today = daily_ranks.iloc[-1]
        def get_safe_past_rank(offset_days):
            if len(daily_ranks) > offset_days: return daily_ranks.iloc[-(offset_days + 1)]
            return daily_ranks.iloc[0]
        metrics_df['Climb_1d'] = get_safe_past_rank(1) - rank_today
        metrics_df['Climb_7d'] = get_safe_past_rank(7) - rank_today
        metrics_df['Climb_14d'] = get_safe_past_rank(14) - rank_today
        metrics_df['Climb_22d'] = get_safe_past_rank(22) - rank_today
    else:
        for c in ['Climb_1d', 'Climb_7d', 'Climb_14d', 'Climb_22d']: metrics_df[c] = 0

    mean_price = price_data_risk.mean()
    std_price = price_data_risk.std()
    z_score = pd.Series(0.0, index=price_data_risk.columns)
    status_valuasi = pd.Series("", index=price_data_risk.columns)
    skor_valuasi = pd.Series(0.0, index=price_data_risk.columns)
    num_products = len(price_data_risk.columns)

    for col in price_data_risk.columns:
        # KARANTINA VALUASI UNTUK PRODUK MUDA
        if young_funds_list is not None and col in young_funds_list:
            z_score[col] = np.nan
            status_valuasi[col] = "Data Terbatas"
            skor_valuasi[col] = np.nan
            continue

        if std_price[col] != 0 and not pd.isna(std_price[col]):
            z_val = (price_data_risk[col].iloc[-1] - mean_price[col]) / std_price[col]
            z_score[col] = z_val
            if z_val >= 3.0: status_valuasi[col] = "Sangat Mahal"
            elif z_val <= -3.0: status_valuasi[col] = "Sangat Murah"
            elif z_val < 3.0 and z_val >= 2.0: status_valuasi[col] = "Mahal"
            elif z_val > -3.0 and z_val <= -2.0: status_valuasi[col] = "Murah"
            elif z_val < 2.0 and z_val >= 1.0: status_valuasi[col] = "Sedikit Mahal"
            elif z_val > -2.0 and z_val <= -1.0: status_valuasi[col] = "Sedikit Murah"
            elif z_val < 1.0 and z_val >= -1.0: status_valuasi[col] = "Fair Price"
            else: status_valuasi[col] = ""

            if z_val > 3.0: skor_valuasi[col] = (1/8) * num_products
            elif 2.0 < z_val <= 3.0: skor_valuasi[col] = (2/8) * num_products
            elif 1.0 < z_val <= 2.0: skor_valuasi[col] = (3/8) * num_products
            elif 0.0 < z_val <= 1.0: skor_valuasi[col] = (4/8) * num_products
            elif -1.0 < z_val <= 0.0: skor_valuasi[col] = (5/8) * num_products
            elif -2.0 < z_val <= -1.0: skor_valuasi[col] = (6/8) * num_products
            elif -3.0 < z_val <= -2.0: skor_valuasi[col] = (7/8) * num_products
            else: skor_valuasi[col] = (8/8) * num_products

    metrics_df['Z_Score'] = z_score
    metrics_df['Status_Valuasi'] = status_valuasi
    metrics_df['Skor_Valuasi'] = skor_valuasi
    return metrics_df


@st.cache_data(max_entries=50, show_spinner=False)
def calculate_rolling_timeseries(price_data, benchmark_series, risk_free_rate, window=63, bench_ticker=""):
    returns = price_data.pct_change().dropna(how='all')
    bench_returns = benchmark_series.pct_change().dropna()

    combined = pd.concat([returns, bench_returns.rename('MARKET')], axis=1).ffill()

    if 'MARKET' not in combined.columns:
        combined['MARKET'] = 0.0

    market_ret = combined['MARKET']
    fund_returns = combined.drop(columns=['MARKET'])

    volatility_ts = fund_returns.rolling(window=window).std() * np.sqrt(252)
    var_market_ts = market_ret.rolling(window=window).var()

    cov_ts = fund_returns.rolling(window=window).cov(market_ret)
    beta_ts = cov_ts.div(var_market_ts, axis=0)

    ann_return_ts = fund_returns.rolling(window=window).mean() * 252
    market_ann_return_ts = market_ret.rolling(window=window).mean() * 252

    sharpe_ts = (ann_return_ts - risk_free_rate) / volatility_ts
    expected_return_ts = risk_free_rate + beta_ts.multiply(market_ann_return_ts - risk_free_rate, axis=0)
    alpha_ts = ann_return_ts - expected_return_ts

    return {
        'Alpha': alpha_ts,
        'Beta': beta_ts,
        'Sharpe_Ratio': sharpe_ts,
        'Volatility': volatility_ts
    }


@st.cache_data(max_entries=50, show_spinner=False)
def calculate_ranking_scores(metrics_df, weights=None, young_funds_list=None):
    w = 1.0 / 24.0 # Angka 24 menyesuaikan karena Inception_Return sudah dihapus
    if weights is None:
        weights = {
            'Return_1W': w, 'Return_1M': w, 'Return_3M': w,
            'Sharpe_Ratio': w, 'Alpha': w, 'Beta': w, 'Volatility': -w,
            'Consist_1d_Top5': w, 'Consist_1d_Top10': w, 'Consist_1d_Top20': w,
            'Consist_7d_Top5': w, 'Consist_7d_Top10': w, 'Consist_7d_Top20': w,
            'Consist_14d_Top5': w, 'Consist_14d_Top10': w, 'Consist_14d_Top20': w,
            'Consist_21d_Top5': w, 'Consist_21d_Top10': w, 'Consist_21d_Top20': w,
            'Climb_1d': w, 'Climb_7d': w, 'Climb_14d': w, 'Climb_22d': w
        }

    df_scaled = metrics_df.copy()

    # DISKUALIFIKASI PRODUK MUDA DARI SEMUA SKORING
    if young_funds_list is not None:
        df_scaled = df_scaled.drop(index=[x for x in young_funds_list if x in df_scaled.index], errors='ignore')

    df_scaled.replace([np.inf, -np.inf], np.nan, inplace=True)

    valid_metrics = [col for col in weights.keys() if col in df_scaled.columns]
    if not valid_metrics: return pd.DataFrame()

    for col in valid_metrics:
        if col in df_scaled.columns:
            values = df_scaled[col].dropna()
            if len(values) > 0:
                min_val = values.min()
                max_val = values.max()
                if max_val > min_val: scaled = (df_scaled[col] - min_val) / (max_val - min_val)
                else: scaled = pd.Series([0.5] * len(df_scaled[col]), index=df_scaled.index)
                if weights[col] < 0: scaled = 1 - scaled
                df_scaled[col + '_scaled'] = scaled * abs(weights[col])

    score_cols = [col + '_scaled' for col in valid_metrics if (col + '_scaled') in df_scaled.columns]

    if score_cols:
        # DISKUALIFIKASI: Deteksi instrumen yang metrik ujiannya NaN
        missing_metrics_mask = df_scaled[valid_metrics].isna().any(axis=1)

        df_scaled['Total_Score'] = df_scaled[score_cols].fillna(0).sum(axis=1)
        df_scaled.loc[missing_metrics_mask, 'Total_Score'] = np.nan # Anulir skor totalnya

        # Hapus instrumen yang tidak lolos syarat umur/metrik dari papan peringkat
        df_scaled = df_scaled.dropna(subset=['Total_Score'])
    else:
        df_scaled['Total_Score'] = 0

    return df_scaled[['Total_Score'] + score_cols].sort_values('Total_Score', ascending=False)


@st.cache_data(max_entries=50, show_spinner=False)
def get_7d_ranking_history(price_data, benchmark_series, risk_free_rate, eval_window=None, custom_weights=None, young_funds_list=None, bench_ticker=""):
    history_ranks = {}
    if len(price_data) < 7: return pd.DataFrame()
    dates = price_data.index[-7:]
    for date in dates:
        sliced_prices = price_data.loc[:date]
        sliced_bench = benchmark_series.loc[:date]
        if len(sliced_prices) < 10: continue
        # INJEKSI PARAMETER BARU
        metrics = calculate_metrics(sliced_prices, sliced_bench, risk_free_rate, eval_window=eval_window, young_funds_list=young_funds_list, bench_ticker=bench_ticker)
        if metrics is not None and not metrics.empty:
            ranks = calculate_ranking_scores(metrics, weights=custom_weights, young_funds_list=young_funds_list)
            if not ranks.empty:
                rank_series = pd.Series(range(1, len(ranks) + 1), index=ranks.index)
                date_str = date.strftime('%d/%m')
                history_ranks[date_str] = rank_series
    return pd.DataFrame(history_ranks)


@st.cache_data(max_entries=50, show_spinner=False)
def get_detailed_ranking_history(price_data_full, benchmark_series_full, risk_free_rate, metric_window, num_columns=10, custom_weights=None, trading_days_interval=1, young_funds_list=None, bench_ticker=""):
    if len(price_data_full) < 1: return pd.DataFrame()
    total_points_needed = (num_columns - 1) * trading_days_interval + 1
    if len(price_data_full) < total_points_needed:
        num_columns = (len(price_data_full) - 1) // trading_days_interval + 1
        if num_columns < 1: return pd.DataFrame()

    eval_dates = price_data_full.iloc[::-trading_days_interval].head(num_columns).index[::-1]
    history_ranks = {}
    top5_streak = {}

    for date in eval_dates:
        idx_today = price_data_full.index.get_loc(date)
        idx_start = max(0, idx_today - metric_window)
        sliced_prices = price_data_full.iloc[idx_start:idx_today+1]
        sliced_bench = benchmark_series_full.iloc[idx_start:idx_today+1]
        if len(sliced_prices) < 10: continue

        # PERBAIKAN FATAL: Inject eval_window dan bench_ticker
        metrics = calculate_metrics(sliced_prices, sliced_bench, risk_free_rate, eval_window=metric_window, young_funds_list=young_funds_list, bench_ticker=bench_ticker)

        if metrics is not None and not metrics.empty:
            if idx_today > 0 and trading_days_interval == 1:
                daily_pct = (price_data_full.iloc[idx_today] / price_data_full.iloc[idx_today-1]) - 1
                stagnant_instruments = daily_pct[daily_pct == 0.0].index
                metrics = metrics.drop(index=stagnant_instruments, errors='ignore')

            if metrics.empty: continue
            ranks = calculate_ranking_scores(metrics, weights=custom_weights, young_funds_list=young_funds_list)
            if not ranks.empty:
                rank_series = pd.Series(range(1, len(ranks) + 1), index=ranks.index)
                date_str = date.strftime('%d/%m/%y')
                history_ranks[date_str] = rank_series
                top5_today = set(rank_series[rank_series <= 5].index)
                for product in price_data_full.columns:
                    if product not in top5_streak: top5_streak[product] = 0
                    if product in top5_today: top5_streak[product] += 1
                    else: top5_streak[product] = 0

    history_df = pd.DataFrame(history_ranks)
    if not history_df.empty: history_df['Streak_Top5'] = pd.Series(top5_streak)
    return history_df


@st.cache_data(max_entries=50, show_spinner=False)
def get_monthly_rankings(price_data, benchmark_series, risk_free_rate):
    """Menghitung ranking awal bulan lalu dan dua bulan lalu"""
    if len(price_data) < 60:
        return pd.Series(), pd.Series()

    today = price_data.index[-1]

    if today.month == 1:
        last_month_start = pd.Timestamp(today.year - 1, 12, 1)
    else:
        last_month_start = pd.Timestamp(today.year, today.month - 1, 1)

    if today.month <= 2:
        two_months_start = pd.Timestamp(today.year - 1, 12 + (today.month - 2), 1)
    else:
        two_months_start = pd.Timestamp(today.year, today.month - 2, 1)

    def get_rank_at_date(target_date):
        available_dates = price_data.index[price_data.index <= target_date]
        if len(available_dates) == 0:
            return pd.Series()

        closest_date = available_dates[-1]
        sliced_prices = price_data.loc[:closest_date]
        sliced_bench = benchmark_series.loc[:closest_date]

        metrics = calculate_metrics(sliced_prices, sliced_bench, risk_free_rate)
        if metrics is None or metrics.empty:
            return pd.Series()

        ranks = calculate_ranking_scores(metrics)
        if ranks.empty:
            return pd.Series()

        return pd.Series(range(1, len(ranks) + 1), index=ranks.index)

    rank_last_month = get_rank_at_date(last_month_start - pd.Timedelta(days=1))
    rank_two_months_ago = get_rank_at_date(two_months_start - pd.Timedelta(days=1))

    return rank_last_month, rank_two_months_ago


@st.cache_data(max_entries=50, show_spinner=False)
def get_period_performance_ranking(price_data, trading_days_interval=5, num_periods=10):
    """Menghitung peringkat return absolut melompat secara presisi mengikuti Trading Days"""
    if price_data.empty or len(price_data) < 2: return pd.DataFrame()

    total_points_needed = num_periods * trading_days_interval + 1
    if len(price_data) < total_points_needed:
        num_periods = (len(price_data) - 1) // trading_days_interval
        if num_periods < 1: return pd.DataFrame()

    sliced_prices = price_data.iloc[::-trading_days_interval].head(num_periods + 1).iloc[::-1]
    period_returns = sliced_prices.pct_change().dropna(how='all')

    # --- ELIMINASI NOL UNTUK INTERVAL HARIAN ---
    if trading_days_interval == 1:
        period_returns = period_returns.replace(0.0, np.nan)

    if period_returns.empty: return pd.DataFrame()

    period_ranks = period_returns.rank(axis=1, ascending=False, method='min')
    period_ranks.index = period_ranks.index.strftime('%d/%m/%y')

    return period_ranks.T


@st.cache_data(max_entries=50, show_spinner=False)
def get_monthly_pct_change(price_data):
    """Menghitung persentase perubahan harga akhir bulan (MoM)."""
    if len(price_data) < 2: return pd.DataFrame()

    try:
        monthly_prices = price_data.resample('ME').last()
    except:
        monthly_prices = price_data.resample('M').last()

    monthly_returns = monthly_prices.pct_change().dropna(how='all') * 100

    # --- ELIMINASI NOL ---
    monthly_returns = monthly_returns.replace(0.0, np.nan)

    monthly_returns.index = monthly_returns.index.strftime('%b %Y')
    return monthly_returns.T


@st.cache_data(max_entries=50, show_spinner=False)
def calculate_daily_leaderboard(price_data, days=5):
    """
    Menghitung perubahan peringkat berdasarkan Return absolut menggunakan
    posisi hari bursa (Trading Days).
    """
    # Pastikan data cukup panjang untuk menghindari error iloc
    if price_data.empty or len(price_data) < (days + 2):
        return pd.DataFrame()

    # [PERBAIKAN KUNCI]: Forward fill untuk menambal data NAV USD yang delay
    # agar tidak menjadi NaN di baris terakhir saat kalkulasi.
    price_data = price_data.ffill()

    # Kalkulasi Return Hari Ini vs 'days' hari bursa lalu
    returns_today = (price_data.iloc[-1] / price_data.iloc[-(days + 1)]) - 1

    # Kalkulasi Return Kemarin vs 'days' hari bursa sebelum kemarin
    returns_yesterday = (price_data.iloc[-2] / price_data.iloc[-(days + 2)]) - 1

    # Format dataframe hari ini
    col_return_name = f'Return_{days}d'
    df_today = returns_today.reset_index()
    df_today.columns = ['Instrument', col_return_name]
    df_today = df_today.dropna()
    df_today['Rank_Today'] = df_today[col_return_name].rank(ascending=False, method='min')

    # Format dataframe kemarin
    df_yesterday = returns_yesterday.reset_index()
    df_yesterday.columns = ['Instrument', 'Return_Yesterday']
    df_yesterday = df_yesterday.dropna()
    df_yesterday['Rank_Yesterday'] = df_yesterday['Return_Yesterday'].rank(ascending=False, method='min')

    # Gabungkan data dan kalkulasi perubahan peringkat
    leaderboard = pd.merge(df_today, df_yesterday[['Instrument', 'Rank_Yesterday']], on='Instrument', how='left')
    leaderboard['Rank_Change'] = leaderboard['Rank_Yesterday'] - leaderboard['Rank_Today']
    leaderboard['Rank_Change'] = leaderboard['Rank_Change'].fillna(0)

    leaderboard = leaderboard.sort_values('Rank_Today').reset_index(drop=True)

    return leaderboard[['Instrument', col_return_name, 'Rank_Today', 'Rank_Change']]


def ensure_unique_columns(df):
    """Memastikan tidak ada duplikat kolom dengan menambahkan suffix jika perlu"""
    if df.columns.duplicated().any():
        df.columns = pd.Index([f"{col}_{i}" if df.columns.duplicated()[i] else col
                               for i, col in enumerate(df.columns)])
    return df


@st.cache_data(max_entries=50, show_spinner=False)
def get_nav_performance(price_data_full):
    """Menghitung tabel NAV Performance dari observasi NAV database."""
    if price_data_full.empty:
        return pd.DataFrame()

    df = price_data_full.copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df = df.apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan)

    perf_data = []

    def calc_return(current_nav, past_nav):
        if pd.isna(current_nav) or pd.isna(past_nav) or past_nav <= 0:
            return np.nan
        return (current_nav / past_nav) - 1

    def get_last_nav_on_or_before(series, target_date):
        past_series = series.loc[:target_date].dropna()
        if past_series.empty:
            return np.nan
        return past_series.iloc[-1]

    def get_first_nav_on_or_after(series, target_date, max_date):
        year_start_series = series.loc[target_date:max_date].dropna()
        if year_start_series.empty:
            return np.nan
        return year_start_series.iloc[0]

    for col in df.columns:
        series = df[col].dropna()
        if series.empty:
            continue

        current_nav = series.iloc[-1]
        nav_date = series.index[-1]
        previous_nav = series.iloc[-2] if len(series) > 1 else np.nan

        ret_1d = calc_return(current_nav, previous_nav)
        ret_1w = calc_return(current_nav, get_last_nav_on_or_before(series, nav_date - pd.DateOffset(days=5)))
        ret_1m = calc_return(current_nav, get_last_nav_on_or_before(series, nav_date - pd.DateOffset(days=22)))
        ret_3m = calc_return(current_nav, get_last_nav_on_or_before(series, nav_date - pd.DateOffset(days=63)))
        ret_1y = calc_return(current_nav, get_last_nav_on_or_before(series, nav_date - pd.DateOffset(days=252)))
        ret_ytd = calc_return(
            current_nav,
            get_first_nav_on_or_after(series, pd.Timestamp(nav_date.year, 1, 1), nav_date)
        )

        perf_data.append({
            "Instrument": col,
            "Date": nav_date.strftime("%Y-%m-%d"),
            "NAV": current_nav,
            "1D": ret_1d,
            "1W": ret_1w,
            "1M": ret_1m,
            "3M": ret_3m,
            "YTD": ret_ytd,
            "1Y": ret_1y
        })

    df_perf = pd.DataFrame(perf_data)

    if not df_perf.empty:
        for c in ["1D", "1W", "1M", "3M", "YTD", "1Y"]:
            df_perf[c] = df_perf[c].apply(lambda x: "-" if pd.isna(x) else f"{x * 100:.2f}%")

        df_perf["NAV"] = df_perf["NAV"].round(4)

    return df_perf
