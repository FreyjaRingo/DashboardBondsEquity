import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import datetime as dt
import refinitiv.data as rd

# ==================== FUNGSI INISIALISASI SESI REFINITIV ====================
def init_refinitiv_session():
    """
    Membuka sesi Refinitiv Platform menggunakan kredensial dari st.secrets.
    """
    try:
        if "refinitiv" not in st.secrets:
            st.error("❌ Konfigurasi Refinitiv tidak ditemukan di secrets.")
            return False

        config = st.secrets["refinitiv"]
        required_keys = ["app_key", "username", "password"]
        missing = [k for k in required_keys if k not in config]
        if missing:
            st.error(f"❌ Secrets Refinitiv tidak lengkap. Butuh: {', '.join(missing)}")
            return False

        # Definisikan sesi platform secara spesifik untuk kredensial dinamis
        session = rd.session.platform.Definition(
            app_key=config["app_key"],
            grant=rd.session.platform.GrantPassword(
                username=config["username"],
                password=config["password"]
            )
        ).get_session()
        
        # Buka sesi dan tetapkan sebagai default untuk penarikan data selanjutnya
        session.open()
        rd.session.set_default(session)
        
        return True
    except Exception as e:
        st.error(f"❌ Gagal membuka sesi Refinitiv: {e}")
        return False

# ==================== KONFIGURASI HALAMAN ====================
st.set_page_config(
    page_title="Investment Dashboard - Reksa Dana Indonesia",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== FUNGSI HELPER UNTUK LOADING DATA ====================
@st.cache_data(ttl=3600)  # Cache data selama 1 jam
def load_all_data(start_date, end_date, custom_equity_tuple=None, custom_bond_tuple=None):
    """
    Memuat dan memproses semua data berdasarkan input kalender user dari Refinitiv.
    """
    START_DATE_STR = start_date.strftime('%Y-%m-%d')
    END_DATE_STR = end_date.strftime('%Y-%m-%d')
    FREQ = 'D'

    # --- Daftar Ticker Default ---
    default_tickers_equity = ['LP68059065', 'LP68452242', 'LP68199650', 'LP68199651', 'LP68683420', 'LP60090155', 'LP65023619', 'LP68684585', 'LP68790535', 'LP63505420', 'LP65023683', 'LP63505451', 'LP65077719', 'LP65108951', 'LP63505436', 'LP68042752', 'LP68166624', 'LP68692654', 'LP68219216', 'LP63505478', 'LP65077766', 'LP65034334', 'LP68852202', 'LP68794971', 'LP68047313', 'LP63500685', 'LP63505555', 'LP63505558', 'LP63505427']
    default_map_equity = {'LP68059065': 'Allianz Alpha Sector Rotation Kelas A', 'LP68452242': 'Allianz SRI-Kehati Index', 'LP68199650': 'Ashmore Dana Ekuitas Nusantara', 'LP68199651': 'Ashmore Dana Progresif Nusantara', 'LP68683420': 'Ashmore Digital Equity Sustainable', 'LP60090155': 'Batavia Dana Saham', 'LP65023619': 'Batavia Dana Saham Optimal', 'LP68684585': 'Batavia Disruptive Equity', 'LP68790535': 'Batavia Index Pefindo I Grade', 'LP63505420': 'BNP Paribas Ekuitas', 'LP65023683': 'BNP Paribas Infrastruktur Plus', 'LP63505451': 'BNP Paribas Pesona', 'LP65077719': 'BNP Paribas Pesona Syariah', 'LP65108951': 'BNP Paribas Solaris', 'LP63505436': 'BRI Mawar', 'LP68042752': 'BRI Mawar Fokus 10', 'LP68166624': 'Eastspring Investments Alpha Navigator Kelas A', 'LP68692654': 'Eastspring IDX ESG Leaders Plus Kelas A', 'LP68219216': 'Mandiri Investa Equity ASEAN 5 Plus', 'LP63505478': 'Manulife Dana Saham Kelas A', 'LP65077766': 'Manulife Saham Andalan', 'LP65034334': 'Maybank Dana Ekuitas', 'LP68852202': 'Maybank Financial Infobank15 Index Kelas C', 'LP68794971': 'Maybank Financial Infobank15 Index Kelas N', 'LP68047313': 'Schroder 90 Plus Equity', 'LP63500685': 'Schroder Dana Istimewa', 'LP63505555': 'Schroder Dana Prestasi', 'LP63505558': 'Schroder Dana Prestasi Plus', 'LP63505427': 'TRIM Kapital'}
    
    default_tickers_bond = ['LP68209699', 'LP68455734', 'LP65023681', 'LP68505148', 'LP65077754', 'LP68190879', 'LP68213698', 'LP65077739', 'LP65077899', 'LP63505468', 'LP65077900', 'LP68626953', 'LP65108953', 'LP68784787', 'LP65077841', 'LP65023680', 'LP68553197']
    default_map_bond = {'LP68209699': 'Ashmore Dana Obligasi Nusantara Kelas A', 'LP68455734': 'Ashmore Dana Obligasi Unggulan Nusantara Kelas A', 'LP65023681': 'Batavia Dana Obligasi Ultima', 'LP68505148': 'BNP Paribas Obligasi Cemerlang', 'LP65077754': 'BNP Paribas Prima II kelas RK1', 'LP68190879': 'Eastspring Investments IDR High Grade Kelas A', 'LP68213698': 'Eastspring Investments Yield Discovery Kelas A', 'LP65077739': 'Mandiri Investa Dana Utama Kelas A', 'LP65077899': 'Manulife Obligasi Negara Indonesia II Kelas A', 'LP63505468': 'Manulife Obligasi Unggulan Kelas A', 'LP65077900': 'Manulife Pendapatan Bulanan II', 'LP68626953': 'Maybank Dana Obligasi Negara', 'LP65108953': 'Maybank Dana Pasti 2', 'LP68784787': 'Maybank Obligasi Syariah Negara', 'LP65077841': 'Schroder Dana Andalan II', 'LP65023680': 'Schroder Dana Mantap Plus II', 'LP68553197': 'Trimegah Fixed Income Plan'}
    
    # Gabungkan dengan custom products jika ada
    tickers_equity = default_tickers_equity.copy()
    map_ticker_equity = default_map_equity.copy()
    
    if custom_equity_tuple:
        custom_equity = dict(custom_equity_tuple)
        for ticker, name in custom_equity.items():
            if ticker not in tickers_equity:
                tickers_equity.append(ticker)
                map_ticker_equity[ticker] = name
    
    tickers_bond = default_tickers_bond.copy()
    map_ticker_bond = default_map_bond.copy()
    
    if custom_bond_tuple:
        custom_bond = dict(custom_bond_tuple)
        for ticker, name in custom_bond.items():
            if ticker not in tickers_bond:
                tickers_bond.append(ticker)
                map_ticker_bond[ticker] = name
    
    tickers_index_saham = ['.JKSE', '.JKLQ45', '.JKIDX80', '.JKIDX30']
    tickers_suku_bunga = ['ID10YT=RR', 'US10YT=RR']
    tickers_mata_uang = ['IDR=']
    tickers_komoditas = ['CLc1']

    # --- Fungsi Ekstraksi yang melempar ERROR jika gagal ---
    # --- Fungsi Ekstraksi yang melempar ERROR jika gagal ---
    def fetch_and_clean_data(ticker_list, field, params, name="Data", batch_size=10):
        if not ticker_list: return pd.DataFrame()
        
        all_dfs = []
        
        # Pecah daftar ticker menjadi batch kecil
        for i in range(0, len(ticker_list), batch_size):
            batch = ticker_list[i:i + batch_size]
            try:
                df_batch = rd.get_data(universe=batch, fields=field, parameters=params)
                if not df_batch.empty:
                    all_dfs.append(df_batch)
            except Exception as e:
                st.warning(f"Gagal menarik sebagian data {name} (Batch {i//batch_size + 1}): {e}")
                
        if not all_dfs:
            raise ValueError(f"{name} kosong dari API Refinitiv setelah semua percobaan.")
            
        # Gabungkan semua batch menjadi satu DataFrame
        df = pd.concat(all_dfs, ignore_index=True)
        
        # Standardisasi nama kolom (seperti perbaikan sebelumnya)
        rename_map = {
            'Net Asset Value': 'NAV',
            'TR.NETASSETVAL': 'NAV',
            'TR.NETASSETVAL.date': 'Date',
            'Price Close': 'Price Close',
            'TR.PriceClose': 'Price Close',
            'TR.PriceClose.date': 'Date',
            'Bid Yield': 'Bid Yield',
            'TR.BIDYIELD': 'Bid Yield',
            'TR.BIDYIELD.date': 'Date'
        }
        df = df.rename(columns=rename_map)
        
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.drop_duplicates(subset=['Instrument', 'Date'], keep='last')
        return df

    params = {'SDate': START_DATE_STR, 'EDate': END_DATE_STR, 'Frq': FREQ}

    # Ekstrak Data
    df_equity_raw = fetch_and_clean_data(tickers_equity, ['TR.NETASSETVAL.date', 'TR.NETASSETVAL'], params, "Equity")
    df_bond_raw = fetch_and_clean_data(tickers_bond, ['TR.NETASSETVAL.date', 'TR.NETASSETVAL'], params, "Bonds")
    df_index_raw = fetch_and_clean_data(tickers_index_saham, ['TR.PriceClose.date', 'TR.PriceClose'], params, "Indeks")
    df_suku_bunga_raw = fetch_and_clean_data(tickers_suku_bunga, ['TR.BIDYIELD', 'TR.BIDYIELD.date'], params, "Suku Bunga")
    
    # Makro 100% stabil dengan get_history (Tanpa memanggil .date agar tidak duplikat)
    try:
        df_mata_uang_raw = rd.get_history(universe=tickers_mata_uang, fields=['TR.MIDPRICE'], start=START_DATE_STR, end=END_DATE_STR)
        if not df_mata_uang_raw.empty:
            if isinstance(df_mata_uang_raw.columns, pd.MultiIndex):
                df_mata_uang_raw.columns = df_mata_uang_raw.columns.get_level_values(0)
            
            # Paksa nama kolom menjadi nama ticker ('IDR=') agar tidak bentrok saat di-melt
            df_mata_uang_raw.columns = [tickers_mata_uang[0]]
            
            df_mata_uang_raw.index.name = 'Date'
            df_mata_uang_raw = df_mata_uang_raw.reset_index()
            df_mata_uang_raw = df_mata_uang_raw.melt(id_vars=['Date'], var_name='Instrument', value_name='Mid Price')
        else:
            raise ValueError("Mata Uang kosong.")
    except Exception as e:
        raise ValueError(f"Gagal memuat Mata Uang: {e}")

    try:
        df_komoditas_raw = rd.get_history(universe=tickers_komoditas, fields=['TR.SETTLEMENTPRICE'], start=START_DATE_STR, end=END_DATE_STR)
        if not df_komoditas_raw.empty:
            if isinstance(df_komoditas_raw.columns, pd.MultiIndex):
                df_komoditas_raw.columns = df_komoditas_raw.columns.get_level_values(0)
            
            # Paksa nama kolom menjadi nama ticker ('CLc1') agar tidak bentrok
            df_komoditas_raw.columns = [tickers_komoditas[0]]
            
            df_komoditas_raw.index.name = 'Date'
            df_komoditas_raw = df_komoditas_raw.reset_index()
            df_komoditas_raw = df_komoditas_raw.melt(id_vars=['Date'], var_name='Instrument', value_name='Settlement Price')
        else:
            raise ValueError("Komoditas kosong.")
    except Exception as e:
        raise ValueError(f"Gagal memuat Komoditas: {e}")

    # --- Penyejajaran Data (Align) ---
    if df_equity_raw.empty:
        st.error("Data Equity kosong. Tidak dapat melanjutkan.")
        st.stop()
    
    df_equity_pivot = df_equity_raw.pivot(index='Date', columns='Instrument', values='NAV')
    master_dates = df_equity_pivot.dropna(how='all').index

    def align_to_master(df_raw, master_dates, value_col_name):
        if df_raw.empty:
            return pd.DataFrame()
        df_pivot = df_raw.pivot(index='Date', columns='Instrument', values=value_col_name)
        df_aligned_pivot = df_pivot.reindex(master_dates).ffill()
        df_aligned = df_aligned_pivot.reset_index().melt(
            id_vars=['Date'], var_name='Instrument', value_name=value_col_name
        ).dropna()
        return df_aligned.sort_values(['Instrument', 'Date']).reset_index(drop=True)

    df_equity_aligned = align_to_master(df_equity_raw, master_dates, 'NAV')
    df_bond_aligned = align_to_master(df_bond_raw, master_dates, 'NAV')
    df_index_aligned = align_to_master(df_index_raw, master_dates, 'Price Close')
    df_suku_bunga_aligned = align_to_master(df_suku_bunga_raw, master_dates, 'Bid Yield')
    df_mata_uang_aligned = align_to_master(df_mata_uang_raw, master_dates, 'Mid Price')    
    df_komoditas_aligned = align_to_master(df_komoditas_raw, master_dates, 'Settlement Price')

    # --- Mapping Nama dan Pivot untuk Analisis ---
    df_equity_aligned['Instrument'] = df_equity_aligned['Instrument'].map(map_ticker_equity)
    df_bond_aligned['Instrument'] = df_bond_aligned['Instrument'].map(map_ticker_bond)

    def safe_pivot(df, val_col):
        if df.empty or 'Date' not in df.columns:
            return pd.DataFrame()
        return df.pivot(index='Date', columns='Instrument', values=val_col).sort_index()

    df_equity_wide = safe_pivot(df_equity_aligned, 'NAV')
    df_bond_wide = safe_pivot(df_bond_aligned, 'NAV')
    df_index_wide = safe_pivot(df_index_aligned, 'Price Close')
    df_suku_bunga_wide = safe_pivot(df_suku_bunga_aligned, 'Bid Yield')
    df_mata_uang_wide = safe_pivot(df_mata_uang_aligned, 'Mid Price')
    df_komoditas_wide = safe_pivot(df_komoditas_aligned, 'Settlement Price')

    # Gabungkan semua data untuk kemudahan akses
    all_data = {
        'equity': df_equity_wide,
        'bond': df_bond_wide,
        'index': df_index_wide,
        'suku_bunga': df_suku_bunga_wide,
        'mata_uang': df_mata_uang_wide,
        'komoditas': df_komoditas_wide,
    }

    return all_data, start_date, end_date

# ==================== FUNGSI UNTUK MENGHITUNG METRIK DAN SKOR ====================
def calculate_metrics(price_data, benchmark_series, risk_free_rate):
    price_data = price_data.dropna(axis=1, how='all')
    price_data = price_data.ffill().bfill()
    returns = price_data.pct_change().dropna(how='all')

    if returns.empty: return None
        
    total_return = (price_data.iloc[-1] / price_data.iloc[0]) - 1
    
    days = len(price_data)
    annualization_factor = 252 / days if days > 0 else 1
    annualized_return = ((1 + total_return) ** annualization_factor) - 1
    
    volatility = returns.std() * np.sqrt(252)
    excess_return = annualized_return - risk_free_rate
    sharpe_ratio = excess_return / volatility

    bench_returns = benchmark_series.pct_change().dropna()
    
    beta = pd.Series([np.nan] * len(returns.columns), index=returns.columns)
    alpha = pd.Series([np.nan] * len(returns.columns), index=returns.columns)
    
    if not bench_returns.empty:
        combined_returns = pd.concat([returns, bench_returns.rename('MARKET')], axis=1)
        cov_matrix = combined_returns.cov()
        var_market = cov_matrix.loc['MARKET', 'MARKET']
        
        if var_market != 0 and not np.isnan(var_market):
            for col in returns.columns:
                if col in cov_matrix.columns and 'MARKET' in cov_matrix.index:
                    beta[col] = cov_matrix.loc['MARKET', col] / var_market

        combined_prices = pd.concat([price_data, benchmark_series.rename('MARKET')], axis=1).ffill().bfill()
        if 'MARKET' in combined_prices.columns and len(combined_prices) > 0:
            market_return = (combined_prices['MARKET'].iloc[-1] / combined_prices['MARKET'].iloc[0]) - 1
            ann_market_return = ((1 + market_return) ** annualization_factor) - 1
        else:
            ann_market_return = 0
            
        expected_return = risk_free_rate + beta * (ann_market_return - risk_free_rate)
        alpha = annualized_return - expected_return

    metrics_df = pd.DataFrame({
        'Total_Return': total_return,
        'Volatility': volatility,
        'Sharpe_Ratio': sharpe_ratio,
        'Beta': beta,
        'Alpha': alpha
    })
    return metrics_df

def calculate_rolling_timeseries(price_data, benchmark_series, risk_free_rate, window=63):
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

def calculate_ranking_scores(metrics_df, weights=None):
    """
    Menghitung skor komposit untuk peringkat berdasarkan metrik.
    """
    if weights is None:
        weights = {'Total_Return': 0.3, 'Sharpe_Ratio': 0.3, 'Volatility': -0.2, 'Alpha': 0.2}
    
    df_scaled = metrics_df.copy()
    valid_metrics = [col for col in weights.keys() if col in df_scaled.columns]
    
    if not valid_metrics:
        return pd.DataFrame()
    
    for col in valid_metrics:
        if col in df_scaled.columns:
            values = df_scaled[col].dropna()
            if len(values) > 0:
                min_val = values.min()
                max_val = values.max()
                if max_val > min_val:
                    scaled = (df_scaled[col] - min_val) / (max_val - min_val)
                else:
                    scaled = pd.Series([0.5] * len(df_scaled[col]), index=df_scaled.index)
                                
                if weights[col] < 0:
                    scaled = 1 - scaled
                df_scaled[col + '_scaled'] = scaled * abs(weights[col])

    score_cols = [col + '_scaled' for col in valid_metrics if (col + '_scaled') in df_scaled.columns]
    if score_cols:
        df_scaled['Total_Score'] = df_scaled[score_cols].sum(axis=1)
    else:
        df_scaled['Total_Score'] = 0

    return df_scaled[['Total_Score'] + score_cols].sort_values('Total_Score', ascending=False)

def get_7d_ranking_history(price_data, benchmark_series, risk_free_rate):
    """Menghitung history ranking 7 hari terakhir"""
    history_ranks = {}
    if len(price_data) < 7: return pd.DataFrame()
        
    dates = price_data.index[-7:]
    
    for date in dates:
        sliced_prices = price_data.loc[:date]
        sliced_bench = benchmark_series.loc[:date]
        
        if len(sliced_prices) < 10: continue
            
        metrics = calculate_metrics(sliced_prices, sliced_bench, risk_free_rate)
        if metrics is not None and not metrics.empty:
            ranks = calculate_ranking_scores(metrics)
            if not ranks.empty:
                rank_series = pd.Series(range(1, len(ranks) + 1), index=ranks.index)
                date_str = date.strftime('%d/%m')
                history_ranks[date_str] = rank_series
                
    return pd.DataFrame(history_ranks)

def get_detailed_ranking_history(price_data, benchmark_series, risk_free_rate, days=30):
    """Menghitung history ranking untuk 30 hari terakhir dengan streak tracker"""
    if len(price_data) < days:
        days = len(price_data) - 1
    
    history_ranks = {}
    top5_streak = {}
    
    # Ambil 30 tanggal terakhir
    dates = price_data.index[-days:]
    
    for date in dates:
        sliced_prices = price_data.loc[:date]
        sliced_bench = benchmark_series.loc[:date]
        
        if len(sliced_prices) < 10: 
            continue
            
        metrics = calculate_metrics(sliced_prices, sliced_bench, risk_free_rate)
        if metrics is not None and not metrics.empty:
            ranks = calculate_ranking_scores(metrics)
            if not ranks.empty:
                # Buat series ranking (1 = terbaik)
                rank_series = pd.Series(range(1, len(ranks) + 1), index=ranks.index)
                date_str = date.strftime('%d/%m')
                history_ranks[date_str] = rank_series
                
                # Hitung streak untuk top 5 (untuk setiap produk)
                top5_today = set(rank_series[rank_series <= 5].index)
                
                for product in rank_series.index:
                    if product not in top5_streak:
                        top5_streak[product] = 0
                    
                    if product in top5_today:
                        top5_streak[product] += 1
                    else:
                        top5_streak[product] = 0
    
    # Buat dataframe untuk display dengan format bullet points
    history_df = pd.DataFrame(history_ranks)
    
    # Tambahkan kolom streak
    streak_series = pd.Series(top5_streak)
    history_df['Streak_Top5'] = streak_series
    
    return history_df

def get_monthly_rankings(price_data, benchmark_series, risk_free_rate):
    """Menghitung ranking awal bulan lalu dan dua bulan lalu"""
    if len(price_data) < 60:  # Minimal 2 bulan data
        return pd.Series(), pd.Series()
    
    # Tanggal hari ini
    today = price_data.index[-1]
    
    # Cari tanggal awal bulan lalu (1 bulan yang lalu)
    if today.month == 1:
        last_month_start = pd.Timestamp(today.year - 1, 12, 1)
    else:
        last_month_start = pd.Timestamp(today.year, today.month - 1, 1)
    
    # Cari tanggal awal dua bulan lalu
    if today.month <= 2:
        two_months_start = pd.Timestamp(today.year - 1, 12 + (today.month - 2), 1)
    else:
        two_months_start = pd.Timestamp(today.year, today.month - 2, 1)
    
    # Fungsi untuk mendapatkan ranking pada tanggal tertentu
    def get_rank_at_date(target_date):
        # Cari tanggal terdekat yang <= target_date dan ada dalam index
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
    
    # Dapatkan ranking untuk setiap periode
    rank_last_month = get_rank_at_date(last_month_start - pd.Timedelta(days=1))
    rank_two_months_ago = get_rank_at_date(two_months_start - pd.Timedelta(days=1))
    
    return rank_last_month, rank_two_months_ago

def calculate_daily_leaderboard(price_data, days=7):
    """
    Menghitung perubahan peringkat harian dalam seminggu terakhir berdasarkan Return.
    """
    if price_data.shape[0] < days + 1:
        return pd.DataFrame()

    price_today = price_data.iloc[-1]
    # Ganti 0 dengan NaN untuk mencegah eror pembagian tak terhingga (Division by Zero)
    price_7d_ago = price_data.iloc[-1 - days].replace(0, np.nan)
    returns_7d = (price_today / price_7d_ago) - 1

    rank_today = returns_7d.sort_values(ascending=False).reset_index()
    rank_today.columns = ['Instrument', 'Return_7d']
    rank_today.dropna(subset=['Return_7d'], inplace=True) # Buang instrumen yang return-nya NaN
    rank_today['Rank_Today'] = range(1, len(rank_today) + 1)

    if price_data.shape[0] > days + 1:
        price_yesterday = price_data.iloc[-2]
        price_8d_ago = price_data.iloc[-2 - days].replace(0, np.nan)
        returns_yesterday = (price_yesterday / price_8d_ago) - 1
        
        rank_yesterday = returns_yesterday.sort_values(ascending=False).reset_index()
        rank_yesterday.columns = ['Instrument', 'Return_Yesterday']
        rank_yesterday.dropna(subset=['Return_Yesterday'], inplace=True)
        rank_yesterday['Rank_Yesterday'] = range(1, len(rank_yesterday) + 1)

        leaderboard = pd.merge(rank_today, rank_yesterday[['Instrument', 'Rank_Yesterday']], on='Instrument', how='left')
        leaderboard['Rank_Change'] = leaderboard['Rank_Yesterday'] - leaderboard['Rank_Today']
        leaderboard['Rank_Change'] = leaderboard['Rank_Change'].fillna(0) # Tangani instrumen baru yang belum ada kemarin
    else:
        leaderboard = rank_today.copy()
        leaderboard['Rank_Change'] = 0

    return leaderboard[['Instrument', 'Return_7d', 'Rank_Today', 'Rank_Change']]

def ensure_unique_columns(df):
    """Memastikan tidak ada duplikat kolom dengan menambahkan suffix jika perlu"""
    if df.columns.duplicated().any():
        df.columns = pd.Index([f"{col}_{i}" if df.columns.duplicated()[i] else col 
                               for i, col in enumerate(df.columns)])
    return df

def validate_ticker(ticker, product_type):
    """Validasi apakah ticker ada di Refinitiv"""
    try:
        df = rd.get_data(universe=[ticker], fields=['TR.NETASSETVAL'])
        return not df.empty
    except:
        return False

# ==================== INISIALISASI SESSION STATE ====================
if 'connected' not in st.session_state:
    st.session_state.connected = False
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

if 'start_date' not in st.session_state:
    st.session_state.start_date = None
if 'end_date' not in st.session_state:
    st.session_state.end_date = dt.datetime.today()
if 'risk_free_rate' not in st.session_state:
    st.session_state.risk_free_rate = 0.065
if 'selected_benchmark_ticker' not in st.session_state:
    st.session_state.selected_benchmark_ticker = '.JKSE'
if 'selected_benchmark_label' not in st.session_state:
    st.session_state.selected_benchmark_label = "IHSG (.JKSE)"
if 'custom_equity' not in st.session_state:
    st.session_state.custom_equity = {}
if 'custom_bond' not in st.session_state:
    st.session_state.custom_bond = {}

# ==================== SIDEBAR ====================
with st.sidebar:
    st.title("⚙️ Pengaturan")
    
    # Bagian Koneksi Refinitiv (menggunakan secrets)
    st.subheader("🔌 Koneksi Refinitiv")
    if st.button("Hubungkan ke Refinitiv Platform"):
        with st.spinner("Menghubungkan..."):
            if init_refinitiv_session():
                st.session_state.connected = True
                st.success("Sesi Refinitiv berhasil dibuka!")

                # Ambil risk-free rate terbaru
                try:
                    df_rf = rd.get_history(universe=['ID10YT=RR'], fields=['TR.BIDYIELD'])
                    if not df_rf.empty:
                        st.session_state.risk_free_rate = float(df_rf['Bid Yield'].iloc[0]) / 100
                        st.info(f"Risk Free Rate terbaru: {st.session_state.risk_free_rate*100:.4f}%")
                except Exception as e:
                    st.warning(f"Gagal menarik Risk Free Rate terbaru. Menggunakan nilai default.")
            else:
                st.session_state.connected = False

    st.divider()
    
    # Pilihan Rentang Waktu untuk Ekstraksi Data
    st.subheader("📅 Tarik Data dari API")
    
    col_d1, col_d2 = st.columns(2)
    with col_d1:
        default_start = dt.datetime.today() - dt.timedelta(days=365)
        input_start_date = st.date_input("Start Date", value=default_start, key="start_date_input")
    with col_d2:
        input_end_date = st.date_input("End Date", value=dt.datetime.today(), key="end_date_input")

    # Tombol Muat Data
    if st.button("📥 Ekstrak Data", type="primary", disabled=not st.session_state.connected):
        if input_start_date >= input_end_date:
            st.error("Start Date harus lebih awal dari End Date!")
        elif st.session_state.connected:
            with st.spinner("Menarik data dari Refinitiv..."):
                try:
                    all_data, start_d, end_d = load_all_data(
                        input_start_date, 
                        input_end_date,
                        tuple(st.session_state.custom_equity.items()),
                        tuple(st.session_state.custom_bond.items())
                    ) 
                    if all_data:
                        # Tidak lagi menduplikasi all_data ke session_state
                        st.session_state.start_date = start_d
                        st.session_state.end_date = end_d
                        st.session_state.data_loaded = True
                        st.success("Data berhasil dimuat!")
                        st.rerun()
                except Exception as e:
                    st.error(f"❌ Pemuatan Dibatalkan: {e}. Silakan periksa koneksi.")
        else:
            st.warning("Hubungkan ke Refinitiv terlebih dahulu.")

    st.divider()
    
    # Manajemen Produk Kustom
    if st.session_state.connected:
        st.subheader("➕ Tambah/Hapus Produk")
        
        # Tab untuk tambah produk
        tab_add_equity, tab_add_bond, tab_delete = st.tabs(["Tambah Equity", "Tambah Bond", "Hapus Produk"])
        
        with tab_add_equity:
            with st.form("form_add_equity"):
                new_eq_ticker = st.text_input("Ticker Equity", placeholder="Contoh: LP68059065")
                new_eq_name = st.text_input("Nama Equity", placeholder="Contoh: Allianz Alpha Sector Rotation")
                
                if st.form_submit_button("Validasi & Tambah"):
                    if new_eq_ticker and new_eq_name:
                        with st.spinner("Memvalidasi ticker..."):
                            if validate_ticker(new_eq_ticker, "Equity"):
                                st.session_state.custom_equity[new_eq_ticker] = new_eq_name
                                st.success(f"✅ {new_eq_name} berhasil ditambahkan!")
                                st.info("Silakan muat ulang data untuk melihat produk baru.")
                            else:
                                st.error("❌ Ticker tidak ditemukan di Refinitiv!")
                    else:
                        st.warning("Harap isi ticker dan nama produk.")
        
        with tab_add_bond:
            with st.form("form_add_bond"):
                new_bd_ticker = st.text_input("Ticker Bond", placeholder="Contoh: LP68209699")
                new_bd_name = st.text_input("Nama Bond", placeholder="Contoh: Ashmore Dana Obligasi Nusantara")
                
                if st.form_submit_button("Validasi & Tambah"):
                    if new_bd_ticker and new_bd_name:
                        with st.spinner("Memvalidasi ticker..."):
                            if validate_ticker(new_bd_ticker, "Bond"):
                                st.session_state.custom_bond[new_bd_ticker] = new_bd_name
                                st.success(f"✅ {new_bd_name} berhasil ditambahkan!")
                                st.info("Silakan muat ulang data untuk melihat produk baru.")
                            else:
                                st.error("❌ Ticker tidak ditemukan di Refinitiv!")
                    else:
                        st.warning("Harap isi ticker dan nama produk.")
        
        with tab_delete:
            st.subheader("🗑️ Hapus Produk Kustom")
            
            # Gabungkan semua produk kustom
            all_custom = {}
            all_custom.update({f"Equity: {v}": k for k, v in st.session_state.custom_equity.items()})
            all_custom.update({f"Bond: {v}": k for k, v in st.session_state.custom_bond.items()})
            
            if all_custom:
                product_to_delete_display = st.selectbox("Pilih produk untuk dihapus", list(all_custom.keys()))
                if st.button("Hapus Produk", type="secondary"):
                    ticker_to_delete = all_custom[product_to_delete_display]
                    if "Equity:" in product_to_delete_display:
                        del st.session_state.custom_equity[ticker_to_delete]
                    else:
                        del st.session_state.custom_bond[ticker_to_delete]
                    st.success(f"Produk {product_to_delete_display} dihapus!")
                    st.info("Silakan muat ulang data untuk melihat perubahan.")
            else:
                st.info("Belum ada produk kustom.")
    
    st.divider()
    
    # Tampilkan Pilihan Interval Analisis HANYA SETELAH DATA DIMUAT
    if st.session_state.data_loaded:
        st.subheader("⏱️ Interval Analisis")
        date_option = st.selectbox(
            "Pilih Interval Metrik & Korelasi",
            options=["3 Bulan", "6 Bulan", "1 Tahun", "Gunakan Semua Data (Sesuai Kalender)"],
            index=3,
            key="interval_analisis"
        )
        
        st.subheader("🎯 Parameter Benchmark")
        benchmark_options = {
            'IHSG (.JKSE)': '.JKSE',
            'LQ45 (.JKLQ45)': '.JKLQ45',
            'IDX30 (.JKIDX30)': '.JKIDX30',
            'IDX80 (.JKIDX80)': '.JKIDX80',
            'Minyak Mentah (CLc1)': 'CLc1',
            'Kurs IDR (IDR=)': 'IDR=',
            'Suku Bunga 10Y (ID10YT=RR)': 'ID10YT=RR',
            'Suku Bunga 10Y US (US10YT=RR)': 'US10YT=RR'
        }
        selected_bench_label = st.selectbox("Pilih Benchmark untuk Beta & Alpha", list(benchmark_options.keys()), key="benchmark_select")
        selected_benchmark_ticker = benchmark_options[selected_bench_label]
        
        # Simpan ke session state
        st.session_state.selected_benchmark_ticker = selected_benchmark_ticker
        st.session_state.selected_benchmark_label = selected_bench_label
    else:
        date_option = "1 Tahun" 
        selected_benchmark_ticker = ".JKSE"
        selected_benchmark_label = "IHSG (.JKSE)"
        
        # Simpan default ke session state
        st.session_state.selected_benchmark_ticker = selected_benchmark_ticker
        st.session_state.selected_benchmark_label = selected_benchmark_label

    st.sidebar.divider()
    st.sidebar.caption("© 2024 Investment Dashboard")

# ==================== HALAMAN UTAMA ====================
st.title("📊 Investment Dashboard - Reksa Dana Indonesia")
if st.session_state.start_date and st.session_state.end_date:
    st.markdown(f"Periode Data: {st.session_state.start_date.strftime('%d %b %Y')} s/d {st.session_state.end_date.strftime('%d %b %Y')}")

if not st.session_state.data_loaded:
    st.info("👈 Silakan hubungkan ke Refinitiv dan muat data dari sidebar untuk memulai.")
    st.stop()

# ==================== AMBIL DATA DARI SESSION STATE ====================
# ==================== AMBIL DATA DARI CACHE ====================
all_data, _, _ = load_all_data(
    st.session_state.start_date, 
    st.session_state.end_date,
    tuple(st.session_state.custom_equity.items()),
    tuple(st.session_state.custom_bond.items())
)
df_equity_full = all_data['equity']
df_bond_full = all_data['bond']
df_index_full = all_data['index']
df_komoditas_full = all_data['komoditas']
df_mata_uang_full = all_data['mata_uang']
df_suku_bunga_full = all_data['suku_bunga']

risk_free_rate = st.session_state.risk_free_rate

# Gunakan benchmark dari session state
selected_benchmark_ticker = st.session_state.selected_benchmark_ticker

# ==================== EKSTRAK BENCHMARK SERIES ====================
def get_benchmark_series(ticker, dfs_dict):
    for df in dfs_dict.values():
        if ticker in df.columns:
            return df[ticker]
    return pd.Series()

full_dfs_dict = {
    'index': df_index_full, 'komoditas': df_komoditas_full, 
    'mata_uang': df_mata_uang_full, 'suku_bunga': df_suku_bunga_full
}
benchmark_series_full = get_benchmark_series(selected_benchmark_ticker, full_dfs_dict)

if benchmark_series_full.empty:
    st.warning(f"Data benchmark {selected_benchmark_ticker} tidak tersedia. Kalkulasi Beta & Alpha disetel ke 0.")
    benchmark_series_full = pd.Series(0.0, index=df_equity_full.index)

# ==================== SLICING DATA BERDASARKAN RENTANG WAKTU DROPDOWN ====================
if date_option == "3 Bulan":
    cutoff_days = 90
elif date_option == "6 Bulan":
    cutoff_days = 180
elif date_option == "1 Tahun":
    cutoff_days = 365
else:
    cutoff_days = (pd.to_datetime(st.session_state.end_date) - pd.to_datetime(st.session_state.start_date)).days 

cutoff_date = pd.to_datetime(st.session_state.end_date) - dt.timedelta(days=cutoff_days)

def safe_slice(df, cutoff):
    if df.empty: return df
    return df[df.index >= cutoff]

# Potong masing-masing instrumen sesuai interval terpilih
df_equity = safe_slice(df_equity_full, cutoff_date)
df_bond = safe_slice(df_bond_full, cutoff_date)
df_index = safe_slice(df_index_full, cutoff_date)
df_komoditas = safe_slice(df_komoditas_full, cutoff_date)
df_mata_uang = safe_slice(df_mata_uang_full, cutoff_date)
df_suku_bunga = safe_slice(df_suku_bunga_full, cutoff_date)

# Gabungkan instrumen inti (Equity, Bond, Index) untuk perhitungan keseluruhan
df_all_instruments = pd.concat([df_equity, df_bond, df_index], axis=1)
df_all_instruments = ensure_unique_columns(df_all_instruments)

benchmark_series_sliced = safe_slice(benchmark_series_full, cutoff_date)

df_all_instruments_full = ensure_unique_columns(pd.concat([df_equity_full, df_bond_full, df_index_full], axis=1))

# Hitung metrik bergulir
rolling_ts_dict = calculate_rolling_timeseries(df_all_instruments_full, benchmark_series_full, risk_free_rate, window=21)
sliced_ts_dict = {k: safe_slice(v, cutoff_date) for k, v in rolling_ts_dict.items()}

# Hitung metrik keseluruhan untuk semua instrumen
metrics_all = calculate_metrics(df_all_instruments, benchmark_series_sliced, risk_free_rate)

if metrics_all is None or metrics_all.empty:
    st.error(f"Gagal menghitung metrik untuk periode {date_option}. Data mungkin tidak mencukupi.")
    st.stop()

# Hitung metrik untuk Equity saja
metrics_equity = None
if not df_equity.empty:
    df_equity_with_benchmark = pd.concat([df_equity, benchmark_series_sliced.to_frame(name='BENCHMARK')], axis=1)
    metrics_equity = calculate_metrics(df_equity, benchmark_series_sliced, risk_free_rate)

# Hitung metrik untuk Bond saja
metrics_bond = None
if not df_bond.empty:
    metrics_bond = calculate_metrics(df_bond, benchmark_series_sliced, risk_free_rate)

# ==================== HITUNG SKOR PERINGKAT ====================
ranked_products_all = calculate_ranking_scores(metrics_all) if metrics_all is not None else pd.DataFrame()
ranked_products_equity = calculate_ranking_scores(metrics_equity) if metrics_equity is not None else pd.DataFrame()
ranked_products_bond = calculate_ranking_scores(metrics_bond) if metrics_bond is not None else pd.DataFrame()

# ==================== HITUNG LEADERBOARD HARIAN ====================
leaderboard_daily = calculate_daily_leaderboard(df_all_instruments, days=7)

# ==================== TABS ====================
tab_overview, tab_ranking_30d, tab_leaderboard_split, tab_correlation, tab_performance_equity, tab_performance_bond, tab_compare = st.tabs([
    "📋 Ringkasan", 
    "📊 Ranking 30 Hari", 
    "🏆 Leaderboard", 
    "📈 Korelasi", 
    "📊 Performa Equity", 
    "📈 Performa Bond", 
    "📉 Perbandingan Historis"
])

# --- Tab 1: Ringkasan ---
with tab_overview:
    st.header("Ringkasan Pasar & Instrumen")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Jumlah Equity", len(df_equity.columns))
    with col2:
        st.metric("Jumlah Bonds", len(df_bond.columns))
    with col3:
        st.metric("Periode (Hari)", df_all_instruments.shape[0])
    with col4:
        st.metric("Risk-Free Rate", f"{risk_free_rate*100:.2f}%")

    st.subheader("Top 10 Produk Semua Kategori (Skor Tertinggi & Riwayat Peringkat 7 Hari)")
    if not ranked_products_all.empty:
        with st.spinner("Mengkalkulasi jejak peringkat 7 hari terakhir..."):
            history_ranks = get_7d_ranking_history(df_all_instruments, benchmark_series_sliced, risk_free_rate)
            
        top_10 = ranked_products_all.head(10).reset_index()
        
        if 'index' in top_10.columns:
            top_10 = top_10.rename(columns={'index': 'Instrument'})
            
        top_10['Total_Score'] = top_10['Total_Score'].round(3)
        
        # Menggabungkan Data Top 10 Hari Ini dengan History Peringkat 7 Hari
        if not history_ranks.empty:
            top_10 = top_10.merge(history_ranks, left_on='Instrument', right_index=True, how='left')
            cols_to_show = ['Instrument', 'Total_Score'] + list(history_ranks.columns)
        else:
            cols_to_show = ['Instrument', 'Total_Score']
            
        st.dataframe(top_10[cols_to_show], use_container_width=True, hide_index=True)
    else:
        st.warning("Tidak ada data peringkat.")

    st.subheader("📈 Pergerakan IHSG (Indeks)")
    if '.JKSE' in df_index.columns:
        fig_ihsg = px.line(df_index, x=df_index.index, y='.JKSE', title='IHSG - Harga Historis')
        st.plotly_chart(fig_ihsg, use_container_width=True)
    else:
        st.warning("Data IHSG tidak tersedia.")

# --- Tab 2: Ranking 30 Hari dengan Bullet Points ---
with tab_ranking_30d:
    st.header("📊 Ranking Tracker 30 Hari Terakhir")
    
    # Pilih tipe produk
    rank_type = st.radio("Pilih Tipe Produk", ["Equity", "Bonds"], horizontal=True, key="rank_30d_type")
    
    if rank_type == "Equity":
        df_tracker = df_equity
        title = "Equity"
    else:
        df_tracker = df_bond
        title = "Bonds"
    
    if not df_tracker.empty and len(df_tracker) >= 30:
        with st.spinner(f"Menghitung history ranking 30 hari untuk {title}..."):
            # Dapatkan history ranking 30 hari
            history_df = get_detailed_ranking_history(
                df_tracker, benchmark_series_sliced, risk_free_rate, days=30
            )
            
            if not history_df.empty and len(history_df.columns) > 1:
                # Pisahkan streak dan data ranking
                if 'Streak_Top5' in history_df.columns:
                    streak_data = history_df['Streak_Top5']
                    rank_data = history_df.drop(columns=['Streak_Top5'])
                else:
                    streak_data = pd.Series(0, index=history_df.index)
                    rank_data = history_df
                
                # Dapatkan ranking bulan lalu dan dua bulan lalu
                rank_last_month, rank_two_months = get_monthly_rankings(
                    df_tracker, benchmark_series_sliced, risk_free_rate
                )
                
                st.subheader(f"🏆 Ranking {title} - 30 Hari Terakhir")
                st.caption("Angka = Peringkat (1 = Terbaik). Warna Hijau = Top 5, Merah = Bottom 5")
                
                # Buat dataframe untuk display dengan format bullet points
                display_data = []
                
                for product in rank_data.index:
                    row = {'Produk': product}
                    
                    # Tambahkan ranking bulan lalu dan dua bulan lalu
                    if not rank_last_month.empty and product in rank_last_month.index:
                        row['Rank Akhir Bulan Lalu'] = f"{int(rank_last_month[product])}"
                    else:
                        row['Rank Akhir Bulan Lalu'] = "-"
                    
                    if not rank_two_months.empty and product in rank_two_months.index:
                        row['Rank Akhir 2 Bulan Lalu'] = f"{int(rank_two_months[product])}"
                    else:
                        row['Rank Akhir 2 Bulan Lalu'] = "-"
                    
                    # Tambahkan streak
                    streak_val = streak_data.get(product, 0)
                    row['Streak Top 5'] = f"{streak_val}/30" if streak_val > 0 else "-"
                    
                    # Tambahkan ranking 30 hari sebagai bullet points
                    for date in rank_data.columns[:30]:  # Max 30 hari
                        if date in rank_data.columns and pd.notna(rank_data.loc[product, date]):
                            rank_val = int(rank_data.loc[product, date])
                            # Format dengan warna berdasarkan nilai
                            if rank_val <= 5:
                                row[date] = f"🟢 {rank_val}"
                            elif rank_val >= len(rank_data) - 4:
                                row[date] = f"🔴 {rank_val}"
                            else:
                                row[date] = f"⚪ {rank_val}"
                        else:
                            row[date] = "-"
                    
                    display_data.append(row)
                
                if display_data:
                    display_df = pd.DataFrame(display_data)
                    
                    # Urutkan kolom: Produk, Rank bulan lalu, Rank 2 bulan lalu, Streak, lalu tanggal-tanggal
                    date_cols = [col for col in display_df.columns if col not in ['Produk', 'Rank Akhir Bulan Lalu', 'Rank Akhir 2 Bulan Lalu', 'Streak Top 5']]
                    col_order = ['Produk', 'Rank Akhir Bulan Lalu', 'Rank Akhir 2 Bulan Lalu', 'Streak Top 5'] + date_cols
                    display_df = display_df[col_order]
                    
                    # Tampilkan dengan styling
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                    
                    # Statistik streak
                    st.subheader("🔥 Top Performers - Streak Terpanjang di Top 5")
                    streak_stats = []
                    for product in streak_data.index:
                        if streak_data[product] > 0:
                            streak_stats.append({
                                'Produk': product,
                                'Streak Top 5': f"{streak_data[product]}/30",
                                'Hari Berturut-turut': streak_data[product]
                            })
                    
                    if streak_stats:
                        streak_df = pd.DataFrame(streak_stats)
                        streak_df = streak_df.sort_values('Hari Berturut-turut', ascending=False).head(10)
                        st.dataframe(streak_df[['Produk', 'Streak Top 5']], use_container_width=True, hide_index=True)
                    else:
                        st.info("Belum ada produk yang masuk Top 5 dalam 30 hari terakhir.")
            else:
                st.warning("Data tidak cukup untuk menampilkan history ranking 30 hari.")
    else:
        st.warning(f"Data {title} tidak mencukupi (minimal 30 hari data).")

# --- Tab 3: Leaderboard Terpisah Equity & Bonds ---
with tab_leaderboard_split:
    st.header("🏆 Leaderboard & Rekomendasi Produk")
    
    lb_type = st.radio("Pilih Tipe Leaderboard", ["Equity", "Bonds"], horizontal=True, key="lb_split_type")
    
    if lb_type == "Equity":
        df_lb = df_equity
        title = "Equity"
    else:
        df_lb = df_bond
        title = "Bonds"
    
    if not df_lb.empty:
        leaderboard = calculate_daily_leaderboard(df_lb, days=7)
         
        if not leaderboard.empty:
            # Tambahkan warna untuk perubahan
            leaderboard['Change_Color'] = leaderboard['Rank_Change'].apply(
                lambda x: '🚀 Top Climber' if x > 0 else ('📉 Top Laggard' if x < 0 else 'Stabil')
            )
            leaderboard['Return_7d'] = leaderboard['Return_7d'] * 100
            leaderboard_display = leaderboard[['Instrument', 'Return_7d', 'Rank_Today', 'Rank_Change', 'Change_Color']].copy()
            leaderboard_display['Return_7d'] = leaderboard_display['Return_7d'].round(2).astype(str) + '%'

            col1, col2 = st.columns(2)

            with col1:
                st.subheader(f"🚀 Top Climbers {title}")
                climbers = leaderboard_display[leaderboard_display['Rank_Change'] > 0].sort_values('Rank_Change', ascending=False).head(10)
                if not climbers.empty:
                    st.dataframe(climbers, hide_index=True, use_container_width=True)
                else:
                    st.info("Tidak ada top climbers dalam periode ini.")

            with col2:
                st.subheader(f"📉 Top Laggards {title}")
                laggards = leaderboard_display[leaderboard_display['Rank_Change'] < 0].sort_values('Rank_Change', ascending=True).head(10)
                if not laggards.empty:
                    st.dataframe(laggards, hide_index=True, use_container_width=True)
                else:
                    st.info("Tidak ada top laggards dalam periode ini.")

            st.subheader(f"📋 Leaderboard Lengkap {title}")
            st.dataframe(leaderboard_display.sort_values('Rank_Today'), hide_index=True, use_container_width=True)
            
            # Rekomendasi berdasarkan tipe
            st.subheader(f"💡 Rekomendasi Produk {title}")
            
            # Top 3 berdasarkan ranking hari ini
            top_3 = leaderboard_display.nsmallest(3, 'Rank_Today')
            
            # Produk dengan kenaikan terbesar
            top_climbers = leaderboard_display[leaderboard_display['Rank_Change'] > 0].nlargest(3, 'Rank_Change')
            
            col_rec1, col_rec2 = st.columns(2)
            
            with col_rec1:
                st.markdown("**🏅 Top 3 Ranking Saat Ini**")
                for idx, row in top_3.iterrows():
                    st.markdown(f"• **{row['Instrument']}** - Return 7d: {row['Return_7d']}")
            
            with col_rec2:
                st.markdown("**📈 Produk dengan Momentum Terbaik**")
                for idx, row in top_climbers.iterrows():
                    st.markdown(f"• **{row['Instrument']}** - Naik {row['Rank_Change']} peringkat")
        else:
            st.warning(f"Data tidak mencukupi untuk menghitung leaderboard {title}.")
    else:
        st.warning(f"Tidak ada data {title}.")

# --- Tab 4: Korelasi ---
with tab_correlation:
    st.header("Analisis Korelasi")

    col_corr1, col_corr2 = st.columns(2)
    with col_corr1:
        grup1 = st.selectbox("Pilih Grup Aset 1 (Sumbu Y)", options=["Equity", "Bonds"], key="corr_grup1")
    with col_corr2:
        grup2 = st.selectbox("Pilih Grup Aset 2 (Sumbu X)", options=["Equity", "Bonds", "Indeks", "Komoditas", "Mata Uang", "Suku Bunga"], key="corr_grup2")

    # Mapping pilihan ke DataFrame
    dict_dfs = {
        "Equity": df_equity,
        "Bonds": df_bond,
        "Indeks": df_index,
        "Komoditas": df_komoditas,
        "Mata Uang": df_mata_uang,
        "Suku Bunga": df_suku_bunga
    }

    df_grup1 = dict_dfs[grup1]
    df_grup2 = dict_dfs[grup2]

    # Menghitung return masing-masing grup
    returns_grup1 = df_grup1.dropna(axis=1, how='all').ffill().bfill().pct_change().dropna(how='all')
    returns_grup2 = df_grup2.dropna(axis=1, how='all').ffill().bfill().pct_change().dropna(how='all')
    
    # Samakan irisan tanggal
    returns_grup1, returns_grup2 = returns_grup1.align(returns_grup2, join='inner', axis=0)

    if not returns_grup1.empty and not returns_grup2.empty:
        if grup1 == grup2:
            title = f"Matriks Korelasi Antar {grup1}"
            corr_matrix = returns_grup1.corr()
            # Masking bagian atas matriks (menyisakan segitiga bawah dan diagonal)
            mask_plot = np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
            corr_matrix_plot = corr_matrix.mask(mask_plot)
        else:
            title = f"Matriks Korelasi {grup1} vs {grup2}"
            corr_dict = {}
            for col2 in returns_grup2.columns:
                corr_dict[col2] = returns_grup1.apply(lambda x: x.corr(returns_grup2[col2]))
            corr_matrix = pd.DataFrame(corr_dict)
            corr_matrix_plot = corr_matrix.copy()

        # Plot heatmap dengan skema warna Merah ke Hijau (RdYlGn)
        fig_corr = px.imshow(
            corr_matrix_plot, text_auto='.2f', aspect="auto",
            color_continuous_scale='RdYlGn', zmin=-1, zmax=1, title=title,
            labels=dict(y=f"Grup 1: {grup1}", x=f"Grup 2: {grup2}", color="Korelasi")
        )
        fig_corr.update_layout(height=800)
        st.plotly_chart(fig_corr, use_container_width=True)

        # Tampilkan korelasi tertinggi dan terendah
        corr_matrix.index.name = 'Asset_1'
        corr_matrix.columns.name = 'Asset_2'
        
        if grup1 == grup2:
            mask_table = np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
            corr_long = corr_matrix.where(mask_table).stack().reset_index()
        else:
            corr_long = corr_matrix.stack().reset_index()

        corr_long.columns = ['Asset_1', 'Asset_2', 'Correlation']
        corr_long = corr_long.dropna()

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🔥 Top 5 Korelasi Positif Tertinggi")
            top_corr = corr_long.sort_values('Correlation', ascending=False).head(5)
            st.dataframe(top_corr, hide_index=True, use_container_width=True)
        with col2:
            st.subheader("❄️ Top 5 Korelasi Terendah")
            bottom_corr = corr_long.sort_values('Correlation', ascending=True).head(5)
            st.dataframe(bottom_corr, hide_index=True, use_container_width=True)
    else:
        st.warning("Data tidak cukup untuk analisis korelasi pada kombinasi grup ini.")
        
# --- Tab 5: Performa & Skor Equity ---
with tab_performance_equity:
    st.header("📊 Performa & Peringkat Produk Equity")
    
    if not df_equity.empty and ranked_products_equity is not None and not ranked_products_equity.empty:
        full_performance = metrics_equity.merge(ranked_products_equity[['Total_Score']], left_index=True, right_index=True, how='left')
        full_performance = full_performance.round(4)

        st.subheader("Semua Metrik dan Skor - Equity")
        st.dataframe(full_performance, use_container_width=True)

        st.subheader("🏆 Mengapa Produk Equity Ini Unggul?")
        top_5 = ranked_products_equity.head(5).index.tolist()
        if top_5:
            selected_top = st.selectbox("Pilih Top 5 Produk Equity untuk Analisis", top_5, key="perf_select_equity")

            if selected_top:
                st.write(f"**Analisis Mendalam untuk: {selected_top}**")
                metrics_top = metrics_equity.loc[selected_top]

                reasons = []
                
                # Cek Return
                if pd.notna(metrics_top['Total_Return']):
                    avg_ret = metrics_equity['Total_Return'].mean() * 100
                    val_ret = metrics_top['Total_Return'] * 100
                    if val_ret > avg_ret:
                        reasons.append(f"✅ **Pertumbuhan Kuat (Return)**: Mencetak profit sebesar **{val_ret:.2f}%**, mengungguli rata-rata equity lain **{avg_ret:.2f}%**.")

                # Cek Sharpe
                if pd.notna(metrics_top['Sharpe_Ratio']):
                    avg_sharpe = metrics_equity['Sharpe_Ratio'].mean()
                    val_sharpe = metrics_top['Sharpe_Ratio']
                    if val_sharpe > avg_sharpe:
                        reasons.append(f"✅ **Risiko Sepadan (Sharpe)**: Rasio Sharpe **{val_sharpe:.2f}** > rata-rata **{avg_sharpe:.2f}**.")

                # Cek Volatilitas
                if pd.notna(metrics_top['Volatility']):
                    avg_vol = metrics_equity['Volatility'].mean() * 100
                    val_vol = metrics_top['Volatility'] * 100
                    if val_vol < avg_vol:
                        reasons.append(f"✅ **Stabil (Volatility)**: Fluktuasi **{val_vol:.2f}%** < rata-rata **{avg_vol:.2f}%**.")

                # Cek Alpha
                if pd.notna(metrics_top['Alpha']):
                    avg_alpha = metrics_equity['Alpha'].mean() * 100
                    val_alpha = metrics_top['Alpha'] * 100
                    if val_alpha > avg_alpha:
                        reasons.append(f"✅ **Alpha Positif**: **{val_alpha:.2f}%** > rata-rata **{avg_alpha:.2f}%**.")
                
                if not reasons:
                    reasons.append("ℹ️ Produk ini unggul karena kombinasi metrik yang stabil.")

                for r in reasons:
                    st.markdown(r)

                # Tampilkan grafik harga historis
                st.subheader("Harga Historis")
                if selected_top in df_equity.columns:
                    fig_price = px.line(df_equity, x=df_equity.index, y=selected_top, title=f'Harga {selected_top}')
                    st.plotly_chart(fig_price, use_container_width=True)
    else:
        st.warning("Tidak ada data equity atau data tidak mencukupi untuk perhitungan.")

# --- Tab 6: Performa & Skor Bond ---
with tab_performance_bond:
    st.header("📈 Performa & Peringkat Produk Bond")
    
    if not df_bond.empty and ranked_products_bond is not None and not ranked_products_bond.empty:
        full_performance = metrics_bond.merge(ranked_products_bond[['Total_Score']], left_index=True, right_index=True, how='left')
        full_performance = full_performance.round(4)

        st.subheader("Semua Metrik dan Skor - Bond")
        st.dataframe(full_performance, use_container_width=True)

        st.subheader("🏆 Mengapa Produk Bond Ini Unggul?")
        top_5 = ranked_products_bond.head(5).index.tolist()
        if top_5:
            selected_top = st.selectbox("Pilih Top 5 Produk Bond untuk Analisis", top_5, key="perf_select_bond")

            if selected_top:
                st.write(f"**Analisis Mendalam untuk: {selected_top}**")
                metrics_top = metrics_bond.loc[selected_top]

                reasons = []
                
                # Cek Return
                if pd.notna(metrics_top['Total_Return']):
                    avg_ret = metrics_bond['Total_Return'].mean() * 100
                    val_ret = metrics_top['Total_Return'] * 100
                    if val_ret > avg_ret:
                        reasons.append(f"✅ **Pertumbuhan Kuat (Return)**: Mencetak profit sebesar **{val_ret:.2f}%**, mengungguli rata-rata bond lain **{avg_ret:.2f}%**.")

                # Cek Sharpe
                if pd.notna(metrics_top['Sharpe_Ratio']):
                    avg_sharpe = metrics_bond['Sharpe_Ratio'].mean()
                    val_sharpe = metrics_top['Sharpe_Ratio']
                    if val_sharpe > avg_sharpe:
                        reasons.append(f"✅ **Risiko Sepadan (Sharpe)**: Rasio Sharpe **{val_sharpe:.2f}** > rata-rata **{avg_sharpe:.2f}**.")

                # Cek Volatilitas
                if pd.notna(metrics_top['Volatility']):
                    avg_vol = metrics_bond['Volatility'].mean() * 100
                    val_vol = metrics_top['Volatility'] * 100
                    if val_vol < avg_vol:
                        reasons.append(f"✅ **Stabil (Volatility)**: Fluktuasi **{val_vol:.2f}%** < rata-rata **{avg_vol:.2f}%**.")

                # Cek Alpha
                if pd.notna(metrics_top['Alpha']):
                    avg_alpha = metrics_bond['Alpha'].mean() * 100
                    val_alpha = metrics_top['Alpha'] * 100
                    if val_alpha > avg_alpha:
                        reasons.append(f"✅ **Alpha Positif**: **{val_alpha:.2f}%** > rata-rata **{avg_alpha:.2f}%**.")
                
                if not reasons:
                    reasons.append("ℹ️ Produk ini unggul karena kombinasi metrik yang stabil.")

                for r in reasons:
                    st.markdown(r)

                # Tampilkan grafik harga historis
                st.subheader("Harga Historis")
                if selected_top in df_bond.columns:
                    fig_price = px.line(df_bond, x=df_bond.index, y=selected_top, title=f'Harga {selected_top}')
                    st.plotly_chart(fig_price, use_container_width=True)
    else:
        st.warning("Tidak ada data bond atau data tidak mencukupi untuk perhitungan.")

# --- Tab 7: Perbandingan Historis (Dengan 3 Tipe Grafik Berbeda) ---
with tab_compare:
    st.header("📉 Perbandingan Historis Antar Instrumen")
    
    # Pilih tipe grafik
    chart_type = st.radio(
        "Pilih Tipe Grafik",
        ["Harga (Normalisasi)", "Return Kumulatif", "Drawdown"],
        horizontal=True,
        key="chart_type_compare"
    )
    
    available_instruments = df_all_instruments.columns.tolist()
    selected_instruments = st.multiselect(
        "Pilih Instrumen untuk Dibandingkan (minimal 2)",
        options=available_instruments,
        default=available_instruments[:min(3, len(available_instruments))] if available_instruments else [],
        key="compare_multiselect"
    )
    
    if len(selected_instruments) >= 2:
        df_compare = df_all_instruments[selected_instruments].copy()
        df_compare = df_compare.ffill().bfill()
        
        if chart_type == "Harga (Normalisasi)":
            st.subheader("📊 Grafik Harga Historis (Ternormalisasi)")
            # Normalisasi harga ke 100 di awal periode
# Normalisasi harga ke 100 di awal periode, hindari pembagian nol
            first_valid = df_compare.replace(0, np.nan).bfill().iloc[0]
            df_normalized = df_compare.div(first_valid) * 100            
            fig_prices = px.line(
                df_normalized, 
                x=df_normalized.index, 
                y=df_normalized.columns,
                title="Perbandingan Kinerja Historis (100 = Nilai Awal Periode)",
                labels={"value": "Nilai Relatif (100 = Awal)", "Date": "Tanggal", "variable": "Instrumen"}
            )
            st.plotly_chart(fig_prices, use_container_width=True)
            
        elif chart_type == "Return Kumulatif":
            st.subheader("📈 Return Kumulatif")
            # Hitung return kumulatif
            returns_cum = (df_compare.pct_change().fillna(0) + 1).cumprod() - 1
            fig_returns = px.line(
                returns_cum * 100,
                x=returns_cum.index,
                y=returns_cum.columns,
                title="Return Kumulatif (%)",
                labels={"value": "Return Kumulatif (%)", "Date": "Tanggal", "variable": "Instrumen"}
            )
            # Tambahkan garis horizontal di 0
            fig_returns.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
            st.plotly_chart(fig_returns, use_container_width=True)
            
        else:  # Drawdown
            st.subheader("📉 Drawdown")
            # Hitung drawdown
            running_max = df_compare.expanding().max()
            drawdown = (df_compare - running_max) / running_max * 100
            
            fig_dd = px.line(
                drawdown,
                x=drawdown.index,
                y=drawdown.columns,
                title="Drawdown dari Nilai Tertinggi (%)",
                labels={"value": "Drawdown (%)", "Date": "Tanggal", "variable": "Instrumen"}
            )
            fig_dd.update_layout(yaxis_tickformat='.1f')
            st.plotly_chart(fig_dd, use_container_width=True)
        
        st.divider()
        
        # Grafik Metrik Time-Series
        st.subheader("📊 Grafik Pergerakan Metrik Harian")
        
        # Filter hanya instrumen yang dipilih untuk grafik metrik
        ts_data = {}
        for metric_name, ts_df in sliced_ts_dict.items():
            available_cols = [col for col in selected_instruments if col in ts_df.columns]
            if available_cols:
                ts_data[metric_name] = ts_df[available_cols]
        
        if ts_data:
            col1, col2 = st.columns(2)
            
            with col1:
                if 'Alpha' in ts_data and not ts_data['Alpha'].empty:
                    fig_alpha = px.line(ts_data['Alpha'], title="Pergerakan Alpha Historis")
                    fig_alpha.update_layout(xaxis_title="Tanggal", yaxis_title="Alpha", legend_title="Instrumen")
                    st.plotly_chart(fig_alpha, use_container_width=True)
                
                if 'Volatility' in ts_data and not ts_data['Volatility'].empty:
                    fig_vol = px.line(ts_data['Volatility'], title="Pergerakan Volatility Historis")
                    fig_vol.update_layout(xaxis_title="Tanggal", yaxis_title="Volatility", legend_title="Instrumen")
                    st.plotly_chart(fig_vol, use_container_width=True)
                
            with col2:
                if 'Beta' in ts_data and not ts_data['Beta'].empty:
                    fig_beta = px.line(ts_data['Beta'], title="Pergerakan Beta Historis")
                    fig_beta.update_layout(xaxis_title="Tanggal", yaxis_title="Beta", legend_title="Instrumen")
                    st.plotly_chart(fig_beta, use_container_width=True)
                
                if 'Sharpe_Ratio' in ts_data and not ts_data['Sharpe_Ratio'].empty:
                    fig_sharpe = px.line(ts_data['Sharpe_Ratio'], title="Pergerakan Sharpe Ratio Historis")
                    fig_sharpe.update_layout(xaxis_title="Tanggal", yaxis_title="Sharpe Ratio", legend_title="Instrumen")
                    st.plotly_chart(fig_sharpe, use_container_width=True)
        else:
            st.info("Tidak ada data metrik time-series untuk instrumen yang dipilih.")
            
    elif len(selected_instruments) == 1:
        st.info("Pilih setidaknya 2 instrumen untuk perbandingan.")
    else:
        st.info("Silakan pilih instrumen dari dropdown di atas untuk memulai perbandingan.")

# ==================== FOOTER ====================
st.divider()
st.caption("Data disediakan oleh Refinitiv. Dashboard ini untuk tujuan informasi dan edukasi.")