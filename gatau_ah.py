import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import datetime as dt
import refinitiv.data as rd
import time

# ==================== FUNGSI INISIALISASI SESI REFINITIV ====================
def init_refinitiv_session(currency='IDR'):
    """
    Membuka sesi Refinitiv Platform menggunakan kredensial dari st.secrets.
    Parameter currency digunakan untuk memilih risk‑free rate yang sesuai.
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
        
        # Ambil risk‑free rate berdasarkan currency
        # Ambil risk‑free rate berdasarkan currency
         # Ambil risk‑free rate berdasarkan currency
        try:
            if currency == 'USD':
                rf_ticker = 'US10YT=RR'
            else:
                rf_ticker = 'ID10YT=RR'
                
            # Gunakan get_history seperti di gatau_ah.py
            df_rf = rd.get_history(universe=[rf_ticker], fields=['TR.BIDYIELD'])
            if not df_rf.empty:
                st.session_state.risk_free_rate = float(df_rf['Bid Yield'].iloc[0]) / 100
                st.info(f"Risk Free Rate ({'USD' if currency=='USD' else 'IDR'}) terbaru: {st.session_state.risk_free_rate*100:.4f}%")
        except Exception as e:
            st.warning(f"Gagal menarik Risk Free Rate terbaru: {e}. Menggunakan nilai default.")
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

# ==================== DAFTAR TICKER DEFAULT (GLOBAL) ====================
# 1. IDR Equity
default_tickers_equity_idr = [
    'LP68059065', 'LP68452242', 'LP68199650', 'LP68199651', 'LP68683420',
    'LP60090155', 'LP65023619', 'LP68684585', 'LP68790535', 'LP63505420',
    'LP65023683', 'LP63505451', 'LP65077719', 'LP65108951', 'LP63505436',
    'LP68042752', 'LP68166624', 'LP68692654', 'LP68219216', 'LP63505478',
    'LP65077766', 'LP65034334', 'LP68852202', 'LP68794971', 'LP68047313',
    'LP63500685', 'LP63505555', 'LP63505558', 'LP63505427'
]
default_map_equity_idr = {
    'LP68059065': 'Allianz Alpha Sector Rotation Kelas A', 'LP68452242': 'Allianz SRI-Kehati Index',
    'LP68199650': 'Ashmore Dana Ekuitas Nusantara', 'LP68199651': 'Ashmore Dana Progresif Nusantara',
    'LP68683420': 'Ashmore Digital Equity Sustainable', 'LP60090155': 'Batavia Dana Saham',
    'LP65023619': 'Batavia Dana Saham Optimal', 'LP68684585': 'Batavia Disruptive Equity',
    'LP68790535': 'Batavia Index Pefindo I Grade', 'LP63505420': 'BNP Paribas Ekuitas',
    'LP65023683': 'BNP Paribas Infrastruktur Plus', 'LP63505451': 'BNP Paribas Pesona',
    'LP65077719': 'BNP Paribas Pesona Syariah', 'LP65108951': 'BNP Paribas Solaris',
    'LP63505436': 'BRI Mawar', 'LP68042752': 'BRI Mawar Fokus 10',
    'LP68166624': 'Eastspring Investments Alpha Navigator Kelas A', 'LP68692654': 'Eastspring IDX ESG Leaders Plus Kelas A',
    'LP68219216': 'Mandiri Investa Equity ASEAN 5 Plus', 'LP63505478': 'Manulife Dana Saham Kelas A',
    'LP65077766': 'Manulife Saham Andalan', 'LP65034334': 'Maybank Dana Ekuitas',
    'LP68852202': 'Maybank Financial Infobank15 Index Kelas C', 'LP68794971': 'Maybank Financial Infobank15 Index Kelas N',
    'LP68047313': 'Schroder 90 Plus Equity', 'LP63500685': 'Schroder Dana Istimewa',
    'LP63505555': 'Schroder Dana Prestasi', 'LP63505558': 'Schroder Dana Prestasi Plus', 'LP63505427': 'TRIM Kapital'
}

# 2. USD Equity
default_tickers_equity_usd = [
    'LP68783082', 'LP68316907', 'LP68640621', 'LP68819234', 'LP68697607',
    'LP68357499', 'LP68657110', 'LP68591441', 'LP68584853', 'LP68611334',
    'LP68383041', 'LP68129672', 'LP68357264', 'LP68582636', 'LP68358633'
]
default_map_equity_usd = {
    'LP68783082': 'Allianz High Dividend Global Sharia Equity DollarA', 'LP68316907': 'Ashmore Dana USD Equity Nusantara',
    'LP68640621': 'Batavia Global ESG Sharia Equity USD', 'LP68819234': 'Batavia India Sharia Equity USD',
    'LP68697607': 'Batavia Technology Sharia Equity USD', 'LP68357499': 'BNP Paribas Cakra Syariah USD RK1',
    'LP68657110': 'BNP Paribas DJIM Glbl Techno Titans 50 Syariah USD', 'LP68591441': 'BNP Paribas Greater China Equity Syariah USD RK1',
    'LP68584853': 'BRI G20 Sharia Equity Dollar', 'LP68611334': 'Eastspring Syariah Greater China Equity USD A',
    'LP68383041': 'Mandiri Global Sharia Equity Dollar Kelas A', 'LP68129672': 'Manulife Greater Indonesia',
    'LP68357264': 'Manulife Saham Syariah Asia Pasifik Dollar AS', 'LP68582636': 'Manulife Saham Syariah Global Dividen Dolar AS(A3)',
    'LP68358633': 'Schroder Global Sharia Equity (USD)'
}

# 3. IDR Fixed Income
default_tickers_bond_idr = [
    'LP68209699', 'LP68455734', 'LP65023681', 'LP68505148', 'LP65077754',
    'LP68190879', 'LP68213698', 'LP65077739', 'LP65077899', 'LP63505468',
    'LP65077900', 'LP68626953', 'LP65108953', 'LP68784787', 'LP65077841',
    'LP65023680', 'LP68553197'
]
default_map_bond_idr = {
    'LP68209699': 'Ashmore Dana Obligasi Nusantara Kelas A', 'LP68455734': 'Ashmore Dana Obligasi Unggulan Nusantara Kelas A',
    'LP65023681': 'Batavia Dana Obligasi Ultima', 'LP68505148': 'BNP Paribas Obligasi Cemerlang',
    'LP65077754': 'BNP Paribas Prima II kelas RK1', 'LP68190879': 'Eastspring Investments IDR High Grade Kelas A',
    'LP68213698': 'Eastspring Investments Yield Discovery Kelas A', 'LP65077739': 'Mandiri Investa Dana Utama Kelas A',
    'LP65077899': 'Manulife Obligasi Negara Indonesia II Kelas A', 'LP63505468': 'Manulife Obligasi Unggulan Kelas A',
    'LP65077900': 'Manulife Pendapatan Bulanan II', 'LP68626953': 'Maybank Dana Obligasi Negara',
    'LP65108953': 'Maybank Dana Pasti 2', 'LP68784787': 'Maybank Obligasi Syariah Negara',
    'LP65077841': 'Schroder Dana Andalan II', 'LP65023680': 'Schroder Dana Mantap Plus II', 'LP68553197': 'Trimegah Fixed Income Plan'
}

# 4. USD Fixed Income
default_tickers_bond_usd = [
    'LP68058109', 'LP65034330', 'LP68653335', 'LP65108964', 'LP68219210', 'LP65077741'
]
default_map_bond_usd = {
    'LP68058109': 'BNP Paribas Prima USD Kelas RK1', 'LP65034330': 'BRI Melati Premium Dollar',
    'LP68653335': 'Eastspring Syariah Fixed Income USD - Kelas A', 'LP65108964': 'Investa Dana Dollar Mandiri Kelas A',
    'LP68219210': 'Manulife USD Fixed Income Kelas A', 'LP65077741': 'Schroder USD Bond Class A'
}

# ==================== FUNGSI HELPER UNTUK LOADING DATA ====================
@st.cache_data(ttl=43200, show_spinner=False)
def load_all_data(start_date, end_date, currency='IDR', custom_equity_tuple=None, custom_bond_tuple=None):
    START_DATE_STR = start_date.strftime('%Y-%m-%d')
    END_DATE_STR = end_date.strftime('%Y-%m-%d')
    FREQ = 'D'

    if currency == 'USD':
        tickers_equity = default_tickers_equity_usd.copy()
        map_ticker_equity = default_map_equity_usd.copy()
        tickers_bond = default_tickers_bond_usd.copy()
        map_ticker_bond = default_map_bond_usd.copy()
        additional_index_tickers = ['.IXIC', '.SPX', '.DXY', '.SSEC']
    else:
        tickers_equity = default_tickers_equity_idr.copy()
        map_ticker_equity = default_map_equity_idr.copy()
        tickers_bond = default_tickers_bond_idr.copy()
        map_ticker_bond = default_map_bond_idr.copy()
        additional_index_tickers = []

    if currency == 'IDR' and custom_equity_tuple:
        for ticker, name in dict(custom_equity_tuple).items():
            if ticker not in tickers_equity:
                tickers_equity.append(ticker)
                map_ticker_equity[ticker] = name
                
    if currency == 'IDR' and custom_bond_tuple:
        for ticker, name in dict(custom_bond_tuple).items():
            if ticker not in tickers_bond:
                tickers_bond.append(ticker)
                map_ticker_bond[ticker] = name

    tickers_index_saham = ['.JKSE', '.JKLQ45', '.JKIDX80', '.JKIDX30'] + additional_index_tickers
    tickers_suku_bunga = ['ID10YT=RR', 'US10YT=RR']
    tickers_mata_uang = ['IDR=']
    tickers_komoditas = ['CLc1']

    # --- Fungsi Ekstraksi dengan batching dan retry ---
    def fetch_and_clean_data(ticker_list, field, params, name="Data", batch_size=5, max_retries=3):
        if not ticker_list: return pd.DataFrame()
        
        all_dfs = []
        
        for i in range(0, len(ticker_list), batch_size):
            batch = ticker_list[i:i + batch_size]
            success = False
            
            for attempt in range(max_retries):
                try:
                    if i > 0 and attempt == 0:
                        time.sleep(1) 
                        
                    df_batch = rd.get_data(universe=batch, fields=field, parameters=params)
                    
                    if not df_batch.empty:
                        all_dfs.append(df_batch)
                    
                    success = True
                    break  # Keluar dari loop pengulangan kalau berhasil
                    
                except Exception as e:
                    if attempt < max_retries - 1:
                        time.sleep(2 ** (attempt + 1)) 
                    else:
                        st.warning(f"Gagal menarik sebagian {name} (Batch {i//batch_size + 1}) setelah {max_retries} percobaan. Error: {e}")
            
        if not all_dfs:
            raise ValueError(f"{name} kosong dari API Refinitiv setelah semua percobaan gagal.")
            
        df = pd.concat(all_dfs, ignore_index=True)
        
        rename_map = {
            'Net Asset Value': 'NAV',
            'TR.NETASSETVAL': 'NAV',
            'TR.NETASSETVAL.date': 'Date',
            'Price Close': 'Price Close',
            'TR.PriceClose': 'Price Close',
            'TR.PriceClose.date': 'Date',
            'Close Price': 'Close Price',
            'TR.CLOSEPRICE': 'Close Price',
            'TR.CLOSEPRICE.date': 'Date',
            'Bid Yield': 'Bid Yield',
            'TR.BIDYIELD': 'Bid Yield',
            'TR.BIDYIELD.date': 'Date',
            # Tambahan untuk Komoditas dan Mata Uang
            'TR.SettlementPrice': 'Settlement Price',
            'TR.SettlementPrice.date': 'Date',
            'TR.SETTLEMENTPRICE': 'Settlement Price',
            'TR.MidPrice': 'Mid Price',
            'TR.MidPrice.date': 'Date',
            'TR.MIDPRICE': 'Mid Price'
        }
        df = df.rename(columns=rename_map)
        
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.drop_duplicates(subset=['Instrument', 'Date'], keep='last')
        return df

    params = {'SDate': START_DATE_STR, 'EDate': END_DATE_STR, 'Frq': FREQ}

    # Ekstrak Data
   # Ekstrak Data dengan satu pintu fungsi yang tahan banting (ber-retry)
    df_equity_raw = fetch_and_clean_data(tickers_equity, ['TR.NETASSETVAL.date', 'TR.NETASSETVAL'], params, "Equity")
    df_bond_raw = fetch_and_clean_data(tickers_bond, ['TR.NETASSETVAL.date', 'TR.NETASSETVAL'], params, "Fixed Income")
    df_index_raw = fetch_and_clean_data(tickers_index_saham, ['TR.PriceClose.date', 'TR.PriceClose'], params, "Indeks")
    df_suku_bunga_raw = fetch_and_clean_data(tickers_suku_bunga, ['TR.BIDYIELD.date', 'TR.BIDYIELD'], params, "Suku Bunga")
    df_mata_uang_raw = fetch_and_clean_data(tickers_mata_uang, ['TR.MidPrice.date', 'TR.MidPrice'], params, "Mata Uang")
    df_komoditas_raw = fetch_and_clean_data(tickers_komoditas, ['TR.SettlementPrice.date', 'TR.SettlementPrice'], params, "Komoditas")

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
def calculate_metrics(price_data, benchmark_series, risk_free_rate, eval_window=None):
    price_data = price_data.dropna(axis=1, how='all')
    price_data = price_data.ffill().bfill()
    
    # 1. Kalkulasi Return Absolut (Menggunakan seluruh data Cut-off)
    returns_full = price_data.pct_change().dropna(how='all')
    if returns_full.empty: return None
        
    inception = (price_data.iloc[-1] / price_data.iloc[0]) - 1
    
    def get_period_return(df, days):
        if len(df) > days:
            return (df.iloc[-1] / df.iloc[-(days + 1)]) - 1
        else:
            return (df.iloc[-1] / df.iloc[0]) - 1
            
    return_1w = get_period_return(price_data, 5)   
    return_1m = get_period_return(price_data, 22)  
    return_3m = get_period_return(price_data, 63)  
    
    # 2. ISOLASI LENGTH DAYS UNTUK RISIKO & RASIO (Mengikuti Interval)
    if eval_window is not None and len(price_data) > eval_window:
        # Memotong data secara paksa dari bawah (terbaru) sebanyak eval_window
        price_data_risk = price_data.tail(eval_window)
        bench_risk = benchmark_series.tail(eval_window)
    else:
        price_data_risk = price_data
        bench_risk = benchmark_series
        
    returns_risk = price_data_risk.pct_change().dropna(how='all')
    bench_returns_risk = bench_risk.pct_change().dropna()
    
    # days_risk sekarang secara dinamis menjadi 22, 63, 126, atau 252 sesuai pilihan interval Anda
    days_risk = len(price_data_risk) 
    annualization_factor = 252 / days_risk if days_risk > 0 else 1
    
    risk_period_return = (price_data_risk.iloc[-1] / price_data_risk.iloc[0]) - 1 if days_risk > 0 else 0
    annualized_return = ((1 + risk_period_return) ** annualization_factor) - 1
    
    # Volatilitas dihitung murni dari sampel length days interval
    volatility_1sd = returns_risk.std() * np.sqrt(252)
    
    excess_return = annualized_return - risk_free_rate
    sharpe_ratio = excess_return / volatility_1sd 

    beta = pd.Series([np.nan] * len(returns_risk.columns), index=returns_risk.columns)
    alpha = pd.Series([np.nan] * len(returns_risk.columns), index=returns_risk.columns)
    
    if not bench_returns_risk.empty:
        combined_returns = pd.concat([returns_risk, bench_returns_risk.rename('MARKET')], axis=1)
        cov_matrix = combined_returns.cov()
        var_market = cov_matrix.loc['MARKET', 'MARKET']
        
        if var_market != 0 and not np.isnan(var_market):
            for col in returns_risk.columns:
                if col in cov_matrix.columns and 'MARKET' in cov_matrix.index:
                    beta[col] = cov_matrix.loc['MARKET', col] / var_market

        combined_prices = pd.concat([price_data_risk, bench_risk.rename('MARKET')], axis=1).ffill().bfill()
        if 'MARKET' in combined_prices.columns and len(combined_prices) > 0:
            market_return = (combined_prices['MARKET'].iloc[-1] / combined_prices['MARKET'].iloc[0]) - 1
            ann_market_return = ((1 + market_return) ** annualization_factor) - 1
        else:
            ann_market_return = 0
            
        expected_return = risk_free_rate + beta * (ann_market_return - risk_free_rate)
        alpha = annualized_return - expected_return

    # Pastikan semua metrik masuk ke DataFrame utama
    metrics_df = pd.DataFrame({
        'Inception_Return': inception,
        'Interval_Return': risk_period_return, 
        'Return_1W': return_1w,
        'Return_1M': return_1m,
        'Return_3M': return_3m,
        'Volatility': volatility_1sd, 
        'Sharpe_Ratio': sharpe_ratio,
        'Beta': beta,
        'Alpha': alpha
    })
    
    # 3. Metrik Konsistensi Peringkat (Berdasarkan Cut-off Penuh)
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
            if idx != -1:
                snapshot_indices.append(price_data.index[idx])
        snapshot_indices = sorted(list(set(snapshot_indices)))

        if len(snapshot_indices) < 2:
            for n in top_ns:
                consist_results[f'Consist_{label}_Top{n}'] = pd.Series(0, index=price_data.columns)
            continue

        sliced_prices = price_data.loc[snapshot_indices]
        period_returns = sliced_prices.pct_change().dropna(how='all')

        if period_returns.empty:
            for n in top_ns:
                consist_results[f'Consist_{label}_Top{n}'] = pd.Series(0, index=price_data.columns)
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
                    else:
                        streak = 0
                scores[col] = count + max_streak
            consist_results[f'Consist_{label}_Top{n}'] = pd.Series(scores)

    df_consist = pd.DataFrame(consist_results)
    metrics_df = pd.concat([metrics_df, df_consist], axis=1)
    
    # 4. Metrik Climber (Berdasarkan Cut-off Penuh)
    daily_ranks = returns_full.rank(axis=1, ascending=False, method='min')
    
    if not daily_ranks.empty:
        rank_today = daily_ranks.iloc[-1]
        
        def get_safe_past_rank(offset_days):
            if len(daily_ranks) > offset_days:
                return daily_ranks.iloc[-(offset_days + 1)]
            return daily_ranks.iloc[0]
            
        metrics_df['Climb_1d'] = get_safe_past_rank(1) - rank_today
        metrics_df['Climb_7d'] = get_safe_past_rank(7) - rank_today
        metrics_df['Climb_14d'] = get_safe_past_rank(14) - rank_today
        metrics_df['Climb_22d'] = get_safe_past_rank(22) - rank_today
    else:
        for c in ['Climb_1d', 'Climb_7d', 'Climb_14d', 'Climb_22d']:
            metrics_df[c] = 0
            
    # 5. Valuasi / Z-Score & Fraksi Skor
    mean_price = price_data_risk.mean()
    std_price = price_data_risk.std()
    
    z_score = pd.Series(0.0, index=price_data_risk.columns)
    status_valuasi = pd.Series("", index=price_data_risk.columns) 
    skor_valuasi = pd.Series(0.0, index=price_data_risk.columns) # Variabel penampung skor baru
    
    # Deteksi total produk sebagai Full Score
    num_products = len(price_data_risk.columns)
    
    for col in price_data_risk.columns:
        if std_price[col] != 0 and not pd.isna(std_price[col]):
            z_val = (price_data_risk[col].iloc[-1] - mean_price[col]) / std_price[col]
            z_score[col] = z_val
            
            # --- A. Penentuan Label Status ---
            if z_val >= 3.0:
                status_valuasi[col] = "🔴 Sangat Mahal"
            elif z_val <= -3.0:
                status_valuasi[col] = "🟢 Sangat Murah"
            elif z_val < 3.0 and z_val >= 2.0:
                status_valuasi[col] = "🔴 Mahal"
            elif z_val > -3.0 and z_val <= -2.0:
                status_valuasi[col] = "🟢 Murah"
            elif z_val < 2.0 and z_val >= 1.0:
                status_valuasi[col] = "🟠 Sedikit Mahal"
            elif z_val > -2.0 and z_val <= -1.0:
                status_valuasi[col] = "🟢 Sedikit Murah"
            elif z_val < 1.0 and z_val >= -1.0:
                status_valuasi[col] = "⚪ Fair Price"
            else:
                status_valuasi[col] = "" 
                
            # --- B. Kalkulasi 8-Bands Skor Berdasarkan Populasi ---
            if z_val > 3.0:
                skor_valuasi[col] = (1/8) * num_products
            elif 2.0 < z_val <= 3.0:
                skor_valuasi[col] = (2/8) * num_products
            elif 1.0 < z_val <= 2.0:
                skor_valuasi[col] = (3/8) * num_products
            elif 0.0 < z_val <= 1.0:
                skor_valuasi[col] = (4/8) * num_products
            elif -1.0 < z_val <= 0.0:
                skor_valuasi[col] = (5/8) * num_products
            elif -2.0 < z_val <= -1.0:
                skor_valuasi[col] = (6/8) * num_products
            elif -3.0 < z_val <= -2.0:
                skor_valuasi[col] = (7/8) * num_products
            else: # z_val <= -3.0
                skor_valuasi[col] = (8/8) * num_products

    metrics_df['Z_Score'] = z_score
    metrics_df['Status_Valuasi'] = status_valuasi
    metrics_df['Skor_Valuasi'] = skor_valuasi
    
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
    """Menghitung skor komposit dari 25 metrik (termasuk 4 metrik momentum perpindahan peringkat)."""
    # Bobot disebar rata ke 25 metrik (1/25 atau ~4.16% per metrik)
    w = 1.0 / 25.0
    if weights is None:
        weights = {
            'Inception_Return': w, 'Return_1W': w, 'Return_1M': w, 'Return_3M': w,
            'Sharpe_Ratio': w, 'Alpha': w, 'Beta': w, 'Volatility': -w,
            'Consist_1d_Top5': w, 'Consist_1d_Top10': w, 'Consist_1d_Top20': w,
            'Consist_7d_Top5': w, 'Consist_7d_Top10': w, 'Consist_7d_Top20': w,
            'Consist_14d_Top5': w, 'Consist_14d_Top10': w, 'Consist_14d_Top20': w,
            'Consist_21d_Top5': w, 'Consist_21d_Top10': w, 'Consist_21d_Top20': w,
            'Climb_1d': w, 'Climb_7d': w, 'Climb_14d': w, 'Climb_22d': w
        }
    
    df_scaled = metrics_df.copy()
    df_scaled.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    valid_metrics = [col for col in weights.keys() if col in df_scaled.columns]
    if not valid_metrics: return pd.DataFrame()
    
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
        df_scaled['Total_Score'] = df_scaled[score_cols].fillna(0).sum(axis=1)
    else:
        df_scaled['Total_Score'] = 0

    return df_scaled[['Total_Score'] + score_cols].sort_values('Total_Score', ascending=False)

def get_7d_ranking_history(price_data, benchmark_series, risk_free_rate, eval_window=None, custom_weights=None):
    """Menghitung history ranking 7 hari terakhir"""
    history_ranks = {}
    if len(price_data) < 7: return pd.DataFrame()
        
    dates = price_data.index[-7:]
    
    for date in dates:
        sliced_prices = price_data.loc[:date]
        sliced_bench = benchmark_series.loc[:date]
        
        if len(sliced_prices) < 10: continue
            
        metrics = calculate_metrics(sliced_prices, sliced_bench, risk_free_rate, eval_window=eval_window)
        if metrics is not None and not metrics.empty:
            ranks = calculate_ranking_scores(metrics, weights=custom_weights)
            if not ranks.empty:
                rank_series = pd.Series(range(1, len(ranks) + 1), index=ranks.index)
                date_str = date.strftime('%d/%m')
                history_ranks[date_str] = rank_series
                
    return pd.DataFrame(history_ranks)

def get_detailed_ranking_history(price_data_full, benchmark_series_full, risk_free_rate, metric_window, num_columns=10, custom_weights=None, trading_days_interval=1):
    """Menghitung history ranking komposit dengan lompatan akurat berbasis Index Array (Trading Days)"""
    if len(price_data_full) < 1:
        return pd.DataFrame()
    
    # Hitung total data historis yang dibutuhkan
    total_points_needed = (num_columns - 1) * trading_days_interval + 1
    if len(price_data_full) < total_points_needed:
        # Jika data kurang, paksa sesuaikan jumlah kolom maksimal yang bisa dibuat
        num_columns = (len(price_data_full) - 1) // trading_days_interval + 1
        if num_columns < 1: return pd.DataFrame()
        
    # Ambil titik evaluasi dengan melompat persis sesuai interval dari array paling belakang
    eval_dates = price_data_full.iloc[::-trading_days_interval].head(num_columns).index[::-1]
    
    history_ranks = {}
    top5_streak = {}
    
    for date in eval_dates:
        idx_today = price_data_full.index.get_loc(date)
        idx_start = max(0, idx_today - metric_window)
        
        sliced_prices = price_data_full.iloc[idx_start:idx_today+1]
        sliced_bench = benchmark_series_full.iloc[idx_start:idx_today+1]
        
        if len(sliced_prices) < 10: 
            continue
            
        metrics = calculate_metrics(sliced_prices, sliced_bench, risk_free_rate)
        if metrics is not None and not metrics.empty:
            ranks = calculate_ranking_scores(metrics, weights=custom_weights)
            if not ranks.empty:
                rank_series = pd.Series(range(1, len(ranks) + 1), index=ranks.index)
                date_str = date.strftime('%d/%m/%y') # Pakai format tahun agar tidak duplikat
                history_ranks[date_str] = rank_series
                
                top5_today = set(rank_series[rank_series <= 5].index)
                for product in rank_series.index:
                    if product not in top5_streak: top5_streak[product] = 0
                    if product in top5_today: top5_streak[product] += 1
                    else: top5_streak[product] = 0
    
    history_df = pd.DataFrame(history_ranks)
    if not history_df.empty:
        history_df['Streak_Top5'] = pd.Series(top5_streak)
    
    return history_df   

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
def get_period_performance_ranking(price_data, trading_days_interval=5, num_periods=10):
    """Menghitung peringkat return absolut melompat secara presisi mengikuti Trading Days"""
    if price_data.empty or len(price_data) < 2: 
        return pd.DataFrame()

    total_points_needed = num_periods * trading_days_interval + 1
    if len(price_data) < total_points_needed:
        num_periods = (len(price_data) - 1) // trading_days_interval
        if num_periods < 1: return pd.DataFrame()

    # Ekstrak mundur dari array dengan jeda (step) sesuai trading interval
    sliced_prices = price_data.iloc[::-trading_days_interval].head(num_periods + 1).iloc[::-1]
    
    period_returns = sliced_prices.pct_change().dropna(how='all')

    if period_returns.empty: 
        return pd.DataFrame()

    period_ranks = period_returns.rank(axis=1, ascending=False, method='min')
    period_ranks.index = period_ranks.index.strftime('%d/%m/%y')

    return period_ranks.T

def get_monthly_pct_change(price_data):
    """Menghitung persentase perubahan harga akhir bulan (MoM)."""
    if len(price_data) < 2: return pd.DataFrame()
    
    # Sampling ke data akhir bulan (Kompatibel untuk Pandas versi lama dan baru)
    try:
        monthly_prices = price_data.resample('ME').last()
    except:
        monthly_prices = price_data.resample('M').last()
        
    # Kalkulasi return persentase
    monthly_returns = monthly_prices.pct_change().dropna(how='all') * 100
    
    # Format tanggal menjadi format Bulan Tahun
    monthly_returns.index = monthly_returns.index.strftime('%b %Y')
    return monthly_returns.T

def calculate_daily_leaderboard(price_data, days=5):
    """
    Menghitung perubahan peringkat berdasarkan Return absolut menggunakan 
    pendekatan Hari Kalender (Calendar Days) sesuai standar Refinitiv.
    """
    if price_data.empty or len(price_data) < 2:
        return pd.DataFrame()

    today_date = price_data.index[-1]
    yesterday_date = price_data.index[-2]

    # Mundur persis 'days' kalender absolut (misal 5 hari kalender)
    target_N_days_ago = today_date - pd.Timedelta(days=days)
    target_N_plus_1_days_ago = yesterday_date - pd.Timedelta(days=days)

    # Cari indeks baris untuk tanggal hari ini dan kemarin
    idx_today = price_data.index.get_loc(today_date)
    idx_yesterday = price_data.index.get_loc(yesterday_date)
    
    # Cari indeks baris untuk N hari lalu. method='pad' (ffill) memastikan 
    # jika target jatuh di hari libur, ia mengambil hari bursa terdekat sebelumnya.
    idx_N_ago = price_data.index.get_indexer([target_N_days_ago], method='pad')[0]
    idx_N_plus_1_ago = price_data.index.get_indexer([target_N_plus_1_days_ago], method='pad')[0]

    # Batalkan jika data historis tidak cukup panjang untuk mundur ke target kalender
    if idx_N_ago == -1 or idx_N_plus_1_ago == -1:
        return pd.DataFrame()

    # Ekstrak harga menggunakan indeks
    price_today = price_data.iloc[idx_today]
    price_yesterday = price_data.iloc[idx_yesterday]
    
    price_N_ago = price_data.iloc[idx_N_ago].replace(0, np.nan)
    price_N_plus_1_ago = price_data.iloc[idx_N_plus_1_ago].replace(0, np.nan)

    # Hitung return point-to-point
    returns_today = (price_today / price_N_ago) - 1
    returns_yesterday = (price_yesterday / price_N_plus_1_ago) - 1

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

    # Gabungkan data dan kalkulasi perubahan
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
if 'fund_currency' not in st.session_state:
    st.session_state.fund_currency = 'IDR'  # 'IDR' atau 'USD'
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
if 'cached_data_keys' not in st.session_state:
    st.session_state.cached_data_keys = set()
    
# ==================== SIDEBAR ====================
with st.sidebar:
    st.title("⚙️ Pengaturan")
    
    # Pilihan mata uang reksa dana
    st.subheader("💱 Mata Uang Reksa Dana")
    new_currency = st.radio(
        "Pilih Mata Uang",
        options=["IDR", "USD"],
        index=0 if st.session_state.fund_currency == 'IDR' else 1,
        key="currency_radio",
        horizontal=True
    )
    if new_currency != st.session_state.fund_currency:
        st.session_state.fund_currency = new_currency
        
        # Cek apakah mata uang ini dengan tanggal aktif saat ini sudah pernah diekstrak
        if st.session_state.start_date and st.session_state.end_date:
            cache_key = f"{new_currency}_{st.session_state.start_date}_{st.session_state.end_date}"
            if cache_key in st.session_state.cached_data_keys:
                st.session_state.data_loaded = True  # Langsung buka dasbor jika ada di cache
            else:
                st.session_state.data_loaded = False # Minta ekstrak jika belum ada
        else:
            st.session_state.data_loaded = False
            
        st.rerun()
    
    # Bagian Koneksi Refinitiv
    st.subheader("🔌 Koneksi Refinitiv")
    if st.button("Hubungkan ke Refinitiv Platform"):
        with st.spinner("Menghubungkan..."):
            if init_refinitiv_session(currency=st.session_state.fund_currency):
                st.session_state.connected = True
                st.success("Sesi Refinitiv berhasil dibuka!")
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
                        currency=st.session_state.fund_currency,
                        custom_equity_tuple=tuple(st.session_state.custom_equity.items()) if st.session_state.fund_currency == 'IDR' else None,
                        custom_bond_tuple=tuple(st.session_state.custom_bond.items()) if st.session_state.fund_currency == 'IDR' else None
                    ) 
                    if all_data:
                        st.session_state.start_date = start_d
                        st.session_state.end_date = end_d
                        st.session_state.data_loaded = True
                        
                        # --- SISIPIKAN PENCATATAN CACHE DI SINI ---
                        cache_key = f"{st.session_state.fund_currency}_{start_d}_{end_d}"
                        st.session_state.cached_data_keys.add(cache_key)
                        # ------------------------------------------
                        
                        st.success("Data berhasil dimuat!")
                        st.rerun()
                        
                except Exception as e:
                    st.error(f"❌ Pemuatan Dibatalkan: {e}. Silakan periksa koneksi.")
        else:
            st.warning("Hubungkan ke Refinitiv terlebih dahulu.")

    st.divider()
    
    # Manajemen Produk Kustom (hanya untuk IDR)
    if st.session_state.connected and st.session_state.fund_currency == 'IDR':
        st.subheader("➕ Tambah/Hapus Produk")
        
        tab_add_equity, tab_add_bond, tab_delete = st.tabs(["Tambah Equity", "Tambah Fixed Income", "Hapus Produk"])
        
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
                new_bd_ticker = st.text_input("Ticker Fixed Income", placeholder="Contoh: LP68209699")
                new_bd_name = st.text_input("Nama Fixed Income", placeholder="Contoh: Ashmore Dana Obligasi Nusantara")
                
                if st.form_submit_button("Validasi & Tambah"):
                    if new_bd_ticker and new_bd_name:
                        with st.spinner("Memvalidasi ticker..."):
                            if validate_ticker(new_bd_ticker, "Fixed Income"):
                                st.session_state.custom_bond[new_bd_ticker] = new_bd_name
                                st.success(f"✅ {new_bd_name} berhasil ditambahkan!")
                                st.info("Silakan muat ulang data untuk melihat produk baru.")
                            else:
                                st.error("❌ Ticker tidak ditemukan di Refinitiv!")
                    else:
                        st.warning("Harap isi ticker dan nama produk.")
        
        with tab_delete:
            st.subheader("🗑️ Hapus Produk Kustom")
            
            all_custom = {}
            all_custom.update({f"Equity: {v}": k for k, v in st.session_state.custom_equity.items()})
            all_custom.update({f"Fixed Income: {v}": k for k, v in st.session_state.custom_bond.items()})
            
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
    
# Tampilkan Pilihan Interval Analisis & Benchmark (setelah data dimuat)
    if st.session_state.data_loaded:
        fetched_start = pd.to_datetime(st.session_state.start_date).date()
        fetched_end = pd.to_datetime(st.session_state.end_date).date()
        
        st.subheader("✂️ Cut-off Data Analisis")
        st.caption("Batasi rentang tanggal historis utama yang ingin dievaluasi (memotong dataset API).")
        
        col_a1, col_a2 = st.columns(2)
        with col_a1:
            raw_start_date = st.date_input("Start Analisis", value=fetched_start, key="ana_start")
        with col_a2:
            raw_end_date = st.date_input("End Analisis", value=fetched_end, key="ana_end")
            
        # --- Validasi & Error Messages ---
        analysis_start_date = raw_start_date
        analysis_end_date = raw_end_date
        
        if raw_start_date < fetched_start:
            st.error(f"⚠️ Start Analisis melampaui data API. Dipaksa mulai dari: {fetched_start.strftime('%d %b %Y')}")
            analysis_start_date = fetched_start
            
        if raw_end_date > fetched_end:
            st.error(f"⚠️ End Analisis melampaui data API. Dipaksa berhenti di: {fetched_end.strftime('%d %b %Y')}")
            analysis_end_date = fetched_end
            
        if analysis_start_date > analysis_end_date:
            st.error("⚠️ Start Analisis tidak boleh lebih besar dari End!")
            analysis_start_date = analysis_end_date
            
        st.info(f"Data aktif: **{analysis_start_date.strftime('%d %b %Y')}** s/d **{analysis_end_date.strftime('%d %b %Y')}**")

        st.subheader("⏱️ Interval Kalkulasi (Rolling)")
        st.caption("Pilih jendela waktu (window) untuk kalkulasi metrik berjalan (Heatmap, Volatilitas, dll).")
        date_option = st.selectbox(
            "Pilih Interval:",
            options=["1 Bulan", "3 Bulan", "6 Bulan", "1 Tahun"],
            index=3, 
            key="interval_analisis"
        )
        # --- TAMBAHAN FILTER SKORING ---
        st.subheader("🎯 Fokus Skoring (Ranking)")
        scoring_mode = st.selectbox(
            "Pilih Metrik Penentu Peringkat:",
            options=["Balanced (Semua Metrik)", "Fokus Return (Profit)", "Fokus Risiko & Rasio", "Fokus Konsistensi", "Fokus Momentum (Climbers)", "Fokus Valuasi (Murah/Mahal)"],
            index=0,
            key="scoring_mode_select"
        )
        
        st.subheader("🎯 Parameter Benchmark")
        # Pilihan benchmark berdasarkan mata uang
        if st.session_state.fund_currency == 'USD':
            benchmark_options = {
                'NASDAQ (.IXIC)': '.IXIC',
                'S&P 500 (.SPX)': '.SPX',
                'DXY (US Dollar Index)': '.DXY',
                'Shanghai Composite (.SSEC)': '.SSEC',
                'US 10Y Treasury Yield (US10YT=RR)': 'US10YT=RR',
                'Minyak Mentah (CLc1)': 'CLc1',
                'Suku Bunga 10Y (ID10YT=RR)': 'ID10YT=RR'
            }
        else:
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
        
        st.session_state.selected_benchmark_ticker = selected_benchmark_ticker
        st.session_state.selected_benchmark_label = selected_bench_label
    else:
        date_option = "1 Tahun" 
        selected_benchmark_ticker = ".JKSE"
        selected_benchmark_label = "IHSG (.JKSE)"
        st.session_state.selected_benchmark_ticker = selected_benchmark_ticker
        st.session_state.selected_benchmark_label = selected_benchmark_label

    st.sidebar.divider()
    st.sidebar.caption("© 2024 Investment Dashboard")

# ==================== HALAMAN UTAMA ====================
st.title("📊 Investment Dashboard - Reksa Dana Indonesia")
if st.session_state.start_date and st.session_state.end_date:
    st.markdown(f"Periode Data: {st.session_state.start_date.strftime('%d %b %Y')} s/d {st.session_state.end_date.strftime('%d %b %Y')}")
    st.markdown(f"Mata Uang Reksa Dana: **{st.session_state.fund_currency}**")

if not st.session_state.data_loaded:
    st.info("👈 Silakan hubungkan ke Refinitiv dan muat data dari sidebar untuk memulai.")
    st.stop()

# ==================== AMBIL DATA DARI CACHE ====================
all_data, _, _ = load_all_data(
    st.session_state.start_date, 
    st.session_state.end_date,
    currency=st.session_state.fund_currency,
    custom_equity_tuple=tuple(st.session_state.custom_equity.items()) if st.session_state.fund_currency == 'IDR' else None,
    custom_bond_tuple=tuple(st.session_state.custom_bond.items()) if st.session_state.fund_currency == 'IDR' else None
)
df_equity_full = all_data['equity']
df_bond_full = all_data['bond']
df_index_full = all_data['index']
df_komoditas_full = all_data['komoditas']
df_mata_uang_full = all_data['mata_uang']
df_suku_bunga_full = all_data['suku_bunga']

risk_free_rate = st.session_state.risk_free_rate
selected_benchmark_ticker = st.session_state.selected_benchmark_ticker
selected_benchmark_label = st.session_state.selected_benchmark_label

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

# ==================== SLICING DATA BERDASARKAN RENTANG WAKTU (CUT-OFF) ====================
# Konversi ke datetime untuk perbandingan dengan indeks Pandas
ana_start_dt = pd.to_datetime(analysis_start_date)
ana_end_dt = pd.to_datetime(analysis_end_date)

def safe_slice(df, start_dt, end_dt):
    if df.empty: return df
    # Memotong murni dengan start date dan end date analisis
    return df[(df.index >= start_dt) & (df.index <= end_dt)]

df_equity = safe_slice(df_equity_full, ana_start_dt, ana_end_dt)
df_bond = safe_slice(df_bond_full, ana_start_dt, ana_end_dt)
df_index = safe_slice(df_index_full, ana_start_dt, ana_end_dt)
df_komoditas = safe_slice(df_komoditas_full, ana_start_dt, ana_end_dt)
df_mata_uang = safe_slice(df_mata_uang_full, ana_start_dt, ana_end_dt)
df_suku_bunga = safe_slice(df_suku_bunga_full, ana_start_dt, ana_end_dt)

df_all_instruments = pd.concat([df_equity, df_bond], axis=1)
df_all_instruments = ensure_unique_columns(df_all_instruments)

benchmark_series_sliced = safe_slice(benchmark_series_full, ana_start_dt, ana_end_dt)

df_all_instruments_full = ensure_unique_columns(pd.concat([df_equity_full, df_bond_full], axis=1))

# --- Tentukan Jendela Evaluasi (Window) berdasarkan Interval ---
if date_option == "1 Bulan":
    cutoff_days = 22
elif date_option == "3 Bulan":
    cutoff_days = 63
elif date_option == "6 Bulan":
    cutoff_days = 126
elif date_option == "1 Tahun":
    cutoff_days = 252
else:
    cutoff_days = 252

metrics_all = calculate_metrics(df_all_instruments, benchmark_series_sliced, risk_free_rate, eval_window=cutoff_days)

if metrics_all is None or metrics_all.empty:
    st.error(f"Gagal menghitung metrik untuk periode {date_option}. Data mungkin tidak mencukupi.")
    st.stop()

metrics_equity = None
if not df_equity.empty:
    metrics_equity = calculate_metrics(df_equity, benchmark_series_sliced, risk_free_rate, eval_window=cutoff_days)

metrics_bond = None
if not df_bond.empty:
    metrics_bond = calculate_metrics(df_bond, benchmark_series_sliced, risk_free_rate, eval_window=cutoff_days)

# --- LOGIKA FILTER PEMBOBOTAN DINAMIS ---
weights_dict = None  # Default (Balanced = dibagi rata 1/25)

# --- LOGIKA FILTER PEMBOBOTAN DINAMIS ---
weights_dict = None  # Default (Balanced = dibagi rata 1/25)

if scoring_mode == "Fokus Return (Profit)":
    weights_dict = { 'Inception_Return': 0.25, 'Return_1W': 0.25, 'Return_1M': 0.25, 'Return_3M': 0.25 }
elif scoring_mode == "Fokus Risiko & Rasio":
    weights_dict = { 'Sharpe_Ratio': 0.25, 'Alpha': 0.25, 'Beta': 0.25, 'Volatility': -0.25 }
elif scoring_mode == "Fokus Konsistensi":
    consist_keys = [f"Consist_{d}_Top{n}" for d in ['1d','7d','14d','21d'] for n in [5,10,20]]
    weights_dict = {k: 1.0/12.0 for k in consist_keys}
elif scoring_mode == "Fokus Momentum (Climbers)":
    climb_keys = ['Climb_1d', 'Climb_7d', 'Climb_14d', 'Climb_22d']
    weights_dict = {k: 0.25 for k in climb_keys}
elif scoring_mode == "Fokus Valuasi (Murah/Mahal)":
    weights_dict = { 'Z_Score': -1.0 }

# Hitung skor dengan menerapkan filter bobot
ranked_products_all = calculate_ranking_scores(metrics_all, weights=weights_dict) if metrics_all is not None else pd.DataFrame()
ranked_products_equity = calculate_ranking_scores(metrics_equity, weights=weights_dict) if metrics_equity is not None else pd.DataFrame()
ranked_products_bond = calculate_ranking_scores(metrics_bond, weights=weights_dict) if metrics_bond is not None else pd.DataFrame()

leaderboard_daily = calculate_daily_leaderboard(df_all_instruments, days=7)

# ==================== TABS ====================
tab_overview, tab_leaderboard_split,  tab_performance, tab_correlation, tab_compare, tab_recommendation = st.tabs([
    "📋 Ringkasan", 
    "🏆 Leaderboard", 
    "📊 Performa & Ranking", 
    "📈 Korelasi",  
    "📉 Perbandingan Historis",
    "🎯 Rekomendasi Refinitiv"
])

# --- Tab 1: Ringkasan ---
with tab_overview:
    st.header("Ringkasan Pasar & Instrumen")
    st.info("ℹ️ Metodologi: Peringkat Top 10 dihitung menggunakan model pembobotan komposit rata masing-masing 20%: Total Return, Sharpe Ratio, Alpha, Beta, dan Volatility. Dihitung secara kumulatif dari awal periode kalender yang dipilih hingga hari ini.")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Jumlah Equity", len(df_equity.columns))
    with col2:
        st.metric("Jumlah Fixed Income", len(df_bond.columns))
    with col3:
        st.metric("Periode (Hari)", df_all_instruments.shape[0])
    with col4:
        st.metric("Risk-Free Rate", f"{risk_free_rate*100:.2f}%")

    st.subheader("Top 10 Produk (Skor Tertinggi & Riwayat Peringkat 7 Hari)")
    
    # Tambahkan Radio Button untuk memisah kategori
    top10_category = st.radio("Pilih Kategori Produk:", ["Equity", "Fixed Income"], horizontal=True, key="top10_radio")
    
    # Logika percabangan data sesuai pilihan radio button
    if top10_category == "Equity":
        ranked_to_show = ranked_products_equity
        df_to_show = df_equity
    else:
        ranked_to_show = ranked_products_bond
        df_to_show = df_bond

    if not ranked_to_show.empty:
        with st.spinner(f"Mengkalkulasi jejak peringkat 7 hari terakhir untuk {top10_category}..."):
            # Menggunakan df_to_show agar peringkat murni diadu di golongannya sendiri
            history_ranks = get_7d_ranking_history(df_to_show, benchmark_series_sliced, risk_free_rate, eval_window=cutoff_days, custom_weights=weights_dict)
        
        top_10 = ranked_to_show.head(10).reset_index()
        if 'index' in top_10.columns:
            top_10 = top_10.rename(columns={'index': 'Instrument'})
            
        top_10['Total_Score'] = top_10['Total_Score'].round(3)
        
        if not history_ranks.empty:
            top_10 = top_10.merge(history_ranks, left_on='Instrument', right_index=True, how='left')
            cols_to_show = ['Instrument', 'Total_Score'] + list(history_ranks.columns)
        else:
            cols_to_show = ['Instrument', 'Total_Score']
            
        st.dataframe(top_10[cols_to_show], use_container_width=True, hide_index=True)
    else:
        st.warning(f"Tidak ada data peringkat untuk {top10_category}.")

    st.subheader("📈 Pergerakan Indeks Acuan")
    
    # Gunakan benchmark_series_sliced karena sudah memuat semua tipe (indeks, makro, komoditas)
    if not benchmark_series_sliced.empty and not (benchmark_series_sliced == 0.0).all():
        # Konversi Pandas Series ke DataFrame untuk Plotly
        df_plot_bench = benchmark_series_sliced.reset_index()
        df_plot_bench.columns = ['Date', 'Value']
        
        fig_idx = px.line(df_plot_bench, x='Date', y='Value', title=f'{selected_benchmark_label} - Harga Historis')
        fig_idx.update_layout(xaxis_title="Tanggal", yaxis_title="Nilai/Harga")
        st.plotly_chart(fig_idx, use_container_width=True)
    else:
        st.warning(f"Data {selected_benchmark_label} tidak tersedia atau bernilai 0.")

# Tab 2-7 (sama seperti sebelumnya, tidak diubah)
# ... (potongan kode untuk tab ranking, leaderboard, korelasi, performa equity, performa bond, perbandingan historis)
# Karena panjang, saya sertakan di bawah sebagai kelanjutan.

# ==================== TAB 2: LEADERBOARD ====================
with tab_leaderboard_split:
    st.header("🏆 Leaderboard & Rekomendasi Produk")
    st.info("ℹ️ **Metodologi:** Berfungsi sebagai indikator *Momentum*. Peringkat diukur murni berdasarkan **Return Absolut 5 Hari Kalender** ((Harga Hari Ini / Harga 5 Hari Lalu) - 1). Mengabaikan risiko dan volatilitas untuk mencari aset dengan tren naik jangka pendek tercepat.")
    lb_type = st.radio("Pilih Tipe Leaderboard", ["Equity", "Fixed Income"], horizontal=True, key="lb_split_type")
    if lb_type == "Equity":
        df_lb = df_equity
        title = "Equity"
    else:
        df_lb = df_bond
        title = "Fixed Income"
    
    if not df_lb.empty:
        # UBAH: Panggil dengan days=5
        leaderboard = calculate_daily_leaderboard(df_lb, days=5)
        if not leaderboard.empty:
            leaderboard['Change_Color'] = leaderboard['Rank_Change'].apply(
                lambda x: '🚀 Top Climber' if x > 0 else ('📉 Top Laggard' if x < 0 else 'Stabil')
            )
            
            # UBAH: Ganti semua Return_7d menjadi Return_5d
            leaderboard['Return_5d'] = leaderboard['Return_5d'] * 100
            leaderboard_display = leaderboard[['Instrument', 'Return_5d', 'Rank_Today', 'Rank_Change', 'Change_Color']].copy()
            leaderboard_display['Return_5d'] = leaderboard_display['Return_5d'].round(2).astype(str) + '%'

            col1, col2 = st.columns(2)
            with col1:
                st.subheader(f"🚀 Top Climbers 5 Days Return {title}")
                climbers = leaderboard_display[leaderboard_display['Rank_Change'] > 0].sort_values('Rank_Change', ascending=False).head(10)
                if not climbers.empty:
                    st.dataframe(climbers, hide_index=True, use_container_width=True)
                else:
                    st.info("Tidak ada top climbers dalam periode ini.")
            with col2:
                st.subheader(f"📉 Top Laggards 5 Days Return {title}")
                laggards = leaderboard_display[leaderboard_display['Rank_Change'] < 0].sort_values('Rank_Change', ascending=True).head(10)
                if not laggards.empty:
                    st.dataframe(laggards, hide_index=True, use_container_width=True)
                else:
                    st.info("Tidak ada top laggards dalam periode ini.")

            st.subheader(f"📋 Leaderboard Lengkap Return 5 Hari {title}")
            st.dataframe(leaderboard_display.sort_values('Rank_Today'), hide_index=True, use_container_width=True)
            
            st.subheader(f"💡 Produk Performa Terbaik Selama 5 Hari {title}")
            top_3 = leaderboard_display.nsmallest(3, 'Rank_Today')
            top_climbers = leaderboard_display[leaderboard_display['Rank_Change'] > 0].nlargest(3, 'Rank_Change')
            col_rec1, col_rec2 = st.columns(2)
            with col_rec1:
                st.markdown("**🏅 Top 3 Performers Saat Ini**")
                for idx, row in top_3.iterrows():
                    # UBAH: Tampilkan Return_5d
                    st.markdown(f"• **{row['Instrument']}** - Return 5d: {row['Return_5d']}")
            # ... (kode Anda sebelumnya) ...
            with col_rec2:
                st.markdown("**Produk dengan Momentum Terbaik**")
                for idx, row in top_climbers.iterrows():
                    st.markdown(f"• **{row['Instrument']}** (Naik {row['Rank_Change']} peringkat)")
            
            # ================= TAMBAHAN SEGMEN PERINGKAT HARIAN =================
            st.divider()
            st.subheader(f"⚡ Peringkat Harian (Daily % Change) - {title}")
            
            if len(df_lb) >= 2:
                # 1. Ekstrak NAV hari ini dan kemarin
                price_today_lb = df_lb.iloc[-1]
                price_yesterday_lb = df_lb.iloc[-2].replace(0, np.nan)
                
                # 2. Hitung Persentase Perubahan
                daily_pct_lb = ((price_today_lb / price_yesterday_lb) - 1) * 100
                
                # 3. Format DataFrame Peringkat
                df_daily_lb = daily_pct_lb.reset_index()
                df_daily_lb.columns = ['Instrument', 'Daily_%_Change']
                df_daily_lb = df_daily_lb.dropna().sort_values('Daily_%_Change', ascending=False)
                df_daily_lb['Rank'] = range(1, len(df_daily_lb) + 1)
                
                # 4. Format teks angka untuk ditampilkan
                df_daily_display = df_daily_lb[['Rank', 'Instrument', 'Daily_%_Change']].copy()
                df_daily_display['Daily_%_Change'] = df_daily_display['Daily_%_Change'].round(2).astype(str) + '%'
                
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    st.markdown("Top Performers (Gainers Harian)")
                    st.dataframe(df_daily_display.head(10), hide_index=True, use_container_width=True)
                with col_d2:
                    st.markdown("Bottom Performers (Losers Harian)")
                    # Diurutkan terbalik agar produk dengan penurunan terdalam ada di baris pertama
                    df_losers = df_daily_lb.tail(10).sort_values('Daily_%_Change', ascending=True).copy()
                    df_losers['Daily_%_Change'] = df_losers['Daily_%_Change'].round(2).astype(str) + '%'
                    st.dataframe(df_losers[['Rank', 'Instrument', 'Daily_%_Change']], hide_index=True, use_container_width=True)
            else:
                st.warning("Data historis tidak cukup untuk menghitung perubahan harian.")
    else:
        st.warning(f"Tidak ada data {title}.")

# ==================== TAB 3: KORELASI ====================
with tab_correlation:
    st.header("Analisis Korelasi")
    st.info("ℹ️ **Metodologi:** Menggunakan **Korelasi Pearson** pada pergerakan *return* harian. Nilai 1 (Hijau) berarti pergerakan searah sempurna, -1 (Merah) berlawanan sempurna, dan 0 (Kuning/Pucat) menunjukkan tidak ada hubungan linier antar aset.")
    col_corr1, col_corr2 = st.columns(2)
    with col_corr1:
        grup1 = st.selectbox("Pilih Grup Aset 1 (Sumbu Y)", options=["Equity", "Fixed Income"], key="corr_grup1")
    with col_corr2:
        grup2 = st.selectbox("Pilih Grup Aset 2 (Sumbu X)", options=["Equity", "Fixed Income", "Indeks", "Komoditas", "Mata Uang", "Suku Bunga"], key="corr_grup2")
    dict_dfs = {
        "Equity": df_equity,
        "Fixed Income": df_bond,
        "Indeks": df_index,
        "Komoditas": df_komoditas,
        "Mata Uang": df_mata_uang,
        "Suku Bunga": df_suku_bunga
    }
    df_grup1 = dict_dfs[grup1]
    df_grup2 = dict_dfs[grup2]
    returns_grup1 = df_grup1.dropna(axis=1, how='all').ffill().bfill().pct_change().dropna(how='all')
    returns_grup2 = df_grup2.dropna(axis=1, how='all').ffill().bfill().pct_change().dropna(how='all')
    returns_grup1, returns_grup2 = returns_grup1.align(returns_grup2, join='inner', axis=0)
    if not returns_grup1.empty and not returns_grup2.empty:
        if grup1 == grup2:
            title = f"Matriks Korelasi Antar {grup1}"
            corr_matrix = returns_grup1.corr()
            mask_plot = np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
            corr_matrix_plot = corr_matrix.mask(mask_plot)
        else:
            title = f"Matriks Korelasi {grup1} vs {grup2}"
            corr_dict = {}
            for col2 in returns_grup2.columns:
                corr_dict[col2] = returns_grup1.apply(lambda x: x.corr(returns_grup2[col2]))
            corr_matrix = pd.DataFrame(corr_dict)
            corr_matrix_plot = corr_matrix.copy()
        fig_corr = px.imshow(
            corr_matrix_plot, text_auto='.2f', aspect="auto",
            color_continuous_scale='RdYlGn', zmin=-1, zmax=1, title=title,
            labels=dict(y=f"Grup 1: {grup1}", x=f"Grup 2: {grup2}", color="Korelasi")
        )
        fig_corr.update_layout(height=800)
        st.plotly_chart(fig_corr, use_container_width=True)
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
        
# ==================== TAB 4: PERFORMA & RANKING (GABUNGAN) ====================
with tab_performance:
    st.header("📊 Performa, Metrik & Riwayat Peringkat")
    
    # Kalkulasi string tanggal untuk deskripsi dinamis
    start_date_str = ana_start_dt.strftime('%d %b %Y')
    end_date_str = ana_end_dt.strftime('%d %b %Y')
    
    st.info(f"""ℹ️ **Metodologi Metrik & Skor Komposit ({date_option} | {start_date_str} s/d {end_date_str}):**
    - **Momentum Return (4 Metrik):** Persentase profit 1W, 1M, 3M, dan Total Return.
    - **Risk & Reward (4 Metrik):** Volatilitas, Sharpe Ratio, Alpha, dan Beta.
    - **Konsistensi Peringkat (12 Metrik):** Frekuensi dan *streak* produk bertahan di Top 5, 10, dan 20 pada berbagai rentang waktu.
    - **Akselerasi Tren / Climbers (4 Metrik):** Perubahan posisi peringkat harian saat ini dibandingkan posisi 1, 7, 14, dan 22 hari perdagangan sebelumnya.
    - **Skor Valuasi (1 Metrik):** Penilaian kewajaran harga berdasarkan deviasi Z-Score (dibagi ke dalam 8 fraksi pita/bands).
    - **Total Skor:** Rata-rata persentil dari ke-25 metrik di atas (distribusi bobot setara 4% per komponen pada mode Balanced).""")
    
    perf_type = st.radio("Pilih Kategori Aset", ["Equity", "Fixed Income"], horizontal=True, key="perf_type_radio")
    
    if perf_type == "Equity":
        df_perf = df_equity
        df_perf_full = df_equity_full # Tambahkan data utuh
        metrics_perf = metrics_equity
        ranked_perf = ranked_products_equity
        title = "Equity"
    else:
        df_perf = df_bond
        df_perf_full = df_bond_full # Tambahkan data utuh
        metrics_perf = metrics_bond
        ranked_perf = ranked_products_bond
        title = "Fixed Income"

    if not df_perf.empty and ranked_perf is not None and not ranked_perf.empty:
        
        # Gabungkan metrik dan seluruh kolom skor (_scaled) di awal agar mudah dipilah
        full_performance = metrics_perf.merge(ranked_perf, left_index=True, right_index=True, how='left')
        
        # --- 1. DUA LEADERBOARD TOP 5 (ATAS) ---
        st.subheader(f"🏆 Leaderboards: Top 5 {title}")
        col_top1, col_top2 = st.columns(2)
            
        with col_top1:
            st.markdown(f"**🌟 Top 5 Composite Score** (Benchmark: {selected_benchmark_label})")
            top5_score = full_performance.sort_values('Total_Score', ascending=False).head(5)
            df_show_score = top5_score[['Total_Score']].copy()
            df_show_score['Total_Score'] = df_show_score['Total_Score'].round(3)
            df_show_score = df_show_score.rename(columns={'Total_Score': 'Skor Komposit'})
            df_show_score.index.name = 'Nama Produk'
            st.dataframe(df_show_score, use_container_width=True)
 
        with col_top2:
            st.markdown(f"**🥇 Top 5 Performa (Return {date_option})**")
            top5_return = full_performance.sort_values('Interval_Return', ascending=False).head(5)
            df_show_return = top5_return[['Interval_Return']].copy()
            df_show_return['Interval_Return'] = (df_show_return['Interval_Return'] * 100).round(2).astype(str) + '%'
            df_show_return = df_show_return.rename(columns={'Interval_Return': f'Return {date_option}'})
            df_show_return.index.name = 'Nama Produk'
            st.dataframe(df_show_return, use_container_width=True)
            
        st.divider()

        # --- 2. ANALISIS MENDALAM PRODUK ---
        st.subheader(f"🔍 Analisis Kekuatan & Kelemahan")
        st.caption(f"Perbandingan kinerja {date_option} terakhir terhadap rata-rata kategori.")
        
        # Menggunakan list dari Top 5 Skor Komposit sebagai opsi dropdown
        top_5_list = top5_score.index.tolist()
        if top_5_list:
            selected_top = st.selectbox(f"Pilih Produk {title} untuk dianalisis:", top_5_list, key="perf_select_top")
            if selected_top:
                metrics_top = metrics_perf.loc[selected_top]
                
                strengths = []
                weaknesses = []
                
                # Cek Metrik Return (Sesuai Interval)
                if pd.notna(metrics_top['Interval_Return']):
                    avg_ret = metrics_perf['Interval_Return'].mean() * 100
                    val_ret = metrics_top['Interval_Return'] * 100
                    if val_ret > avg_ret:
                        strengths.append(f"• Return ({date_option}): Profit {val_ret:.2f}%, di atas rata-rata {avg_ret:.2f}%.")
                    elif val_ret < avg_ret:
                        weaknesses.append(f"• Return ({date_option}): Profit {val_ret:.2f}%, tertinggal dari rata-rata {avg_ret:.2f}%.")
                
                # Cek Metrik Sharpe
                if pd.notna(metrics_top['Sharpe_Ratio']):
                    avg_sharpe = metrics_perf['Sharpe_Ratio'].mean()
                    val_sharpe = metrics_top['Sharpe_Ratio']
                    if val_sharpe > avg_sharpe:
                        strengths.append(f"• Sharpe Ratio: Nilai {val_sharpe:.2f} lebih tinggi dari rata-rata {avg_sharpe:.2f}.")
                    elif val_sharpe < avg_sharpe:
                        weaknesses.append(f"• Sharpe Ratio: Nilai {val_sharpe:.2f} lebih rendah dari rata-rata {avg_sharpe:.2f}.")
                
                # Cek Metrik Volatility (Desimal)
                if pd.notna(metrics_top['Volatility']):
                    avg_vol = metrics_perf['Volatility'].mean()
                    val_vol = metrics_top['Volatility']
                    if val_vol < avg_vol:
                        strengths.append(f"• Volatility: Fluktuasi {val_vol:.4f} lebih stabil dibanding rata-rata {avg_vol:.4f}.")
                    elif val_vol > avg_vol:
                        weaknesses.append(f"• Volatility: Fluktuasi {val_vol:.4f} lebih berisiko dibanding rata-rata {avg_vol:.4f}.")
                
                # Cek Metrik Alpha (Desimal)
                if pd.notna(metrics_top['Alpha']):
                    avg_alpha = metrics_perf['Alpha'].mean()
                    val_alpha = metrics_top['Alpha']
                    if val_alpha > avg_alpha:
                        strengths.append(f"• Alpha (Benchmark: {selected_benchmark_label}): Nilai {val_alpha:.4f} mengungguli rata-rata {avg_alpha:.4f}.")
                    elif val_alpha < avg_alpha:
                        weaknesses.append(f"• Alpha (Benchmark: {selected_benchmark_label}): Nilai {val_alpha:.4f} di bawah rata-rata {avg_alpha:.4f}.")
                
                # Tampilan UI 2 Kolom
                col_str, col_weak = st.columns(2)
                with col_str:
                    st.write("Kekuatan:")
                    if strengths:
                        for s in strengths:
                            st.write(s)
                    else:
                        st.write("Tidak ada keunggulan mencolok dibanding rata-rata.")
                        
                with col_weak:
                    st.write("Kelemahan:")
                    if weaknesses:
                        for w in weaknesses:
                            st.write(w)
                    else:
                        st.write("**Tidak ada kelemahan**")
                        st.write("Tidak ada metrik utama di bawah rata-rata.")

        st.divider()
        
        # --- 3. TABEL METRIK & SKOR LENGKAP ---
        st.subheader(f"📋 Tabel Metrik & Skor Lengkap - {title} (Benchmark: {selected_benchmark_label})")
        st.caption(f"Fokus Evaluasi: **{scoring_mode}**. Tabel ini menampilkan nilai mentah (raw) dari masing-masing metrik untuk perbandingan langsung.")        
        
        # [Perbaikan]: Menyalin langsung dari metrik mentah lalu menyuntikkan Total Skor agar tidak ada kolom yang terhapus
        full_performance_display = metrics_perf.copy()
        if ranked_perf is not None and 'Total_Score' in ranked_perf.columns:
            full_performance_display['Total_Score'] = ranked_perf['Total_Score']
        else:
            full_performance_display['Total_Score'] = 0
            
        full_performance_display = full_performance_display.sort_values('Total_Score', ascending=False)
        full_performance_display.index.name = 'Nama Produk'
        
        # 1. Klasifikasi Kelompok Metrik
        return_metrics = ['Inception_Return', 'Interval_Return', 'Return_1W', 'Return_1M', 'Return_3M']
        risk_metrics = ['Sharpe_Ratio', 'Alpha', 'Beta', 'Volatility']
        consist_metrics = [
            'Consist_1d_Top5', 'Consist_1d_Top10', 'Consist_1d_Top20',
            'Consist_7d_Top5', 'Consist_7d_Top10', 'Consist_7d_Top20',
            'Consist_14d_Top5', 'Consist_14d_Top10', 'Consist_14d_Top20',
            'Consist_21d_Top5', 'Consist_21d_Top10', 'Consist_21d_Top20'
        ]
        climb_metrics = ['Climb_1d', 'Climb_7d', 'Climb_14d', 'Climb_22d']
        val_metrics = ['Z_Score', 'Status_Valuasi', 'Skor_Valuasi']
        
        # 2. Filter Dinamis Sesuai Mode di Sidebar
        active_metrics = []
        if scoring_mode == "Balanced (Semua Metrik)":
            active_metrics = return_metrics + risk_metrics + consist_metrics + climb_metrics + val_metrics
        elif scoring_mode == "Fokus Return (Profit)":
            active_metrics = return_metrics
        elif scoring_mode == "Fokus Risiko & Rasio":
            active_metrics = risk_metrics
        elif scoring_mode == "Fokus Konsistensi":
            active_metrics = consist_metrics
        elif scoring_mode == "Fokus Momentum (Climbers)":
            active_metrics = climb_metrics
        elif scoring_mode == "Fokus Valuasi (Murah/Mahal)":
            active_metrics = val_metrics
            
        # 3. Bangun Urutan Kolom
        cols_order = ['Total_Score']
        for metric in active_metrics:
            if metric in full_performance_display.columns:
                cols_order.append(metric)
                    
        # 4. Potong Dataframe Hanya untuk Kolom Aktif
        full_performance_display = full_performance_display[cols_order]
        
        # 5. Eksekusi Formatting Tampilan
        cols_to_format = {
            'Inception_Return': "{:.2%}", 'Interval_Return': "{:.2%}", 'Return_1W': "{:.2%}", 'Return_1M': "{:.2%}", 'Return_3M': "{:.2%}",
            'Volatility': "{:.4f}", 
            'Sharpe_Ratio': "{:.4f}", 
            'Beta': "{:.4f}", 
            'Alpha': "{:.4f}",
            'Z_Score': "{:+.2f} SD", 
            'Skor_Valuasi': "{:.2f}",
            'Total_Score': "{:.3f}"
        }
        
        for col in full_performance_display.columns:
            if col.startswith('Consist_'):
                cols_to_format[col] = "{:.0f}"
            elif col.startswith('Climb_'):
                cols_to_format[col] = "{:+.0f}" 
                
        final_format = {k: v for k, v in cols_to_format.items() if k in full_performance_display.columns}
        
        st.dataframe(full_performance_display.style.format(final_format), use_container_width=True)

        st.divider()
        
        # --- 3. HEATMAP ANALISIS LANJUTAN (BAWAH) ---
        st.subheader(f"🔥 Heatmap Analisis Lanjutan - {title} (Benchmark: {selected_benchmark_label})")
        st.caption("Pilih interval lompatan waktu dan jumlah kolom evaluasi. Sistem akan memotong data murni berdasarkan urutan indeks array (Trading Days), menjamin jumlah kolom yang presisi.")
        
        # Panel Kendali Terpadu untuk Heatmap
        col_h1, col_h2, col_h3 = st.columns(3)
        with col_h1:
            period_map = {
                "Harian": 1, 
                "Mingguan (5 Hari Bursa)": 5, 
                "2 Mingguan (10 Hari)": 10, 
                "3 Mingguan (15 Hari)": 15, 
                "Bulanan (21 Hari)": 21
            }
            selected_period_label = st.selectbox("Lompatan Interval:", list(period_map.keys()), index=1, key="heat_interval")
            trading_interval = period_map[selected_period_label]
        with col_h2:
            num_columns = st.selectbox("Jumlah Kolom (Periode):", [5, 10, 15, 20, 30], index=1, key="heat_columns")
        with col_h3:
            selected_top_n = st.selectbox("Target Peringkat (Highlights):", [5, 10, 20], index=0, key="heat_top_n")

        # Sistem Peringatan Jika Data Historis dari API Tidak Cukup Panjang
        required_days = num_columns * trading_interval
        if len(df_perf_full) < required_days:
            st.warning(f"⚠️ Data yang ditarik dari API hanya mencakup {len(df_perf_full)} hari bursa. Untuk merender {num_columns} kolom dengan interval '{selected_period_label}', Anda harus mengubah 'Start Date' di sidebar menjadi lebih lama (minimal {(required_days/252):.1f} tahun ke belakang).")
        
        def get_custom_highlights(df_ranks, top_n=10):
            stats = []
            for inst, row in df_ranks.iterrows():
                numeric_row = pd.to_numeric(row, errors='coerce')
                is_top = (numeric_row <= top_n) & (numeric_row > 0)
                total_in = is_top.sum()
                
                streak, max_streak = 0, 0
                for val in is_top:
                    if val:
                        streak += 1
                        if streak > max_streak: max_streak = streak
                    else: 
                        streak = 0
                        
                if total_in > 0:
                    stats.append({
                        'Instrument': inst, 
                        f'Total Masuk Top {top_n}': total_in, 
                        'Streak Terlama': max_streak
                    })
                    
            if stats:
                df_stats = pd.DataFrame(stats).set_index('Instrument')
                return df_stats.sort_values(by=[f'Total Masuk Top {top_n}', 'Streak Terlama'], ascending=[False, False]).head(10)
            return pd.DataFrame()

        tab_heat1, tab_heat2, tab_heat3 = st.tabs(["1. Peringkat Skor Komposit", "2. Peringkat Performa Absolut", "3. Return Bulanan (MoM %)"])
        
        with tab_heat1:
            st.markdown(f"**Heatmap Riwayat Peringkat Komposit (Skoring Dinamis)**")
            with st.spinner("Mengkalkulasi komposit skor historis..."):
                history_score_df = get_detailed_ranking_history(
                    df_perf_full, benchmark_series_full, risk_free_rate, 
                    metric_window=cutoff_days, num_columns=num_columns, 
                    custom_weights=weights_dict, trading_days_interval=trading_interval
                )
                if not history_score_df.empty and len(history_score_df.columns) > 1:
                    rank_score_data = history_score_df.drop(columns=['Streak_Top5'], errors='ignore')
                    rank_score_data = rank_score_data.sort_values(by=rank_score_data.columns[-1], ascending=True)
                    
                    highlight_score = get_custom_highlights(rank_score_data, top_n=selected_top_n)
                    if not highlight_score.empty:
                        st.markdown(f"🌟 **Top Highlights (Paling Sering Masuk Peringkat 1-{selected_top_n}):**")
                        st.dataframe(highlight_score, use_container_width=True)
                    
                    st.dataframe(rank_score_data.style.background_gradient(cmap='RdYlGn_r', axis=None).format(precision=0, na_rep="-"), use_container_width=True)
                else:
                    st.warning("Data tidak mencukupi untuk memvisualisasikan heatmap skor komposit.")

        with tab_heat2:
            st.markdown(f"**Heatmap Peringkat Performa (Kenaikan Harga Absolut)**")
            
            period_ranks_df = get_period_performance_ranking(
                df_perf_full, 
                trading_days_interval=trading_interval, 
                num_periods=num_columns
            )
            
            if not period_ranks_df.empty:
                period_ranks_df = period_ranks_df.sort_values(by=period_ranks_df.columns[-1], ascending=True)
                highlight_period = get_custom_highlights(period_ranks_df, top_n=selected_top_n)
                
                if not highlight_period.empty:
                    st.markdown(f"🌟 **Highlights (Paling Konsisten Masuk Top {selected_top_n}):**")
                    st.dataframe(highlight_period, use_container_width=True)
                    
                st.dataframe(period_ranks_df.style.background_gradient(cmap='RdYlGn_r', axis=None).format(precision=0, na_rep="-"), use_container_width=True)
            else:
                st.warning("Data historis tidak mencukupi untuk membuat heatmap performa dengan parameter tersebut.")

        with tab_heat3:
            st.markdown(f"**Heatmap Persentase Perubahan Bulanan ({num_columns} Bulan Terakhir)**")
            monthly_pct_df = get_monthly_pct_change(df_perf_full)
            if not monthly_pct_df.empty:
                # Potong kolom tepat sebanyak jumlah periode yang diminta
                monthly_pct_df = monthly_pct_df.iloc[:, -num_columns:]
                
                monthly_ranks_temp = monthly_pct_df.rank(axis=0, ascending=False, method='min')
                highlight_monthly = get_custom_highlights(monthly_ranks_temp, top_n=selected_top_n)
                
                if not highlight_monthly.empty:
                    st.markdown(f"🌟 **Highlights (Paling Sering Masuk Top {selected_top_n} Profit Bulanan Tertinggi):**")
                    st.dataframe(highlight_monthly, use_container_width=True)
                
                monthly_pct_df['Rata-rata MoM'] = monthly_pct_df.mean(axis=1)
                monthly_pct_df = monthly_pct_df.sort_values('Rata-rata MoM', ascending=False)
                st.dataframe(monthly_pct_df.style.background_gradient(cmap='RdYlGn', axis=None).format("{:.2f}%", na_rep="-"), use_container_width=True)
            else:
                st.warning("Data tidak mencukupi untuk membentuk interval bulanan.")
                
    else:
        st.warning(f"Tidak ada data {title} atau data tidak mencukupi untuk perhitungan performa.")

# ==================== TAB 5: PERBANDINGAN HISTORIS ====================
with tab_compare:
    st.header("📉 Perbandingan Historis & Analisis Volatilitas")
    st.info(f"""ℹ️ **Panduan Analisis Grafik ({date_option} | {start_date_str} s/d {end_date_str}):**
    - **Kinerja Absolut & Relatif:** Melacak tren Harga (NAV) aktual, akumulasi keuntungan (Return Kumulatif), dan risiko penurunan terdalam dari titik puncak (Drawdown).
    - **Pita Volatilitas (Standard Deviation Bands):** Memvisualisasikan area kewajaran harga. Harga yang menyentuh pita atas (+2 atau +3 SD) mengindikasikan area jenuh beli (*Overbought*/Mahal), sedangkan sentuhan di pita bawah (-2 atau -3 SD) menunjukkan jenuh jual (*Oversold*/Murah).
    - **Pergerakan Metrik Harian (Rolling):** Memantau tren perubahan metrik **Alpha, Beta (terhadap {selected_benchmark_label}), Sharpe Ratio, dan Volatilitas** secara dinamis dari hari ke hari, berguna untuk melihat apakah kinerja manajer investasi konsisten atau hanya kebetulan di satu waktu.""")
    
    available_instruments = df_all_instruments.columns.tolist()
    selected_instruments = st.multiselect(
        "Pilih Instrumen untuk Dibandingkan",
        options=available_instruments,
        default=available_instruments[:min(2, len(available_instruments))] if available_instruments else [],
        key="compare_multiselect"
    )
    
    # Syarat diubah menjadi minimal 1 instrumen agar analisis volatilitas tunggal dapat dilakukan
    if len(selected_instruments) >= 1:
        df_compare = df_all_instruments[selected_instruments].copy()
        df_compare = df_compare.ffill().bfill()
        
        legend_layout = dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5, title=None)
        
        # --- 1. Kinerja Absolut & Relatif ---
        st.subheader("📊 Kinerja Absolut & Relatif")
        
        fig_prices = px.line(df_compare, x=df_compare.index, y=df_compare.columns, title="Harga Historis Aktual (NAV)")
        fig_prices.update_layout(xaxis_title="Tanggal", yaxis_title="Harga", legend=legend_layout)
        st.plotly_chart(fig_prices, use_container_width=True)
        
        # Kalkulasi Return Kumulatif
        returns_cum = (df_compare.pct_change().fillna(0) + 1).cumprod() - 1
        df_returns_pct = returns_cum * 100
        
        fig_returns = px.line(df_returns_pct, x=df_returns_pct.index, y=df_returns_pct.columns, title="Return Kumulatif (%)")
        fig_returns.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        
        # --- Tambahkan anotasi angka (persentase) di ujung kanan garis ---
        if not df_returns_pct.empty:
            last_date = df_returns_pct.index[-1]
            for col in df_returns_pct.columns:
                last_val = df_returns_pct[col].iloc[-1]
                
                # Menambahkan label teks tanpa panah tepat di samping titik data terakhir
                fig_returns.add_annotation(
                    x=last_date,
                    y=last_val,
                    text=f"<b>{last_val:.2f}%</b>",
                    showarrow=False,
                    xanchor="left",
                    xshift=8, 
                    font=dict(size=11)
                )

        # Tambahkan margin kanan (r=60) agar angka tidak terpotong batas kanvas grafik
        fig_returns.update_layout(
            xaxis_title="Tanggal", 
            yaxis_title="Return (%)", 
            legend=legend_layout,
            margin=dict(r=60) 
        )
        st.plotly_chart(fig_returns, use_container_width=True)
        
        running_max = df_compare.expanding().max()
        drawdown = (df_compare - running_max) / running_max * 100
        fig_dd = px.line(drawdown, x=drawdown.index, y=drawdown.columns, title="Drawdown dari Nilai Tertinggi (%)")
        fig_dd.update_layout(xaxis_title="Tanggal", yaxis_title="Drawdown (%)", yaxis_tickformat='.1f', legend=legend_layout)
        st.plotly_chart(fig_dd, use_container_width=True)
        
        st.divider()
        
        # --- 2. Analisis Volatilitas Dinamis ---
        total_days_full = len(df_all_instruments_full)
        
        # Menentukan window secara dinamis mengikuti opsi di sidebar
        if date_option == "1 Bulan":
            target_window = 22
        elif date_option == "3 Bulan":
            target_window = 63
        elif date_option == "6 Bulan":
            target_window = 126
        elif date_option == "1 Tahun":
            target_window = 252
        else:
            target_window = 252
            
        # Pengaman: Jika data mentah kurang panjang, sesuaikan otomatis agar grafik tidak error
        if target_window >= total_days_full - 5:
            dynamic_window = max(22, total_days_full // 3) 
            st.warning(f"Data historis ({total_days_full} hari) tidak cukup panjang untuk rolling {target_window} hari secara penuh. Window disesuaikan menjadi {dynamic_window} hari.")
        else:
            dynamic_window = target_window

        st.subheader(f"📈 Pita Volatilitas Harga Mentah / NAV (Rolling {dynamic_window} Hari)")
        
        # --- TOGGLE TEMA GRAFIK ---
        chart_theme = st.radio("Pilih Tema Visual Grafik:", ["Dark Theme", "Light Theme"], horizontal=True, key="band_theme_radio")

        for inst in selected_instruments:
            # Gunakan ffill().bfill() agar indeks waktu tidak terputus
            inst_nav_full = df_all_instruments_full[inst].ffill().bfill()
            
            # Kalkulasi Pita Volatilitas (Bands) menggunakan window yang sama dengan sidebar
            roll_mean_full = inst_nav_full.rolling(window=dynamic_window).mean()
            roll_std_full = inst_nav_full.rolling(window=dynamic_window).std()
            
            upper_1sd_full = roll_mean_full + (1 * roll_std_full)
            lower_1sd_full = roll_mean_full - (1 * roll_std_full)
            upper_2sd_full = roll_mean_full + (2 * roll_std_full)
            lower_2sd_full = roll_mean_full - (2 * roll_std_full)
            upper_3sd_full = roll_mean_full + (3 * roll_std_full)
            lower_3sd_full = roll_mean_full - (3 * roll_std_full)
            
            # Potong (slice) rentang waktu untuk tampilan layar
            inst_nav = safe_slice(inst_nav_full, ana_start_dt, ana_end_dt)
            roll_mean = safe_slice(roll_mean_full, ana_start_dt, ana_end_dt)
            upper_1sd = safe_slice(upper_1sd_full, ana_start_dt, ana_end_dt)
            lower_1sd = safe_slice(lower_1sd_full, ana_start_dt, ana_end_dt)
            upper_2sd = safe_slice(upper_2sd_full, ana_start_dt, ana_end_dt)
            lower_2sd = safe_slice(lower_2sd_full, ana_start_dt, ana_end_dt)
            upper_3sd = safe_slice(upper_3sd_full, ana_start_dt, ana_end_dt)
            lower_3sd = safe_slice(lower_3sd_full, ana_start_dt, ana_end_dt)

            fig_band = go.Figure()
            
            # --- KONFIGURASI WARNA BERDASARKAN TEMA ---
            if chart_theme == "Dark Theme":
                nav_color = 'white'
                mean_color = 'cyan'
                sd1_color = 'rgba(0, 255, 127, 0.9)' # Terang: Spring Green
                sd2_color = 'rgba(255, 215, 0, 0.9)' # Terang: Gold
                sd3_color = 'rgba(255, 69, 0, 0.9)'  # Terang: Red Orange
                template_style = "plotly_dark"
            else:
                nav_color = 'black'
                mean_color = 'blue'
                sd1_color = 'rgba(44, 160, 44, 0.6)'
                sd2_color = 'rgba(255, 127, 14, 0.6)'
                sd3_color = 'rgba(214, 39, 40, 0.6)'
                template_style = "plotly_white"

            # Injeksi warna dinamis ke dalam trace grafik
            fig_band.add_trace(go.Scatter(x=inst_nav.index, y=inst_nav, mode='lines', name='NAV Aktual', line=dict(color=nav_color, width=2.5)))
            fig_band.add_trace(go.Scatter(x=roll_mean.index, y=roll_mean, mode='lines', name=f'Mean ({dynamic_window}d)', line=dict(color=mean_color, width=1.5, dash='dot')))
            
            fig_band.add_trace(go.Scatter(x=upper_1sd.index, y=upper_1sd, mode='lines', name='+1 SD', line=dict(color=sd1_color, width=1, dash='dash')))
            fig_band.add_trace(go.Scatter(x=lower_1sd.index, y=lower_1sd, mode='lines', name='-1 SD', line=dict(color=sd1_color, width=1, dash='dash')))
            
            fig_band.add_trace(go.Scatter(x=upper_2sd.index, y=upper_2sd, mode='lines', name='+2 SD', line=dict(color=sd2_color, width=1, dash='dash')))
            fig_band.add_trace(go.Scatter(x=lower_2sd.index, y=lower_2sd, mode='lines', name='-2 SD', line=dict(color=sd2_color, width=1, dash='dash')))
            
            fig_band.add_trace(go.Scatter(x=upper_3sd.index, y=upper_3sd, mode='lines', name='+3 SD', line=dict(color=sd3_color, width=1, dash='dash')))
            fig_band.add_trace(go.Scatter(x=lower_3sd.index, y=lower_3sd, mode='lines', name='-3 SD', line=dict(color=sd3_color, width=1, dash='dash')))
            
            fig_band.update_layout(
                title=f"Distribusi Harga & Pita Volatilitas: {inst}", 
                xaxis_title="Tanggal", 
                yaxis_title="NAV / Harga", 
                legend=legend_layout, 
                hovermode="x unified",
                template=template_style # Paksa template bawaan Plotly
            )
            
            # PENTING: theme=None mematikan override warna default dari Streamlit
            st.plotly_chart(fig_band, use_container_width=True, theme=None) 
            
        st.divider()
                
        # --- 3. Pergerakan Metrik Harian ---
        st.subheader(f"📊 Grafik Pergerakan Metrik Harian (Rolling {dynamic_window} Hari)")
        
        # Kalkulasi metrik rolling menggunakan dynamic_window yang sama
        df_selected_full = df_all_instruments_full[selected_instruments]
        dynamic_ts = calculate_rolling_timeseries(df_selected_full, benchmark_series_full, risk_free_rate, window=dynamic_window)
        sliced_ts_dict = {k: safe_slice(v, ana_start_dt, ana_end_dt) for k, v in dynamic_ts.items()}
        
        ts_data = {}
        for metric_name, ts_df in sliced_ts_dict.items():
            available_cols = [col for col in selected_instruments if col in ts_df.columns]
            if available_cols:
                ts_data[metric_name] = ts_df[available_cols]
                
        if ts_data:
            if 'Alpha' in ts_data and not ts_data['Alpha'].empty:
                fig_alpha = px.line(ts_data['Alpha'], title=f"Pergerakan Alpha dengan Benchmark {selected_benchmark_label} ({dynamic_window} Hari)")
                fig_alpha.update_layout(xaxis_title="Tanggal", yaxis_title="Alpha", legend=legend_layout)
                st.plotly_chart(fig_alpha, use_container_width=True)
                
            if 'Beta' in ts_data and not ts_data['Beta'].empty:
                fig_beta = px.line(ts_data['Beta'], title=f"Pergerakan Beta dengan Benchmark {selected_benchmark_label} ({dynamic_window} Hari)")
                fig_beta.update_layout(xaxis_title="Tanggal", yaxis_title="Beta", legend=legend_layout)
                st.plotly_chart(fig_beta, use_container_width=True)
                
            if 'Sharpe_Ratio' in ts_data and not ts_data['Sharpe_Ratio'].empty:
                fig_sharpe = px.line(ts_data['Sharpe_Ratio'], title=f"Pergerakan Sharpe Ratio dengan Benchmark {selected_benchmark_label} ({dynamic_window} Hari)")
                fig_sharpe.update_layout(xaxis_title="Tanggal", yaxis_title="Sharpe Ratio", legend=legend_layout)
                st.plotly_chart(fig_sharpe, use_container_width=True)
                
            if 'Volatility' in ts_data and not ts_data['Volatility'].empty:
                fig_vol = px.line(ts_data['Volatility'], title=f"Pergerakan Risk (Std Dev, {dynamic_window} Hari)")
                fig_vol.update_layout(xaxis_title="Tanggal", yaxis_title="Volatility", legend=legend_layout)
                st.plotly_chart(fig_vol, use_container_width=True)
        else:
            st.info("Tidak ada data metrik time-series untuk instrumen yang dipilih.")
            
    else:
        st.info("Silakan pilih instrumen dari dropdown di atas untuk memulai analisis.")
        
# ==================== TAB 6: REKOMENDASI FUNDAMENTAL (MANUAL UPLOAD) ====================
with tab_recommendation:
    st.header("🎯 Peringkat Fundamental (Data Manual)")
    st.info("Unggah file Excel atau CSV berisi metrik fundamental. Sistem memprioritaskan skor tinggi untuk Alpha/Sharpe/Treynor dan skor rendah untuk StdDev.")

    # 1. Definisikan Template Contoh Data
    template_data = pd.DataFrame({
        'Nama Produk': ['Reksa Dana A', 'Reksa Dana B', 'Reksa Dana C'],
        'Alpha': [0.0521, 0.0310, 0.0415],
        'Beta': [1.05, 0.95, 1.10],
        'Sharpe': [0.12, 0.09, 0.15],
        'Treynor': [0.08, 0.05, 0.10],
        'StdDev': [0.15, 0.11, 0.18]
    })

    # 2. Komponen Upload File
    uploaded_file = st.file_uploader("Unggah Dokumen Metrik (Excel / CSV)", type=["xlsx", "csv"], key="fund_uploader")
    
    # Flag status validasi
    data_valid = False
    df_fund = pd.DataFrame()

    # 3. Proses Validasi File
    if uploaded_file is not None:
        try:
            if uploaded_file.name.endswith('.csv'):
                df_fund = pd.read_csv(uploaded_file)
            else:
                df_fund = pd.read_excel(uploaded_file)
            
            # Standarisasi kolom identitas
            if 'Nama Produk' not in df_fund.columns and 'Instrument' in df_fund.columns:
                df_fund = df_fund.rename(columns={'Instrument': 'Nama Produk'})
                
            # Cek ketersediaan kolom wajib
            required_metrics = ['Alpha', 'Beta', 'Sharpe', 'Treynor', 'StdDev']
            available_metrics = [c for c in required_metrics if c in df_fund.columns]
            
            if 'Nama Produk' in df_fund.columns and len(available_metrics) > 0:
                data_valid = True
            else:
                st.error("❌ Format file tidak sesuai! Pastikan terdapat kolom 'Nama Produk' dan minimal satu kolom metrik yang penulisannya persis seperti contoh.")
        except Exception as e:
            st.error(f"❌ Gagal membaca dokumen: {e}")

    # 4. Logika Tampilan (Render UI)
    # 4. Logika Tampilan (Render UI)
    if not data_valid:
        # Jika belum ada file atau file salah, tampilkan panduan & template
        st.write("### 💡 Contoh Format Tabel yang Diterima")
        st.caption("Pastikan nama kolom di baris pertama persis seperti contoh di bawah ini. Kolom yang tidak tersedia/kosong akan diabaikan secara otomatis.")
        st.dataframe(template_data, hide_index=True, use_container_width=True)
        
    else:
        # Jika file valid, sembunyikan template dan jalankan kalkulasi
        df_fund = df_fund.set_index('Nama Produk')
        
        # Konversi tipe data ke numerik
        for col in available_metrics:
            df_fund[col] = pd.to_numeric(df_fund[col], errors='coerce')
            
        # Hapus baris yang tidak memiliki angka sama sekali di bagian metrik
        df_fund = df_fund.dropna(subset=available_metrics, how='all')
        
        if not df_fund.empty:
            with st.spinner("Mengkalkulasi peringkat komposit..."):
                # Ranking: Skor 1 untuk metrik tertinggi
                if 'Alpha' in df_fund.columns: df_fund['Rank_Alpha'] = df_fund['Alpha'].rank(ascending=False, method='min')
                if 'Sharpe' in df_fund.columns: df_fund['Rank_Sharpe'] = df_fund['Sharpe'].rank(ascending=False, method='min')
                if 'Treynor' in df_fund.columns: df_fund['Rank_Treynor'] = df_fund['Treynor'].rank(ascending=False, method='min')
                
                # Beta dikalkulasi sesuai kodemu (skor tinggi = nilai besar)
                if 'Beta' in df_fund.columns: df_fund['Rank_Beta'] = df_fund['Beta'].rank(ascending=False, method='max')  
                
                # Ranking: Skor 1 untuk metrik terendah (Risiko)
                if 'StdDev' in df_fund.columns: df_fund['Rank_StdDev'] = df_fund['StdDev'].rank(ascending=True, method='min')
                
                rank_cols = [c for c in df_fund.columns if c.startswith('Rank_')]
                
                rank_cols = [c for c in df_fund.columns if c.startswith('Rank_')]
                
                if rank_cols:
                    # Hitung Skor Akhir (Rata-rata peringkat)
                    df_fund['Total_Score'] = df_fund[rank_cols].mean(axis=1)
                    df_fund['Final_Rank'] = df_fund['Total_Score'].rank(ascending=True, method='min')
                    
                    # Urutkan dari Peringkat 1
                    df_fund = df_fund.sort_values('Final_Rank')
                    
                    # Susun kolom berdampingan (Nilai Mentah -> Peringkatnya)
                    cols_order = []
                    for col in available_metrics:
                        cols_order.append(col)
                        rank_col_name = f'Rank_{col}'
                        if rank_col_name in df_fund.columns:
                            cols_order.append(rank_col_name)
                    
                    # Masukkan kolom tambahan di luar metrik utama jika ada
                    extra_cols = [c for c in df_fund.columns if c not in cols_order and c not in ['Total_Score', 'Final_Rank']]
                    
                    # Gabungkan urutan akhir
                    final_cols = extra_cols + cols_order + ['Total_Score', 'Final_Rank']
                    df_display = df_fund[final_cols].copy()
                    
                    # Ubah nama kolom agar rapi saat ditampilkan
                    rename_map = {
                        'Total_Score': 'Total Skor',
                        'Final_Rank': 'Peringkat Akhir'
                    }
                    for col in available_metrics:
                        rename_map[f'Rank_{col}'] = f'Rank {col}'
                        
                    df_display = df_display.rename(columns=rename_map)
                    
                    # Atur jumlah desimal untuk semua kolom dengan nama baru
                    format_dict = {}
                    for col in df_display.columns:
                        if col in available_metrics:
                            format_dict[col] = "{:.4f}"
                        elif col.startswith('Rank ') or col == 'Peringkat Akhir':
                            format_dict[col] = "{:.0f}"
                        elif col == 'Total Skor':
                            format_dict[col] = "{:.2f}"
                    
                    # Warnai baris pemenang
                    styled_df = (
                        df_display.style
                        .background_gradient(subset=['Peringkat Akhir'], cmap='RdYlGn_r')
                        .format(format_dict)
                    )
                    
                    st.success("✅ Dokumen berhasil diproses.")
                    st.subheader("📋 Peringkat Fundamental Komposit")
                    st.dataframe(styled_df, use_container_width=True, height=600)
                else:
                    st.warning("Data tidak memiliki kolom performa/risiko yang valid untuk dihitung peringkatnya.")
        else:
            st.warning("Data kosong setelah dibersihkan. Pastikan sel metrik berisi angka yang valid.")
# ==================== FOOTER ====================
st.divider()
st.caption("Data disediakan oleh Refinitiv. Dashboard ini untuk tujuan informasi dan edukasi.")
