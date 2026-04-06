import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import datetime as dt
import refinitiv.data as rd
import time
# ... (import Anda yang lain) ...
from supabase import create_client, Client

# ==================== KONEKSI SUPABASE ====================
@st.cache_resource
def init_supabase() -> Client:
    """Membangun koneksi ke database Supabase secara global."""
    try:
        url: str = st.secrets["supabase"]["url"]
        key: str = st.secrets["supabase"]["key"]
        return create_client(url, key)
    except KeyError as e:
        st.error(f"❌ Kredensial Supabase tidak ditemukan di secrets.toml: missing {e}")
        st.stop()
    except Exception as e:
        st.error(f"❌ Gagal menginisialisasi klien Supabase: {e}")
        st.stop()

# Eksekusi pembuatan klien dan simpan di memori global
supabase = init_supabase()

# ==================== FUNGSI BACA DB ====================
def get_sync_start_date(): 
    """Membaca tanggal update terakhir dari tiap instrumen/tabel utama, lalu mengambil tanggal yang paling tertinggal."""
    dates = []
    tables = ["mf_nav_daily", "gov_bonds_prices_daily", "macro_daily"]
    
    for table in tables:
        try:
            response = supabase.table(table).select("date").order("date", desc=True).limit(1).execute()
            if response.data:
                dates.append(pd.to_datetime(response.data[0]['date']).date())
        except Exception:
            pass 
            
    if dates:
        # Ambil tanggal yang paling lama (paling tertinggal) dari tanggal-tanggal update terakhir
        return min(dates)
    return dt.datetime.today().date() - dt.timedelta(days=30)

@st.cache_data(ttl=3600, show_spinner=False)
def load_master_instruments():
    """Mengambil Master Data murni dari Database"""
    mf_data = supabase.table("mf_instruments").select("*").execute().data
    bond_data = supabase.table("gov_bonds_instruments").select("*").execute().data
    macro_data = supabase.table("macro_instruments").select("*").execute().data
    return mf_data, bond_data, macro_data

def fetch_from_supabase(table_name, id_col, tickers, start_date, end_date):
    """Fungsi pembantu untuk menarik data masif dari Supabase dengan Pagination & Ordering."""
    if not tickers: return pd.DataFrame()
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    all_data = []
    page_size = 1000
    offset = 0
    
    while True:
        try:
            res = supabase.table(table_name).select("*") \
                .in_(id_col, tickers) \
                .gte('date', start_str) \
                .lte('date', end_str) \
                .order('date') \
                .range(offset, offset + page_size - 1) \
                .execute()
                
            if not res.data:
                break
                
            all_data.extend(res.data)
            
            if len(res.data) < page_size:
                break 
                
            offset += page_size
        except Exception as e:
            st.error(f"Error fetching from {table_name}: {e}")
            break
            
    if not all_data: return pd.DataFrame()
    
    df = pd.DataFrame(all_data)
    # HAPUS .dt.date agar menjadi DatetimeIndex murni Pandas
    df['Date'] = pd.to_datetime(df['date']) 
    return df

# ==================== FUNGSI INISIALISASI SESI REFINITIV ====================
def init_refinitiv_session():
    """Hanya membuka sesi Refinitiv tanpa menarik data RFR awal."""
    try:
        config = st.secrets["refinitiv"]
        session = rd.session.platform.Definition(
            app_key=config["app_key"],
            grant=rd.session.platform.GrantPassword(
                username=config["username"],
                password=config["password"]
            )
        ).get_session()
        
        session.open()
        rd.session.set_default(session)
        return True
    except Exception as e:
        st.error(f"❌ Gagal membuka sesi Refinitiv: {e}")
        return False

# ==================== FUNGSI DELTA SYNC (SINKRONISASI HARIAN) ====================
def run_daily_sync(start_date, end_date):
    """Menarik delta data dari Refinitiv dan mengirimnya ke Supabase."""
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    params = {'SDate': start_str, 'EDate': end_str, 'Frq': 'D'}
    
    def process_and_upload(df_raw, table_name, value_cols, rename_mapping):
        if df_raw is None or df_raw.empty: return
        df_raw = df_raw.loc[:, ~df_raw.columns.duplicated()]
        df_clean = df_raw.rename(columns=rename_mapping)
        if 'date' not in df_clean.columns: return
        existing_val_cols = [col for col in value_cols if col in df_clean.columns]
        if not existing_val_cols: return
        df_clean[existing_val_cols] = df_clean[existing_val_cols].replace(r'^\s*$', np.nan, regex=True)
        df_clean = df_clean.dropna(subset=['date', existing_val_cols[0]]) 
        if df_clean.empty: return
        df_clean['date'] = pd.to_datetime(df_clean['date']).dt.strftime('%Y-%m-%d')
        id_col = 'isin_code' if 'isin_code' in df_clean.columns else 'ticker'
        df_clean = df_clean.drop_duplicates(subset=[id_col, 'date'], keep='last')
        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)
        payload = df_clean.to_dict(orient='records')
        for i in range(0, len(payload), 1000):
            try: supabase.table(table_name).upsert(payload[i:i+1000]).execute()
            except: pass

    mf_master, bond_master, macro_master = load_master_instruments()

    # 1. Sync Reksa Dana
    tickers_mf = [x['ticker'] for x in mf_master]
    for i in range(0, len(tickers_mf), 15):
        try:
            df_mf = rd.get_data(universe=tickers_mf[i:i+15], fields=['TR.NETASSETVAL.date', 'TR.NETASSETVAL'], parameters=params)
            process_and_upload(df_mf, "mf_nav_daily", ["nav"], {'Instrument': 'ticker', 'Date': 'date', 'TR.NETASSETVAL.date': 'date', 'TR.NETASSETVAL': 'nav', 'Net Asset Value': 'nav'})
        except: pass

    # 2. Sync Obligasi
    tickers_bonds = [x['isin_code'] for x in bond_master]
    for i in range(0, len(tickers_bonds), 20):
        try:
            df_bonds = rd.get_data(universe=tickers_bonds[i:i+20], fields=['TR.ASKPRICE.date', 'TR.ASKPRICE', 'TR.BIDYIELD'], parameters=params)
            mapping_bonds = {'Instrument': 'isin_code', 'Date': 'date', 'Ask Price': 'ask_price', 'Bid Yield': 'ask_yield', 'TR.ASKPRICE.date': 'date', 'TR.ASKPRICE': 'ask_price', 'TR.BIDYIELD': 'ask_yield'}
            process_and_upload(df_bonds, "gov_bonds_prices_daily", ["ask_price", "ask_yield"], mapping_bonds)
        except: pass
        
    # (Untuk Macro di run_daily_sync tetap sama seperti kode sebelumnya, cukup gunakan macro_master)

    # 3. Sync Makro Gabungan
    macro_configs = [
        (['.JKSE', '.JKLQ45', '.JKIDX30', '.JKIDX80', '.IXIC', '.SPX', '.DXY', '.SSEC'], ['TR.PriceClose.date', 'TR.PriceClose']),
        (['ID10YT=RR', 'US10YT=RR'], ['TR.ClosePrice.date', 'TR.ClosePrice']),
        (['IDR='], ['TR.AmericaCloseBidPrice.date', 'TR.AmericaCloseBidPrice']),
        (['CLc1'], ['TR.cLOSEPrice.date', 'TR.ClosePrice'])
    ]
    for tickers, fields in macro_configs:
        try:
            df_mac = rd.get_data(universe=tickers, fields=fields, parameters=params)
            mapping_mac = {'Instrument': 'ticker', 'Date': 'date', fields[0]: 'date', fields[1]: 'value', 'Price Close': 'value', 'TR.PriceClose': 'value', 'Close Price': 'value', 'TR.ClosePrice': 'value', 'America Close Bid Price': 'value', 'America  Close Bid Price': 'value', 'cLOSE Price': 'value', 'TR.AmericaCloseBidPrice': 'value'}
            process_and_upload(df_mac, "macro_daily", ["value"], mapping_mac)
        except: pass

def backfill_new_instrument(table_dest, id_col, ticker, fields, value_cols, rename_mapping, start_date_str):
    """Fungsi mandiri untuk menarik data historis instrumen baru berdasarkan tanggal spesifik."""
    end_str = dt.datetime.today().strftime('%Y-%m-%d')
    params = {'SDate': start_date_str, 'EDate': end_str, 'Frq': 'D'}
    
    try:
        df_raw = rd.get_data(universe=[ticker], fields=fields, parameters=params)
    except Exception as e:
        st.error(f"❌ Refinitiv Error: {e}")
        return False

    if df_raw is None or df_raw.empty:
        st.warning("Data historis tidak ditemukan di Refinitiv.")
        return False

    # Pembersihan Data
    df_raw = df_raw.loc[:, ~df_raw.columns.duplicated()]

    # 1. Pencegahan Tumpang Tindih Tanggal
    date_candidates = [c for c in df_raw.columns if c.lower().endswith('.date')]
    if date_candidates and 'Date' in df_raw.columns:
        df_raw = df_raw.drop(columns=['Date']) 

    df_clean = df_raw.rename(columns=rename_mapping)
    
    if 'date' not in df_clean.columns: return False
    existing_val_cols = [col for col in value_cols if col in df_clean.columns]
    if not existing_val_cols: return False
    
    val_col = existing_val_cols[0]

    # 2. Konversi Numerik dan Penghapusan Data Kosong
    df_clean[val_col] = pd.to_numeric(df_clean[val_col], errors='coerce')
    df_clean[val_col] = df_clean[val_col].replace(0.0, np.nan)
    df_clean = df_clean.dropna(subset=['date', val_col]) 
    if df_clean.empty: return False

    df_clean = df_clean.reset_index(drop=True)

    # 3. Pemotongan Harga Stagnan (Anti-Padding) - Tetap digunakan untuk berjaga-jaga
    s = df_clean[val_col]
    diffs = s.diff().abs()
    active_mask = diffs > 0.0001 
    active_mask.iloc[0] = True 
    
    active_indices = active_mask[active_mask == True].index
    
    if len(active_indices) > 1:
        first_real_move_idx = active_indices[1]
        start_idx = max(0, first_real_move_idx - 1)
        df_clean = df_clean.iloc[start_idx:]
    else:
        if len(df_clean) > 30:
            df_clean = df_clean.tail(30) 

    # 4. Format dan Persiapan Upload
    df_clean['date'] = pd.to_datetime(df_clean['date']).dt.strftime('%Y-%m-%d')
    id_mapped = 'isin_code' if id_col == 'isin_code' else 'ticker'
    df_clean = df_clean.drop_duplicates(subset=[id_mapped, 'date'], keep='last')
    
    df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)
    
    payload = df_clean.to_dict(orient='records')  
    # Upload ke Supabase dalam batch 1000
    for i in range(0, len(payload), 1000):
        try:
            supabase.table(table_dest).upsert(payload[i:i+1000]).execute()
        except Exception as e:
            st.error(f"DB Error: {e}")
            return False
            
    return True

# ==================== KONFIGURASI HALAMAN ====================
st.set_page_config(
    page_title="Investment Dashboard - Reksa Dana Indonesia",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== FUNGSI HELPER UNTUK LOADING DATA (DB ONLY) ====================
@st.cache_data(ttl=43200, show_spinner=False)
def load_all_data(start_date, end_date, currency='IDR'):
    mf_master, bond_master, macro_master = load_master_instruments()
    
    # 1. Filter & Mapping Reksa Dana
    mf_filtered = [x for x in mf_master if x['currency'] == currency]
    tickers_equity = [x['ticker'] for x in mf_filtered if x['fund_type'] == 'Equity']
    tickers_bond = [x['ticker'] for x in mf_filtered if x['fund_type'] == 'Fixed Income']
    map_ticker_mf = {x['ticker']: x['name'] for x in mf_filtered}
    
    # 2. Filter & Mapping Obligasi Negara
    bond_filtered = [x for x in bond_master if x['currency'] == currency]
    tickers_gov_bonds = [x['isin_code'] for x in bond_filtered]
    map_isin_bond = {x['isin_code']: x['name'] for x in bond_filtered}
    
    # 3. Filter & Mapping Makro (Langsung ambil semua tanpa filter currency)
    tickers_index_saham = [x['ticker'] for x in macro_master if x['category'] == 'Index']
    tickers_suku_bunga = [x['ticker'] for x in macro_master if x['category'] == 'Interest Rate']
    tickers_mata_uang = [x['ticker'] for x in macro_master if x['category'] == 'Currency']
    tickers_komoditas = [x['ticker'] for x in macro_master if x['category'] == 'Commodity']
    tickers_macro = [x['ticker'] for x in macro_master]

    # ==================== TARIK DARI SUPABASE ====================
    df_mf_raw = fetch_from_supabase('mf_nav_daily', 'ticker', tickers_equity + tickers_bond, start_date, end_date)
    df_gov_raw = fetch_from_supabase('gov_bonds_prices_daily', 'isin_code', tickers_gov_bonds, start_date, end_date)
    df_macro_raw = fetch_from_supabase('macro_daily', 'ticker', tickers_macro, start_date, end_date)

    def safe_pivot(df, id_col, val_col):
        if df.empty: return pd.DataFrame()
        df = df.drop_duplicates(subset=[id_col, 'Date'], keep='last')
        return df.pivot(index='Date', columns=id_col, values=val_col).sort_index()

    # Ekstrak Reksa Dana
    df_equity_wide, df_bond_wide = pd.DataFrame(), pd.DataFrame()
    if not df_mf_raw.empty:
        df_equity_sub = df_mf_raw[df_mf_raw['ticker'].isin(tickers_equity)].copy()
        df_bond_sub = df_mf_raw[df_mf_raw['ticker'].isin(tickers_bond)].copy()
        
        df_equity_sub['Instrument'] = df_equity_sub['ticker'].map(map_ticker_mf).fillna(df_equity_sub['ticker'])
        df_bond_sub['Instrument'] = df_bond_sub['ticker'].map(map_ticker_mf).fillna(df_bond_sub['ticker'])
        
        df_equity_wide = safe_pivot(df_equity_sub, 'Instrument', 'nav')
        df_bond_wide = safe_pivot(df_bond_sub, 'Instrument', 'nav')

    # Ekstrak Obligasi Negara
    df_gov_bonds_price, df_gov_bonds_yield = pd.DataFrame(), pd.DataFrame()
    if not df_gov_raw.empty:
        df_gov_raw['Instrument'] = df_gov_raw['isin_code'].map(map_isin_bond).fillna(df_gov_raw['isin_code'])
        df_gov_bonds_price = safe_pivot(df_gov_raw, 'Instrument', 'ask_price')
        df_gov_bonds_yield = safe_pivot(df_gov_raw, 'Instrument', 'ask_yield')

    # Ekstrak Makro
    df_index_wide, df_suku_bunga_wide, df_mata_uang_wide, df_komoditas_wide = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    if not df_macro_raw.empty:
        df_idx_sub = df_macro_raw[df_macro_raw['ticker'].isin(tickers_index_saham)]
        df_suku_sub = df_macro_raw[df_macro_raw['ticker'].isin(tickers_suku_bunga)]
        df_uang_sub = df_macro_raw[df_macro_raw['ticker'].isin(tickers_mata_uang)]
        df_komo_sub = df_macro_raw[df_macro_raw['ticker'].isin(tickers_komoditas)]
        
        df_index_wide = safe_pivot(df_idx_sub, 'ticker', 'value')
        df_suku_bunga_wide = safe_pivot(df_suku_sub, 'ticker', 'value')
        df_mata_uang_wide = safe_pivot(df_uang_sub, 'ticker', 'value')
        df_komoditas_wide = safe_pivot(df_komo_sub, 'ticker', 'value')

    if df_equity_wide.empty:
        st.error("Data Equity kosong di database untuk mata uang ini.")
        st.stop()

    # --- PEMBERSIH ZOMBIE DAYS ---
    def clean_trading_days(df):
        """Sebuah hari diakui sebagai hari bursa jika ADA minimal 1 produk yang update harga."""
        if df.empty: return df
        df_filled = df.ffill()
        pct = df_filled.pct_change()
        
        valid_days = pct.isna().all(axis=1) | ((pct.abs() > 0).sum(axis=1) > 0)
        return df.loc[valid_days]

    # 1. Bersihkan data Equity dan Bond secara mandiri (Independen)
    df_equity_wide = clean_trading_days(df_equity_wide)
    df_bond_wide = clean_trading_days(df_bond_wide)
    
    # 2. GABUNGKAN KALENDER (Union): Jangan biarkan Equity mendikte Bond
    # Jika Equity libur tapi Bond jalan (atau sebaliknya), tanggal tetap diakui.
    master_dates = df_equity_wide.index.union(df_bond_wide.index).sort_values()

    def align_wide_df(df_wide):
        if df_wide.empty: return df_wide
        # 3. Selaraskan semua data ke kalender gabungan
        return df_wide.reindex(master_dates).ffill()

    return {
        'equity': align_wide_df(df_equity_wide), 'bond': align_wide_df(df_bond_wide),
        'index': align_wide_df(df_index_wide), 'suku_bunga': align_wide_df(df_suku_bunga_wide),
        'mata_uang': align_wide_df(df_mata_uang_wide), 'komoditas': align_wide_df(df_komoditas_wide),
        'gov_bonds_price': align_wide_df(df_gov_bonds_price), 'gov_bonds_yield': align_wide_df(df_gov_bonds_yield)
    }, start_date, end_date

@st.cache_data(max_entries=50, show_spinner=False)
def calculate_metrics(price_data, benchmark_series, risk_free_rate, eval_window=None, young_funds_list=None, bench_ticker=""):
    price_data = price_data.dropna(axis=1, how='all')
    price_data = price_data.ffill().bfill()
    
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
    days = (price_data_risk.index[-1] - price_data_risk.index[0]).days
    annualization_factor = 252 / days * len(returns_risk)
    
    risk_period_return = (price_data_risk.iloc[-1] / price_data_risk.iloc[0]) - 1 if days_risk > 0 else 0
    annualized_return = ((1 + risk_period_return) ** annualization_factor) - 1
    
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
        metrics_df['climb_21d'] = get_safe_past_rank(22) - rank_today
    else:
        for c in ['Climb_1d', 'Climb_7d', 'Climb_14d', 'climb_21d']: metrics_df[c] = 0
            
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
            status_valuasi[col] = "⚠️ Data Terbatas"
            skor_valuasi[col] = np.nan 
            continue
            
        if std_price[col] != 0 and not pd.isna(std_price[col]):
            z_val = (price_data_risk[col].iloc[-1] - mean_price[col]) / std_price[col]
            z_score[col] = z_val
            if z_val >= 3.0: status_valuasi[col] = "🔴 Sangat Mahal"
            elif z_val <= -3.0: status_valuasi[col] = "🟢 Sangat Murah"
            elif z_val < 3.0 and z_val >= 2.0: status_valuasi[col] = "🔴 Mahal"
            elif z_val > -3.0 and z_val <= -2.0: status_valuasi[col] = "🟢 Murah"
            elif z_val < 2.0 and z_val >= 1.0: status_valuasi[col] = "🟠 Sedikit Mahal"
            elif z_val > -2.0 and z_val <= -1.0: status_valuasi[col] = "🟢 Sedikit Murah"
            elif z_val < 1.0 and z_val >= -1.0: status_valuasi[col] = "⚪ Fair Price"
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
def calculate_ranking_scores(metrics_df, weights=None):
    w = 1.0 / 25.0
    if weights is None:
        weights = {
            'Return_1W': w, 'Return_1M': w, 'Return_3M': w,
            'Sharpe_Ratio': w, 'Alpha': w, 'Beta': w, 'Volatility': -w,
            'Consist_1d_Top5': w, 'Consist_1d_Top10': w, 'Consist_1d_Top20': w,
            'Consist_7d_Top5': w, 'Consist_7d_Top10': w, 'Consist_7d_Top20': w,
            'Consist_14d_Top5': w, 'Consist_14d_Top10': w, 'Consist_14d_Top20': w,
            'Consist_21d_Top5': w, 'Consist_21d_Top10': w, 'Consist_21d_Top20': w,
            'Climb_1d': w, 'Climb_7d': w, 'Climb_14d': w, 'climb_21d': w
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
            ranks = calculate_ranking_scores(metrics, weights=custom_weights)
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
            ranks = calculate_ranking_scores(metrics, weights=custom_weights)
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

def validate_ticker(ticker, product_type):
    """Validasi apakah ticker ada di Refinitiv"""
    try:
        df = rd.get_data(universe=[ticker], fields=['TR.NETASSETVAL'])
        return not df.empty
    except:
        return False

# ==================== GLOBAL CACHE REGISTRY ====================
@st.cache_resource
def get_global_cache_registry():
    # Set ini bertahan secara global di server, tidak hilang saat refresh
    return set()

global_cache = get_global_cache_registry()

# ==================== INISIALISASI SESSION STATE ====================
if 'connected' not in st.session_state:
    st.session_state.connected = False

# 1. Tetapkan format tanggal default yang pasti sejak awal
default_end_date = dt.datetime.today().date()
# UBAH: Mundur 20 tahun (365 * 20) agar rentang memori menampung data tua untuk deteksi umur
default_start_date = default_end_date - dt.timedelta(days=365 * 20)

if 'fund_currency' not in st.session_state:
    st.session_state.fund_currency = 'IDR'
if 'start_date' not in st.session_state:
    st.session_state.start_date = default_start_date
if 'end_date' not in st.session_state:
    st.session_state.end_date = default_end_date

if 'gov_bonds_loaded' not in st.session_state:
    st.session_state.gov_bonds_loaded = False

# UBAH BLOK INI:
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = True # Paksa menjadi True agar aplikasi langsung memuat
        
    # Cek cache sekunder (Obligasi Negara)
    bonds_key = f"BONDS_{default_start_date.strftime('%Y-%m-%d')}_{default_end_date.strftime('%Y-%m-%d')}"
    if bonds_key in global_cache:
        st.session_state.gov_bonds_loaded = True

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
    st.sidebar.divider()
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
        # HAPUS LOGIKA CACHE KEY DI SINI, CUKUP RERUN SAJA
        st.rerun() # Ini akan membuat halaman reload dan load_all_data otomatis menyesuaikan
    
    st.divider()

    # ==========================================
    # BLOK KONEKSI & SINKRONISASI DATA
    # ==========================================
    st.subheader("🔄 Koneksi & Update Data")
    
    # Indikator Status Koneksi API
    if st.session_state.connected:
        st.success("✅ Terhubung ke API Refinitiv")
    else:
        st.warning("⚠️ API Refinitiv Belum Terhubung")

    col_btn1, col_btn2 = st.columns(2)
    
    with col_btn1:
        # 1. TOMBOL KONEKSI
        if st.button("🔌 Connect", use_container_width=True):
            with st.spinner("Menghubungkan..."):
                if init_refinitiv_session():
                    st.session_state.connected = True
                    st.rerun()
                else:
                    st.error("Koneksi gagal.")
                    
    with col_btn2:
        # 2. TOMBOL UPDATE DATA (Aktif hanya jika sudah connect)
        if st.button("📥 Update", type="primary", use_container_width=True, disabled=not st.session_state.connected):
            with st.spinner("Mengecek data tertinggal..."):
                start_d = get_sync_start_date() 
                end_d = dt.datetime.today().date()
                
                if start_d >= end_d:
                    st.info("✅ Database sudah mutakhir.")
                else:
                    st.write(f"Menarik: **{start_d.strftime('%d/%m/%y')}** - **{end_d.strftime('%d/%m/%y')}**")
                    try:
                        run_daily_sync(start_d, end_d)
                        st.session_state.end_date = end_d
                        
                        # --- TAMBAHKAN BARIS INI ---
                        load_all_data.clear() 
                        # ---------------------------
                        
                        st.success("Berhasil diupdate!")
                        time.sleep(1.5)
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Update Gagal: {e}")
                
    
    # Manajemen Produk Kustom (hanya untuk IDR)
   # ==========================================
    # MANAJEMEN INSTRUMEN (CRUD & AUTO-BACKFILL)
    # ==========================================
    st.subheader("⚙️ Database Instrumen (CRUD)")
    
    tab_mf, tab_macro, tab_del = st.tabs(["Reksa Dana", "Makro", "Hapus"])
    #,, tab_bond "Obligasi"
    # 1. FORM REKSA DANA
    with tab_mf:
        with st.form("form_add_mf"):
            mf_ticker = st.text_input("Ticker (LP...)", placeholder="Contoh: LP68059065")
            mf_name = st.text_input("Nama Reksa Dana")
            mf_type = st.selectbox("Fund Type", ["Equity", "Fixed Income"])
            mf_curr = st.selectbox("Mata Uang", ["IDR", "USD"], key="mf_c")
            mf_release_date = st.date_input("Tanggal Rilis Produk", value=dt.date(2024, 1, 1), help="Sistem akan menarik data mulai dari tanggal ini.")
            
            if st.form_submit_button("Tambah & Tarik Data"):
                if mf_ticker and mf_name:
                    with st.spinner(f"Menarik Historis dari {mf_release_date.strftime('%Y')}..."):
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                if not st.session_state.connected:
                                    st.session_state.connected = init_refinitiv_session()
                                    
                                if st.session_state.connected:
                                    if validate_ticker(mf_ticker, "MF"):
                                        supabase.table("mf_instruments").upsert([{"ticker": mf_ticker, "name": mf_name, "fund_type": mf_type, "currency": mf_curr}]).execute()
                                        
                                        # Inject Start Date dinamis
                                        success = backfill_new_instrument("mf_nav_daily", "ticker", mf_ticker, ['TR.NETASSETVAL.date', 'TR.NETASSETVAL'], ["nav"], {'Instrument': 'ticker', 'Date': 'date', 'TR.NETASSETVAL.date': 'date', 'TR.NETASSETVAL': 'nav', 'Net Asset Value': 'nav'}, start_date_str=mf_release_date.strftime('%Y-%m-%d'))
                                        
                                        load_master_instruments.clear()
                                        load_all_data.clear()
                                        
                                        if success: st.success(f"✅ {mf_name} ditambahkan!")
                                        else: st.warning(f"⚠️ {mf_name} tersimpan, historis kosong.")
                                        time.sleep(1)
                                        st.rerun()
                                        break
                                    else:
                                        if attempt < max_retries - 1:
                                            st.warning(f"⚠️ Percobaan {attempt + 1} gagal. Mengulang...")
                                            time.sleep(5)
                                        else: st.error("❌ Ticker tidak ditemukan!")
                                else:
                                    if attempt < max_retries - 1: time.sleep(5)
                                    else: st.error("❌ Gagal terhubung ke API Refinitiv.")
                            except Exception as e:
                                if attempt < max_retries - 1: time.sleep(5)
                                else: st.error(f"❌ Gagal total: {e}")
                else: st.warning("Isi Ticker dan Nama!")

    # # 2. FORM OBLIGASI NEGARA
    # with tab_bond:
    #     with st.form("form_add_bond"):
    #         bond_isin = st.text_input("ISIN Code / Ticker", placeholder="Contoh: IDFR0100=")
    #         bond_name = st.text_input("Nama Obligasi", placeholder="Contoh: FR100")
    #         bond_curr = st.selectbox("Mata Uang", ["IDR", "USD"], key="bd_c")
    #         bond_release_date = st.date_input("Tanggal Rilis Obligasi", value=dt.date(2024, 1, 1))
            
    #         if st.form_submit_button("Tambah & Tarik Data"):
    #             if bond_isin and bond_name:
    #                 with st.spinner("Menarik Historis Obligasi..."):
    #                     if not st.session_state.connected:
    #                         st.session_state.connected = init_refinitiv_session()
                            
    #                     if st.session_state.connected:
    #                         supabase.table("gov_bonds_instruments").upsert([{"isin_code": bond_isin, "name": bond_name, "currency": bond_curr}]).execute()
                            
    #                         # Inject Start Date dinamis
    #                         success = backfill_new_instrument("gov_bonds_prices_daily", "isin_code", bond_isin, ['TR.ASKPRICE.date', 'TR.ASKPRICE', 'TR.BIDYIELD'], ["ask_price", "ask_yield"], {'Instrument': 'isin_code', 'Date': 'date', 'Ask Price': 'ask_price', 'Bid Yield': 'ask_yield', 'TR.ASKPRICE.date': 'date', 'TR.ASKPRICE': 'ask_price', 'TR.BIDYIELD': 'ask_yield'}, start_date_str=bond_release_date.strftime('%Y-%m-%d'))
                            
    #                         load_master_instruments.clear()
    #                         load_all_data.clear()
                            
    #                         if success: st.success("✅ Obligasi ditambahkan!")
    #                         else: st.warning("⚠️ Obligasi ditambahkan, historis kosong.")
    #                         time.sleep(1)
    #                         st.rerun()
    #                     else: st.error("❌ Gagal terhubung ke API Refinitiv.")
    #             else: st.warning("Isi ISIN dan Nama!")

    # 3. FORM MAKRO
    with tab_macro:
        with st.form("form_add_macro"):
            mac_ticker = st.text_input("Ticker Makro", placeholder="Contoh: .JKSE")
            mac_name = st.text_input("Nama Makro", placeholder="Contoh: IHSG")
            mac_cat = st.selectbox("Kategori", ["Index", "Interest Rate", "Currency", "Commodity"])
            mac_metric = st.selectbox("Metric Type API", ["Price Close", "Close Price", "America Close Bid Price"])
            
            if st.form_submit_button("Tambah & Backfill 25 Thn"):
                if mac_ticker and mac_name:
                    with st.spinner("Menarik Historis Makro (25 Tahun)..."):
                        if not st.session_state.connected:
                            st.session_state.connected = init_refinitiv_session()
                            
                        if st.session_state.connected:
                            supabase.table("macro_instruments").upsert([
                                {"ticker": mac_ticker, "name": mac_name, "category": mac_cat, "metric_type": mac_metric}
                            ]).execute()
                            
                            field_code = "TR.PriceClose" if mac_metric == "Price Close" else ("TR.AmericaCloseBidPrice" if mac_metric == "America Close Bid Price" else "TR.ClosePrice")
                            
                            # Makro menggunakan hardcode 2000-01-01
                            success = backfill_new_instrument("macro_daily", "ticker", mac_ticker, [f"{field_code}.date", field_code], ["value"], {'Instrument': 'ticker', 'Date': 'date', f'{field_code}.date': 'date', field_code: 'value', 'Price Close': 'value', 'Close Price': 'value', 'America Close Bid Price': 'value', 'cLOSE Price': 'value'}, start_date_str="2000-01-01")
                            
                            load_master_instruments.clear()
                            load_all_data.clear()
                            
                            if success: st.success("✅ Makro ditambahkan!")
                            else: st.warning("⚠️ Makro ditambahkan, historis kosong.")
                            time.sleep(1)
                            st.rerun()
                        else: st.error("❌ Gagal terhubung ke API Refinitiv.")
                else: st.warning("Lengkapi data makro!")

    # 4. FORM HAPUS (DELETE)
    with tab_del:
        mf_master, bond_master, macro_master = load_master_instruments()
        
        # PERBAIKAN 2: Mendaftarkan target tabel harian ke dalam tuple untuk Cascade Delete
        all_del_opts = {f"MF: {x['name']}": ("mf_instruments", "ticker", x['ticker'], "mf_nav_daily") for x in mf_master}
        all_del_opts.update({f"Bond: {x['name']}": ("gov_bonds_instruments", "isin_code", x['isin_code'], "gov_bonds_prices_daily") for x in bond_master})
        all_del_opts.update({f"Macro: {x['name']}": ("macro_instruments", "ticker", x['ticker'], "macro_daily") for x in macro_master})
        
        if all_del_opts:
            sel_del = st.selectbox("Pilih Instrumen untuk Dihapus:", list(all_del_opts.keys()))
            if st.button("🗑️ Hapus Instrumen & Riwayat", type="secondary"):
                t_master, col_name, val_id, t_daily = all_del_opts[sel_del]
                
                with st.spinner(f"Menghapus {sel_del} secara permanen..."):
                    # 1. Hapus Ribuan Data Historis di Tabel Harian
                    supabase.table(t_daily).delete().eq(col_name, val_id).execute()
                    
                    # 2. Hapus Identitas di Tabel Master
                    supabase.table(t_master).delete().eq(col_name, val_id).execute()
                    
                    # 3. Bakar Cache
                    load_master_instruments.clear()
                    load_all_data.clear()
                    
                    st.success(f"Berhasil dihapus dari master data beserta seluruh riwayat hariannya.")
                    time.sleep(1.5)
                    st.rerun()
        else:
            st.info("Database instrumen kosong.")
    
    
    # ==========================================
    # KONTROL ANALISIS (TAMPIL JIKA DATA LOADED)
    # ==========================================
    if st.session_state.data_loaded:
        fetched_start = pd.to_datetime(st.session_state.start_date).date()
        fetched_end = pd.to_datetime(st.session_state.end_date).date()
        
        st.subheader("✂️ Cut-off Data Analisis")
        st.caption("Batasi rentang historis untuk komputasi metrik.")
        
        # Tambahkan batas eksplisit agar bisa memilih tahun 2000 ke atas
        min_date_allowed = dt.date(2000, 1, 1)
        max_date_allowed = dt.datetime.today().date()
        
        default_start = max(fetched_start, fetched_end - dt.timedelta(days=365))

        col_a1, col_a2 = st.columns(2)
        with col_a1:
            raw_start_date = st.date_input("Start", value=default_start, min_value=min_date_allowed, max_value=max_date_allowed, key="ana_start")
        with col_a2: 
            raw_end_date = st.date_input("End", value=fetched_end, min_value=min_date_allowed, max_value=max_date_allowed, key="ana_end")
            
        analysis_start_date = max(raw_start_date, fetched_start)
        analysis_end_date = min(raw_end_date, fetched_end)
        if analysis_start_date > analysis_end_date:
            analysis_start_date = analysis_end_date 
            
        st.info(f"Aktif: **{analysis_start_date.strftime('%d/%m/%y')}** - **{analysis_end_date.strftime('%d/%m/%y')}**")

        st.subheader("⏱️ Parameter Kalkulasi")
        # UBAH: Tambahkan opsi 3 Tahun dan 5 Tahun
        date_option = st.selectbox("Interval Rolling:", ["1 Bulan", "3 Bulan", "6 Bulan", "1 Tahun"], index=3, key="interval_analisis")
        
        scoring_mode = st.selectbox(
            "Fokus Skoring:",
            ["Balanced (Semua Metrik)", "Fokus Return (Profit)", "Fokus Risiko & Rasio", "Fokus Konsistensi", "Fokus Momentum (Climbers)", "Fokus Valuasi (Murah/Mahal)"],
            index=0, key="scoring_mode_select"
        )
        
        # Pilihan benchmark digabung dan tidak dibatasi currency
        benchmark_options = {
            'IHSG (.JKSE)': '.JKSE', 'LQ45 (.JKLQ45)': '.JKLQ45', 'IDX30': '.JKIDX30', 
            'IDX80': '.JKIDX80', 'NASDAQ (.IXIC)': '.IXIC', 'S&P 500 (.SPX)': '.SPX', 
            'Shanghai (.SSEC)': '.SSEC', 'DXY Index': '.DXY', 'Kurs IDR': 'IDR=',
            'Crude Oil (CLc1)': 'CLc1', 'IDR 10Y Yield': 'ID10YT=RR', 'US 10Y Yield': 'US10YT=RR'
        }
            
        selected_bench_label = st.selectbox("Benchmark Alpha & Beta", list(benchmark_options.keys()), key="benchmark_select")
        
        st.session_state.selected_benchmark_ticker = benchmark_options[selected_bench_label]
    else:
        # Fallback jika data belum diload
        st.session_state.selected_benchmark_ticker = ".JKSE"
        st.session_state.selected_benchmark_label = "IHSG (.JKSE)"

    st.sidebar.divider()
    st.sidebar.caption("© 2026 Investment Dashboard")

# ==================== HALAMAN UTAMA ====================
st.title("📊 Investment Dashboard - Reksa Dana Indonesia")
if st.session_state.start_date and st.session_state.end_date:
    st.markdown(f"Periode Data: {st.session_state.start_date.strftime('%d %b %Y')} s/d {st.session_state.end_date.strftime('%d %b %Y')}")
    st.markdown(f"Mata Uang Reksa Dana: **{st.session_state.fund_currency}**")
    
# ==================== AMBIL DATA DARI CACHE ====================
all_data, _, _ = load_all_data(
    st.session_state.start_date, 
    st.session_state.end_date,
    currency=st.session_state.fund_currency
)

df_equity_full = all_data['equity']
df_bond_full = all_data['bond']
df_index_full = all_data['index']
df_komoditas_full = all_data['komoditas']
df_mata_uang_full = all_data['mata_uang']
df_suku_bunga_full = all_data['suku_bunga']

# --- TAMBAHAN: INFORMASI UPDATED AT ---
# Mencari tanggal ketersediaan data paling akhir dari seluruh tabel
list_of_dfs = [df_equity_full, df_bond_full, df_index_full, df_komoditas_full, df_mata_uang_full, df_suku_bunga_full]
latest_dates = [df.index.max() for df in list_of_dfs if not df.empty and df.index.max() is not pd.NaT]

if latest_dates:
    latest_update = max(latest_dates)
    st.caption(f"🔄 **Data Last Updated At:** {latest_update.strftime('%d %b %Y')}")

# --- DETEKSI REKSA DANA MUDA (< 1 TAHUN / < 252 Hari Bursa) ---
# --- DETEKSI REKSA DANA MUDA (< 1 TAHUN KALENDER DARI DATABASE) ---
# --- DETEKSI REKSA DANA MUDA (< 1 TAHUN KALENDER DARI DATABASE) ---
def get_young_funds(df):
    young = []
    today = pd.Timestamp(dt.datetime.today().date())
    
    for col in df.columns:
        # 1. Hapus nilai 0.0 (API sering mengisi masa lalu dengan 0)
        s = df[col].replace(0.0, np.nan).dropna()
        if s.empty: continue
        
        # 2. Deteksi 'Padding' Harga (Harga flat masa lalu dari API)
        # Cari kapan harga mulai aktif bergerak / berfluktuasi
        pct = s.pct_change()
        active_moves = pct[pct != 0.0].dropna()
        
        if not active_moves.empty:
            # Rilis asli adalah 1 hari sebelum harga pertama kali bergerak
            first_move_date = active_moves.index[0]
            first_move_idx = s.index.get_loc(first_move_date)
            
            if first_move_idx > 0:
                real_inception_date = s.index[first_move_idx - 1]
            else:
                real_inception_date = s.index[0]
        else:
            # Jika produk tidak pernah bergerak sama sekali
            real_inception_date = s.index[0]
            
        # 3. Hitung umur kalender murni
        if (today - real_inception_date).days <= 365:
            young.append(col)
             
    return young

young_equities = get_young_funds(df_equity_full)
young_bonds = get_young_funds(df_bond_full)
young_all = tuple(young_equities + young_bonds)

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

# --- TARIK VARIABEL DARI SESSION STATE ---
selected_benchmark_ticker = st.session_state.selected_benchmark_ticker
selected_benchmark_label = st.session_state.selected_benchmark_label

benchmark_series_full = get_benchmark_series(selected_benchmark_ticker, full_dfs_dict)

if benchmark_series_full.empty:
    st.warning(f"Data benchmark {selected_benchmark_label} tidak tersedia. Kalkulasi Beta & Alpha disetel ke 0.")
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

# Ekstrak df_gov_bonds dari all_data
df_gov_bonds_price_full = all_data.get('gov_bonds_price', pd.DataFrame())
df_gov_bonds_yield_full = all_data.get('gov_bonds_yield', pd.DataFrame())

df_gov_bonds_price = safe_slice(df_gov_bonds_price_full, ana_start_dt, ana_end_dt)
df_gov_bonds_yield = safe_slice(df_gov_bonds_yield_full, ana_start_dt, ana_end_dt)

# ==================== RISK FREE RATE DINAMIS ====================
rf_ticker = 'US10YT=RR' if st.session_state.fund_currency == 'USD' else 'ID10YT=RR'
if rf_ticker in df_suku_bunga.columns and not df_suku_bunga.empty:
    # Ambil yield di hari terakhir pada interval analisis yang dipilih pengguna
    dynamic_rf_rate = float(df_suku_bunga[rf_ticker].iloc[-1]) / 100
else:
    dynamic_rf_rate = 0.065 # Fallback

# Timpa variabel global risk_free_rate untuk dipakai oleh fungsi calculate_metrics dkk
risk_free_rate = dynamic_rf_rate
st.sidebar.caption(f"Risk Free Rate Aktual ({ana_end_dt.strftime('%d/%m/%y')}): **{risk_free_rate*100:.2f}%**")
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

# MENGIRIM VARIABEL young_all ke fungsi kalkulasi
metrics_all = calculate_metrics(df_all_instruments, benchmark_series_sliced, risk_free_rate, eval_window=cutoff_days, young_funds_list=young_all, bench_ticker=selected_benchmark_ticker)

if metrics_all is None or metrics_all.empty:
    st.error(f"Gagal menghitung metrik untuk periode {date_option}. Data mungkin tidak mencukupi.")
    st.stop()

metrics_equity = None
if not df_equity.empty:
    metrics_equity = calculate_metrics(df_equity, benchmark_series_sliced, risk_free_rate, eval_window=cutoff_days, young_funds_list=young_all, bench_ticker=selected_benchmark_ticker)

metrics_bond = None
if not df_bond.empty:
    metrics_bond = calculate_metrics(df_bond, benchmark_series_sliced, risk_free_rate, eval_window=cutoff_days, young_funds_list=young_all, bench_ticker=selected_benchmark_ticker)

# --- LOGIKA FILTER PEMBOBOTAN DINAMIS ---
weights_dict = None  # Default (Balanced = dibagi rata 1/25)

if scoring_mode == "Fokus Return (Profit)":
    w_ret = 1.0 / 3.0
    weights_dict = { 'Return_1W': w_ret, 'Return_1M': w_ret, 'Return_3M': w_ret }
elif scoring_mode == "Fokus Risiko & Rasio":
    weights_dict = { 'Sharpe_Ratio': 0.25, 'Alpha': 0.25, 'Beta': 0.25, 'Volatility': -0.25 }
elif scoring_mode == "Fokus Konsistensi":
    consist_keys = [f"Consist_{d}_Top{n}" for d in ['1d','7d','14d','21d'] for n in [5,10,20]]
    weights_dict = {k: 1.0/12.0 for k in consist_keys}
elif scoring_mode == "Fokus Momentum (Climbers)":
    climb_keys = ['Climb_1d', 'Climb_7d', 'Climb_14d', 'climb_21d']
    weights_dict = {k: 0.25 for k in climb_keys}
elif scoring_mode == "Fokus Valuasi (Murah/Mahal)":
    weights_dict = { 'Z_Score': -1.0 }

# Hitung skor dengan menerapkan filter bobot
ranked_products_all = calculate_ranking_scores(metrics_all, weights=weights_dict) if metrics_all is not None else pd.DataFrame()
ranked_products_equity = calculate_ranking_scores(metrics_equity, weights=weights_dict) if metrics_equity is not None else pd.DataFrame()
ranked_products_bond = calculate_ranking_scores(metrics_bond, weights=weights_dict) if metrics_bond is not None else pd.DataFrame()

leaderboard_daily = calculate_daily_leaderboard(df_all_instruments, days=7)

# ==================== TABS ====================
tab_overview, tab_leaderboard_split,  tab_performance, tab_correlation, tab_compare  = st.tabs([
    "📋 Ringkasan", 
    "🏆 Leaderboard", 
    "📊 Performa & Ranking", 
    "📈 Korelasi",  
    "📉 Perbandingan Historis",
    # tab_rekomendasi refinitinv
    #"🏛️ Obligasi Negara" tab_gov_bonds
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

    st.subheader("Top 10 Produk (Skor Tertinggi & Riwayat Peringkat Skor 7 Hari)")
    
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
            history_ranks = get_7d_ranking_history(df_to_show, benchmark_series_sliced, risk_free_rate, eval_window=cutoff_days, custom_weights=weights_dict, young_funds_list=young_all, bench_ticker=selected_benchmark_ticker)
            
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

    # --- TAMBAHAN: TABEL KHUSUS REKSA DANA MUDA ---
    young_list_to_show = young_equities if top10_category == "Equity" else young_bonds
    metrics_to_show = metrics_equity if top10_category == "Equity" else metrics_bond

    if young_list_to_show and metrics_to_show is not None:
        st.divider()
        st.warning(f"⚠️ Terdapat **{len(young_list_to_show)} {top10_category}** yang baru diluncurkan (Umur < 1 Tahun).")
        st.caption(f"Instrumen ini dianulir dari evaluasi Skor Komposit Risiko, namun diperingkat secara independen di bawah ini murni berdasarkan kinerja profit absolut pada interval analisis **{date_option}**.")
        
        # Ekstrak metrik khusus untuk produk muda
        df_young = metrics_to_show.loc[metrics_to_show.index.isin(young_list_to_show)].copy()
        
        if not df_young.empty:
            # Ranking performa (Interval Return) khusus di dalam kelompok produk muda
            df_young['Peringkat_Performa'] = df_young['Interval_Return'].rank(ascending=False, method='min')
            df_young = df_young.sort_values('Peringkat_Performa').reset_index()
            df_young = df_young.rename(columns={'index': 'Nama Instrumen'})
            
            # Format teks ke persentase
            df_young[f'Return ({date_option})'] = (df_young['Interval_Return'] * 100).round(2).astype(str) + '%'
            df_young['Return Sejak Rilis (Inception)'] = (df_young['Inception_Return'] * 100).round(2).astype(str) + '%'
            
            # Buang desimal pada kolom peringkat
            df_young['Peringkat_Performa'] = df_young['Peringkat_Performa'].fillna(0).astype(int)
            
            st.dataframe(
                df_young[['Peringkat_Performa', 'Nama Instrumen', f'Return ({date_option})', 'Return Sejak Rilis (Inception)']], 
                hide_index=True, 
                use_container_width=True
            )
            st.divider()
    st.subheader(f"📈 Tren Pasar: {selected_benchmark_label}")
    if not benchmark_series_sliced.empty:
        # Kalkulasi persentase perubahan dari awal periode untuk keterangan tambahan
        bench_start_val = benchmark_series_sliced.iloc[0]
        bench_end_val = benchmark_series_sliced.iloc[-1]
        bench_pct_change = ((bench_end_val / bench_start_val) - 1) * 100
        
        st.caption(f"Pergerakan nilai **{selected_benchmark_label}**.")
        
        fig_bench = px.line(
            x=benchmark_series_sliced.index, 
            y=benchmark_series_sliced.values
        )
        
        # Penyesuaian layout agar grafik terlihat bersih dan area bawahnya terisi warna (area chart)
        fig_bench.update_layout(
            xaxis_title="",
            yaxis_title="Nilai / Harga",
            hovermode="x unified",
            margin=dict(l=0, r=0, t=10, b=0),
            height=300
        )
        fig_bench.update_traces(
            fill='tozeroy', 
            line_color='rgba(29, 161, 242, 0.8)', 
            fillcolor='rgba(29, 161, 242, 0.1)'
        )
        
        st.plotly_chart(fig_bench, use_container_width=True)
    else:
        st.warning(f"Data historis untuk benchmark {selected_benchmark_label} tidak tersedia pada rentang waktu ini.")
    st.divider()

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
        leaderboard = calculate_daily_leaderboard(df_equity_full if lb_type == "Equity" else df_bond_full, days=5)
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
            
            df_daily_full = df_equity_full if lb_type == "Equity" else df_bond_full
            
            if not df_daily_full.empty and len(df_daily_full) >= 2:
                df_daily_full = df_daily_full.ffill()
                
                # 1. PROTEKSI HARI LIBUR & WEEKEND
                # Cek pergerakan harga. Jika yang bergerak kurang dari 3 produk, anggap pasar libur
                pct_change_all = df_daily_full.pct_change()
                valid_trading_days = df_daily_full.loc[(pct_change_all.abs() > 0).sum(axis=1) > 2]
                
                if len(valid_trading_days) >= 2:
                    # Ambil hari bursa aktif terakhir dan sebelumnya
                    price_today_lb = valid_trading_days.iloc[-1]
                    price_yesterday_lb = valid_trading_days.iloc[-2].replace(0, np.nan)
                    
                    # Ekstrak Tanggal untuk dimunculkan di layar
                    date_today_str = valid_trading_days.index[-1].strftime('%d %b %Y')
                    date_yesterday_str = valid_trading_days.index[-2].strftime('%d %b %Y')
                    
                    st.subheader(f"⚡ Peringkat Harian (Daily % Change) - {title}")
                    st.caption(f"🗓️ Membandingkan penutupan: **{date_today_str}** vs **{date_yesterday_str}**")
                    
                    # 2. Hitung Persentase Perubahan
                    daily_pct_lb = ((price_today_lb / price_yesterday_lb) - 1) * 100
                    
                    # 3. Format DataFrame Peringkat
                    df_daily_lb = daily_pct_lb.reset_index()
                    df_daily_lb.columns = ['Instrument', 'Daily_%_Change']
                    
                    # ELIMINASI: Buang produk yang persis 0.0 (artinya MI belum rilis NAV terbaru)
                    df_daily_lb = df_daily_lb.dropna()
                    df_daily_lb = df_daily_lb[df_daily_lb['Daily_%_Change'] != 0.0]
                    
                    df_daily_lb = df_daily_lb.sort_values('Daily_%_Change', ascending=False)
                    df_daily_lb['Rank'] = range(1, len(df_daily_lb) + 1)
                    
                    # 4. Format teks angka
                    df_daily_display = df_daily_lb[['Rank', 'Instrument', 'Daily_%_Change']].copy()
                    df_daily_display['Daily_%_Change'] = df_daily_display['Daily_%_Change'].round(2).astype(str) + '%'
                    
                    col_d1, col_d2 = st.columns(2)
                    with col_d1:
                        st.markdown("**Top Performers (Gainers)**")
                        if not df_daily_display.empty:
                            st.dataframe(df_daily_display.head(10), hide_index=True, use_container_width=True)
                        else:
                            st.info("Belum ada produk yang naik.")
                    with col_d2:
                        st.markdown("**Bottom Performers (Losers)**")
                        if not df_daily_display.empty:
                            df_losers = df_daily_lb.tail(10).sort_values('Daily_%_Change', ascending=True).copy()
                            df_losers['Daily_%_Change'] = df_losers['Daily_%_Change'].round(2).astype(str) + '%'
                            st.dataframe(df_losers[['Rank', 'Instrument', 'Daily_%_Change']], hide_index=True, use_container_width=True)
                        else:
                            st.info("Belum ada produk yang turun.")
                else:
                    st.warning("Data hari bursa aktif tidak mencukupi.")
            else:
                st.warning("Data historis tidak cukup untuk menghitung perubahan harian.")
    else:
        st.warning(f"Tidak ada data {title}.")

# ==================== TAB 3: KORELASI ====================
with tab_correlation:
    st.header("Analisis Korelasi")
    st.info("ℹ️ **Metodologi:** Menggunakan **Korelasi Pearson** pada pergerakan *return* harian. Nilai 1 (Hijau) berarti pergerakan searah sempurna, -1 (Merah) berlawanan sempurna, dan 0 (Kuning/Pucat) menunjukkan tidak ada hubungan linier antar aset.")
    
    # --- 1. Ekstrak Daftar Manajer Investasi (MI) Dinamis ---
    all_fund_names = list(df_equity.columns) + list(df_bond.columns)
    mi_set = set()
    for name in all_fund_names:
        if name.startswith("BNP Paribas"): 
            mi_set.add("BNP Paribas")
        elif name.startswith("Eastspring"): 
            mi_set.add("Eastspring")
        else: 
            mi_set.add(name.split()[0]) # Ambil kata pertama (Maybank, Schroder, Batavia, dll)
            
    mi_list = ["Semua"] + sorted(list(mi_set))
    
    # --- 2. Konfigurasi Filter Sumbu X dan Y ---
    col_corr1, col_corr2 = st.columns(2)
    with col_corr1:
        grup1 = st.selectbox("Pilih Grup Aset 1 (Sumbu Y)", options=["Equity", "Fixed Income"], key="corr_grup1")
        filter_mi1 = st.selectbox(f"Filter MI {grup1} (Sumbu Y):", options=mi_list, index=0, key="mi_grup1")
        
    with col_corr2:
        grup2 = st.selectbox("Pilih Grup Aset 2 (Sumbu X)", options=["Equity", "Fixed Income", "Indeks", "Komoditas", "Mata Uang", "Suku Bunga"], key="corr_grup2")
        # Filter MI di Sumbu X hanya relevan jika yang dipilih adalah reksa dana
        if grup2 in ["Equity", "Fixed Income"]:
            filter_mi2 = st.selectbox(f"Filter MI {grup2} (Sumbu X):", options=mi_list, index=0, key="mi_grup2")
        else:
            filter_mi2 = "Semua"
            st.selectbox(f"Filter MI (Tidak berlaku untuk {grup2}):", options=["-"], disabled=True)

    # --- 3. Tarik Data Utama ---
    dict_dfs = {
        "Equity": df_equity,
        "Fixed Income": df_bond,
        "Indeks": df_index,
        "Komoditas": df_komoditas,
        "Mata Uang": df_mata_uang,
        "Suku Bunga": df_suku_bunga
    }
    
    df_grup1 = dict_dfs[grup1].copy()
    df_grup2 = dict_dfs[grup2].copy()
    
    # --- 4. Eksekusi Pemotongan Kolom Berdasarkan MI ---
    if filter_mi1 != "Semua":
        cols_to_keep = [c for c in df_grup1.columns if c.startswith(filter_mi1)]
        df_grup1 = df_grup1[cols_to_keep]
        
    if filter_mi2 != "Semua" and grup2 in ["Equity", "Fixed Income"]:
        cols_to_keep = [c for c in df_grup2.columns if c.startswith(filter_mi2)]
        df_grup2 = df_grup2[cols_to_keep]

    # --- 5. Kalkulasi Return & Matriks ---
    returns_grup1 = df_grup1.dropna(axis=1, how='all').ffill().bfill().pct_change().dropna(how='all')
    returns_grup2 = df_grup2.dropna(axis=1, how='all').ffill().bfill().pct_change().dropna(how='all')
    returns_grup1, returns_grup2 = returns_grup1.align(returns_grup2, join='inner', axis=0)
    
    if not returns_grup1.empty and not returns_grup2.empty:
        # Jika membandingkan dua dataset yang persis sama (termasuk filter MI-nya sama)
        if grup1 == grup2 and filter_mi1 == filter_mi2:
            title_suffix = filter_mi1 if filter_mi1 != "Semua" else ""
            title = f"Matriks Korelasi Internal {grup1} {title_suffix}".strip()
            
            corr_matrix = returns_grup1.corr()
            mask_plot = np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
            corr_matrix_plot = corr_matrix.mask(mask_plot)
        else:
            title_y = f"{filter_mi1} {grup1}" if filter_mi1 != "Semua" else grup1
            title_x = f"{filter_mi2} {grup2}" if filter_mi2 != "Semua" else grup2
            title = f"Matriks Korelasi: {title_y} vs {title_x}"
            
            corr_dict = {}
            for col2 in returns_grup2.columns:
                corr_dict[col2] = returns_grup1.apply(lambda x: x.corr(returns_grup2[col2]))
            corr_matrix = pd.DataFrame(corr_dict)
            corr_matrix_plot = corr_matrix.copy()
            
        fig_corr = px.imshow(
            corr_matrix_plot, text_auto='.2f', aspect="auto",
            color_continuous_scale='RdYlGn', zmin=-1, zmax=1, title=title,
            labels=dict(y=f"Sumbu Y", x=f"Sumbu X", color="Korelasi")
        )
        fig_corr.update_layout(height=800)
        st.plotly_chart(fig_corr, use_container_width=True)
        
        corr_matrix.index.name = 'Asset_1'
        corr_matrix.columns.name = 'Asset_2'
        
        if grup1 == grup2 and filter_mi1 == filter_mi2:
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
        st.warning("Data tidak cukup atau filter MI tidak menemukan instrumen yang relevan pada grup yang dipilih.")
        
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
    - **Total Skor:** Rata-rata persentil dari ke-24 metrik di atas (distribusi bobot setara 4% per komponen pada mode Balanced).""")
    
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
            st.markdown(f"**🌟 Top 5 Composite Score** (Benchmark: {selected_bench_label})")
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
        st.subheader(f"📋 Tabel Metrik & Skor Lengkap - {title} (Benchmark: {selected_bench_label})")
        st.caption(f"Fokus Evaluasi: **{scoring_mode}**. Tabel ini menampilkan nilai mentah (raw) dari masing-masing metrik untuk perbandingan langsung.")        
        
        # [Perbaikan]: Menyalin langsung dari metrik mentah lalu menyuntikkan Total Skor agar tidak ada kolom yang terhapus
        full_performance_display = metrics_perf.copy()
        if ranked_perf is not None and 'Total_Score' in ranked_perf.columns:
            full_performance_display['Total_Score'] = ranked_perf['Total_Score']
        else:
            full_performance_display['Total_Score'] = 0
            
        full_performance_display = full_performance_display.sort_values('Total_Score', ascending=False, na_position='last')
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
        climb_metrics = ['Climb_1d', 'Climb_7d', 'Climb_14d', 'climb_21d']
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
        
        # --- HIGHLIGHT KUNING UNTUK REKSA DANA MUDA DI TABEL SKOR ---
        def highlight_perf_young(row):
            # Cek apakah nama index (nama produk) masuk daftar young_all
            if row.name in young_all:
                return ['background-color: rgba(255, 215, 0, 0.2)'] * len(row)
            return [''] * len(row)

        styled_performance_table = full_performance_display.style.apply(highlight_perf_young, axis=1).format(final_format, na_rep="-")
        
        st.caption("*(Catatan: Baris yang di-highlight **Kuning** adalah instrumen berumur kurang dari 1 tahun)*")
        st.dataframe(styled_performance_table, use_container_width=True)

        st.divider()
        
        # --- 3. HEATMAP ANALISIS LANJUTAN (BAWAH) ---
        st.subheader(f"🔥 Heatmap Analisis Lanjutan - {title} (Benchmark: {selected_bench_label})")
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
                    custom_weights=weights_dict, trading_days_interval=trading_interval,
                    young_funds_list=young_all, bench_ticker=selected_benchmark_ticker
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
        # Kalkulasi Return Kumulatif
        returns_cum = (df_compare.pct_change().fillna(0) + 1).cumprod() - 1
        df_returns_pct = returns_cum * 100
        
        fig_returns = px.line(df_returns_pct, x=df_returns_pct.index, y=df_returns_pct.columns, title="Return Kumulatif (%)")
        fig_returns.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        
        # --- Ekstrak warna otomatis dari Plotly Express ---
        line_colors = {}
        for trace in fig_returns.data:
            line_colors[trace.name] = trace.line.color

        # --- Tambahkan anotasi angka (persentase) berlatar warna ---
        if not df_returns_pct.empty:
            last_date = df_returns_pct.index[-1]
            for col in df_returns_pct.columns:
                last_val = df_returns_pct[col].iloc[-1]
                
                # Ambil warna garis, gunakan abu-abu sebagai cadangan jika tidak ditemukan
                bg_color = line_colors.get(col, "gray")
                
                fig_returns.add_annotation(
                    x=last_date,
                    y=last_val,
                    text=f"<b>{last_val:.2f}%</b>",
                    showarrow=False,
                    xanchor="left",
                    xshift=8, 
                    font=dict(size=11, color="white"), # Ubah font menjadi putih agar terbaca
                    bgcolor=bg_color,                  # Latar belakang mengikuti warna garis
                    borderpad=3,                       # Jarak antara teks dan tepi kotak warna
                    opacity=0.9                        # Sedikit transparansi agar elegan
                )

        fig_returns.update_layout(
            xaxis_title="Tanggal", 
            yaxis_title="Return (%)", 
            legend=legend_layout,
            margin=dict(r=70) # Margin diperlebar sedikit lagi untuk ruang kotak warna
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
        dynamic_ts = calculate_rolling_timeseries(df_selected_full, benchmark_series_full, risk_free_rate, window=dynamic_window, bench_ticker=selected_benchmark_ticker)
        sliced_ts_dict = {k: safe_slice(v, ana_start_dt, ana_end_dt) for k, v in dynamic_ts.items()}
        
        ts_data = {}
        for metric_name, ts_df in sliced_ts_dict.items():
            available_cols = [col for col in selected_instruments if col in ts_df.columns]
            if available_cols:
                ts_data[metric_name] = ts_df[available_cols]
                
        if ts_data:
            if 'Alpha' in ts_data and not ts_data['Alpha'].empty:
                fig_alpha = px.line(ts_data['Alpha'], title=f"Pergerakan Alpha dengan Benchmark {selected_bench_label} ({dynamic_window} Hari)")
                fig_alpha.update_layout(xaxis_title="Tanggal", yaxis_title="Alpha", legend=legend_layout)
                st.plotly_chart(fig_alpha, use_container_width=True)
                
            if 'Beta' in ts_data and not ts_data['Beta'].empty:
                fig_beta = px.line(ts_data['Beta'], title=f"Pergerakan Beta dengan Benchmark {selected_bench_label} ({dynamic_window} Hari)")
                fig_beta.update_layout(xaxis_title="Tanggal", yaxis_title="Beta", legend=legend_layout)
                st.plotly_chart(fig_beta, use_container_width=True)
                
            if 'Sharpe_Ratio' in ts_data and not ts_data['Sharpe_Ratio'].empty:
                fig_sharpe = px.line(ts_data['Sharpe_Ratio'], title=f"Pergerakan Sharpe Ratio dengan Benchmark {selected_bench_label} ({dynamic_window} Hari)")
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
       
# ==================== TAB 6: GRAFIK OBLIGASI NEGARA ====================
# with tab_gov_bonds:
#     st.header("🏛️ Grafik Obligasi Negara (SBN/SUN/Sukuk)")
    
#     # Gunakan data utuh (_full) agar rentang waktu bisa ditarik independen dari sidebar
#     if not df_gov_bonds_price_full.empty:
#         available_gov_bonds = df_gov_bonds_price_full.columns.tolist()
        
#         selected_gov_bonds = st.multiselect(
#             "Pilih Seri Obligasi untuk Ditampilkan:",
#             options=available_gov_bonds,
#             default=available_gov_bonds[:min(3, len(available_gov_bonds))] if available_gov_bonds else [],
#             key="gov_bonds_multiselect"
#         )
        
#         if selected_gov_bonds:
#             st.divider()
            
#             # --- PANEL KONTROL WAKTU ---
#             st.subheader("⏱️ Kontrol Rentang & Jarak Waktu")
#             col_t1, col_t2 = st.columns(2)
#             with col_t1:
#                 interval_mapping = {
#                     "Harian (1D)": "1D", "5 Hari (5D)": "5D", "10 Hari (10D)": "10D",
#                     "1 Bulan (1M)": "1M", "3 Bulan (3M)": "3M", "6 Bulan (6M)": "6M",
#                     "1 Tahun (1Y)": "1Y", "3 Tahun (3Y)": "3Y", "5 Tahun (5Y)": "5Y",
#                     "10 Tahun (10Y)": "10Y"
#                 }
#                 selected_label_gran = st.selectbox("Interval Grafik Mentah (Granularitas):", list(interval_mapping.keys()), index=0, key="bond_granularity")
#                 gran_code = interval_mapping[selected_label_gran]
                
#             with col_t2:
#                 yield_options = {"YTD": "YTD", "1 Bulan": "1M", "3 Bulan": "3M", "6 Bulan": "6M", "1 Tahun": "1Y", "2 Tahun": "2Y", "3 Tahun": "3Y", "5 Tahun": "5Y"}
#                 selected_label_rebase = st.selectbox("Rentang Waktu Grafik Persentase (Rebasing):", list(yield_options.keys()), index=4, key="bond_rebase")
#                 rebase_code = yield_options[selected_label_rebase]

#             # --- FUNGSI PEMBANTU (HELPER FUNCTIONS) ---
#             def apply_granularity(df, code):
#                 if df.empty or code == "1D": return df
#                 if code == "5D": return df.iloc[::-5][::-1]
#                 if code == "10D": return df.iloc[::-10][::-1]
                
#                 resample_new = {"1M": "ME", "3M": "3ME", "6M": "6ME", "1Y": "YE", "3Y": "3YE", "5Y": "5YE", "10Y": "10YE"}
#                 resample_old = {"1M": "M", "3M": "3M", "6M": "6M", "1Y": "Y", "3Y": "3Y", "5Y": "5Y", "10Y": "10Y"}
#                 try: return df.resample(resample_new[code]).last().dropna(how='all')
#                 except: return df.resample(resample_old[code]).last().dropna(how='all')

#             def apply_rebasing(df, y_code):
#                 if df.empty: return pd.DataFrame()
#                 latest_date = df.index.max()
#                 if y_code == "YTD": start_date = pd.Timestamp(latest_date.year, 1, 1)
#                 elif "M" in y_code: start_date = latest_date - pd.DateOffset(months=int(y_code.replace("M", "")))
#                 elif "Y" in y_code: start_date = latest_date - pd.DateOffset(years=int(y_code.replace("Y", "")))
#                 else: start_date = df.index.min()

#                 df_sliced = df.loc[start_date:latest_date].copy()
#                 if not df_sliced.empty and len(df_sliced) > 1:
#                     df_rebased = (df_sliced / df_sliced.iloc[0]) - 1
#                     return df_rebased * 100
#                 return pd.DataFrame()

#             def add_end_annotations(fig, df_plot):
#                 if df_plot.empty: return fig
#                 line_colors = {trace.name: trace.line.color for trace in fig.data}
#                 last_date = df_plot.index[-1]
#                 for col in df_plot.columns:
#                     last_val = df_plot[col].dropna().iloc[-1] if not df_plot[col].dropna().empty else None
#                     if last_val is not None:
#                         bg_color = line_colors.get(col, "gray")
#                         fig.add_annotation(
#                             x=last_date, y=last_val, text=f"<b>{last_val:.2f}%</b>",
#                             showarrow=False, xanchor="left", xshift=8, 
#                             font=dict(size=11, color="white"), bgcolor=bg_color, borderpad=3, opacity=0.9
#                         )
#                 fig.update_layout(margin=dict(r=70))
#                 return fig

#             legend_layout_gov = dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5, title=None)

#             # Tarik data utuh untuk instrumen terpilih dan tambal data bolong
#             df_price_raw = df_gov_bonds_price_full[selected_gov_bonds].ffill().bfill()
#             df_yield_raw = df_gov_bonds_yield_full[selected_gov_bonds].ffill().bfill() if not df_gov_bonds_yield_full.empty else pd.DataFrame()

#             # ==========================================
#             # SEGMEN 1: ASK PRICE (HARGA PENAWARAN)
#             # ==========================================
#             st.divider()
#             st.subheader("📊 Analisis Harga Obligasi (Ask Price)")
            
#             tab_p1, tab_p2 = st.tabs(["1. Harga Mentah (Granularitas)", "2. Persentase Kenaikan (Rebasing)"])
            
#             with tab_p1:
#                 df_p_gran = apply_granularity(df_price_raw, gran_code)
#                 fig_p1 = px.line(df_p_gran, x=df_p_gran.index, y=df_p_gran.columns, title=f"Harga Aktual - Interval {selected_label_gran}")
#                 if gran_code != "1D": fig_p1.update_traces(mode='lines+markers')
#                 fig_p1.update_layout(xaxis_title="Tanggal", yaxis_title="Ask Price", legend=legend_layout_gov, hovermode="x unified")
#                 st.plotly_chart(fig_p1, use_container_width=True)

#             with tab_p2:
#                 df_p_rebase = apply_rebasing(df_price_raw, rebase_code)
#                 if not df_p_rebase.empty:
#                     fig_p2 = px.line(df_p_rebase, x=df_p_rebase.index, y=df_p_rebase.columns, title=f"Persentase Kenaikan Harga ({selected_label_rebase})")
#                     fig_p2.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
#                     fig_p2.update_yaxes(ticksuffix="%")
#                     fig_p2 = add_end_annotations(fig_p2, df_p_rebase)
#                     fig_p2.update_layout(xaxis_title="Tanggal", yaxis_title="Perubahan Harga (%)", legend=legend_layout_gov, hovermode="x unified")
#                     st.plotly_chart(fig_p2, use_container_width=True)
#                 else:
#                     st.warning("Data tidak mencukupi untuk Rebasing.")

#             # ==========================================
#             # SEGMEN 2: ASK YIELD (IMBAL HASIL)
#             # ==========================================
#             st.divider()
#             st.subheader("📉 Analisis Imbal Hasil (Ask Yield)")
            
#             if not df_yield_raw.empty:
#                 tab_y1, tab_y2 = st.tabs(["1. Yield Mentah (Granularitas)", "2. Persentase Perubahan (Rebasing)"])
                
#                 with tab_y1:
#                     df_y_gran = apply_granularity(df_yield_raw, gran_code)
#                     fig_y1 = px.line(df_y_gran, x=df_y_gran.index, y=df_y_gran.columns, title=f"Yield Aktual (%) - Interval {selected_label_gran}")
#                     if gran_code != "1D": fig_y1.update_traces(mode='lines+markers')
#                     fig_y1.update_layout(xaxis_title="Tanggal", yaxis_title="Ask Yield (%)", legend=legend_layout_gov, hovermode="x unified")
#                     st.plotly_chart(fig_y1, use_container_width=True)

#                 with tab_y2:
#                     df_y_rebase = apply_rebasing(df_yield_raw, rebase_code)
#                     if not df_y_rebase.empty:
#                         fig_y2 = px.line(df_y_rebase, x=df_y_rebase.index, y=df_y_rebase.columns, title=f"Persentase Perubahan Yield ({selected_label_rebase})")
#                         fig_y2.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
#                         fig_y2.update_yaxes(ticksuffix="%")
#                         fig_y2 = add_end_annotations(fig_y2, df_y_rebase)
#                         fig_y2.update_layout(xaxis_title="Tanggal", yaxis_title="Perubahan Yield (%)", legend=legend_layout_gov, hovermode="x unified")
#                         st.plotly_chart(fig_y2, use_container_width=True)
#                     else:
#                         st.warning("Data Yield tidak mencukupi untuk Rebasing.")
#             else:
#                 st.warning("Data Yield tidak tersedia sama sekali di database.")
#         else:
#             st.info("Pilih minimal 1 seri obligasi.")
#     else:
#         st.warning("Data Obligasi Negara tidak tersedia di database.")

#     st.divider()
# # ==================== TAB 7: REKOMENDASI FUNDAMENTAL (MANUAL UPLOAD) ====================
# with tab_recommendation:
#     st.header("🎯 Peringkat Fundamental (Data Manual)")
#     st.info("Unggah file Excel atau CSV berisi metrik fundamental. Sistem memprioritaskan skor tinggi untuk Alpha/Sharpe/Treynor dan skor rendah untuk StdDev.")

#     # 1. Definisikan Template Contoh Data
#     template_data = pd.DataFrame({
#         'Nama Produk': ['Reksa Dana A', 'Reksa Dana B', 'Reksa Dana C'],
#         'Alpha': [0.0521, 0.0310, 0.0415],
#         'Beta': [1.05, 0.95, 1.10],
#         'Sharpe': [0.12, 0.09, 0.15],
#         'Treynor': [0.08, 0.05, 0.10],
#         'StdDev': [0.15, 0.11, 0.18]
#     })

#     # 2. Komponen Upload File
#     uploaded_file = st.file_uploader("Unggah Dokumen Metrik (Excel / CSV)", type=["xlsx", "csv"], key="fund_uploader")
    
#     # Flag status validasi
#     data_valid = False
#     df_fund = pd.DataFrame()

#     # 3. Proses Validasi File
#     if uploaded_file is not None:
#         try:
#             if uploaded_file.name.endswith('.csv'):
#                 df_fund = pd.read_csv(uploaded_file)
#             else:
#                 df_fund = pd.read_excel(uploaded_file)
            
#             # Standarisasi kolom identitas
#             if 'Nama Produk' not in df_fund.columns and 'Instrument' in df_fund.columns:
#                 df_fund = df_fund.rename(columns={'Instrument': 'Nama Produk'})
                
#             # Cek ketersediaan kolom wajib
#             required_metrics = ['Alpha', 'Beta', 'Sharpe', 'Treynor', 'StdDev']
#             available_metrics = [c for c in required_metrics if c in df_fund.columns]
            
#             if 'Nama Produk' in df_fund.columns and len(available_metrics) > 0:
#                 data_valid = True
#             else:
#                 st.error("❌ Format file tidak sesuai! Pastikan terdapat kolom 'Nama Produk' dan minimal satu kolom metrik yang penulisannya persis seperti contoh.")
#         except Exception as e:
#             st.error(f"❌ Gagal membaca dokumen: {e}")

#     # 4. Logika Tampilan (Render UI)
#     if not data_valid:
#         # Jika belum ada file atau file salah, tampilkan panduan & template
#         st.write("### 💡 Contoh Format Tabel yang Diterima")
#         st.caption("Pastikan nama kolom di baris pertama persis seperti contoh di bawah ini. Kolom yang tidak tersedia/kosong akan diabaikan secara otomatis.")
#         st.dataframe(template_data, hide_index=True, use_container_width=True)
        
#     else:
#         # Jika file valid, sembunyikan template dan jalankan kalkulasi
#         df_fund = df_fund.set_index('Nama Produk')
        
#         # Konversi tipe data ke numerik
#         for col in available_metrics:
#             df_fund[col] = pd.to_numeric(df_fund[col], errors='coerce')
            
#         # Hapus baris yang tidak memiliki angka sama sekali di bagian metrik
#         df_fund = df_fund.dropna(subset=available_metrics, how='all')
        
#         if not df_fund.empty:
#             with st.spinner("Mengkalkulasi peringkat komposit..."):
#                 # Ranking: Skor 1 untuk metrik tertinggi
#                 if 'Alpha' in df_fund.columns: df_fund['Rank_Alpha'] = df_fund['Alpha'].rank(ascending=False, method='min')
#                 if 'Sharpe' in df_fund.columns: df_fund['Rank_Sharpe'] = df_fund['Sharpe'].rank(ascending=False, method='min')
#                 if 'Treynor' in df_fund.columns: df_fund['Rank_Treynor'] = df_fund['Treynor'].rank(ascending=False, method='min')
                
#                 # Beta dikalkulasi sesuai kodemu (skor tinggi = nilai besar)
#                 if 'Beta' in df_fund.columns: df_fund['Rank_Beta'] = df_fund['Beta'].rank(ascending=False, method='max')  
                
#                 # Ranking: Skor 1 untuk metrik terendah (Risiko)
#                 if 'StdDev' in df_fund.columns: df_fund['Rank_StdDev'] = df_fund['StdDev'].rank(ascending=True, method='min')
                
#                 rank_cols = [c for c in df_fund.columns if c.startswith('Rank_')]
 
#                 if rank_cols:
#                     # Hitung Skor Akhir (Rata-rata peringkat)
#                     df_fund['Total_Score'] = df_fund[rank_cols].mean(axis=1)
#                     df_fund['Final_Rank'] = df_fund['Total_Score'].rank(ascending=True, method='min')
                    
#                     # Urutkan dari Peringkat 1
#                     df_fund = df_fund.sort_values('Final_Rank')
                    
#                     # Susun kolom berdampingan (Nilai Mentah -> Peringkatnya)
#                     cols_order = []
#                     for col in available_metrics:
#                         cols_order.append(col)
#                         rank_col_name = f'Rank_{col}'
#                         if rank_col_name in df_fund.columns:
#                             cols_order.append(rank_col_name)
                    
#                     # Masukkan kolom tambahan di luar metrik utama jika ada
#                     extra_cols = [c for c in df_fund.columns if c not in cols_order and c not in ['Total_Score', 'Final_Rank']]
                    
#                     # Gabungkan urutan akhir
#                     final_cols = extra_cols + cols_order + ['Total_Score', 'Final_Rank']
#                     df_display = df_fund[final_cols].copy()
                    
#                     # Ubah nama kolom agar rapi saat ditampilkan
#                     rename_map = {
#                         'Total_Score': 'Total Skor',
#                         'Final_Rank': 'Peringkat Akhir'
#                     }
#                     for col in available_metrics:
#                         rename_map[f'Rank_{col}'] = f'Rank {col}'
                        
#                     df_display = df_display.rename(columns=rename_map)
                    
#                     # Atur jumlah desimal untuk semua kolom dengan nama baru
#                     format_dict = {}
#                     for col in df_display.columns:
#                         if col in available_metrics:
#                             format_dict[col] = "{:.4f}"
#                         elif col.startswith('Rank ') or col == 'Peringkat Akhir':
#                             format_dict[col] = "{:.0f}"
#                         elif col == 'Total Skor':
#                             format_dict[col] = "{:.2f}"
                    
#                     # Warnai baris pemenang
#                     styled_df = (
#                         df_display.style
#                         .background_gradient(subset=['Peringkat Akhir'], cmap='RdYlGn_r')
#                         .format(format_dict)
#                     )
                    
#                     st.success("✅ Dokumen berhasil diproses.")
#                     st.subheader("📋 Peringkat Fundamental Komposit")
#                     st.dataframe(styled_df, use_container_width=True, height=600)
#                 else:
#                     st.warning("Data tidak memiliki kolom performa/risiko yang valid untuk dihitung peringkatnya.")
#         else:
#             st.warning("Data kosong setelah dibersihkan. Pastikan sel metrik berisi angka yang valid.")
# ==================== FOOTER ====================
st.divider()
st.caption("Data disediakan oleh Refinitiv. Dashboard ini untuk tujuan informasi dan edukasi.")
