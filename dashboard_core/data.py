import concurrent.futures
import time

import pandas as pd
import streamlit as st
from supabase import Client, create_client


@st.cache_resource
def init_supabase() -> Client:
    """Membangun koneksi ke database Supabase secara global."""
    try:
        url: str = st.secrets["supabase"]["url"]
        key: str = st.secrets["supabase"]["key"]
        return create_client(url, key)
    except KeyError as e:
        st.error(f"Kredensial Supabase tidak ditemukan di secrets.toml: missing {e}")
        st.stop()
    except Exception as e:
        st.error(f"Gagal menginisialisasi klien Supabase: {e}")
        st.stop()


# Eksekusi pembuatan klien dan simpan di memori global
supabase = init_supabase()

@st.cache_data(ttl=3600, show_spinner=False)
def load_master_instruments():
    """Mengambil Master Data murni dari Database"""
    mf_data = supabase.table("mf_instruments").select("*").execute().data
    bond_data = supabase.table("gov_bonds_instruments").select("*").execute().data
    macro_data = supabase.table("macro_instruments").select("*").execute().data
    return mf_data, bond_data, macro_data


def fetch_from_supabase(table_name, id_col, tickers, start_date, end_date, select_cols=None):
    """Menarik data masif dari Supabase dengan sistem Pagination, Fault-Tolerance, & Pacing."""
    if not tickers: return pd.DataFrame()

    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    all_data = []
    page_size = 1000
    page_pause_seconds = 0.02
    offset = 0
    select_expr = select_cols or "*"

    global supabase # Deklarasi global agar fungsi bisa me-restart koneksi jika mati

    while True:
        success = False
        data_chunk = []

        for attempt in range(3):
            try:
                res = supabase.table(table_name).select(select_expr) \
                    .in_(id_col, tickers) \
                    .gte('date', start_str) \
                    .lte('date', end_str) \
                    .order('date') \
                    .range(offset, offset + page_size - 1) \
                    .execute()

                data_chunk = res.data if res.data else []
                success = True
                break

            except Exception as e:
                if attempt < 2:
                    time.sleep(2.0) # Jeda nafas 2 detik sebelum mencoba lagi

                    # Jika pipa jaringan diputus server, paksa restart koneksi klien
                    if "Broken pipe" in str(e) or "ConnectionTerminated" in str(e):
                        init_supabase.clear()
                        supabase = init_supabase()
                else:
                    # Ubah menjadi warning agar aplikasi tidak crash dan tetap
                    # melanjutkan kalkulasi dengan sisa 58.000 data yang berhasil ditarik
                    st.warning(f"Penarikan {table_name} dihentikan pada data ke-{offset} karena pembatasan server (Rate Limit). Melanjutkan dengan data yang ada...")

        if not success:
            break

        if not data_chunk:
            break

        all_data.extend(data_chunk)

        if len(data_chunk) < page_size:
            break

        offset += page_size

        # --- PACING: BERI JEDA TIPIS SETIAP HALAMAN ---
        # Mencegah server memutus koneksi akibat request yang terlalu rapat
        time.sleep(page_pause_seconds)

    if not all_data: return pd.DataFrame()

    df = pd.DataFrame(all_data)
    df['Date'] = pd.to_datetime(df['date'])
    return df


@st.cache_data(ttl=1800, show_spinner=False) # TTL diubah menjadi 30 menit
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

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        future_mf = executor.submit(fetch_from_supabase, 'mf_nav_daily', 'ticker', tickers_equity + tickers_bond, start_date, end_date, 'date,ticker,nav')
        future_gov = executor.submit(fetch_from_supabase, 'gov_bonds_prices_daily', 'isin_code', tickers_gov_bonds, start_date, end_date, 'date,isin_code,ask_price,ask_yield')
        future_macro = executor.submit(fetch_from_supabase, 'macro_daily', 'ticker', tickers_macro, start_date, end_date, 'date,ticker,value,volume')

        df_mf_raw = future_mf.result()
        df_gov_raw = future_gov.result()
        df_macro_raw = future_macro.result()

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
    df_index_wide, df_index_vol_wide, df_suku_bunga_wide, df_mata_uang_wide, df_komoditas_wide = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    if not df_macro_raw.empty:
        df_idx_sub = df_macro_raw[df_macro_raw['ticker'].isin(tickers_index_saham)]
        df_suku_sub = df_macro_raw[df_macro_raw['ticker'].isin(tickers_suku_bunga)]
        df_uang_sub = df_macro_raw[df_macro_raw['ticker'].isin(tickers_mata_uang)]
        df_komo_sub = df_macro_raw[df_macro_raw['ticker'].isin(tickers_komoditas)]

        df_index_wide = safe_pivot(df_idx_sub, 'ticker', 'value')

        # --- EKSTRAKSI VOLUME INDEKS ---
        if 'volume' in df_idx_sub.columns:
            df_index_vol_wide = safe_pivot(df_idx_sub, 'ticker', 'volume')

        df_suku_bunga_wide = safe_pivot(df_suku_sub, 'ticker', 'value')
        df_mata_uang_wide = safe_pivot(df_uang_sub, 'ticker', 'value')
        df_komoditas_wide = safe_pivot(df_komo_sub, 'ticker', 'value')

    if df_equity_wide.empty:
        st.warning("Data Equity kosong di database untuk mata uang ini.")
        # Mengembalikan dictionary kosong dengan dataframe kosong agar tidak merusak logika selanjutnya
        return {
            'equity': df_equity_wide, 'bond': df_bond_wide,
            'index': df_index_wide, 'index_vol': df_index_vol_wide,
            'suku_bunga': df_suku_bunga_wide,
            'suku_bunga_raw': df_suku_bunga_wide,
            'mata_uang': df_mata_uang_wide, 'komoditas': df_komoditas_wide,
            'gov_bonds_price': df_gov_bonds_price, 'gov_bonds_yield': df_gov_bonds_yield
        }, start_date, end_date

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
        'index': align_wide_df(df_index_wide), 'index_vol': align_wide_df(df_index_vol_wide),
        'suku_bunga': align_wide_df(df_suku_bunga_wide),
        'suku_bunga_raw': df_suku_bunga_wide,
        'mata_uang': align_wide_df(df_mata_uang_wide), 'komoditas': align_wide_df(df_komoditas_wide),
        'gov_bonds_price': align_wide_df(df_gov_bonds_price), 'gov_bonds_yield': align_wide_df(df_gov_bonds_yield)
    }, start_date, end_date

