import concurrent.futures
import datetime as dt
import time

import numpy as np
import pandas as pd
import refinitiv.data as rd
import streamlit as st

from .data import load_master_instruments, supabase


SYNC_TABLES = ["mf_nav_daily", "gov_bonds_prices_daily", "macro_daily"]

def get_sync_start_dates():
    """Membaca tanggal update terakhir per tabel agar sync tidak ikut tertahan tabel lain."""
    fallback_date = dt.datetime.today().date() - dt.timedelta(days=30)
    start_dates = {}

    for table in SYNC_TABLES:
        try:
            response = supabase.table(table).select("date").order("date", desc=True).limit(1).execute()
            if response.data:
                start_dates[table] = pd.to_datetime(response.data[0]['date']).date()
            else:
                start_dates[table] = fallback_date
        except Exception as e:
            print(f"Gagal membaca tanggal sync {table}: {e}")
            start_dates[table] = fallback_date

    return start_dates


def get_sync_start_date():
    """Kompatibilitas lama: ambil tanggal paling tertinggal dari semua tabel."""
    return min(get_sync_start_dates().values())


def has_pending_sync(start_dates, end_date):
    return any(start_date < end_date for start_date in start_dates.values())


_global_refinitiv_password = None

def init_refinitiv_session(silent=False, password=None):
    """Membuka sesi Refinitiv dengan proteksi deteksi password salah."""
    global _global_refinitiv_password
    if password:
        _global_refinitiv_password = password

    use_password = password or _global_refinitiv_password

    if not use_password:
        if not silent:
            st.warning("⚠️ Password Refinitiv belum diinput.")
        else:
            print("Gagal membuka sesi Refinitiv (CRON): Password kosong")
        return False

    try:
        config = st.secrets["refinitiv"]
        session = rd.session.platform.Definition(
            app_key=config["app_key"],
            grant=rd.session.platform.GrantPassword(
                username=config["username"],
                password=use_password
            )
        ).get_session()

        session.open()

        # Proteksi Lapis 1: Jika status session langsung "Closed" sesaat usai dibuka
        if session.open_state.name == "Closed":
            if not silent:
                st.error("❌ Akses Ditolak: Password yang Anda masukkan salah atau kredensial kedaluwarsa.")
            # Reset cache password global jika gagal
            _global_refinitiv_password = None
            return False

        rd.session.set_default(session)
        return True

    except Exception as e:
        error_msg = str(e).lower()
        if not silent:
            # Proteksi Lapis 2: Tangkap error lemparan dari API Refinitiv / sistem HTTPX
            if "401" in error_msg or "400" in error_msg or "invalid_grant" in error_msg or "unauthorized" in error_msg or "password" in error_msg:
                st.error("❌ Autentikasi Gagal: Password Refinitiv salah.")
            else:
                st.error(f"❌ Terjadi kesalahan jaringan / API Refinitiv: {e}")
        else:
            print(f"Gagal membuka sesi Refinitiv (CRON): {e}")

        # Reset cache password global agar tidak terkunci di state memori yang salah
        _global_refinitiv_password = None
        return False


def run_daily_sync(start_date, end_date, progress_callback=None, max_workers=4):
    """Menarik delta data dari Refinitiv dan mengirimnya ke Supabase."""
    if isinstance(start_date, dict):
        start_dates = start_date
    else:
        start_dates = {table: start_date for table in SYNC_TABLES}

    def report(done, total, message):
        if progress_callback:
            progress_callback(done, total, message)
        else:
            print(message)

    def get_params(table_name):
        table_start = start_dates.get(table_name, end_date)
        if table_start >= end_date:
            return None
        return {
            'SDate': table_start.strftime('%Y-%m-%d'),
            'EDate': end_date.strftime('%Y-%m-%d'),
            'Frq': 'D'
        }

    def process_and_upload(df_raw, table_name, value_cols, rename_mapping):
        if df_raw is None or df_raw.empty: return 0
        df_raw = df_raw.loc[:, ~df_raw.columns.duplicated()]
        df_clean = df_raw.rename(columns=rename_mapping)
        if 'date' not in df_clean.columns: return 0
        existing_val_cols = [col for col in value_cols if col in df_clean.columns]
        if not existing_val_cols: return 0
        df_clean[existing_val_cols] = df_clean[existing_val_cols].replace(r'^\s*$', np.nan, regex=True)
        df_clean = df_clean.dropna(subset=['date', existing_val_cols[0]])
        if df_clean.empty: return 0
        df_clean['date'] = pd.to_datetime(df_clean['date']).dt.strftime('%Y-%m-%d')
        id_col = 'isin_code' if 'isin_code' in df_clean.columns else 'ticker'
        df_clean = df_clean.drop_duplicates(subset=[id_col, 'date'], keep='last')
        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)
        payload = df_clean.to_dict(orient='records')
        uploaded = 0
        for i in range(0, len(payload), 1000):
            try:
                supabase.table(table_name).upsert(payload[i:i+1000]).execute()
                uploaded += len(payload[i:i+1000])
            except Exception as e:
                raise RuntimeError(f"Upload {table_name} batch {i//1000 + 1} gagal: {e}")
        return uploaded

    mf_master, bond_master, macro_master = load_master_instruments()
    jobs = []

    # 1. Sync Reksa Dana
    params_mf = get_params("mf_nav_daily")
    if params_mf:
        tickers_mf = [x['ticker'] for x in mf_master]
        for i in range(0, len(tickers_mf), 15):
            jobs.append({
                "label": f"Reksa Dana {i//15 + 1}",
                "table": "mf_nav_daily",
                "tickers": tickers_mf[i:i+15],
                "fields": ['TR.NETASSETVAL.date', 'TR.NETASSETVAL'],
                "params": params_mf,
                "value_cols": ["nav"],
                "mapping": {'Instrument': 'ticker', 'Date': 'date', 'TR.NETASSETVAL.date': 'date', 'TR.NETASSETVAL': 'nav', 'Net Asset Value': 'nav'}
            })

    # 2. Sync Obligasi
    params_bonds = get_params("gov_bonds_prices_daily")
    if params_bonds:
        tickers_bonds = [x['isin_code'] for x in bond_master]
        mapping_bonds = {'Instrument': 'isin_code', 'Date': 'date', 'Ask Price': 'ask_price', 'Bid Yield': 'ask_yield', 'TR.ASKPRICE.date': 'date', 'TR.ASKPRICE': 'ask_price', 'TR.BIDYIELD': 'ask_yield'}
        for i in range(0, len(tickers_bonds), 20):
            jobs.append({
                "label": f"Obligasi {i//20 + 1}",
                "table": "gov_bonds_prices_daily",
                "tickers": tickers_bonds[i:i+20],
                "fields": ['TR.ASKPRICE.date', 'TR.ASKPRICE', 'TR.BIDYIELD'],
                "params": params_bonds,
                "value_cols": ["ask_price", "ask_yield"],
                "mapping": mapping_bonds
            })

    # 3. Sync Makro Gabungan
    params_macro = get_params("macro_daily")
    if params_macro:
        macro_configs = [
            (['.JKSE', '.JKLQ45', '.JKIDX30', '.JKIDX80', '.IXIC', '.SPX', '.DXY', '.SSEC', '.DJI'], ['TR.PriceClose.date', 'TR.PriceClose', 'TR.Volume']),
            (['ID10YT=RR', 'US10YT=RR'], ['TR.ASKYIELD.date', 'TR.ASKYIELD']),
            (['IDR='], ['TR.AmericaCloseBidPrice.date', 'TR.AmericaCloseBidPrice']),
            (['CLc1'], ['TR.cLOSEPrice.date', 'TR.ClosePrice'])
        ]
        for idx, (tickers, fields) in enumerate(macro_configs, start=1):
            mapping_mac = {
                'Instrument': 'ticker', 'Date': 'date', fields[0]: 'date', fields[1]: 'value',
                'Price Close': 'value', 'TR.PriceClose': 'value', 'Close Price': 'value',
                'TR.ClosePrice': 'value', 'America Close Bid Price': 'value',
                'America  Close Bid Price': 'value', 'cLOSE Price': 'value',
                'TR.AmericaCloseBidPrice': 'value',
                'Ask Yield': 'value', 'TR.ASKYIELD': 'value',
                'Volume': 'volume', 'TR.Volume': 'volume', 'TR.Volume.date': 'date'
            }
            jobs.append({
                "label": f"Makro {idx}",
                "table": "macro_daily",
                "tickers": tickers,
                "fields": fields,
                "params": params_macro,
                "value_cols": ["value", "volume"],
                "mapping": mapping_mac
            })

    total_jobs = len(jobs)
    if total_jobs == 0:
        report(1, 1, "Semua tabel sudah mutakhir.")
        return {"uploaded": 0, "failed": 0, "jobs": 0}

    def fetch_job(job):
        return rd.get_data(universe=job["tickers"], fields=job["fields"], parameters=job["params"])

    uploaded_total = 0
    done_jobs = 0
    retry_delay_seconds = 5
    workers = max(1, min(max_workers, total_jobs))
    report(0, total_jobs, f"Memulai {total_jobs} batch Refinitiv dengan {workers} worker...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_job = {executor.submit(fetch_job, job): job for job in jobs}
        while future_to_job:
            for future in concurrent.futures.as_completed(list(future_to_job.keys())):
                job = future_to_job.pop(future)
                try:
                    df_raw = future.result()
                    uploaded = process_and_upload(df_raw, job["table"], job["value_cols"], job["mapping"])
                    uploaded_total += uploaded
                    done_jobs += 1
                    report(done_jobs, total_jobs, f"Selesai {done_jobs}/{total_jobs}: {job['label']} ({uploaded} baris)")
                except Exception as e:
                    report(
                        done_jobs,
                        total_jobs,
                        f"Batch gagal, ulang dalam {retry_delay_seconds} detik: {job['label']} - {e}"
                    )
                    time.sleep(retry_delay_seconds)
                    future_to_job[executor.submit(fetch_job, job)] = job

    return {"uploaded": uploaded_total, "failed": 0, "jobs": total_jobs}


def backfill_new_instrument(table_dest, id_col, ticker, fields, value_cols, rename_mapping, start_date_str):
    """Fungsi mandiri untuk menarik data historis instrumen baru berdasarkan tanggal spesifik."""
    end_str = dt.datetime.today().strftime('%Y-%m-%d')
    params = {'SDate': start_date_str, 'EDate': end_str, 'Frq': 'D'}

    try:
        df_raw = rd.get_data(universe=[ticker], fields=fields, parameters=params)
    except Exception as e:
        st.error(f"Refinitiv Error: {e}")
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


def validate_ticker(ticker, product_type):
    """Validasi apakah ticker ada di Refinitiv"""
    field_by_type = {
        "MF": "TR.NETASSETVAL",
        "BOND": "TR.ASKPRICE",
        "MACRO": "TR.PriceClose",
    }
    field = field_by_type.get(str(product_type).upper(), "TR.PriceClose")
    try:
        df = rd.get_data(universe=[ticker], fields=[field])
        return not df.empty
    except Exception as e:
        print(f"[WARN] validate_ticker({ticker}, {product_type}) gagal: {e}")
        return False


def get_instrument_launch_date(ticker, field):
    """Ambil tanggal rilis/terbit dari Refinitiv secara otomatis"""
    try:
        df = rd.get_data(universe=[ticker], fields=[field])
        if not df.empty and pd.notna(df[field].iloc[0]):
            raw_val = df[field].iloc[0]
            # Convert ke string YYYY-MM-DD
            return str(pd.to_datetime(raw_val).date())
        return "2000-01-01"
    except Exception as e:
        print(f"[WARN] get_instrument_launch_date({ticker}, {field}) gagal, fallback 2000-01-01: {e}")
        return "2000-01-01"
