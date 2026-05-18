import streamlit as st
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timezone, timedelta
from types import SimpleNamespace
import time
import threading

from dashboard_core.data import load_all_data, load_master_instruments, supabase
from dashboard_core.metrics import (
    calculate_metrics,
    calculate_ranking_scores,
    ensure_unique_columns,
)
from dashboard_core.sync import (
    backfill_new_instrument,
    get_instrument_launch_date,
    get_sync_start_dates,
    has_pending_sync,
    init_refinitiv_session,
    run_daily_sync,
    validate_ticker,
)
from dashboard_core.views.bonds import render_bonds
from dashboard_core.views.compare import render_compare
from dashboard_core.views.correlation import render_correlation
from dashboard_core.views.leaderboard import render_leaderboard
from dashboard_core.views.overview import render_overview
from dashboard_core.views.performance import render_performance

# ==================== LATAR BELAKANG SINKRONISASI (CRON JOB) ====================
@st.cache_resource
def start_cron_job():
    """Jalankan Auto-Pull Refinitiv setiap jam 05:00 Pagi Waktu Jakarta (WIB)."""
    def job():
        try:
            if not init_refinitiv_session(silent=True): return
            end_d = dt.datetime.today().date()
            start_dates = get_sync_start_dates()
            if has_pending_sync(start_dates, end_d):
                print("Memulai auto-sync data dari Refinitiv ke Database...")
                run_daily_sync(start_dates, end_d)
                load_all_data.clear() # Bersihkan cache dashboard agar memuat data terbaru
                print("Auto-sync selesai!")
        except Exception as e:
            print(f"Error pada CRON JOB: {e}")

    def run_scheduler():
        wib = timezone(timedelta(hours=7)) # UTC+7 Jakarta
        while True:
            now_wib = dt.datetime.now(wib)
            # Mengeksekusi penarikan jika pukul 05:00 teng
            if now_wib.hour == 5 and now_wib.minute == 0:
                job()
                time.sleep(61) # Menunggu 1 menit agar tidak looping di menit yang sama
            time.sleep(30) # Mengecek setiap 30 detik

    # Jadikan daemon thread
    thread = threading.Thread(target=run_scheduler, daemon=True)
    thread.start()
    return thread

# Memulai tracker waktu tanpa memblokir aplikasi
start_cron_job()

# ==================== KONFIGURASI HALAMAN ====================
st.set_page_config(
    page_title="Investment Dashboard - Reksa Dana Indonesia",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== ROUTING HALAMAN ====================
def set_admin_false():
    st.session_state.is_admin = False

def set_admin_true():
    st.session_state.is_admin = True

if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False

try:
    pg = st.navigation([
        st.Page(set_admin_false, title="Umum", url_path="umum"),
        st.Page(set_admin_true, title="Admin", url_path="host")
    ], position="hidden")
    pg.run()
except Exception as e:
    pass

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
    st.title("Pengaturan")
    st.sidebar.divider()
    # Pilihan mata uang reksa dana
    st.subheader("Mata Uang Reksa Dana")
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

    if st.session_state.is_admin:
        # ==========================================
        # BLOK KONEKSI & SINKRONISASI DATA
        # ==========================================
        st.subheader("Koneksi & Update Data")

        # 1. Gunakan st.empty() sebagai wadah penampung dinamis
        status_indicator = st.empty()

        if st.session_state.connected:
            status_indicator.success("Terhubung ke API Refinitiv")
        else:
            status_indicator.warning("API Refinitiv Belum Terhubung")

        ref_password_input = st.text_input("Password Refinitiv", type="password", help="Masukkan password untuk Refinitiv Workspace")

        col_btn1, col_btn2 = st.columns(2)

        with col_btn1:
            # 1. TOMBOL KONEKSI
            if st.button("Connect", use_container_width=True):
                with st.spinner("Mengevaluasi kredensial..."):
                    # 2. Tangkap hasil dari fungsi (True / False)
                    is_connected = init_refinitiv_session(password=ref_password_input)

                    # 3. Timpa memori sistem secara eksplisit
                    st.session_state.connected = is_connected

                    # 4. Ubah indikator di atas secara instan tanpa perlu memuat ulang seluruh halaman
                    if is_connected:
                        status_indicator.success("Terhubung ke API Refinitiv")
                        time.sleep(1) # Beri jeda 1 detik agar hijau sukses terlihat
                        st.rerun()
                    else:
                        status_indicator.warning("API Refinitiv Belum Terhubung")
                        # Tidak ada st.rerun() di sini agar st.error("Autentikasi Gagal") tetap tayang di layar

        with col_btn2:
            # 2. TOMBOL UPDATE DATA (Aktif hanya jika sudah connect)
            if st.button("Update", type="primary", use_container_width=True, disabled=not st.session_state.connected):
                with st.spinner("Mengecek data tertinggal..."):
                    start_dates = get_sync_start_dates()
                    end_d = dt.datetime.today().date()

                    if not has_pending_sync(start_dates, end_d):
                        st.info("Database sudah mutakhir.")
                    else:
                        pending_ranges = {
                            "Reksa Dana": start_dates["mf_nav_daily"],
                            "Obligasi": start_dates["gov_bonds_prices_daily"],
                            "Makro": start_dates["macro_daily"]
                        }
                        active_ranges = [
                            f"{name}: **{start.strftime('%d/%m/%y')} - {end_d.strftime('%d/%m/%y')}**"
                            for name, start in pending_ranges.items()
                            if start < end_d
                        ]
                        st.write("Menarik data: " + " | ".join(active_ranges))
                        progress_bar = st.progress(0)
                        sync_status = st.empty()

                        def update_sync_progress(done, total, message):
                            sync_status.write(message)
                            if total:
                                progress_bar.progress(min(1.0, done / total))

                        try:
                            sync_result = run_daily_sync(start_dates, end_d, progress_callback=update_sync_progress)
                            st.session_state.end_date = end_d

                            # --- TAMBAHKAN BARIS INI ---
                            load_all_data.clear()
                            # ---------------------------

                            if sync_result["failed"]:
                                st.warning(f"Update selesai dengan {sync_result['failed']} batch gagal. Berhasil upload {sync_result['uploaded']} baris.")
                            else:
                                st.success(f"Berhasil diupdate! {sync_result['uploaded']} baris tersinkron.")
                            time.sleep(1.5)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Update Gagal: {e}")


        # Manajemen Produk Kustom (hanya untuk IDR)
       # ==========================================
    if st.session_state.is_admin and not st.session_state.connected:
        st.warning("Silakan hubungkan API Refinitiv (masukkan password) terlebih dahulu untuk mengakses fitur Manajemen Database (CRUD).")

    if st.session_state.is_admin and st.session_state.connected:
        # ==========================================
        # MANAJEMEN INSTRUMEN (CRUD & AUTO-BACKFILL)
        # ==========================================
        st.subheader("Database Instrumen (CRUD)")

        tab_mf, tab_bond, tab_macro, tab_del = st.tabs(["Reksa Dana", "Obligasi", "Makro", "Hapus"])
        # 1. FORM REKSA DANA
        with tab_mf:
            with st.form("form_add_mf"):
                mf_ticker = st.text_input("Ticker (LP...)", placeholder="Contoh: LP68059065")
                mf_name = st.text_input("Nama Reksa Dana")
                mf_type = st.selectbox("Fund Type", ["Equity", "Fixed Income"])
                mf_curr = st.selectbox("Mata Uang", ["IDR", "USD"], key="mf_c")

                if st.form_submit_button("Tambah & Tarik Data"):
                    if mf_ticker and mf_name:
                        with st.spinner("Menarik Historis..."):
                            max_retries = 3
                            for attempt in range(max_retries):
                                try:
                                    if not st.session_state.connected:
                                        st.session_state.connected = init_refinitiv_session()

                                    if st.session_state.connected:
                                        if validate_ticker(mf_ticker, "MF"):
                                            # Ambil Launch Date
                                            launch_date_str = get_instrument_launch_date(mf_ticker, "TR.FundLaunchDate")
                                            st.info(f"Mulai menarik data dari tanggal rilis terdeteksi: {launch_date_str}")

                                            supabase.table("mf_instruments").upsert([{"ticker": mf_ticker, "name": mf_name, "fund_type": mf_type, "currency": mf_curr}]).execute()

                                            # Inject Start Date dinamis
                                            success = backfill_new_instrument("mf_nav_daily", "ticker", mf_ticker, ['TR.NETASSETVAL.date', 'TR.NETASSETVAL'], ["nav"], {'Instrument': 'ticker', 'Date': 'date', 'TR.NETASSETVAL.date': 'date', 'TR.NETASSETVAL': 'nav', 'Net Asset Value': 'nav'}, start_date_str=launch_date_str)

                                            load_master_instruments.clear()
                                            load_all_data.clear()

                                            if success: st.success(f"{mf_name} ditambahkan!")
                                            else: st.warning(f"{mf_name} tersimpan, historis kosong.")
                                            time.sleep(1)
                                            st.rerun()
                                            break
                                        else:
                                            if attempt < max_retries - 1:
                                                st.warning(f"Percobaan {attempt + 1} gagal. Mengulang...")
                                                time.sleep(5)
                                            else: st.error("Ticker tidak ditemukan!")
                                    else:
                                        if attempt < max_retries - 1: time.sleep(5)
                                        else: st.error("Gagal terhubung ke API Refinitiv.")
                                except Exception as e:
                                    if attempt < max_retries - 1: time.sleep(5)
                                    else: st.error(f"Gagal total: {e}")
                    else: st.warning("Isi Ticker dan Nama!")

        # 2. FORM OBLIGASI NEGARA
        with tab_bond:
            with st.form("form_add_bond"):
                bond_isin = st.text_input("ISIN Code / Ticker", placeholder="Contoh: IDFR0100=")
                bond_name = st.text_input("Nama Obligasi", placeholder="Contoh: FR100")
                bond_curr = st.selectbox("Mata Uang", ["IDR", "USD"], key="bd_c")

                if st.form_submit_button("Tambah & Tarik Data"):
                    if bond_isin and bond_name:
                        with st.spinner("Menarik Historis Obligasi..."):
                            if not st.session_state.connected:
                                st.session_state.connected = init_refinitiv_session()

                            if st.session_state.connected:
                                # Ambil Issue Date
                                issue_date_str = get_instrument_launch_date(bond_isin, "ISSUE_DATE")
                                st.info(f"Mulai menarik data dari tanggal terbit terdeteksi: {issue_date_str}")

                                supabase.table("gov_bonds_instruments").upsert([{"isin_code": bond_isin, "name": bond_name, "currency": bond_curr}]).execute()

                                # Inject Start Date dinamis
                                success = backfill_new_instrument("gov_bonds_prices_daily", "isin_code", bond_isin, ['TR.ASKPRICE.date', 'TR.ASKPRICE', 'TR.BIDYIELD'], ["ask_price", "ask_yield"], {'Instrument': 'isin_code', 'Date': 'date', 'Ask Price': 'ask_price', 'Bid Yield': 'ask_yield', 'TR.ASKPRICE.date': 'date', 'TR.ASKPRICE': 'ask_price', 'TR.BIDYIELD': 'ask_yield'}, start_date_str=issue_date_str)

                                load_master_instruments.clear()
                                load_all_data.clear()

                                if success: st.success("Obligasi ditambahkan!")
                                else: st.warning("Obligasi ditambahkan, historis kosong.")
                                time.sleep(1)
                                st.rerun()
                            else: st.error("Gagal terhubung ke API Refinitiv.")
                    else: st.warning("Isi ISIN dan Nama!")

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
                                if mac_cat == "Index":
                                    fields_to_pull = [f"{field_code}.date", field_code, 'TR.Volume']
                                    val_cols = ["value", "volume"]
                                    rename_map = {'Instrument': 'ticker', 'Date': 'date', f'{field_code}.date': 'date', field_code: 'value', 'Price Close': 'value', 'Close Price': 'value', 'America Close Bid Price': 'value', 'cLOSE Price': 'value', 'Volume': 'volume', 'TR.Volume': 'volume', 'TR.Volume.date': 'date'}
                                else:
                                    fields_to_pull = [f"{field_code}.date", field_code]
                                    val_cols = ["value"]
                                    rename_map = {'Instrument': 'ticker', 'Date': 'date', f'{field_code}.date': 'date', field_code: 'value', 'Price Close': 'value', 'Close Price': 'value', 'America Close Bid Price': 'value', 'cLOSE Price': 'value'}

                                success = backfill_new_instrument("macro_daily", "ticker", mac_ticker, fields_to_pull, val_cols, rename_map, start_date_str="2000-01-01")

                                load_master_instruments.clear()
                                load_all_data.clear()

                                if success: st.success("Makro ditambahkan!")
                                else: st.warning("Makro ditambahkan, historis kosong.")
                                time.sleep(1)
                                st.rerun()
                            else: st.error("Gagal terhubung ke API Refinitiv.")
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
                if st.button("Hapus Instrumen & Riwayat", type="secondary"):
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

        st.subheader("Cut-off Data Analisis")
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

        st.subheader("Parameter Kalkulasi")
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
            'Dow Jones (.DJI)': '.DJI', 'Shanghai (.SSEC)': '.SSEC', 'DXY Index': '.DXY',
            'Kurs IDR': 'IDR=', 'Crude Oil (CLc1)': 'CLc1',
            'IDR 10Y Yield': 'ID10YT=RR', 'US 10Y Yield': 'US10YT=RR'
        }

        selected_bench_label = st.selectbox("Benchmark Alpha & Beta", list(benchmark_options.keys()), key="benchmark_select")

        st.session_state.selected_benchmark_ticker = benchmark_options[selected_bench_label]
        st.session_state.selected_benchmark_label = selected_bench_label
    else:
        # Fallback jika data belum diload
        st.session_state.selected_benchmark_ticker = ".JKSE"
        st.session_state.selected_benchmark_label = "IHSG (.JKSE)"

    st.sidebar.divider()
    st.sidebar.caption("© 2026 Investment Dashboard")

# ==================== HALAMAN UTAMA ====================
@st.fragment(run_every='30m')
def render_main_dashboard():
    st.title("Investment Dashboard - Reksa Dana Indonesia")
    try:
        display_start = analysis_start_date
        display_end = analysis_end_date
    except NameError:
        display_start = st.session_state.start_date
        display_end = st.session_state.end_date
    if display_start and display_end:
        st.markdown(f"Periode Data: **{display_start.strftime('%d %b %Y')} s/d {display_end.strftime('%d %b %Y')}**")
        st.markdown(f"Mata Uang Reksa Dana: **{st.session_state.fund_currency}**")

    # ==================== AMBIL DATA DARI CACHE ====================
    load_start_date = display_start or st.session_state.start_date
    load_end_date = display_end or st.session_state.end_date
    all_data, _, _ = load_all_data(
        load_start_date,
        load_end_date,
        currency=st.session_state.fund_currency
    )

    df_equity_full = all_data['equity']
    df_bond_full = all_data['bond']
    df_index_full = all_data['index']
    df_index_vol_full = all_data.get('index_vol', pd.DataFrame())
    df_komoditas_full = all_data['komoditas']
    df_mata_uang_full = all_data['mata_uang']
    df_suku_bunga_full = all_data['suku_bunga']

    # --- TAMBAHAN: INFORMASI UPDATED AT ---
    # Mencari tanggal ketersediaan data paling akhir dari seluruh tabel
    list_of_dfs = [df_equity_full, df_bond_full, df_index_full, df_komoditas_full, df_mata_uang_full, df_suku_bunga_full]
    latest_dates = [df.index.max() for df in list_of_dfs if not df.empty and df.index.max() is not pd.NaT]

    if latest_dates:
        latest_update = max(latest_dates)

    # --- DETEKSI REKSA DANA MUDA (< 1 TAHUN / < 252 Hari Bursa) ---


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
    selected_bench_label = selected_benchmark_label

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
    df_index_vol = safe_slice(df_index_vol_full, ana_start_dt, ana_end_dt)
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

    df_suku_bunga_raw = safe_slice(all_data.get('suku_bunga_raw', pd.DataFrame()), ana_start_dt, ana_end_dt)

    if rf_ticker in df_suku_bunga_raw.columns and not df_suku_bunga_raw.empty:
        # Ambil yield di hari terakhir pada interval analisis yang dipilih pengguna
        dynamic_rf_rate = float(df_suku_bunga_raw[rf_ticker].ffill().dropna().iloc[-1]) / 100
    elif rf_ticker in df_suku_bunga.columns and not df_suku_bunga.empty:
        dynamic_rf_rate = float(df_suku_bunga[rf_ticker].iloc[-1]) / 100
    else:
        dynamic_rf_rate = 0.065 # Fallback

    # Timpa variabel global risk_free_rate untuk dipakai oleh fungsi calculate_metrics dkk
    risk_free_rate = dynamic_rf_rate

    # --- HIGHLIGHT INFO CARDS ---
    st.write("") # Spacer
    col_info1, col_info2 = st.columns(2)
    with col_info1:
        try:
            if latest_dates:
                st.info(f"📅 **Latest Updated Data:** {latest_update.strftime('%d %b %Y')}")
        except NameError:
            pass
    with col_info2:
        st.success(f"📈 **Risk Free Rate ({ana_end_dt.strftime('%d %b %Y')}):** {risk_free_rate*100:.2f}%")
    st.write("") # Spacer
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
    # elif date_option == "3 Tahun":
    #     cutoff_days = 252 * 3
    # elif date_option == "5 Tahun":
    #     cutoff_days = 252 * 5
    else:
        cutoff_days = 252

    def get_young_funds_dynamic(df):
        young = []
        # Patokan umur menggunakan total hari pada rentang analisis (cut-off)
        expected_days = len(df)

        # Pencegahan error jika rentang analisis yang dipilih terlalu pendek
        if expected_days < 10:
            return young

        for col in df.columns:
            s = df[col].replace(0.0, np.nan).dropna()
            # Jika jumlah hari produk kurang dari 90% total kalender analisis, karantina
            if len(s) < (expected_days * 0.9):
                young.append(col)
        return young

    young_equities = get_young_funds_dynamic(df_equity)
    young_bonds = get_young_funds_dynamic(df_bond)
    young_all = tuple(young_equities + young_bonds)

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
        climb_keys = ['Climb_1d', 'Climb_7d', 'Climb_14d', 'Climb_22d']
        weights_dict = {k: 0.25 for k in climb_keys}
    elif scoring_mode == "Fokus Valuasi (Murah/Mahal)":
        weights_dict = { 'Z_Score': -1.0 }

    # Hitung skor dengan menerapkan filter bobot
    # Hitung skor dengan menerapkan filter bobot
    ranked_products_all = calculate_ranking_scores(metrics_all, weights=weights_dict, young_funds_list=young_all) if metrics_all is not None else pd.DataFrame()
    ranked_products_equity = calculate_ranking_scores(metrics_equity, weights=weights_dict, young_funds_list=young_all) if metrics_equity is not None else pd.DataFrame()
    ranked_products_bond = calculate_ranking_scores(metrics_bond, weights=weights_dict, young_funds_list=young_all) if metrics_bond is not None else pd.DataFrame()
    start_date_str = ana_start_dt.strftime('%d %b %Y')
    end_date_str = ana_end_dt.strftime('%d %b %Y')

    # ==================== TABS ====================
    tab_overview, tab_leaderboard_split,  tab_performance, tab_correlation, tab_compare, tab_gov_bonds = st.tabs([
        "Ringkasan",
        "Leaderboard",
        "Performa & Ranking",
        "Korelasi",
        "Perbandingan Historis",
        "Obligasi Negara",
        #"Rekomendasi", tab_recommendation
    ])

    ctx = SimpleNamespace(**locals())
    render_overview(tab_overview, ctx)
    render_leaderboard(tab_leaderboard_split, ctx)
    render_correlation(tab_correlation, ctx)
    render_performance(tab_performance, ctx)
    render_compare(tab_compare, ctx)
    render_bonds(tab_gov_bonds, ctx)

    # ==================== TAB 7: REKOMENDASI REFINITIV ====================
    # with tab_recommendation:
    #     st.header("Rekomendasi Fundamental (Refinitiv 1 Tahun)")
    #     st.info("Data ini ditarik secara berkala dari sistem backend lokal dan disimpan di database cloud untuk menjamin kecepatan dan stabilitas.")

    #     col_info, col_btn = st.columns([3, 1])
    #     with col_btn:
    #         # Tombol ini murni untuk men-trigger penarikan ulang dari database, bukan dari API
    #         refresh_btn = st.button("Muat Ulang Database", use_container_width=True)

    #     # Menarik data dari tabel khusus metrik yang sudah diisi oleh script lokalmu
    #     with st.spinner("Mengambil metrik fundamental dari database..."):
    #         try:
    #             # Panggil Supabase
    #             response = supabase.table("mf_refinitiv_metrics").select("*").execute()

    #             if response.data:
    #                 df_metrics = pd.DataFrame(response.data)

    #                 # Rapikan kolom dan atur ticker sebagai index
    #                 df_metrics = df_metrics.set_index('ticker')

    #                 # Cek kapan terakhir kali data ini disinkronkan oleh laptopmu
    #                 if 'updated_at' in df_metrics.columns:
    #                     latest_sync = pd.to_datetime(df_metrics['updated_at']).max()
    #                     st.caption(f"**Update Terakhir:** {latest_sync.strftime('%d %b %Y, %H:%M WIB')}")
    #                     df_metrics = df_metrics.drop(columns=['updated_at'])

    #                 # Tampilkan tabel dengan rapi
    #                 st.dataframe(
    #                     df_metrics.style.format("{:.4f}", na_rep="-")
    #                               .background_gradient(cmap='RdYlGn', subset=['sharpe_1y', 'alpha_1y']),
    #                     use_container_width=True,
    #                     height=500
    #                 )
    #             else:
    #                 st.warning("Belum ada data metrik di database. Pastikan script lokal penarik Refinitiv di laptopmu sudah dijalankan.")
    #         except Exception as e:
    #             st.error(f"Gagal membaca database: {e}")
    # ==================== TAB 7: REKOMENDASI FUNDAMENTAL (MANUAL UPLOAD) ====================
    # with tab_recommendation:
    #     st.header("Peringkat Fundamental (Data Manual)")
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
    #                 st.error("Format file tidak sesuai! Pastikan terdapat kolom 'Nama Produk' dan minimal satu kolom metrik yang penulisannya persis seperti contoh.")
    #         except Exception as e:
    #             st.error(f"Gagal membaca dokumen: {e}")

    #     # 4. Logika Tampilan (Render UI)
    #     if not data_valid:
    #         # Jika belum ada file atau file salah, tampilkan panduan & template
    #         st.write("### Contoh Format Tabel yang Diterima")
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

    #                     st.success("Dokumen berhasil diproses.")
    #                     st.subheader("Peringkat Fundamental Komposit")
    #                     st.dataframe(styled_df, use_container_width=True, height=600)
    #                 else:
    #                     st.warning("Data tidak memiliki kolom performa/risiko yang valid untuk dihitung peringkatnya.")
    #         else:
    #             st.warning("Data kosong setelah dibersihkan. Pastikan sel metrik berisi angka yang valid.")
    # ==================== FOOTER ====================
    st.divider()
    st.caption("Data disediakan oleh Refinitiv. Dashboard ini untuk tujuan informasi dan edukasi.")
render_main_dashboard()
