"""Sync job untuk data sektor + FX dari Refinitiv ke Supabase.

Mengikuti pola dashboard_core/sync.py:
- incremental: mulai dari tanggal terakhir per tabel
- batch beberapa RIC per panggilan rd.get_data (1-2 call per market per hari)
- upsert ke Supabase per 1000 baris
Dipanggil oleh cron 05:00 WIB di gatau_ah.py dan tombol admin di halaman sektor.
"""

import datetime as dt

import numpy as np
import pandas as pd
import refinitiv.data as rd

from .data import supabase
from .sector_data import FX_PAIRS, load_sector_instruments

SECTOR_SYNC_TABLES = ["sector_prices_daily", "fx_daily"]
BATCH_SIZE = 15          # RIC per panggilan Refinitiv
MAX_BATCH_RETRIES = 3    # jangan retry tanpa batas (beda dari sync.py lama)

PRICE_FIELDS = ["TR.PriceClose.date", "TR.PriceClose"]
PRICE_RENAME = {
    "Instrument": "ric",
    "Date": "date",
    "TR.PriceClose.date": "date",
    "TR.PriceClose": "close",
    "Price Close": "close",
}
FX_FIELDS = ["TR.MIDPRICE.date", "TR.MIDPRICE"]
FX_RENAME = {
    "Instrument": "pair",
    "Date": "date",
    "TR.MIDPRICE.date": "date",
    "TR.MIDPRICE": "close",
    "Mid Price": "close",
}
# Fallback field FX bila TR.MIDPRICE kosong (pola repo utk IDR=)
FX_FIELDS_ALT = ["TR.AmericaCloseBidPrice.date", "TR.AmericaCloseBidPrice"]
FX_RENAME_ALT = {
    "Instrument": "pair",
    "Date": "date",
    "TR.AmericaCloseBidPrice.date": "date",
    "TR.AmericaCloseBidPrice": "close",
    "America Close Bid Price": "close",
}


def get_sector_sync_start_dates():
    """Tanggal terakhir per tabel sektor; fallback 30 hari jika tabel kosong."""
    fallback = dt.datetime.today().date() - dt.timedelta(days=30)
    starts = {}
    for table in SECTOR_SYNC_TABLES:
        try:
            res = supabase.table(table).select("date").order("date", desc=True).limit(1).execute()
            starts[table] = pd.to_datetime(res.data[0]["date"]).date() if res.data else fallback
        except Exception as e:
            print(f"Gagal membaca tanggal sync {table}: {e}")
            starts[table] = fallback
    return starts


def has_pending_sector_sync(start_dates, end_date):
    return any(s < end_date for s in start_dates.values())


def _clean_upload(df_raw, table, id_col, rename_map):
    """Bersihkan hasil rd.get_data lalu upsert. Return jumlah baris terunggah."""
    if df_raw is None or df_raw.empty:
        return 0
    df = df_raw.loc[:, ~df_raw.columns.duplicated()]
    # Hindari kolom Date ganda saat field .date ikut terkirim
    date_candidates = [c for c in df.columns if str(c).lower().endswith(".date")]
    if date_candidates and "Date" in df.columns:
        df = df.drop(columns=["Date"])
    df = df.rename(columns=rename_map)
    if "date" not in df.columns or "close" not in df.columns:
        return 0
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.replace(r"^\s*$", np.nan, regex=True).dropna(subset=["date", "close"])
    if df.empty:
        return 0
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = df[[id_col, "date", "close"]].drop_duplicates(subset=[id_col, "date"], keep="last")
    df = df.astype(object).where(pd.notnull(df), None)
    payload = df.to_dict(orient="records")
    uploaded = 0
    for i in range(0, len(payload), 1000):
        supabase.table(table).upsert(payload[i : i + 1000]).execute()
        uploaded += len(payload[i : i + 1000])
    return uploaded


def _pull_batch(rics, fields, params):
    return rd.get_data(universe=rics, fields=fields, parameters=params)


def run_sector_sync(start_dates=None, end_date=None, progress_callback=None):
    """Tarik delta harga sektor + FX. Session Refinitiv harus sudah terbuka
    (init_refinitiv_session dari dashboard_core.sync)."""
    end_date = end_date or dt.datetime.today().date()
    start_dates = start_dates or get_sector_sync_start_dates()

    def report(done, total, msg):
        if progress_callback:
            progress_callback(done, total, msg)
        else:
            print(msg)

    master = load_sector_instruments()
    jobs = []

    # 1. Harga sektor per market
    px_start = start_dates.get("sector_prices_daily")
    if px_start and px_start < end_date and not master.empty:
        params = {"SDate": px_start.strftime("%Y-%m-%d"), "EDate": end_date.strftime("%Y-%m-%d"), "Frq": "D"}
        for market, grp in master.groupby("market"):
            rics = grp["ric"].tolist()
            for i in range(0, len(rics), BATCH_SIZE):
                jobs.append({
                    "label": f"Sektor {market} {i // BATCH_SIZE + 1}",
                    "table": "sector_prices_daily", "id_col": "ric",
                    "rics": rics[i : i + BATCH_SIZE],
                    "fields": PRICE_FIELDS, "rename": PRICE_RENAME, "params": params,
                    "alt": None,
                })

    # 2. FX
    fx_start = start_dates.get("fx_daily")
    if fx_start and fx_start < end_date:
        params_fx = {"SDate": fx_start.strftime("%Y-%m-%d"), "EDate": end_date.strftime("%Y-%m-%d"), "Frq": "D"}
        jobs.append({
            "label": "FX", "table": "fx_daily", "id_col": "pair",
            "rics": FX_PAIRS, "fields": FX_FIELDS, "rename": FX_RENAME, "params": params_fx,
            "alt": (FX_FIELDS_ALT, FX_RENAME_ALT),
        })

    total = len(jobs)
    if total == 0:
        report(1, 1, "Data sektor & FX sudah mutakhir.")
        return {"uploaded": 0, "failed": 0, "jobs": 0}

    uploaded_total, failed = 0, 0
    for n, job in enumerate(jobs, start=1):
        ok = False
        for attempt in range(MAX_BATCH_RETRIES):
            try:
                df_raw = _pull_batch(job["rics"], job["fields"], job["params"])
                up = _clean_upload(df_raw, job["table"], job["id_col"], job["rename"])
                if up == 0 and job["alt"]:
                    alt_fields, alt_rename = job["alt"]
                    df_raw = _pull_batch(job["rics"], alt_fields, job["params"])
                    up = _clean_upload(df_raw, job["table"], job["id_col"], alt_rename)
                uploaded_total += up
                ok = True
                report(n, total, f"Selesai {n}/{total}: {job['label']} ({up} baris)")
                break
            except Exception as e:
                report(n, total, f"{job['label']} percobaan {attempt + 1} gagal: {e}")
        if not ok:
            failed += 1

    return {"uploaded": uploaded_total, "failed": failed, "jobs": total}


def backfill_sectors(start_date_str="2015-01-01", progress_callback=None):
    """Backfill penuh (sekali di awal). IDX-IC otomatis hanya punya data mulai 2021
    karena Refinitiv mengembalikan data sejak inception saja."""
    start = pd.to_datetime(start_date_str).date()
    fake_starts = {"sector_prices_daily": start, "fx_daily": start}
    return run_sector_sync(fake_starts, dt.datetime.today().date(), progress_callback)


def verify_sector_rics(progress_callback=None):
    """Cek RIC mana yang mengembalikan data (tes entitlement Fase 0).

    Returns DataFrame: ric, name, market, status ('OK'/'KOSONG'/'ERROR').
    """
    master = load_sector_instruments()
    rows = []
    for i, r in master.iterrows():
        status = "ERROR"
        try:
            df = rd.get_data(universe=[r["ric"]], fields=["TR.PriceClose"])
            val = df.iloc[0, -1] if df is not None and not df.empty else None
            status = "OK" if pd.notna(val) else "KOSONG"
        except Exception:
            status = "ERROR"
        rows.append({"ric": r["ric"], "name": r["name"], "market": r["market"], "status": status})
        if progress_callback:
            progress_callback(len(rows), len(master), f"Verifikasi {r['ric']}: {status}")
    return pd.DataFrame(rows)
