#!/usr/bin/env python3
"""Sync standalone (di luar Streamlit) — solusi cron thread yang rapuh.

Menarik delta data Reksa Dana/Obligasi/Makro + Sektor/FX dari Refinitiv ke Supabase.
Kredensial: app_key/username Refinitiv & Supabase dibaca dari .streamlit/secrets.toml
(seperti aplikasi), password Refinitiv dari env var REFINITIV_PASSWORD atau argumen.

Pemakaian:
    set REFINITIV_PASSWORD=xxx        (Windows)  |  export REFINITIV_PASSWORD=xxx (Linux)
    python scripts/run_sync.py                   # sync semua
    python scripts/run_sync.py --sector-only     # hanya sektor + FX
    python scripts/run_sync.py --mf-only         # hanya reksa dana/obligasi/makro
    python scripts/run_sync.py --backfill-sector 2015-01-01

Penjadwalan (pilih salah satu, jam 05:00 WIB):
  * Windows Task Scheduler: Action = python <repo>\\scripts\\run_sync.py, Trigger = Daily 05:00.
  * Linux cron:  0 5 * * *  cd /path/repo && REFINITIV_PASSWORD=xxx python scripts/run_sync.py
  * GitHub Actions: schedule cron '0 22 * * *' (UTC = 05:00 WIB) + secrets repo.

Keuntungan vs cron thread di gatau_ah.py: tetap jalan walau app Streamlit
sedang tidak hidup / di-suspend hosting.
"""

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)  # agar st.secrets menemukan .streamlit/secrets.toml


def main():
    ap = argparse.ArgumentParser(description="Sync Refinitiv -> Supabase (standalone)")
    ap.add_argument("--password", default=None, help="Password Refinitiv (default: env REFINITIV_PASSWORD)")
    ap.add_argument("--mf-only", action="store_true", help="Hanya reksa dana/obligasi/makro")
    ap.add_argument("--sector-only", action="store_true", help="Hanya sektor + FX")
    ap.add_argument("--backfill-sector", metavar="YYYY-MM-DD", default=None,
                    help="Backfill penuh sektor mulai tanggal ini (bukan incremental)")
    args = ap.parse_args()

    password = args.password or os.environ.get("REFINITIV_PASSWORD")
    if not password:
        print("[ERROR] Password Refinitiv tidak ada. Set env REFINITIV_PASSWORD atau --password.")
        return 2

    from dashboard_core.sync import (
        get_sync_start_dates,
        has_pending_sync,
        init_refinitiv_session,
        run_daily_sync,
    )

    if not init_refinitiv_session(silent=True, password=password):
        print("[ERROR] Gagal membuka sesi Refinitiv.")
        return 1

    end_d = dt.datetime.today().date()
    rc = 0

    # 1. Reksa dana / obligasi / makro
    if not args.sector_only:
        try:
            starts = get_sync_start_dates()
            if has_pending_sync(starts, end_d):
                res = run_daily_sync(starts, end_d)
                print(f"[OK] MF/Bond/Makro: {res['uploaded']} baris, {res['failed']} gagal.")
            else:
                print("[OK] MF/Bond/Makro sudah mutakhir.")
        except Exception as e:
            print(f"[ERROR] Sync MF/Bond/Makro gagal: {e}")
            rc = 1

    # 2. Sektor + FX
    if not args.mf_only:
        try:
            from dashboard_core.sector_sync import (
                backfill_sectors,
                get_sector_sync_start_dates,
                has_pending_sector_sync,
                run_sector_sync,
            )
            if args.backfill_sector:
                res = backfill_sectors(args.backfill_sector)
                print(f"[OK] Backfill sektor: {res['uploaded']} baris, {res['failed']} batch gagal.")
            else:
                sec_starts = get_sector_sync_start_dates()
                if has_pending_sector_sync(sec_starts, end_d):
                    res = run_sector_sync(sec_starts, end_d)
                    print(f"[OK] Sektor/FX: {res['uploaded']} baris, {res['failed']} batch gagal.")
                    if res["failed"]:
                        rc = 1
                else:
                    print("[OK] Sektor/FX sudah mutakhir.")
        except Exception as e:
            print(f"[ERROR] Sync sektor gagal: {e}")
            rc = 1

    print(f"[SELESAI] {dt.datetime.now():%Y-%m-%d %H:%M:%S} exit={rc}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
