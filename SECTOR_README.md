# Dashboard Kekuatan Sektoral — Panduan Setup

Implementasi dari `PLAN_Sector_Strength_Dashboard.md`. Tiga halaman baru (Sector Strength, Sector RRG, Cross-Market) diakses lewat radio "Halaman" di sidebar aplikasi utama.

## File baru

```
sql/sector_schema.sql                     → DDL + seed ±45 indeks sektor + FX
dashboard_core/sector_data.py             → loader Supabase (master, harga, FX)
dashboard_core/sector_sync.py             → sync harian, backfill, verifikasi RIC
dashboard_core/sector_metrics.py          → multi-TF returns, RS, composite score, SMA, Δrank
dashboard_core/rrg_plotly.py              → RRG interaktif (reuse kalkulasi rrg_module.py)
dashboard_core/views/sector_common.py     → helper bersama + panel Admin Sektor
dashboard_core/views/sector_strength.py   → Halaman 1
dashboard_core/views/sector_rrg.py        → Halaman 2
dashboard_core/views/cross_market.py      → Halaman 3
```

`gatau_ah.py` dimodifikasi: (1) selector halaman di sidebar, (2) cron 05:00 WIB kini ikut men-sync sektor + FX (dibungkus try/except sendiri agar tidak mengganggu sync reksa dana).

## Langkah setup (Fase 0–1)

1. **Buat tabel & seed**: jalankan `sql/sector_schema.sql` di Supabase SQL Editor.
2. **Verifikasi RIC** (WAJIB — semua RIC seed berstatus indikatif, `verified=false`):
   - Buka halaman *Sector Strength* → sidebar → **Admin Sektor** → masukkan password Refinitiv → **Connect** → **Verifikasi RIC**.
   - RIC berstatus `KOSONG`/`ERROR`: cari RIC yang benar di Workspace, misalnya:
     ```python
     import refinitiv.data as rd
     rd.discovery.search(query="IDX Sector Energy", filter="SearchAllCategoryv2 eq 'Indices'")
     ```
   - Perbaiki via SQL: `update sector_instruments set ric='<RIC benar>', verified=true where ric='<RIC lama>';`
     (karena `ric` adalah PK dan berelasi ke harga, perbaiki SEBELUM backfill).
3. **Entitlement ditolak untuk S&P/CSI?** Fallback ETF proxy — ganti baris instrumen dengan
   `XLK/XLF/XLE/...` (US) atau ETF onshore China; kolom `sector_key` & `benchmark_ric` tetap sama.
4. **Backfill**: Admin Sektor → **Backfill 10 Thn** (IDX-IC otomatis hanya sejak 2021).
5. Selanjutnya incremental otomatis via cron 05:00 WIB, atau manual lewat **Update Harian**.

## Catatan desain (sesuai plan §4)

- Hanya `close` disimpan; semua return dihitung on-the-fly di pandas.
- Return dihitung per kalender market masing-masing; lintas market memakai union tanggal + ffill maks 5 hari (`align_cross_market`).
- Mata uang default lokal; toggle USD hanya di halaman Cross-Market (butuh `fx_daily`: IDR=, CNY=, HKD=).
- Composite score: 40/30/20/10 pada RS 1M/3M/6M/12M, z-score antar sektor; opsi momentum 12-1 via toggle.
- RRG default mingguan; batch sync retry maksimal 3× (tidak infinite-loop).

## Belum dikerjakan (fase lanjutan sesuai plan)

- Drill-down Dow Jones US subsector.
- Job mingguan re-pull 30 hari terakhir untuk menangkap koreksi harga vendor (mitigasi §8) —
  sementara bisa manual: `backfill_sectors(start_date_str=<30 hari lalu>)`.
- QA angka vs Workspace/TradingView setelah backfill nyata (§7 Fase 6).
