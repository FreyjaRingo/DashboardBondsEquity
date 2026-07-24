-- ============================================================
-- SKEMA DASHBOARD KEKUATAN SEKTORAL (US / ID / CN Onshore / CN Offshore-HK)
-- Jalankan di Supabase SQL Editor SATU KALI.
-- Catatan: RIC bertanda verified=false masih INDIKATIF —
-- wajib diverifikasi via rd.discovery.search() di Refinitiv Workspace (lihat SECTOR_README.md),
-- lalu UPDATE kolom ric & set verified=true.
-- ============================================================

create table if not exists sector_instruments (
    ric            text primary key,
    name           text not null,
    market         text not null,             -- 'US' | 'ID' | 'CN' | 'HK'
    sector_key     text,                      -- kunci kanonik lintas market (null untuk benchmark)
    benchmark_ric  text not null,
    currency       text not null,             -- 'USD' | 'IDR' | 'CNY' | 'HKD'
    is_benchmark   boolean not null default false,
    active         boolean not null default true,
    verified       boolean not null default false
);

create table if not exists sector_prices_daily (
    ric   text not null,
    date  date not null,
    close double precision,
    primary key (ric, date)
);
create index if not exists idx_sector_prices_date on sector_prices_daily(date);

create table if not exists fx_daily (
    pair  text not null,
    date  date not null,
    close double precision,
    primary key (pair, date)
);

-- ============================================================
-- SEED: BENCHMARK
-- ============================================================
insert into sector_instruments (ric, name, market, sector_key, benchmark_ric, currency, is_benchmark, verified) values
('.SPX',    'S&P 500',                'US', null, '.SPX',    'USD', true, true),
('.JKSE',   'IHSG (Jakarta Composite)','ID', null, '.JKSE',   'IDR', true, true),
('.CSI300', 'CSI 300',                'CN', null, '.CSI300', 'CNY', true, false),
('.HSCI',   'Hang Seng Composite',    'HK', null, '.HSCI',   'HKD', true, false)
on conflict (ric) do nothing;

-- ============================================================
-- SEED: US — S&P 500 GICS Level-1 (11 sektor)
-- ============================================================
insert into sector_instruments (ric, name, market, sector_key, benchmark_ric, currency, verified) values
('.SPNY',   'S&P 500 Energy',                 'US', 'energy',        '.SPX', 'USD', false),
('.SPLRCM', 'S&P 500 Materials',              'US', 'materials',     '.SPX', 'USD', false),
('.SPLRCI', 'S&P 500 Industrials',            'US', 'industrials',   '.SPX', 'USD', false),
('.SPLRCD', 'S&P 500 Consumer Discretionary', 'US', 'cons_disc',     '.SPX', 'USD', false),
('.SPLRCS', 'S&P 500 Consumer Staples',       'US', 'cons_staples',  '.SPX', 'USD', false),
('.SPXHC',  'S&P 500 Health Care',            'US', 'health_care',   '.SPX', 'USD', false),
('.SPSY',   'S&P 500 Financials',             'US', 'financials',    '.SPX', 'USD', false),
('.SPLRCT', 'S&P 500 Information Technology', 'US', 'info_tech',     '.SPX', 'USD', false),
('.SPLRCL', 'S&P 500 Communication Services', 'US', 'comm_services', '.SPX', 'USD', false),
('.SPLRCU', 'S&P 500 Utilities',              'US', 'utilities',     '.SPX', 'USD', false),
('.SPLRCR', 'S&P 500 Real Estate',            'US', 'real_estate',   '.SPX', 'USD', false)
on conflict (ric) do nothing;

-- ============================================================
-- SEED: INDONESIA — IDX-IC (11 sektor, history mulai 2021)
-- RIC INDIKATIF — verifikasi wajib (pola umum: .IDX... atau varian lain)
-- ============================================================
insert into sector_instruments (ric, name, market, sector_key, benchmark_ric, currency, verified) values
('.IDXENERGY',  'IDX Energy',                    'ID', 'energy',           '.JKSE', 'IDR', false),
('.IDXBASIC',   'IDX Basic Materials',           'ID', 'materials',        '.JKSE', 'IDR', false),
('.IDXINDUST',  'IDX Industrials',               'ID', 'industrials',      '.JKSE', 'IDR', false),
('.IDXNONCYC',  'IDX Consumer Non-Cyclicals',    'ID', 'cons_staples',     '.JKSE', 'IDR', false),
('.IDXCYCLIC',  'IDX Consumer Cyclicals',        'ID', 'cons_disc',        '.JKSE', 'IDR', false),
('.IDXHEALTH',  'IDX Healthcare',                'ID', 'health_care',      '.JKSE', 'IDR', false),
('.IDXFINANCE', 'IDX Financials',                'ID', 'financials',       '.JKSE', 'IDR', false),
('.IDXPROPERT', 'IDX Property & Real Estate',    'ID', 'real_estate',      '.JKSE', 'IDR', false),
('.IDXTECHNO',  'IDX Technology',                'ID', 'info_tech',        '.JKSE', 'IDR', false),
('.IDXINFRA',   'IDX Infrastructures',           'ID', 'infrastructure',   '.JKSE', 'IDR', false),
('.IDXTRANS',   'IDX Transportation & Logistics','ID', 'transport_logistics','.JKSE','IDR', false)
on conflict (ric) do nothing;

-- ============================================================
-- SEED: CHINA ONSHORE — CSI 300 Sector (kode 000908-000917 + Real Estate)
-- RIC INDIKATIF (pola umum: .CSIxxxxxx) — verifikasi wajib
-- ============================================================
insert into sector_instruments (ric, name, market, sector_key, benchmark_ric, currency, verified) values
('.CSI000908', 'CSI 300 Energy',                 'CN', 'energy',        '.CSI300', 'CNY', false),
('.CSI000909', 'CSI 300 Materials',              'CN', 'materials',     '.CSI300', 'CNY', false),
('.CSI000910', 'CSI 300 Industrials',            'CN', 'industrials',   '.CSI300', 'CNY', false),
('.CSI000911', 'CSI 300 Consumer Discretionary', 'CN', 'cons_disc',     '.CSI300', 'CNY', false),
('.CSI000912', 'CSI 300 Consumer Staples',       'CN', 'cons_staples',  '.CSI300', 'CNY', false),
('.CSI000913', 'CSI 300 Health Care',            'CN', 'health_care',   '.CSI300', 'CNY', false),
('.CSI000914', 'CSI 300 Financials',             'CN', 'financials',    '.CSI300', 'CNY', false),
('.CSI000915', 'CSI 300 Information Technology', 'CN', 'info_tech',     '.CSI300', 'CNY', false),
('.CSI000916', 'CSI 300 Communication Services', 'CN', 'comm_services', '.CSI300', 'CNY', false),
('.CSI000917', 'CSI 300 Utilities',              'CN', 'utilities',     '.CSI300', 'CNY', false),
('.CSI000952', 'CSI 300 Real Estate',            'CN', 'real_estate',   '.CSI300', 'CNY', false)
on conflict (ric) do nothing;

-- ============================================================
-- SEED: CHINA OFFSHORE / HK — Hang Seng Composite Industry Indices
-- RIC INDIKATIF — verifikasi wajib
-- ============================================================
insert into sector_instruments (ric, name, market, sector_key, benchmark_ric, currency, verified) values
('.HSCIE',  'HSCI Energy',                    'HK', 'energy',        '.HSCI', 'HKD', false),
('.HSCIM',  'HSCI Materials',                 'HK', 'materials',     '.HSCI', 'HKD', false),
('.HSCIID', 'HSCI Industrials',               'HK', 'industrials',   '.HSCI', 'HKD', false),
('.HSCICD', 'HSCI Consumer Discretionary',    'HK', 'cons_disc',     '.HSCI', 'HKD', false),
('.HSCICS', 'HSCI Consumer Staples',          'HK', 'cons_staples',  '.HSCI', 'HKD', false),
('.HSCIH',  'HSCI Healthcare',                'HK', 'health_care',   '.HSCI', 'HKD', false),
('.HSCIF',  'HSCI Financials',                'HK', 'financials',    '.HSCI', 'HKD', false),
('.HSCIIT', 'HSCI Information Technology',    'HK', 'info_tech',     '.HSCI', 'HKD', false),
('.HSCITL', 'HSCI Telecommunications',        'HK', 'comm_services', '.HSCI', 'HKD', false),
('.HSCIU',  'HSCI Utilities',                 'HK', 'utilities',     '.HSCI', 'HKD', false),
('.HSCIPC', 'HSCI Properties & Construction', 'HK', 'real_estate',   '.HSCI', 'HKD', false)
on conflict (ric) do nothing;
