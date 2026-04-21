import refinitiv.data as rd
from supabase import create_client
import pandas as pd
import datetime as dt
import time

# Konfigurasi Supabase
SUPABASE_URL = "https://bjcidijclqdahimqyusc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJqY2lkaWpjbHFkYWhpbXF5dXNjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTA5NTc3NCwiZXhwIjoyMDkwNjcxNzc0fQ.V3_ipd7uTc_iA5UAAR12zucQdUXyr-7X4nLGQ5zOsrg" # WAJIB SERVICE ROLE KEY

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Daftar Indeks Makro
MACRO_INDICES = [
    { 'ticker': '.JKSE', 'name': 'IHSG', 'category': 'Index', 'metric_type': 'Price Close' },
    { 'ticker': '.JKLQ45', 'name': 'LQ45', 'category': 'Index', 'metric_type': 'Price Close' },
    { 'ticker': '.JKIDX30', 'name': 'IDX30', 'category': 'Index', 'metric_type': 'Price Close' },
    { 'ticker': '.JKIDX80', 'name': 'IDX80', 'category': 'Index', 'metric_type': 'Price Close' },
    { 'ticker': '.IXIC', 'name': 'NASDAQ', 'category': 'Index', 'metric_type': 'Price Close' },
    { 'ticker': '.SPX', 'name': 'S&P 500', 'category': 'Index', 'metric_type': 'Price Close' },
    { 'ticker': '.DXY', 'name': 'US Dollar Index', 'category': 'Index', 'metric_type': 'Price Close' },
    { 'ticker': '.SSEC', 'name': 'Shanghai Composite', 'category': 'Index', 'metric_type': 'Price Close'},
    { 'ticker': '.DJI', 'name': 'Dow Jones Index', 'category': 'Index', 'metric_type': 'Price Close'}
]

def backfill_macro_volume():
    print("Membuka sesi Refinitiv...")
    rd.open_session()
    
    start_date = "2000-01-01"
    end_date = dt.datetime.today().strftime('%Y-%m-%d')
    
    try:
        for item in MACRO_INDICES:
            ticker = item['ticker']
            name = item['name']
            
            # 1. Pastikan instrumen sudah terdaftar di master tabel untuk menghindari error Foreign Key
            print(f"Mendaftarkan {name} ({ticker}) ke master instrumen...")
            try:
                supabase.table("macro_instruments").upsert([{
                    "ticker": item['ticker'],
                    "name": item['name'],
                    "category": item['category'],
                    "metric_type": item['metric_type']
                }]).execute()
            except Exception as e:
                print(f"⚠️ Gagal mendaftarkan master instrumen untuk {ticker}: {e}")
                
            print(f"Menarik Harga dan Volume untuk {name} ({ticker}) dari {start_date} hingga {end_date}...")
            
            df_raw = rd.get_data(
                universe=[ticker],
                fields=['TR.PriceClose.date', 'TR.PriceClose', 'TR.Volume'],
                parameters={'SDate': start_date, 'EDate': end_date, 'Frq': 'D'}
            )
            
            if df_raw is not None and not df_raw.empty:
                df_raw = df_raw.loc[:, ~df_raw.columns.duplicated()]
                
                # Pemetaan Kolom
                rename_mapping = {
                    'Instrument': 'ticker',
                    'Date': 'date',
                    'TR.PriceClose.date': 'date',
                    'Price Close': 'value',
                    'TR.PriceClose': 'value',
                    'Volume': 'volume',
                    'TR.Volume.date': 'date',
                    'TR.Volume': 'volume'
                }
                df_clean = df_raw.rename(columns=rename_mapping)
                
                # Pastikan kolom volume ada
                if 'volume' not in df_clean.columns:
                    df_clean['volume'] = None
                    
                # Hapus data jika tanggal kosong atau value (harga) kosong untuk menghindari error db
                df_clean = df_clean.dropna(subset=['date', 'value'])
                if df_clean.empty:
                    print(f"⚠️ Data valid kosong untuk {ticker} setelah dibersihkan.")
                    continue
                    
                df_clean['date'] = pd.to_datetime(df_clean['date']).dt.strftime('%Y-%m-%d')
                
                # 1. ELIMINASI DUPLIKAT
                df_clean = df_clean.drop_duplicates(subset=['ticker', 'date'], keep='last')
                
                # 2. Ganti NaN dengan None agar Supabase tidak menolak payload
                df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)
                
                payload = df_clean.to_dict(orient='records')
                
                print(f"Mengunggah {len(payload)} baris data untuk {ticker} ke Supabase...")
                # Upsert batch per 1000 baris untuk menghindari timeout
                for i in range(0, len(payload), 1000):
                    supabase.table("macro_daily").upsert(payload[i:i+1000]).execute()
                    time.sleep(0.1) # Pacing
                    
                print(f"✅ Backfill Volume untuk {ticker} selesai.\n")
            else:
                print(f"⚠️ Data kosong dari Refinitiv untuk {ticker}.\n")
                
        print("🎉 Semua ticker telah selesai diproses.")
            
    except Exception as e:
        print(f"❌ Terjadi kesalahan: {e}")
    finally:
        rd.close_session()

if __name__ == "__main__":  
    backfill_macro_volume()
