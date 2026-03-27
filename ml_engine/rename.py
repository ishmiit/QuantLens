import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# --- CONFIG ---
DB_CONFIG = {"dbname": "quantlens", "user": "postgres", "password": "admin123", "host": "127.0.0.1"}
INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"

def rename_isin_to_tickers():
    try:
        # 1. Download official Upstox Instrument Master
        print("📥 Downloading official instrument mapping...")
        df = pd.read_csv(INSTRUMENT_URL)
        
        # 2. Create the mapping dictionary { 'NSE_EQ|ISIN' : 'TICKER' }
        # We handle both NSE Equity and Indices
        mapping = dict(zip(df['instrument_key'], df['tradingsymbol']))
        
        # 3. Connect to Database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # 4. Get list of unique ISINs currently in your table
        cur.execute("SELECT DISTINCT symbol FROM ml_training_data_v2 WHERE symbol LIKE 'NSE_%';")
        isins_in_db = [row[0] for row in cur.fetchall()]
        
        if not isins_in_db:
            print("✅ No ISIN-style symbols found. Your table might already be clean!")
            return

        print(f"🔄 Found {len(isins_in_db)} symbols to rename. Starting migration...")

        # 5. Execute Updates
        updated_count = 0
        for isin in isins_in_db:
            ticker = mapping.get(isin)
            if ticker:
                cur.execute(
                    "UPDATE ml_training_data_v2 SET symbol = %s WHERE symbol = %s",
                    (ticker, isin)
                )
                updated_count += 1
                print(f"  [OK] {isin} -> {ticker}")
            else:
                print(f"  [SKIP] No mapping found for {isin}")

        conn.commit()
        print(f"\n✨ Success! {updated_count} symbols updated to official tickers.")

    except Exception as e:
        print(f"❌ Error: {e}")
        if 'conn' in locals(): conn.rollback()
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    rename_isin_to_tickers()