import upstox_client
from upstox_client.rest import ApiException
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import time
import os
import urllib3
from datetime import datetime, timedelta
from tqdm import tqdm

# --- CONFIG ---
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI0TEJSWlAiLCJqdGkiOiI2OTgyZmE4ZDIyOTcxMTc1ZDM3ZjczNDQiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc3MDE5MTUwMSwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzcwMjQyNDAwfQ.2SHBLPCDsMShyVe-VG5-wjANyx0dENCPLaPGYsoASeA" 
TICKERS = [
    # Indices
    "Nifty 50", "SENSEX", "Nifty Bank",
    
    # Banking & Finance
    "HDFCBANK", "ICICIBANK", "AXISBANK", "SBIN", "KOTAKBANK", "BAJFINANCE", 
    "BAJAJFINSV", "CHOLAFIN", "SHRIRAMFIN", "PFC", "RECLTD", "MUTHOOTFIN", 
    "JIOFIN", "HDFCLIFE", "SBILIFE", "LICHSGFIN", "BANDHANBNK", "IDFCFIRSTB", 
    "AUBANK", "CANBK", "PNB", "BANKBARODA", "IDBI", "FEDERALBNK", "INDUSINDBK", 
    "YESBANK", "RBLBANK", "LTF", "M&MFIN", "POONAWALLA", "PIRAMALFIN", "IEX", "MCX",
    
    # IT & Technology
    "TCS", "INFY", "HCLTECH", "WIPRO", "TECHM", "LTIM", "PERSISTENT", "COFORGE", 
    "MPHASIS", "KPITTECH", "TATAELXSI", "LTTS", "CYIENT", "BSOFT", "ZENSARTECH", 
    "SONATSOFTW", "OFSS", "MASTEK", "TATACOMM", "HFCL",
    
    # Energy, Power & Commodities
    "RELIANCE", "ONGC", "BPCL", "IOC", "GAIL", "HINDPETRO", "PETRONET", "OIL", 
    "COALINDIA", "NTPC", "POWERGRID", "ADANIPOWER", "ADANIGREEN", "ADANIENSOL", 
    "TATAPOWER", "NHPC", "SJVN", "SUZLON", "IREDA", "CESC",
    
    # Auto & Ancillaries
    "M&M", "MARUTI", "TMPV", "BAJAJ-AUTO", "EICHERMOT", "TVSMOTOR", "HEROMOTOCO", 
    "TIINDIA", "ASHOKLEY", "BALKRISIND", "MRF", "BOSCHLTD", "SONACOMS", "MOTHERSON", 
    "APOLLOTYRE", "JKTYRE", "CEATLTD", "EXIDEIND", "ARE&M",
    
    # FMCG & Consumer Durables
    "HINDUNILVR", "ITC", "NESTLEIND", "BRITANNIA", "TATACONSUM", "VBL", "GODREJCP", 
    "DABUR", "MARICO", "COLPAL", "TITAN", "HAVELLS", "DIXON", "VOLTAS", "KAYNES", 
    "BLUESTARCO", "POLYCAB", "KEI", "BATAINDIA", "RELAXO", "PAGEIND",
    
    # Retail & Ecommerce
    "TRENT", "DMART", "ABFRL", "NYKAA", "PAYTM", "POLICYBZR", "DELHIVERY", 
    "EASEMYTRIP", "ZOMATO",
    
    # Metals & Mining
    "TATASTEEL", "JSWSTEEL", "HINDALCO", "VEDL", "JSL", "NATIONALUM", "NMDC", 
    "SAIL", "HINDZINC", "WELCORP",
    
    # Healthcare & Pharma
    "SUNPHARMA", "CIPLA", "DRREDDY", "DIVISLAB", "ZYDUSLIFE", "MANKIND", 
    "TORNTPHARM", "LUPIN", "AUROPHARMA", "ALKEM", "APOLLOHOSP", "MAXHEALTH", 
    "FORTIS", "GLOBAL", "SYNGENE", "LAURUSLABS", "GRANULES", "GLAND", 
    "METROPOLIS", "LALPATHLAB",
    
    # Infrastructure, Industrials & Realty
    "LT", "BEL", "HAL", "BHEL", "ABB", "SIEMENS", "CUMMINSIND", "MAZDOCK", 
    "GRASIM", "RVNL", "IRFC", "IRCON", "BDL", "COCHINSHIP", "GRSE", "DLF", 
    "LODHA", "GODREJPROP", "OBEROIRLTY", "PHOENIXLTD", "PRESTIGE", "BRIGADE", 
    "SOBHA", "KNRCON", "PNCINFRA",
    
    # Cement
    "ULTRACEMCO", "SHREECEM", "ACC", "AMBUJACEM", "DALBHARAT", "JKCEMENT", 
    "RAMCOCEM", "INDIACEM",
    
    # Chemicals & Others
    "SRF", "PIDILITIND", "LINDEINDIA", "SOLARINDS", "GUJGASLTD", "TATACHEM", 
    "AARTIIND", "DEEPAKNTR", "ATUL", "NAVINFLUOR", "ADANIPORTS", "CONCOR", 
    "GMRAIRPORT", "ETERNAL", "INDHOTEL", "BLUEDART", "AWL", "PATANJALI", 
    "IGL", "MGL", "UBL", "UNITDSPR", "JUBLFOOD", "DEVYANI", "SAPPHIRE"
]
DB_CONFIG = {"dbname": "quantlens", "user": "postgres", "password": "admin123", "host": "127.0.0.1"}
PROGRESS_FILE = "upstox_progress.txt"

def get_instrument_keys(tickers):
    print("Updating Instrument Map...")
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    df = pd.read_csv(url)
    mapping = {**dict(zip(df[df['exchange'] == 'NSE_EQ']['tradingsymbol'], df[df['exchange'] == 'NSE_EQ']['instrument_key'])),
               **dict(zip(df[df['exchange'] == 'NSE_INDEX']['tradingsymbol'], df[df['exchange'] == 'NSE_INDEX']['instrument_key']))}
    return [mapping[t] for t in tickers if t in mapping]

def fetch_upstox_data():
    configuration = upstox_client.Configuration()
    configuration.access_token = ACCESS_TOKEN
    
    # --- PERSISTENT SESSION LOGIC ---
    # This reuses the same connection to avoid flagging your IP with constant SSL handshakes
    http = urllib3.PoolManager(
        cert_reqs='CERT_REQUIRED',
        retries=urllib3.util.Retry(total=3, backoff_factor=2)
    )
    api_client = upstox_client.ApiClient(configuration)
    api_client.rest_client.pool_manager = http 
    
    api_instance = upstox_client.HistoryApi(api_client)
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Create the new v3 table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ml_training_data_v3 (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(100),
            timestamp TIMESTAMPTZ,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            rvol NUMERIC DEFAULT 0,
            change_percent NUMERIC DEFAULT 0,
            cluster_id INTEGER DEFAULT 0,
            rsi NUMERIC DEFAULT 0,
            dist_sma_20 NUMERIC DEFAULT 0,
            volatility NUMERIC DEFAULT 0,
            adx NUMERIC DEFAULT 0,
            obv NUMERIC DEFAULT 0,
            bb_pb NUMERIC DEFAULT 0,
            vwap_dist NUMERIC DEFAULT 0,
            target_intraday INTEGER DEFAULT 0,
            target_swing INTEGER DEFAULT 0,
            UNIQUE(symbol, timestamp)
        );
    """)
    conn.commit()

    keys_to_fetch = get_instrument_keys(TICKERS)
    finished = set(line.strip() for line in open(PROGRESS_FILE)) if os.path.exists(PROGRESS_FILE) else set()

    for inst_key in keys_to_fetch:
        if inst_key in finished: continue
        
        current_to = (datetime.now() - timedelta(days=1)).date()
        stop_limit = datetime(2021, 1, 1).date()
        pivot_date = datetime(2022, 1, 1).date()
        
        total_days = (current_to - stop_limit).days
        pbar = tqdm(total=total_days, desc=f"📦 {inst_key}", unit="day")

        while current_to > stop_limit:
            if current_to >= pivot_date:
                interval, days_step = '30minute', 14
            else:
                interval, days_step = 'day', 100
            
            current_from = max(current_to - timedelta(days=days_step), stop_limit)
            
            success = False
            attempts = 0
            while not success and attempts < 5:
                try:
                    response = api_instance.get_historical_candle_data1(
                        inst_key, interval, 
                        current_to.strftime('%Y-%m-%d'), 
                        current_from.strftime('%Y-%m-%d'), 
                        '2.0'
                    )
                    
                    if response.data and response.data.candles:
                        rows = [(inst_key, datetime.strptime(c[0], "%Y-%m-%dT%H:%M:%S%z"), c[1], c[2], c[3], c[4], c[5], 0.0, 0.0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0) 
                                for c in response.data.candles]
                        execute_values(cur, """INSERT INTO ml_training_data_v3 (symbol, timestamp, open, high, low, close, volume, rvol, change_percent, cluster_id, rsi, dist_sma_20, volatility, adx, obv, bb_pb, vwap_dist, target_intraday, target_swing) 
                                               VALUES %s ON CONFLICT DO NOTHING""", rows)
                        conn.commit()
                    
                    success = True
                    pbar.update((current_to - current_from).days + 1)
                    current_to = current_from - timedelta(days=1)
                    time.sleep(1.2) # INCREASED: Gentler pace to avoid IP block

                except Exception as e:
                    attempts += 1
                    wait_time = attempts * 3 # INCREASED: Longer backoff for IP safety
                    if attempts < 5:
                        pbar.write(f" ⚠️ Connection glitch: {e}. Cooling down {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        pbar.write(f" ❌ Skipping chunk at {current_to}.")
                        current_to -= timedelta(days=1)
                        pbar.update(1)

        pbar.close()
        with open(PROGRESS_FILE, "a") as f: f.write(f"{inst_key}\n")

    cur.close()
    conn.close()

if __name__ == "__main__":
    fetch_upstox_data()