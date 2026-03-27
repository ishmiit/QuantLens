import time
import psycopg2
import pandas as pd
import os
import subprocess
from datetime import datetime, date
import upstox_client
import zlib
import requests
import io
import numpy as np
import joblib
import urllib.parse
from cachetools import TTLCache

# Import your automated auth and config
import auto_auth
import config

sniper_model = joblib.load("intraday_model.joblib")
voyager_model = joblib.load("swing_model.joblib")

# --- CONFIGURATION ---
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "quantlens"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "admin123"),
    "host": os.getenv("DB_HOST", "127.0.0.1")
}

# --- GLOBAL CACHE & STATE ---
HIGH_52W_CACHE = {}
AVG_VOL_CACHE = {}
INSTRUMENT_MAP = {}  
DB_CONN = None 
NIFTY_50_INSTRUMENTS = []
AI_FEATURE_CACHE = {}
HISTORICAL_CACHE = TTLCache(maxsize=1000, ttl=60)

def load_ai_features_from_db():
    global AI_FEATURE_CACHE
    print("🧠 Loading ML Features from Master...")
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Added cluster_id to the SELECT
            cur.execute("SELECT symbol, rsi, volatility, dist_sma20, cluster_id FROM ticker_master")
            rows = cur.fetchall()
            AI_FEATURE_CACHE = {row[0]: {
                'rsi': float(row[1]), 
                'volatility': float(row[2]), 
                'dist_sma_20': float(row[3]),
                'cluster_id': int(row[4]) if row[4] is not None else 0 # Default to 0
            } for row in rows}
        print(f"✅ AI Features Loaded.")
    except Exception as e:
        print(f"❌ AI Feature Loading Error: {e}")

# --- 1. AUTOMATED TOKEN HANDSHAKE ---
def get_valid_token():
    token_file = "token.txt"
    if os.path.exists(token_file):
        file_time = datetime.fromtimestamp(os.path.getmtime(token_file))
        if file_time.date() == datetime.now().date():
            with open(token_file, "r") as f:
                return f.read().strip()
    
    print("🔄 Token missing or expired. Launching Auto-Login...")
    new_token = auto_auth.get_access_token()
    if new_token:
        with open(token_file, "w") as f: f.write(new_token)
        return new_token
    else:
        raise Exception("❌ Failed to automate login. Check auto_auth.py and config.py")

# --- 2. INITIALIZATION ---
ACCESS_TOKEN = get_valid_token()
configuration = upstox_client.Configuration()
configuration.access_token = ACCESS_TOKEN
api_client = upstox_client.ApiClient(configuration)
market_api = upstox_client.MarketQuoteApi(api_client)

RAW_SYMBOLS = [
    "NSE_INDEX|Nifty%50", "BSE_INDEX|SENSEX", "NSE_INDEX|Nifty Bank",
    "NSE_EQ|HDFCBANK", "NSE_EQ|ICICIBANK", "NSE_EQ|AXISBANK", "NSE_EQ|SBIN", "NSE_EQ|KOTAKBANK", 
    "NSE_EQ|BAJFINANCE", "NSE_EQ|BAJAJFINSV", "NSE_EQ|CHOLAFIN", "NSE_EQ|SHRIRAMFIN", "NSE_EQ|PFC", 
    "NSE_EQ|RECLTD", "NSE_EQ|MUTHOOTFIN", "NSE_EQ|JIOFIN", "NSE_EQ|HDFCLIFE", "NSE_EQ|SBILIFE", 
    "NSE_EQ|LICHSGFIN", "NSE_EQ|BANDHANBNK", "NSE_EQ|IDFCFIRSTB", "NSE_EQ|AUBANK", "NSE_EQ|CANBK",
    "NSE_EQ|PNB", "NSE_EQ|BANKBARODA", "NSE_EQ|IDBI", "NSE_EQ|FEDERALBNK", "NSE_EQ|INDUSINDBK", 
    "NSE_EQ|YESBANK", "NSE_EQ|RBLBANK", "NSE_EQ|LTF", "NSE_EQ|M&MFIN", 
    "NSE_EQ|POONAWALLA", "NSE_EQ|PIRAMALFIN", "NSE_EQ|IEX", "NSE_EQ|MCX",
    "NSE_EQ|TCS", "NSE_EQ|INFY", "NSE_EQ|HCLTECH", "NSE_EQ|WIPRO", "NSE_EQ|TECHM", "NSE_EQ|LTM", 
    "NSE_EQ|PERSISTENT", "NSE_EQ|COFORGE", "NSE_EQ|MPHASIS", "NSE_EQ|KPITTECH", "NSE_EQ|TATAELXSI",
    "NSE_EQ|LTTS", "NSE_EQ|CYIENT", "NSE_EQ|BSOFT", "NSE_EQ|ZENSARTECH", "NSE_EQ|SONATSOFTW",
    "NSE_EQ|OFSS", "NSE_EQ|MASTEK", "NSE_EQ|TATACOMM", "NSE_EQ|HFCL",
    "NSE_EQ|RELIANCE", "NSE_EQ|ONGC", "NSE_EQ|BPCL", "NSE_EQ|IOC", "NSE_EQ|GAIL", "NSE_EQ|HINDPETRO", 
    "NSE_EQ|PETRONET", "NSE_EQ|OIL", "NSE_EQ|COALINDIA", "NSE_EQ|NTPC", "NSE_EQ|POWERGRID", 
    "NSE_EQ|ADANIPOWER", "NSE_EQ|ADANIGREEN", "NSE_EQ|ADANIENSOL", "NSE_EQ|TATAPOWER", 
    "NSE_EQ|NHPC", "NSE_EQ|SJVN", "NSE_EQ|SUZLON", "NSE_EQ|IREDA", "NSE_EQ|CESC",
    "NSE_EQ|M&M", "NSE_EQ|MARUTI", "NSE_EQ|TMPV", "NSE_EQ|BAJAJ-AUTO", "NSE_EQ|EICHERMOT", 
    "NSE_EQ|TVSMOTOR", "NSE_EQ|HEROMOTOCO", "NSE_EQ|TIINDIA", "NSE_EQ|ASHOKLEY", "NSE_EQ|BALKRISIND", 
    "NSE_EQ|MRF", "NSE_EQ|BOSCHLTD", "NSE_EQ|SONACOMS", "NSE_EQ|MOTHERSON", "NSE_EQ|APOLLOTYRE",
    "NSE_EQ|JKTYRE", "NSE_EQ|CEATLTD", "NSE_EQ|EXIDEIND", "NSE_EQ|ARE&M",
    "NSE_EQ|HINDUNILVR", "NSE_EQ|ITC", "NSE_EQ|NESTLEIND", "NSE_EQ|BRITANNIA", "NSE_EQ|TATACONSUM", 
    "NSE_EQ|VBL", "NSE_EQ|GODREJCP", "NSE_EQ|DABUR", "NSE_EQ|MARICO", "NSE_EQ|COLPAL", 
    "NSE_EQ|TITAN", "NSE_EQ|HAVELLS", "NSE_EQ|DIXON", "NSE_EQ|VOLTAS", "NSE_EQ|KAYNES", 
    "NSE_EQ|BLUESTARCO", "NSE_EQ|POLYCAB", "NSE_EQ|KEI", "NSE_EQ|BATAINDIA", "NSE_EQ|RELAXO",
    "NSE_EQ|PAGEIND", "NSE_EQ|TRENT", "NSE_EQ|DMART", "NSE_EQ|ABFRL", "NSE_EQ|NYKAA",
    "NSE_EQ|TATASTEEL", "NSE_EQ|JSWSTEEL", "NSE_EQ|HINDALCO", "NSE_EQ|VEDL", "NSE_EQ|JSL", 
    "NSE_EQ|NATIONALUM", "NSE_EQ|NMDC", "NSE_EQ|SAIL", "NSE_EQ|HINDZINC", "NSE_EQ|WELCORP",
    "NSE_EQ|SUNPHARMA", "NSE_EQ|CIPLA", "NSE_EQ|DRREDDY", "NSE_EQ|DIVISLAB", "NSE_EQ|ZYDUSLIFE", 
    "NSE_EQ|MANKIND", "NSE_EQ|TORNTPHARM", "NSE_EQ|LUPIN", "NSE_EQ|AUROPHARMA", "NSE_EQ|ALKEM", 
    "NSE_EQ|APOLLOHOSP", "NSE_EQ|MAXHEALTH", "NSE_EQ|FORTIS", "NSE_EQ|GLOBAL", "NSE_EQ|SYNGENE",
    "NSE_EQ|LAURUSLABS", "NSE_EQ|GRANULES", "NSE_EQ|GLAND", "NSE_EQ|METROPOLIS", "NSE_EQ|LALPATHLAB",
    "NSE_EQ|LT", "NSE_EQ|BEL", "NSE_EQ|HAL", "NSE_EQ|BHEL", "NSE_EQ|ABB", "NSE_EQ|SIEMENS", 
    "NSE_EQ|CUMMINSIND", "NSE_EQ|MAZDOCK", "NSE_EQ|GRASIM", "NSE_EQ|RVNL", "NSE_EQ|IRFC", 
    "NSE_EQ|IRCON", "NSE_EQ|BDL", "NSE_EQ|COCHINSHIP", "NSE_EQ|GRSE",
    "NSE_EQ|DLF", "NSE_EQ|LODHA", "NSE_EQ|GODREJPROP", "NSE_EQ|OBEROIRLTY", "NSE_EQ|PHOENIXLTD",
    "NSE_EQ|PRESTIGE", "NSE_EQ|BRIGADE", "NSE_EQ|SOBHA", "NSE_EQ|KNRCON", "NSE_EQ|PNCINFRA",
    "NSE_EQ|ULTRACEMCO", "NSE_EQ|SHREECEM", "NSE_EQ|ACC", "NSE_EQ|AMBUJACEM", "NSE_EQ|DALBHARAT",
    "NSE_EQ|JKCEMENT", "NSE_EQ|RAMCOCEM", "NSE_EQ|INDIACEM",
    "NSE_EQ|SRF", "NSE_EQ|PIDILITIND", "NSE_EQ|LINDEINDIA", "NSE_EQ|SOLARINDS", "NSE_EQ|GUJGASLTD", 
    "NSE_EQ|TATACHEM", "NSE_EQ|AARTIIND", "NSE_EQ|DEEPAKNTR", "NSE_EQ|ATUL", "NSE_EQ|NAVINFLUOR",
    "NSE_EQ|ADANIPORTS", "NSE_EQ|CONCOR", "NSE_EQ|GMRAIRPORT", "NSE_EQ|ETERNAL", "NSE_EQ|PAYTM", 
    "NSE_EQ|POLICYBZR", "NSE_EQ|DELHIVERY", "NSE_EQ|INDHOTEL", "NSE_EQ|EASEMYTRIP", "NSE_EQ|BLUEDART",
    "NSE_EQ|AWL", "NSE_EQ|PATANJALI", "NSE_EQ|IGL", "NSE_EQ|MGL", 
    "NSE_EQ|UBL", "NSE_EQ|UNITDSPR", "NSE_EQ|JUBLFOOD", "NSE_EQ|DEVYANI", "NSE_EQ|SAPPHIRE"
]
def generate_live_signals(current_data_row):
    # STRICT 12-item list as required by V6 models
    cols = [
        'rvol', 'change_percent', 'cluster_id', 'rsi', 'dist_sma_20', 
        'volatility', 'adx', 'obv', 'bb_pb', 'vwap_dist', 'day_of_week', 'time_float'
    ]
    
    # Ensure all columns exist in current_data_row, filling with neutral defaults if missing
    for col in cols:
        if col not in current_data_row:
            current_data_row[col] = 0.0
            
    features = pd.DataFrame([current_data_row])[cols]
    
    # Typo Handling: If the model was specifically trained with 'change_perceent', 
    # we rename the column right before inference. 
    # Based on Lead Quant instructions, we check both models' expectation.
    for model in [sniper_model, voyager_model]:
        if hasattr(model, 'feature_names_in_') and 'change_perceent' in model.feature_names_in_:
            features = features.rename(columns={'change_percent': 'change_perceent'})
            break
    
    intraday_conf = sniper_model.predict_proba(features)[0][1]
    swing_conf = voyager_model.predict_proba(features)[0][1]
    
    return {
    "sniper_score": float(round(intraday_conf * 100, 2)),
    "voyager_score": float(round(swing_conf * 100, 2))
}
    
def get_actionable_signal(price, sniper_prob, voyager_prob, volatility):
    # Determine which strategy is firing
    score = max(sniper_prob, voyager_prob)
    strat_name = "INTRADAY" if sniper_prob > voyager_prob else "SWING"
    
    # 75% Filter
    if score < 0.75:
        return {"action": "NEUTRAL", "details": None}

    # Calculate levels based on Volatility (Standard Deviation/ATR logic)
    # If volatility is 1.5%, we set SL at 1.5% and Target at 3%
    sl_pct = volatility if volatility > 0.005 else 0.01  # Floor of 1% SL
    target_pct = sl_pct * 2  # Standard 1:2 Risk/Reward

    return {
        "action": "BUY",
        "strategy": strat_name,
        "confidence": f"{round(score * 100, 2)}%",
        "entry": round(price, 2),
        "target": round(price * (1 + target_pct), 2),
        "stop_loss": round(price * (1 - sl_pct), 2)
    }
    
def get_db_connection():
    global DB_CONN
    if DB_CONN is None or DB_CONN.closed:
        DB_CONN = psycopg2.connect(**DB_CONFIG)
    return DB_CONN

def fetch_historical_data(clean_symbol):
    global HISTORICAL_CACHE
    if clean_symbol in HISTORICAL_CACHE:
        return HISTORICAL_CACHE[clean_symbol].copy()
        
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Handle index symbols which might have different formatting in DB
        db_key = clean_symbol
        if clean_symbol == "NIFTY_50": db_key = "NSE_INDEX|Nifty 50"
        elif clean_symbol == "NIFTY_BANK": db_key = "NSE_INDEX|Nifty Bank"
        elif clean_symbol == "SENSEX": db_key = "BSE_INDEX|SENSEX"
        else:
            # Need to find the instrument key for equity symbols
            db_key = INSTRUMENT_MAP.get(clean_symbol) or clean_symbol

        # Fallback alias for LTM/LTIM corporate action
        if clean_symbol.upper() in ["LTM", "LTIM"]:
            query = "SELECT timestamp, open, high, low, close, volume FROM ml_training_data_v3 WHERE symbol IN (%s, 'LTIM', 'NSE_EQ|LTIM', 'LTM', 'NSE_EQ|LTM') ORDER BY timestamp DESC LIMIT 50"
            cur.execute(query, (db_key,))
        else:
            query = "SELECT timestamp, open, high, low, close, volume FROM ml_training_data_v3 WHERE symbol = %s ORDER BY timestamp DESC LIMIT 50"
            cur.execute(query, (db_key,))
            
        rows = cur.fetchall()
        cur.close()
        
        if rows:
            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = df.iloc[::-1].reset_index(drop=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            HISTORICAL_CACHE[clean_symbol] = df
            return df.copy()
    except Exception as e:
        print(f"❌ Error fetching historical for {clean_symbol}: {e}")
    return pd.DataFrame()

def calculate_institutional_features(symbol, current_price, current_vol, open_price):
    # Ported from server.py / filler.py
    adx_val, obv_val, bb_pb_val, vwap_dist_val = 0.0, 0.0, 0.5, 0.0
    
    hist_df = fetch_historical_data(symbol)
    if not hist_df.empty and len(hist_df) > 20:
        now_ts = pd.Timestamp.now(tz='UTC')
        live_row = pd.DataFrame([{
            'timestamp': now_ts,
            'open': open_price,
            'high': current_price, 
            'low': current_price,
            'close': current_price,
            'volume': current_vol
        }])
        df = pd.concat([hist_df, live_row], ignore_index=True)
                                                                         
        close = df['close']
        high = df['high']
        low = df['low']
        volume = df['volume']
        
        # 1. ATR (for ADX)
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr_series = tr.rolling(window=14).mean()
        
        # 2. ADX (14-period)
        up_move = high - high.shift(1)
        down_move = low.shift(1) - low
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
        
        with np.errstate(divide='ignore', invalid='ignore'):
            plus_di = 100 * (pd.Series(plus_dm).rolling(window=14).mean() / atr_series)
            minus_di = 100 * (pd.Series(minus_dm).rolling(window=14).mean() / atr_series)
            dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).abs()
        adx_val = dx.rolling(window=14).mean().replace([np.inf, -np.inf], np.nan).fillna(0).iloc[-1]
        
        # 3. OBV 
        delta_val = close.diff()
        obv_val = (np.sign(delta_val) * volume).fillna(0).cumsum().iloc[-1]
        
        # 4. Bollinger Band %B (20-period)
        sma20 = close.rolling(window=20).mean()
        std20 = close.rolling(window=20).std()
        upper_band = sma20 + (2 * std20)
        lower_band = sma20 - (2 * std20)
        with np.errstate(divide='ignore', invalid='ignore'):
            pb = (close - lower_band) / (upper_band - lower_band)
        bb_pb_val = pb.replace([np.inf, -np.inf], np.nan).fillna(0.5).iloc[-1]
        
        # 5. VWAP Distance (Daily reset)
        df['date'] = df['timestamp'].dt.date
        typical_price = (high + low + close) / 3
        cum_vol = df.groupby('date')['volume'].cumsum()
        temp_df = pd.DataFrame({'tp_vol': typical_price * volume, 'date': df['date']})
        cum_tp_vol = temp_df.groupby('date')['tp_vol'].cumsum()
        vwap = cum_tp_vol / cum_vol
        with np.errstate(divide='ignore', invalid='ignore'):
            vw_dist = (close - vwap) / vwap
        vwap_dist_val = vw_dist.replace([np.inf, -np.inf], np.nan).fillna(0).iloc[-1]
        
    return {
        'adx': float(adx_val),
        'obv': float(obv_val),
        'bb_pb': float(bb_pb_val),
        'vwap_dist': float(vwap_dist_val)
    }

def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        fval = float(value)
        if np.isnan(fval) or np.isinf(fval):
            return default
        return fval
    except (ValueError, TypeError):
        return default

def clean_symbol_name(raw_key, raw_t_symbol=None):
    base = str(raw_t_symbol if raw_t_symbol else raw_key)
    symbol = base.replace('|', ':').split(':')[-1].upper().strip()
    
    if "NIFTY" in symbol and "50" in symbol: return "NIFTY_50"
    if "BANK" in symbol: return "NIFTY_BANK"
    if "SENSEX" in symbol: return "SENSEX"
    
    return symbol.replace('-EQ', '').replace('.NS', '').strip()

def get_real_instrument_keys(human_symbols):
    global INSTRUMENT_MAP
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    headers = {'User-Agent': 'Mozilla/5.0'}
    print("📡 Syncing with Upstox Master List...")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        content = zlib.decompress(response.content, zlib.MAX_WBITS | 16)
        df = pd.read_csv(io.BytesIO(content))
        
        for _, row in df.iterrows():
            t_symbol = str(row['tradingsymbol']).strip().upper()
            i_key = str(row['instrument_key']).strip()
            INSTRUMENT_MAP[t_symbol] = i_key
            if "-EQ" in t_symbol:
                INSTRUMENT_MAP[t_symbol.replace("-EQ", "")] = i_key

        valid_keys = []
        for symbol in human_symbols:
            clean_name = symbol.split('|')[-1].replace('%50', ' 50').replace('%20', ' ').strip().upper()
            
            if "NIFTY 50" in clean_name:
                key = INSTRUMENT_MAP.get("NIFTY 50") or "NSE_INDEX|Nifty 50"
            elif "SENSEX" in clean_name:
                key = INSTRUMENT_MAP.get("SENSEX") or "BSE_INDEX|SENSEX"
            elif "BANK" in clean_name:
                key = INSTRUMENT_MAP.get("NIFTY BANK") or "NSE_INDEX|Nifty Bank"
            else:
                key = INSTRUMENT_MAP.get(clean_name)
                
            if key:
                valid_keys.append(key)
            else:
                print(f"⚠️ Missing from Upstox Master: {clean_name}")
        
        return list(set(valid_keys))
    except Exception as e:
        print(f"❌ Master List Sync Failed: {e}")
        return []

def load_avg_vol_from_db():
    """Crucial for RVOL: Loads avg volumes from your Master table into memory"""
    global AVG_VOL_CACHE
    print("📊 Loading Avg Volume Cache from DB...")
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT symbol, avg_vol_20d FROM ticker_master")
            rows = cur.fetchall()
            AVG_VOL_CACHE = {row[0]: float(row[1]) for row in rows if row[1] > 0}
        print(f"✅ Volume Cache Primed: {len(AVG_VOL_CACHE)} symbols.")
    except Exception as e:
        print(f"❌ Volume Cache Loading Error: {e}")

def update_master_cache():
    print("📥 Warming up Master Context (ticker_master)...")
    if not NIFTY_50_INSTRUMENTS: return
    
    master_data = []
    batch_size = 50 

    # We loop in steps of 50
    for i in range(0, len(NIFTY_50_INSTRUMENTS), batch_size):
        chunk = NIFTY_50_INSTRUMENTS[i : i + batch_size]
        instruments_string = ",".join([c.replace("&", "%26") for c in chunk])
        
        try:
            # ONE call for 50 stocks
            api_response = market_api.get_full_market_quote(instruments_string, '2.0')
            
            if api_response and api_response.data:
                for instrument_key, details in api_response.data.items():
                    # Extract trading symbol safely
                    t_symbol = getattr(details, 'trading_symbol', None)
                    db_symbol = clean_symbol_name(instrument_key, t_symbol)
                    
                    prev_close = safe_float(getattr(details, 'last_close', 0.0))
                    high_52 = safe_float(getattr(details, 'fifty_two_week_high', 0.0))
                    
                    HIGH_52W_CACHE[db_symbol] = high_52
                    sector = "Index" if "INDEX" in instrument_key.upper() else "Equity"

                    master_data.append((
    db_symbol, prev_close, 0, 0.0, 50.0, 0.0, high_52, sector, 0.0  # Added a 0.0 for volatility
))
            
            # Breathe for 1 second between these big batches
            time.sleep(1.0) 
            
        except Exception as e:
            print(f"⚠️ Master Cache Batch Error: {e}")
            if "429" in str(e):
                print("🚫 Rate limit hit during warmup. Cooling down...")
                time.sleep(10)

    if master_data:
        save_to_master_db(master_data)
    print(f"✅ Master Table Primed: {len(master_data)} entries.")
    
def save_to_master_db(data_list):
    query = """
    INSERT INTO ticker_master (symbol, prev_close, avg_vol_20d, atr, rsi, dist_sma20, dist_52wh, sector, volatility)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol) DO UPDATE SET 
        dist_52wh=EXCLUDED.dist_52wh;
"""
    execute_batch(query, data_list)

def save_to_live_db(data_list):
    query = """
        INSERT INTO ticker_live (symbol, price, open_price, pct_change, rvol, ai_signal, confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol) DO UPDATE SET 
            price=EXCLUDED.price, 
            open_price=EXCLUDED.open_price,
            pct_change=EXCLUDED.pct_change, 
            rvol=EXCLUDED.rvol, 
            timestamp=CURRENT_TIMESTAMP;
    """
    execute_batch(query, data_list)

def execute_batch(query, data):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.executemany(query, data)
            conn.commit()
    except Exception as e:
        import traceback
        print(f"❌ DB Error: {e}")
        traceback.print_exc()
        if DB_CONN: DB_CONN.rollback()

def fetch_market_data():
    now = datetime.now()
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    minutes_passed = max(1, (now - market_open).total_seconds() / 60)
    day_fraction = min(1.0, minutes_passed / 375) if now > market_open else 0.01
    
    # Calculate time features for the model
    time_float = now.hour + now.minute / 60.0
    day_of_week = now.weekday()

    try:
        to_save_live = []
        batch_size = 50 
        
        for i in range(0, len(NIFTY_50_INSTRUMENTS), batch_size):
            chunk = NIFTY_50_INSTRUMENTS[i:i + batch_size]
            
            # --- INNER TRY BLOCK DEFEATS SILENT SKIPPING ---
            try:
                instruments_string = ",".join([c.replace("&", "%26") for c in chunk])
                api_response = market_api.get_full_market_quote(instruments_string, '2.0')
                
                if not api_response or not api_response.data: 
                    continue
                
                for key, details in api_response.data.items():
                    db_symbol = clean_symbol_name(key, getattr(details, 'trading_symbol', None))
                    price = safe_float(details.last_price)
                    
                    # 1. Price & Change Calculation
                    open_price = 0.0
                    if hasattr(details, 'ohlc') and details.ohlc:
                        open_price = safe_float(details.ohlc.open)
                    
                    if open_price > 0:
                        change_pct = round(((price - open_price) / open_price * 100), 2)
                    else:
                        prev_close = safe_float(getattr(details, 'last_close', 0))
                        change_pct = round(((price - prev_close) / prev_close * 100), 2) if prev_close > 0 else 0.0
                    
                    # 2. RVOL Calculation
                    current_vol = safe_float(getattr(details, 'volume', 0))
                    avg_vol = AVG_VOL_CACHE.get(db_symbol, 1.0)
                    rvol = round(current_vol / (avg_vol * day_fraction), 2) if (avg_vol * day_fraction) > 0 else 1.0

                    # 3. Fetch ML Features from Cache (Volatility, RSI, SMA)
                    # Providing defaults if symbol isn't in master yet
                    features = AI_FEATURE_CACHE.get(db_symbol, {
                        'rsi': 50.0, 
                        'volatility': 0.015, 
                        'dist_sma_20': 0.0
                    })

                    # 4. Generate AI Prediction Scores
                    # Calculate missing institutional features (ADX, OBV, %B, VWAP Dist)
                    inst_features = calculate_institutional_features(
                        db_symbol, 
                        price, 
                        current_vol, 
                        open_price if open_price > 0 else price
                    )
                    
                    data_row = {
                        "rvol": rvol,
                        "change_percent": change_pct,
                        "cluster_id": features.get('cluster_id', 0),
                        "rsi": features['rsi'],
                        "dist_sma_20": features['dist_sma_20'],
                        "volatility": features['volatility'],
                        "adx": inst_features['adx'],
                        "obv": inst_features['obv'],
                        "bb_pb": inst_features['bb_pb'],
                        "vwap_dist": inst_features['vwap_dist'],
                        "day_of_week": day_of_week,
                        "time_float": time_float
                    }
                    
                    scores = generate_live_signals(data_row)
                    
                    # 5. Determine Actionable Signal (BUY/NEUTRAL) and Levels
                    # Convert scores back to 0-1 scale for the signal logic
                    signal_info = get_actionable_signal(
                        price, 
                        scores['sniper_score'] / 100, 
                        scores['voyager_score'] / 100, 
                        features['volatility']
                    )

                    # 6. Append to batch for DB update
                    # We save the action (BUY/NEUTRAL) and the highest confidence score
                    max_conf = float(max(scores['sniper_score'], scores['voyager_score']))

                    to_save_live.append((
                    str(db_symbol), 
                    float(price), 
                    float(open_price), 
                    float(change_pct), 
                    float(rvol), 
                    str(signal_info['action']), 
                    max_conf # Already converted to float above
                    ))
                    
            except upstox_client.rest.ApiException as api_e:
                if api_e.status == 401:
                    print("🚫 401 UNAUTHORIZED: Active token expired. Plase clear token.txt or restart app.")
                else:
                    print(f"⚠️ Upstox API Error mapping chunk starting at {i}: {api_e}")
            except Exception as loop_e:
                print(f"⚠️ Unknown chunk formatting error: {loop_e}")
            
            # Rate limiting safety
            time.sleep(0.5)

        if to_save_live:
            save_to_live_db(to_save_live)
            # Optional: Print only if a BUY signal is detected
            buys = [s[0] for s in to_save_live if s[5] == "BUY"]
            log_msg = f"🚀 {now.strftime('%H:%M:%S')} | Sync: {len(to_save_live)} symbols"
            if buys:
                log_msg += f" | 🔥 BUYS: {', '.join(buys)}"
            print(log_msg)

    except Exception as e:
        import traceback
        print(f"❌ Market Fetch Error: {e}")
        traceback.print_exc()

def check_and_run_master_refresh():
    """Run refresh_master.py once per calendar day, skip if already done today."""
    cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "master_refresh_date.txt")
    today_str = date.today().isoformat()  # e.g. '2026-03-04'

    # Check if we already refreshed today
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            cached_date = f.read().strip()
        if cached_date == today_str:
            print(f"✅ Master DB already up to date for today ({today_str}). Skipping refresh.")
            return

    # Need to refresh
    print(f"🔄 Running daily Master DB refresh for {today_str}...")
    refresh_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "refresh_master.py")

    try:
        subprocess.run(["python", refresh_script], check=True)
        # Only write the cache AFTER successful completion
        with open(cache_file, "w") as f:
            f.write(today_str)
        print(f"✅ Master DB refresh completed successfully. Cache updated to {today_str}.")
    except subprocess.CalledProcessError as e:
        print(f"❌ refresh_master.py FAILED (exit code {e.returncode}). Cache NOT updated — will retry on next boot.")
    except FileNotFoundError:
        print(f"❌ refresh_master.py not found at: {refresh_script}")

if __name__ == "__main__":
    # 0. Daily Master Refresh Gate
    check_and_run_master_refresh()

    # 1. Map symbols to Upstox keys
    NIFTY_50_INSTRUMENTS = get_real_instrument_keys(RAW_SYMBOLS)
    
    # 2. Update Master DB (prices/52w high)
    update_master_cache()
    
    # 3. Load Average Volume into memory for RVOL calculation
    load_avg_vol_from_db()
    
    load_ai_features_from_db()
    
    print("🚦 Engine Starting Live Loop...")
    while True:
        try:
            fetch_market_data()
            time.sleep(1)
        except KeyboardInterrupt: break
        except Exception as e:
            print(f"Loop Error: {e}")
            time.sleep(2)