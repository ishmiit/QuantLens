from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import traceback
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel  # <--- ADD THIS
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import asyncio
import json
import urllib.parse
from decimal import Decimal
import joblib
import os
import requests
import zlib
import io
import pandas as pd
import numpy as np
from cachetools import TTLCache

INSTRUMENT_MAP = {}
KEY_TO_SYMBOL = {}  # The Rosetta Stone Mapper
HISTORICAL_CACHE = TTLCache(maxsize=1000, ttl=60)

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

# --- EXECUTION THRESHOLDS (Based on raw XGBoost confidence) ---
SNIPER_THRESHOLD = 0.54
VOYAGER_THRESHOLD = 0.54

# --- PORTFOLIO SCALING (V6.0 Apex Parameters) ---
FIXED_TRADE_ALLOCATION = 5000.0
INITIAL_CAPITAL = 100000.0

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://quant-lens.vercel.app", 
        "http://localhost:5173", 
        "http://localhost:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {"status": "awake"}

import sys

# Strictly pull from the environment
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("🔴 FATAL: DATABASE_URL environment variable is not set in Render!")
    sys.exit(1)

# SQLAlchemy strictly requires 'postgresql://' not 'postgres://'
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

@app.on_event("startup")
def startup_event():
    print("🟢 ACTIVE ROUTES:")
    for route in app.routes:
        print(f" - {route.path} ({route.name})")
    print("🟢 SERVER READY")
    
    # --- PHASE 1: DATABASE SCHEMA ---
    import sys
    conn = None
    try:
        conn = get_db_connection()
        print("🟢 Neon Database Connection Successful!")
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS active_trades (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(50) NOT NULL,
                entry_price NUMERIC,
                stop_loss NUMERIC,
                target NUMERIC,
                quantity INTEGER,
                trade_type VARCHAR(20),
                status VARCHAR(20) DEFAULT 'OPEN',
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # --- SCHEMA HARDENING: APEX PERSISTENCE ---
        cur.execute("ALTER TABLE ticker_live ADD COLUMN IF NOT EXISTS init_vol_rupees NUMERIC DEFAULT 0")
        cur.execute("ALTER TABLE ticker_live ADD COLUMN IF NOT EXISTS entry_time TIMESTAMP")
        
        conn.commit()
        cur.close()
        print("✅ DB Schema Verified & Hardened: init_vol_rupees and entry_time columns ensured.")
    except psycopg2.Error as e:
        print(f"🔴 CRITICAL DB ERROR: {str(e)}")
    except Exception as e:
        print(f"🔴 UNKNOWN DB ERROR: {str(e)}")
    finally:
        if conn:
            conn.close()
    
    # --- PHASE 2: UPSTOX INSTRUMENTS (Non-blocking: failure here must NOT kill the server) ---
    print("📡 Downloading Upstox Master Instrument List...")
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        content = zlib.decompress(response.content, zlib.MAX_WBITS | 16)
        df = pd.read_csv(io.BytesIO(content))
        
        global INSTRUMENT_MAP, KEY_TO_SYMBOL
        for _, row in df.iterrows():
            t_symbol = str(row['tradingsymbol']).strip().upper()
            i_key = str(row['instrument_key']).strip()
            INSTRUMENT_MAP[t_symbol] = i_key
            KEY_TO_SYMBOL[i_key] = t_symbol
            if "-EQ" in t_symbol:
                clean_sym = t_symbol.replace("-EQ", "")
                INSTRUMENT_MAP[clean_sym] = i_key
                # LTM / LTIM Corporate Action Rosetta Stone logic
                if clean_sym == "LTM":
                    INSTRUMENT_MAP["LTIM"] = i_key
        print(f"✅ Loaded {len(INSTRUMENT_MAP)} instrument keys and reverse mapping.")
    except requests.exceptions.Timeout:
        print("⚠️ Upstox CSV download timed out after 15s. Server will continue without instrument map.")
    except Exception as e:
        import traceback
        print(f"❌ Failed to load Upstox instruments (non-fatal): {e}")
        traceback.print_exc()
    
    print("🚀 Startup complete. Server is ready to accept connections.")

class ForgeTrade(BaseModel):
    symbol: str
    entryPrice: float
    quantity: int
    stop_loss: float
    target_price: float
    probability: float
    signal: str

class TradePayload(BaseModel):
    symbol: str
    entry_price: float
    stop_loss: float
    target: float
    quantity: int
    trade_type: str

# --- GLOBAL SIGNAL CACHE (The Debouncer) ---
sent_signals = {}

# --- LOAD MODELS ---
sniper_model = None
voyager_model = None

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.join(BASE_DIR, "..", "..", "python_engine", "src")

sniper_path = os.path.join(MODEL_DIR, "intraday_model.joblib")
voyager_path = os.path.join(MODEL_DIR, "swing_model.joblib")

try:
    if os.path.exists(sniper_path):
        sniper_model = joblib.load(sniper_path)
        print("🎯 Sniper Model Loaded (Intraday)")
    else:
        print(f"⚠️ Sniper model missing at: {sniper_path}")
        
    if os.path.exists(voyager_path):
        voyager_model = joblib.load(voyager_path)
        print("🚢 Voyager Model Loaded (Swing)")
    else:
        print(f"⚠️ Voyager model missing at: {voyager_path}")
except Exception as e:
    print(f"❌ Error loading models: {e}")

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_instrument_key(clean_symbol):
    clean_name = clean_symbol.upper().strip()
    if "NIFTY_50" in clean_name:
        return INSTRUMENT_MAP.get("NIFTY 50") or "NSE_INDEX|Nifty 50"
    elif "SENSEX" in clean_name:
        return INSTRUMENT_MAP.get("SENSEX") or "BSE_INDEX|SENSEX"
    elif "NIFTY_BANK" in clean_name:
        return INSTRUMENT_MAP.get("NIFTY BANK") or "NSE_INDEX|Nifty Bank"
        
    if clean_name == "LTM":
        return INSTRUMENT_MAP.get("LTM") or INSTRUMENT_MAP.get("LTIM")
    elif clean_name == "LTIM":
        return INSTRUMENT_MAP.get("LTIM") or INSTRUMENT_MAP.get("LTM")
        
    return INSTRUMENT_MAP.get(clean_name)

def fetch_historical_data(clean_symbol, conn):
    global HISTORICAL_CACHE
    if clean_symbol in HISTORICAL_CACHE:
        return HISTORICAL_CACHE[clean_symbol].copy()
        
    instrument_key = get_instrument_key(clean_symbol)
    if not instrument_key:
        print(f"⚠️ No instrument key found for {clean_symbol}")
        return pd.DataFrame()
        
    try:
        cur = conn.cursor()
        # Rosetta Translation: Handle mapping for both clean symbols and raw keys gracefully
        db_key = get_instrument_key(clean_symbol) if "|" not in clean_symbol else clean_symbol
        
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

# --- WEEKEND API FALLBACK ---
@app.get("/api/market-quotes")
async def get_market_quotes():
    conn = None
    try:
        valid_keys = []
        for symbol in RAW_SYMBOLS:
            clean_sym = symbol.split("|")[-1]
            if clean_sym == "Nifty%50": clean_sym = "NIFTY_50"
            elif clean_sym == "Nifty Bank": clean_sym = "NIFTY_BANK"
            elif clean_sym == "SENSEX": clean_sym = "SENSEX"
            
            # Safely get the key, handling potential missing entries
            key = get_instrument_key(clean_sym)
            if key:
                valid_keys.append(key)
                
        if not valid_keys:
            return {"error": "No valid instrument keys found to fetch."}
            
        joined_keys = ",".join(valid_keys)
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT value FROM system_config WHERE key = 'UPSTOX_TOKEN'")
        token_row = cur.fetchone()
        if not token_row:
            return {"error": "UPSTOX_TOKEN missing"}
        access_token = token_row[0]
        
        url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={joined_keys}"
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        response = requests.get(url, headers=headers)
        return response.json()
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}
    finally:
        if conn: conn.close()

# --- RESET ROUTE ---
@app.get("/reset-signals")
async def reset_signals():
    global sent_signals
    sent_signals = {}
    print("🧹 Signal cache cleared manually via API")
    return {"status": "success", "message": "Signal cache cleared"}

def run_monte_carlo(price, target, stop_loss, volatility_pct, days=20, sims=1000000):
    if price <= 0 or volatility_pct <= 0 or target == stop_loss:
        return 0.0
        
    # Fully vectorized path generation for 1M simulations
    rand_vals = np.random.normal(0, 1, (sims, days - 1))
    multipliers = 1 + rand_vals * volatility_pct
    paths = np.cumprod(multipliers, axis=1) * price
    
    # Determine which threshold is hit first across the matrix
    is_long = target > price
    if is_long:
        hit_target = paths >= target
        hit_sl = paths <= stop_loss
    else:
        hit_target = paths <= target
        hit_sl = paths >= stop_loss
        
    # Find the step index of the first hit (argmax returns 0 if all False)
    target_idx = np.argmax(hit_target, axis=1)
    sl_idx = np.argmax(hit_sl, axis=1)
    
    # Apply a high index (days) where no hit occurred, so it doesn't default to 0
    target_idx = np.where(hit_target.any(axis=1), target_idx, days)
    sl_idx = np.where(hit_sl.any(axis=1), sl_idx, days)
    
    # A path is a win if it hits the target strictly before hitting the stop loss
    wins = np.sum(target_idx < sl_idx)
    
    return round((float(wins) / sims) * 100, 1)

def apply_conviction_logic(stock, conn=None, run_mc=False):
    global sent_signals
    symbol = stock.get('symbol', 'UNKNOWN')
    
    try:
        # 1. Server-Side Truth Extraction
        price_raw = stock.get('price')
        price = float(price_raw) if price_raw is not None else 0.0
        
        # BAD DATA GUARD: Prevents ZeroDivisionError on glitches
        if price <= 0:
            return stock
        
        # RVOL Normalizer logic as a Ratio
        rvol_raw = stock.get('rvol') if stock.get('rvol') is not None else stock.get('rvol_ratio')
        rvol = float(rvol_raw) if rvol_raw is not None else 0.0
        if rvol > 50:  # If it's stored as raw volume/percentage instead of ratio
            rvol = round(rvol / 100.0, 2)
            
        # Percentage Change Priority Logic
        db_pct_raw = stock.get('live_pct') if stock.get('live_pct') is not None else stock.get('pct_change')
        db_pct = float(db_pct_raw) if db_pct_raw is not None else 0.0
        
        prev_close_raw = stock.get('prev_close')
        prev_close = float(prev_close_raw) if prev_close_raw is not None else 0.0
        
        # Safeguard: Prevent division by extremely low / 0.0 previous closes which cause 50,000% inflation
        if prev_close > 0.01 and price > 0:
            # Priority 1: Force manual percentage math from master
            pct_change = round(((price - prev_close) / prev_close) * 100, 2)
        else:
            # Priority 2: Fallback to whatever Upstox live feed sent, or 0.0 gracefully
            pct_change = round(db_pct, 2) if db_pct else 0.0
            
        # Sanity check: Data anomaly detection for 500% glitches
        is_anomaly = False
        if abs(pct_change) > 100.0:
            print(f"⚠️ DATA ANOMALY DETECTED: {symbol} showing {pct_change}% daily change. Capping at 100%.")
            pct_change = 100.0 if pct_change > 0 else -100.0
            is_anomaly = True
            
        # Target RELIANCE for debug logging
        if symbol == 'RELIANCE':
            print(f"🔍 DEBUG RELIANCE | Live: {price} | PrevClose: {prev_close} | DB_Pct: {db_pct} | Calc_Pct: {pct_change}")
            
    except Exception as e:
        print(f"❌ Extraction Error for {symbol}: {e}")
        pct_change = 0.0
        price = 0.0
        rvol = 0.0

    # 2. Sync values & Fallbacks
    safe_atr = float(stock.get('atr') or (price * 0.015))
    safe_rsi = float(stock.get('rsi') or 50.0)
    safe_dist_sma20 = float(stock.get('dist_sma20') or 0.0)

    # NEW HISTORICAL CALCS
    adx_val, obv_val, bb_pb_val, vwap_dist_val = 0.0, 0.0, 0.5, 0.0
    signal = 'BUY' if pct_change >= 0 else 'SELL' # Initial guess
    stop_loss, target_price = 0.0, 0.0
    
    if conn:
        try:
            hist_df = fetch_historical_data(symbol, conn)
            if not hist_df.empty and len(hist_df) > 20:
                now_ts = pd.Timestamp.now(tz='UTC')
                live_row = pd.DataFrame([{
                    'timestamp': now_ts,
                    'open': float(stock.get('open_price') or price),
                    'high': float(price), 
                    'low': float(price),
                    'close': float(price),
                    'volume': float(stock.get('volume') or stock.get('rvol') or 0.0)
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
                
                # 5. ADX (14-period)
                up_move = high - high.shift(1)
                down_move = low.shift(1) - low
                plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
                minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
                
                with np.errstate(divide='ignore', invalid='ignore'):
                    plus_di = 100 * (pd.Series(plus_dm).rolling(window=14).mean() / atr_series)
                    minus_di = 100 * (pd.Series(minus_dm).rolling(window=14).mean() / atr_series)
                    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).abs()
                adx_val = dx.rolling(window=14).mean().replace([np.inf, -np.inf], np.nan).fillna(0).iloc[-1]
                
                # 6. OBV 
                delta_val = close.diff()
                obv_val = (np.sign(delta_val) * volume).fillna(0).cumsum().iloc[-1]
                
                # 7. Bollinger Band %B (20-period)
                sma20 = close.rolling(window=20).mean()
                std20 = close.rolling(window=20).std()
                upper_band = sma20 + (2 * std20)
                lower_band = sma20 - (2 * std20)
                with np.errstate(divide='ignore', invalid='ignore'):
                    pb = (close - lower_band) / (upper_band - lower_band)
                bb_pb_val = pb.replace([np.inf, -np.inf], np.nan).fillna(0.5).iloc[-1]
                
                # 8. VWAP Distance (Daily reset)
                df['date'] = df['timestamp'].dt.date
                typical_price = (high + low + close) / 3
                cum_vol = df.groupby('date')['volume'].cumsum()
                temp_df = pd.DataFrame({'tp_vol': typical_price * volume, 'date': df['date']})
                cum_tp_vol = temp_df.groupby('date')['tp_vol'].cumsum()
                vwap = cum_tp_vol / cum_vol
                with np.errstate(divide='ignore', invalid='ignore'):
                    vw_dist = (close - vwap) / vwap
                vwap_dist_val = vw_dist.replace([np.inf, -np.inf], np.nan).fillna(0).iloc[-1]
                
        except Exception as e:
            print(f"⚠️ Feature Calculation Error for {symbol}: {e}")

    # 3. Model Preparation & Inference (Signal might be refined here)

    # 4. Model Preparation & Inference
    try:
        from datetime import datetime

        now = datetime.now()
        day_of_week = now.weekday()
        time_float = now.hour + (now.minute / 60.0)

        # Volatility is strictly extracted, fallback to atr scaling
        computed_volatility = stock.get('volatility')
        if computed_volatility is None or computed_volatility == 0.0:
             computed_volatility = (safe_atr / price) if price > 0 else 0.015

        # STRICT ML FEATURE MAPPING (12 columns from train_engine.py)
        feature_cols = [
            'rvol', 'change_percent', 'cluster_id', 'rsi', 'dist_sma_20', 
            'volatility', 'adx', 'obv', 'bb_pb', 'vwap_dist', 'day_of_week', 'time_float'
        ]
        feature_dict = {
            'rvol': [rvol],
            'change_percent': [pct_change],
            'cluster_id': [float(stock.get('cluster_id') or 0.0)],
            'rsi': [safe_rsi],
            'dist_sma_20': [safe_dist_sma20],
            'volatility': [float(computed_volatility)],
            'adx': [float(adx_val)],
            'obv': [float(obv_val)],
            'bb_pb': [float(bb_pb_val)],
            'vwap_dist': [float(vwap_dist_val)],
            'day_of_week': [float(day_of_week)],
            'time_float': [float(time_float)]
        }
        
        feature_data = pd.DataFrame(feature_dict)[feature_cols]
        
        prob_sniper = float(sniper_model.predict_proba(feature_data)[0][1]) if sniper_model else 0.0
        prob_voyager = float(voyager_model.predict_proba(feature_data)[0][1]) if voyager_model else 0.0
    except Exception as e:
        import traceback
        print(f"❌ Inference Error for {symbol}: {e}")
        traceback.print_exc()
        prob_sniper = 0.0
        prob_voyager = 0.0
        stock['ml_error'] = str(e)
        stock['ml_traceback'] = traceback.format_exc()
        
    probability = round(max(prob_sniper, prob_voyager) * 100, 1)

    # 5. Model Overwrite (Apex V6.0 - 0.54 Raw Threshold)
    ai_mode = None
    is_conviction = False
    
    # SNIPER_THRESHOLD = 0.54, VOYAGER_THRESHOLD = 0.54 (defined as constants)
    if prob_voyager >= VOYAGER_THRESHOLD:
        ai_mode = "VOYAGER (SWING)"
        is_conviction = True
        signal = 'BUY' # V6.0 Apex is a Long-Biased model
    elif prob_sniper >= SNIPER_THRESHOLD:
        ai_mode = "SNIPER (INTRADAY)"
        is_conviction = True
        signal = 'BUY' # V6.0 Apex is a Long-Biased model

    # 5.0 Finalize Base SL/TP based on the final decided signal
    if signal == 'BUY':
        stop_loss = round(price - (safe_atr * 1.5), 2)
        target_price = round(price * 1.02, 2)
    else:
        stop_loss = round(price + (safe_atr * 1.5), 2)
        target_price = round(price * 0.98, 2)

    # 5.1 LIVE APEX POSITION MANAGEMENT (Ported from backtest.py V6.0)
    current_entry_time = stock.get('entry_time')
    current_init_vol = float(stock.get('init_vol_rupees') or 0.0)
    current_sl = float(stock.get('stop_loss') or 0.0)

    if is_conviction:
        # Initialize new position state if not already tracked
        if not current_entry_time or current_init_vol == 0:
            current_entry_time = pd.Timestamp.now(tz='UTC')
            # Use computed_volatility from Step 4
            current_init_vol = price * max(float(computed_volatility), 0.02)
            current_sl = price - current_init_vol if signal == 'BUY' else price + current_init_vol
            
            # Persist the initialization to DB
            if conn:
                try:
                    cur_update = conn.cursor()
                    cur_update.execute("""
                        UPDATE ticker_live 
                        SET entry_time = %s, init_vol_rupees = %s, stop_loss = %s 
                        WHERE symbol = %s
                    """, (current_entry_time, current_init_vol, current_sl, symbol))
                    conn.commit()
                    cur_update.close()
                except Exception as e:
                    print(f"⚠️ Persist Init error for {symbol}: {e}")

        # --- THE 4-STAGE APEX TRAILING HIERARCHY ---
        days_held = (pd.Timestamp.now(tz='UTC') - pd.Timestamp(current_entry_time)).days
        pnl_pct = ((price - float(stock.get('entry_price') or price)) / float(stock.get('entry_price') or price)) * 100
        exit_reason = "TRAILING_EXIT"
        
        # 1. STRUCTURAL BREAK GUARD (SMA 20)
        if safe_dist_sma20 < -0.5 and signal == 'BUY':
             is_conviction = False # Signal exit logic downstream or set SL to price
             current_sl = price 
             exit_reason = "STRUCTURAL_BREAK"
             
        # 2. PEAK CAPTURE (Profit > 25% or RSI > 80)
        elif pnl_pct > 25.0 or safe_rsi > 80.0:
            new_sl = price - (0.4 * current_init_vol)
            if new_sl > current_sl:
                current_sl = new_sl
                exit_reason = "PEAK_CAPTURE"
                
        # 3. VOLATILITY-SCALED MILKING (Profit >= 10%)
        elif pnl_pct >= 10.0:
            new_sl = price - (0.8 * current_init_vol)
            if new_sl > current_sl:
                current_sl = new_sl
                exit_reason = "MILKING_TRAIL"
                
        # 4. STANDARD PHASE
        else:
            new_sl = price - current_init_vol if signal == 'BUY' else price + current_init_vol
            if signal == 'BUY':
                if new_sl > current_sl: current_sl = new_sl
            else:
                if new_sl < current_sl: current_sl = new_sl
        
        # 5. TIME-BASED DE-RISKING (BE after 5 days)
        if days_held >= 5 and pnl_pct > 2.0:
             if signal == 'BUY':
                 current_sl = max(current_sl, float(stock.get('entry_price') or price) * 1.005)
             else:
                 current_sl = min(current_sl, float(stock.get('entry_price') or price) * 0.995)

        # PERSIST TRAILING SL TO DB (Mandatory for server persistence)
        if conn:
            try:
                cur_trail = conn.cursor()
                cur_trail.execute("UPDATE ticker_live SET stop_loss = %s WHERE symbol = %s", (current_sl, symbol))
                conn.commit()
                cur_trail.close()
            except Exception as e:
                print(f"⚠️ Trailing SL Persist Error for {symbol}: {e}")

        stock['stop_loss'] = round(current_sl, 2)
        stop_loss = stock['stop_loss'] # Synchronize local variable
        stock['exit_reason'] = exit_reason
        
        # Consistent Target Price update
        if signal == 'BUY':
            stock['target_price'] = round(price * (1.05 if ai_mode == "VOYAGER (SWING)" else 1.01), 2)
        else:
            stock['target_price'] = round(price * (0.95 if ai_mode == "VOYAGER (SWING)" else 0.99), 2)
        target_price = stock['target_price'] # Synchronize local variable

    # 6. Check for Manual Override via Forge (respect values set by update in execute-forge)
    if stock.get('ai_mode') == 'MANUAL FORGE' or stock.get('is_manual_forge'):
        ai_mode = 'MANUAL FORGE'
        is_conviction = True
        signal = stock.get('ai_signal') or signal
        probability = float(stock.get('confidence') or probability)
        # Priority: If manual values exist in the incoming stock object, use them
        stop_loss = float(stock.get('stop_loss') or stop_loss)
        target_price = float(stock.get('target_price') or target_price)
        # Update dictionaries to sync
        stock['stop_loss'] = stop_loss
        stock['target_price'] = target_price

    # 6.5 Calculate Percentage Risks & Monte Carlo Probabilities
    sl_pct = 0.0
    tp_pct = 0.0
    mc_win_rate = 0.0
    if price > 0:
        sl_pct = round(abs((price - stop_loss) / price * 100), 2)
        tp_pct = round(abs((target_price - price) / price * 100), 2)
        
        # Isolate Heavy Monte Carlo execution to only run if requested
        if run_mc:
            volatility_pct = safe_atr / price
            mc_win_rate = run_monte_carlo(price, target_price, stop_loss, volatility_pct)

    # 7. Provide Unified Object Back to Frontend
    stock.update({
        'price': price,
        'changePercent': pct_change,
        'pct_change': pct_change,
        'is_anomaly': is_anomaly,
        'rvol': rvol,
        'rvol_ratio': rvol,
        'atr': safe_atr,
        'rsi': safe_rsi,
        'dist_sma20': safe_dist_sma20,
        'isConviction': is_conviction,
        'aiMode': ai_mode,
        'ai_mode': ai_mode,
        'newTrigger': False,
        'signal': signal,
        'probability': probability,
        'confidence': probability,
        'ai_confidence': round(max(prob_sniper, prob_voyager), 4), # RAW PROBABILITY FOR FRONTEND
        'stop_loss': stop_loss,
        'stopLoss': stop_loss,
        'target_price': target_price,
        'targetPrice': target_price,
        'sl_pct': sl_pct,
        'tp_pct': tp_pct,
        'mc_win_rate': mc_win_rate
    })
    
    return stock



# --- NEW: FORGE AUDIT ENDPOINT ---
@app.get("/audit/{symbol}")
async def get_audit(symbol: str):
    symbol = urllib.parse.unquote(symbol)
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        SELECT 
            l.symbol, l.price, l.pct_change as live_pct, l.rvol,
            m.atr, m.rsi, m.dist_sma20, m.prev_close
        FROM ticker_live l
        LEFT JOIN ticker_master m ON l.symbol = m.symbol
        WHERE l.symbol = %s
    """
    
    # FIX: Use .upper() instead of .toUpperCase()
    cur.execute(query, (symbol.upper(),))
    stock = cur.fetchone()
    cur.close()
    conn.close()

    if not stock:
        # Debugging: check terminal to see if the symbol actually matched
        print(f"⚠️ Audit Failed: {symbol.upper()} not found in DB.")
        return {"error": f"Symbol {symbol} not found in live feed"}

    # Debugging: check terminal to see if ATR is actually coming from Master
    print(f"✅ Audit Success: {symbol.upper()} - ATR: {stock.get('atr')}, Price: {stock.get('price')}")

    # Request the isolated Monte Carlo compute specifically for the audit view
    conn_hist = get_db_connection()
    try:
        processed_data = apply_conviction_logic(stock, conn=conn_hist, run_mc=True)
    finally:
        conn_hist.close()
    return processed_data
            
@app.post("/execute-forge") 
async def execute_forge(trade: ForgeTrade):
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # We update the ticker_live table to "force" this symbol into a Conviction state.
        # This uses the parameters you manually tuned in the Forge UI.
        query = """
            UPDATE ticker_live 
            SET 
                ai_signal = %s,
                ai_mode = %s,
                confidence = %s,
                price = %s,
                stop_loss = %s,
                target_price = %s,
                is_manual_forge = TRUE
            WHERE symbol = %s
        """
        
        # Note: 'ai_mode' is set to 'FORGE' so you can distinguish it from auto-signals
        cursor.execute(query, (
            trade.signal,
            "MANUAL FORGE",
            trade.probability,
            trade.entryPrice,
            trade.stop_loss,
            trade.target_price,
            trade.symbol
        ))
        
        conn.commit()
        cursor.close()
        print(f"🚀 Forge Execution: {trade.symbol} @ {trade.entryPrice}")
        return {"status": "success", "message": f"Trade for {trade.symbol} live"}
        
    except Exception as e:
        print(f"❌ Execution Error: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if conn:
            conn.close()

# --- ADD TO server.py ---
@app.post("/close-trade/{symbol}")
async def close_trade(symbol: str):
    symbol = urllib.parse.unquote(symbol)
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Reset the conviction fields to 'clear' the trade
        query = """
            UPDATE ticker_live 
            SET 
                ai_signal = NULL,
                ai_mode = NULL,
                confidence = 0,
                stop_loss = NULL,
                target_price = NULL,
                is_manual_forge = FALSE
            WHERE symbol = %s
        """
        cursor.execute(query, (symbol.upper(),))
        conn.commit()
        cursor.close()
        return {"status": "success", "message": f"Closed position for {symbol}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        if conn: conn.close()

@app.post("/api/trades")
async def create_trade(trade: TradePayload):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO active_trades (symbol, entry_price, stop_loss, target, quantity, trade_type)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        cursor.execute(query, (
            trade.symbol.upper(),
            trade.entry_price,
            trade.stop_loss,
            trade.target,
            trade.quantity,
            trade.trade_type
        ))
        trade_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        return {"status": "success", "message": f"Trade {trade_id} persisted", "id": trade_id}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        if conn: conn.close()

@app.get("/api/trades")
async def get_trades():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT * FROM active_trades WHERE status = 'OPEN' ORDER BY timestamp DESC")
        trades = cursor.fetchall()
        cursor.close()
        return trades
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        if conn: conn.close()
        
@app.delete("/api/trades/clear")
async def clear_all_trades():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # 1. Clear persisted trades
        cursor.execute("TRUNCATE TABLE active_trades")
        # 2. Reset convictions in ticker_live
        cursor.execute("""
            UPDATE ticker_live 
            SET 
                ai_mode = NULL, 
                ai_signal = NULL, 
                confidence = 0, 
                is_manual_forge = FALSE,
                init_vol_rupees = 0
        """)
        # Safe reset for entry_time if it exists
        try:
            cursor.execute("UPDATE ticker_live SET entry_time = NULL")
        except:
            pass
            
        conn.commit()
        cursor.close()
        return {"status": "success", "message": "All trades permanently cleared"}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        if conn: conn.close()

@app.delete("/api/trades/{symbol}")
async def delete_individual_trade(symbol: str):
    symbol = urllib.parse.unquote(symbol).upper()
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # 1. Delete from active_trades
        cursor.execute("DELETE FROM active_trades WHERE symbol = %s", (symbol,))
        # 2. Reset specific ticker conviction
        cursor.execute("""
            UPDATE ticker_live 
            SET 
                ai_mode = NULL, 
                ai_signal = NULL, 
                confidence = 0, 
                is_manual_forge = FALSE,
                init_vol_rupees = 0
            WHERE symbol = %s
        """, (symbol,))
        try:
            cursor.execute("UPDATE ticker_live SET entry_time = NULL WHERE symbol = %s", (symbol,))
        except:
            pass
            
        conn.commit()
        cursor.close()
        return {"status": "success", "message": f"Trade for {symbol} permanently deleted"}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        if conn: conn.close()
        
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("🟢 WebSocket Connection Accepted")
    
    conn = None
    try:
        # STEP 2: Open DB connection AFTER the handshake is established
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print("🔗 WS DB Session Opened")
        except Exception as e:
            print(f"🔴 EXPOSED WS DB ERROR: {str(e)}")
            traceback.print_exc() 
            await websocket.send_json({"error": "Database connection failed"})
            return
        
        # STEP 3: Enter the data loop
        while True:
            try:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                try:
                    cursor.execute("""
                        SELECT 
                            l.symbol, l.price, l.rvol, l.pct_change as live_pct,
                            l.stop_loss, l.target_price, l.confidence, l.ai_mode, l.ai_signal,
                            m.prev_close, m.rsi, m.dist_sma20, m.atr, m.dist_52wh, m.sector
                        FROM ticker_live l
                        LEFT JOIN ticker_master m ON l.symbol = m.symbol
                        ORDER BY 
                            CASE 
                                WHEN l.symbol = 'NIFTY_50' THEN 0
                                WHEN l.symbol = 'SENSEX' THEN 1
                                WHEN l.symbol = 'NIFTY_BANK' THEN 2
                                ELSE 3
                            END
                        LIMIT 250
                    """)
                    raw_stocks = cursor.fetchall()
                    
                    # --- PORTFOLIO CAPACITY CALCULATION ---
                    cursor.execute("SELECT COUNT(*) as open_count FROM active_trades WHERE status = 'OPEN'")
                    cap_row = cursor.fetchone()
                    open_count = cap_row['open_count'] if cap_row else 0
                finally:
                    cursor.close()
                
                total_equity = INITIAL_CAPITAL
                max_positions = int(total_equity / FIXED_TRADE_ALLOCATION)
                capacity_full = (open_count >= max_positions)
                
                if raw_stocks:
                    processed_stocks = []
                    for stock_row in raw_stocks:
                        stock_dict = dict(stock_row)
                        
                        raw_db_key = stock_dict.get('symbol')
                        mapped_symbol = KEY_TO_SYMBOL.get(raw_db_key, raw_db_key)
                        stock_dict['symbol'] = mapped_symbol
                        
                        processed_stock = apply_conviction_logic(stock_dict, conn=conn)
                        
                        if processed_stock.get('isConviction') and capacity_full:
                            processed_stock['isConviction'] = False
                            processed_stock['ai_mode'] = f"{processed_stock['ai_mode']} (SKIPPED: FULL)"
                            processed_stock['capacity_full'] = True
                        else:
                            processed_stock['capacity_full'] = capacity_full

                        processed_stocks.append(processed_stock)

                    try:
                        await websocket.send_text(json.dumps(processed_stocks, cls=DecimalEncoder))
                    except (WebSocketDisconnect, RuntimeError):
                        print("ℹ️ Client disconnected during send. Ending loop.")
                        break
                
                await asyncio.sleep(1) 

            except (WebSocketDisconnect, RuntimeError):
                print("ℹ️ Client disconnected. Ending loop.")
                break
            except Exception as loop_err:
                import traceback
                print(f"❌ Internal Loop Error: {loop_err}")
                traceback.print_exc()
                if conn.closed:
                    print("💀 DB Connection lost during loop. Breaking.")
                    break
                await asyncio.sleep(2)
            
    except WebSocketDisconnect:
        print("ℹ️ WebSocket Connection Closed by Client.")
    except Exception as e:
        print(f"🔴 WebSocket Error: {e}")
    finally:
        print("🔌 WebSocket Closed")
        if conn and not conn.closed:
            conn.close()
            print("🔌 DB Connection Closed for WebSocket session")

if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
