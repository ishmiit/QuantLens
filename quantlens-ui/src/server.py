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
import xgboost as xgb
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

# --- IN-MEMORY PASSTHROUGH STATE ---
active_connections: set = set()   # Connected WebSocket clients
in_memory_ticks: list = []         # Latest processed tick data, broadcast to all clients
AVG_VOL_CACHE: dict = {}           # Avg 20d volume per symbol (loaded from ticker_master on startup)
AI_FEATURE_CACHE: dict = {}        # RSI / volatility / SMA dist per symbol (from ticker_master)

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
async def startup_event():
    print("🟢 ACTIVE ROUTES:")
    for route in app.routes:
        print(f" - {route.path} ({route.name})")
    print("🟢 SERVER READY")

    # --- PHASE 1: DATABASE SCHEMA ---
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
        # legacy ALTER code removed to strictly preserve schema
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ticker_history (
                id SERIAL PRIMARY KEY,
                symbol TEXT,
                price DECIMAL,
                ai_signal TEXT,
                confidence DECIMAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ticker_history_symbol ON ticker_history(symbol)")
        conn.commit()
        cur.close()
        print("✅ DB Schema Verified & Hardened.")
    except psycopg2.Error as e:
        print(f"🔴 CRITICAL DB ERROR: {str(e)}")
    except Exception as e:
        print(f"🔴 UNKNOWN DB ERROR: {str(e)}")
    finally:
        if conn:
            conn.close()

    # --- PHASE 2: LAUNCH IN-MEMORY LIVE FEED ---
    # The feed task itself will download the instrument CSV on first boot.
    # Historical baseline is pre-seeded by sync_history.py via GitHub Actions CRON.
    print("🚀 Startup complete. Launching background tasks...")
    asyncio.create_task(data_janitor_loop())
    asyncio.create_task(upstox_live_feed())

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
last_history_write = {}

async def data_janitor_loop():
    while True:
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ticker_history WHERE timestamp < NOW() - INTERVAL '24 hours';")
            conn.commit()
            cursor.close()
            print("🧹 [Janitor] Purged historical data older than 24 hours.")
        except Exception as e:
            print(f"❌ [Janitor Error]: {e}")
        finally:
            if conn:
                conn.close()
        await asyncio.sleep(3600)  # Sleep for 1 hour

# --- LOAD MODELS (native XGBoost JSON — no pickle, no joblib) ---
sniper_model  = None
voyager_model = None

import os

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR  = os.path.join(BASE_DIR, "..", "..", "ml_engine")

sniper_path  = os.path.join(MODEL_DIR, "model_intraday.json")
voyager_path = os.path.join(MODEL_DIR, "model_swing.json")

try:
    if os.path.exists(sniper_path):
        sniper_model = xgb.Booster()
        sniper_model.load_model(sniper_path)
        print("🎯 Sniper Model Loaded (Intraday XGBoost JSON)")
    else:
        print(f"⚠️ Sniper model not found at: {sniper_path}")

    if os.path.exists(voyager_path):
        voyager_model = xgb.Booster()
        voyager_model.load_model(voyager_path)
        print("🛢️ Voyager Model Loaded (Swing XGBoost JSON)")
    else:
        print(f"⚠️ Voyager model not found at: {voyager_path}")
except Exception as e:
    print(f"❌ Error loading XGBoost models: {e}")


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_instrument_key(symbol):
    symbol = symbol.split(':')[-1].upper().strip()
    
    # Force strict Upstox index formatting
    if symbol in ['NIFTY 50', 'NIFTY_50', 'NIFTY50']:
        instrument_key = 'NSE_INDEX|Nifty 50'
    elif symbol in ['BANK NIFTY', 'NIFTY BANK', 'NIFTY_BANK']:
        instrument_key = 'NSE_INDEX|Nifty Bank'
    elif symbol == 'SENSEX':
        instrument_key = 'BSE_INDEX|SENSEX'
    else:
        if symbol == "LTM":
            instrument_key = INSTRUMENT_MAP.get("LTM") or INSTRUMENT_MAP.get("LTIM")
        elif symbol == "LTIM":
            instrument_key = INSTRUMENT_MAP.get("LTIM") or INSTRUMENT_MAP.get("LTM")
        else:
            instrument_key = INSTRUMENT_MAP.get(symbol)
            
    return instrument_key

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
        # The ISIN Rosetta Fix: Query by clean symbol, ISIN, and legacy format
        db_key = get_instrument_key(clean_symbol) if "|" not in clean_symbol else clean_symbol
        legacy_key = f"NSE_EQ|{clean_symbol}-EQ"
        
        if clean_symbol.upper() in ["LTM", "LTIM"]:
            query = "SELECT timestamp, open, high, low, close, volume FROM ml_training_data_v3 WHERE symbol IN (%s, %s, %s, 'LTIM', 'NSE_EQ|LTIM', 'LTM', 'NSE_EQ|LTM') ORDER BY timestamp DESC LIMIT 50"
            cur.execute(query, (clean_symbol, db_key, legacy_key))
        else:
            query = "SELECT timestamp, open, high, low, close, volume FROM ml_training_data_v3 WHERE symbol IN (%s, %s, %s) ORDER BY timestamp DESC LIMIT 50"
            cur.execute(query, (clean_symbol, db_key, legacy_key))
            
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

import pandas as pd
import xgboost as xgb
from datetime import datetime
import pytz

EXPECTED_FEATURES = ['price', 'rsi', 'volatility', 'dist_sma_20', 'rvol', 'change_percent', 
                     'cluster_id', 'adx', 'obv', 'bb_pb', 'vwap_dist', 'day_of_week', 
                     'hour', 'minute', 'time_float']

def get_live_prediction(model, current_price, cached_data, symbol="Unknown"):
    """
    Permissive inference pipeline.
    Uses default baseline values if technical data is still syncing to prevent total system abort.
    """
    if not cached_data:
        print(f"⏳ [{symbol}] AI Waiting: Indicator cache is completely empty. Forge hasn't finished yet.")
        cached_data = {}
        
    try:
        ist = datetime.now(pytz.timezone('Asia/Kolkata'))
        
        # Permissive extraction - mirrors local setup
        safe_data = {
            'price': float(current_price),
            'rsi': float(cached_data.get('rsi', 50.0)) if cached_data else 50.0,
            'volatility': float(cached_data.get('volatility', 0.01)) if cached_data else 0.01,
            'dist_sma_20': float(cached_data.get('dist_sma_20', 0.0)) if cached_data else 0.0,
            'rvol': float(cached_data.get('rvol', 1.0)) if cached_data else 1.0,
            'change_percent': float(cached_data.get('change_percent', 0.0)) if cached_data else 0.0,
            'cluster_id': int(cached_data.get('cluster_id', 0)) if cached_data else 0,
            'adx': float(cached_data.get('adx', 20.0)) if cached_data else 20.0,
            'obv': float(cached_data.get('obv', 0.0)) if cached_data else 0.0,
            'bb_pb': float(cached_data.get('bb_pb', 0.5)) if cached_data else 0.5,
            'vwap_dist': float(cached_data.get('vwap_dist', 0.0)) if cached_data else 0.0,
            'day_of_week': int(ist.weekday()),
            'hour': int(ist.hour),
            'minute': int(ist.minute),
            'time_float': float(ist.hour + ist.minute / 60.0)
        }
            
        # Convert to DataFrame to guarantee perfect XGBoost column alignment.
        df = pd.DataFrame([safe_data], columns=EXPECTED_FEATURES)
        
        # Execute Inference
        dmatrix = xgb.DMatrix(df)
        prob = float(model.predict(dmatrix)[0])
        
        return prob
        
    except Exception as e:
        print(f"❌ [INFERENCE CRASH - {symbol}]: {str(e)}")
        return 0.0

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
        
        prev_close_raw = stock.get('prev_close')
        prev_close = float(prev_close_raw) if prev_close_raw is not None else 0.0

        # 🌙 WEEKEND / OFF-HOURS FALLBACK
        if price <= 0 and prev_close > 0:
            price = prev_close
        
        # BAD DATA GUARD: Prevents ZeroDivisionError if both are 0
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
        # Volatility fallback calculation
        computed_volatility = stock.get('volatility')
        if computed_volatility is None or computed_volatility == 0.0:
            computed_volatility = (safe_atr / price) if price > 0 else 0.015

        # Populate the dictionary so our Hard Gate accepts it natively
        stock['rsi'] = safe_rsi
        stock['volatility'] = computed_volatility
        stock['dist_sma_20'] = safe_dist_sma20
        stock['rvol'] = rvol
        stock['change_percent'] = pct_change
        stock['adx'] = adx_val
        stock['obv'] = obv_val
        stock['bb_pb'] = bb_pb_val
        stock['vwap_dist'] = vwap_dist_val

        prob_sniper = get_live_prediction(sniper_model, price, stock, symbol)
        prob_voyager = get_live_prediction(voyager_model, price, stock, symbol)
        
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

    # 5.0 Finalize Base SL/TP based on the final decided signal.
    # Target must always exceed SL distance by the minimum ratio —
    # ATR*1.5 can be >2%, so we anchor TP at ATR*3.0 (2:1 ratio).
    if signal == 'BUY':
        sl_distance = safe_atr * 1.5
        stop_loss    = round(price - sl_distance, 2)
        target_price = round(price + (sl_distance * 2.0), 2)   # minimum 2:1 RR
    else:
        sl_distance  = safe_atr * 1.5
        stop_loss    = round(price + sl_distance, 2)
        target_price = round(price - (sl_distance * 2.0), 2)   # minimum 2:1 RR

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
            
            # Persist logic removed (schema locked)

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

        # PERSIST TRAILING SL TO DB removed (schema locked)

        stock['stop_loss'] = round(current_sl, 2)
        stop_loss = stock['stop_loss'] # Synchronize local variable
        stock['exit_reason'] = exit_reason
        
        # Target Price: VOYAGER 5%, SNIPER 1%.
        # These are always relative to the CONVICTION entry, so they should
        # always beat the trailing SL distance (which starts at ~2% volatility).
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

    # 6.4 HARD RISK:REWARD GUARD — enforce minimum 1:2 ratio across ALL code paths.
    # If the SL distance is >= the TP distance, the setup is invalid.
    # Push the target price out until RR >= 2:1.
    if price > 0 and stop_loss != 0 and target_price != 0:
        sl_dist = abs(price - stop_loss)
        tp_dist = abs(target_price - price)
        if tp_dist < sl_dist * 2.0:
            min_tp_dist = sl_dist * 2.0
            if signal == 'BUY':
                target_price = round(price + min_tp_dist, 2)
            else:
                target_price = round(price - min_tp_dist, 2)
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



# --- FORGE AUDIT ENDPOINT ---
@app.get("/audit/{symbol}")
async def get_audit(symbol: str):
    clean_symbol = urllib.parse.unquote(symbol).upper()
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. Fetch chart history
        cur.execute("""
            SELECT timestamp, price, ai_signal, confidence 
            FROM ticker_history 
            WHERE symbol = %s 
            ORDER BY timestamp ASC
        """, (clean_symbol,))
        rows = cur.fetchall()

        chart_data = []
        for r in rows:
            chart_data.append({
                "timestamp": str(r['timestamp']),
                "close": float(r['price']),
                "ai_signal": str(r['ai_signal']),
                "confidence": float(r['confidence'])
            })

        # 2. Fetch the live snapshot from ticker_live (kept for reference; main lookup is below)
        cur.execute("""
            SELECT price, pct_change, rvol, ai_signal, confidence
            FROM ticker_live
            WHERE symbol = %s LIMIT 1
        """, (clean_symbol,))
        live_row = cur.fetchone()
        cur.close()

        # 3. Also check the in-memory feed as the most up-to-date source.
        # The Upstox SDK stores symbols in colon format: "NSE_EQ:ASHOKLEY"
        # so we must match flexibly against all known variants.
        live_price = 0.0
        prev_close = 0.0
        pct_change = 0.0
        rvol = 1.0

        search_variants = {
            clean_symbol,
            f"NSE_EQ:{clean_symbol}",
            f"NSE_EQ:{clean_symbol}-EQ",
            f"{clean_symbol}-EQ",
        }

        # Priority 1: in-memory ticks (freshest) — check all symbol variants
        for tick in in_memory_ticks:
            tick_sym = tick.get('symbol', '').upper()
            # Also normalise: split on colon/pipe and strip -EQ to get bare symbol
            tick_bare = tick_sym.split(':')[-1].split('|')[-1].replace('-EQ', '')
            if tick_sym in search_variants or tick_bare == clean_symbol:
                live_price = float(tick.get('price') or 0.0)
                prev_close = float(tick.get('prev_close') or 0.0)
                pct_change = float(tick.get('pct_change') or 0.0)
                rvol       = float(tick.get('rvol') or 1.0)
                break

        # Priority 2: ticker_live DB — query all known symbol variants
        if live_price <= 0:
            cur2 = conn.cursor(cursor_factory=RealDictCursor)
            cur2.execute("""
                SELECT price, pct_change, rvol FROM ticker_live
                WHERE symbol = ANY(%s) LIMIT 1
            """, (list(search_variants),))
            live_row2 = cur2.fetchone()
            cur2.close()
            if live_row2:
                live_price = float(live_row2['price'] or 0.0)
                pct_change = float(live_row2['pct_change'] or 0.0)
                rvol       = float(live_row2['rvol'] or 1.0)

        # Priority 3: last known price from ticker_history (already fetched above)
        if live_price <= 0 and chart_data:
            live_price = chart_data[-1]['close']

        # 4. Grab ML inference details from the in-memory tick if available
        # This guarantees the Forge audit returns EXACTLY the same prediction
        # as the live convictions dashboard, rather than recalculating it with
        # a new time delta.
        probability = 0.0
        signal = 'BUY'
        sl_pct = 2.0
        tp_pct = 5.0
        stop_loss = 0.0
        target_price = 0.0
        mc_win_rate = 0.0
        aiMode = None
        isConviction = False

        found_tick = None
        for tick in in_memory_ticks:
            tick_sym = tick.get('symbol', '').upper()
            tick_bare = tick_sym.split(':')[-1].split('|')[-1].replace('-EQ', '')
            if tick_sym in search_variants or tick_bare == clean_symbol:
                found_tick = tick
                break

        if found_tick:
            probability = float(found_tick.get('probability', 0.0))
            signal = found_tick.get('signal', 'BUY')
            sl_pct = float(found_tick.get('sl_pct', 2.0))
            tp_pct = float(found_tick.get('tp_pct', 5.0))
            stop_loss = float(found_tick.get('stop_loss', 0.0))
            target_price = float(found_tick.get('target_price', 0.0))
            mc_win_rate = float(found_tick.get('mc_win_rate', 0.0))
            aiMode = found_tick.get('aiMode')
            isConviction = found_tick.get('isConviction', False)
        else:
            # Fallback to DB if live feed hasn't caught it yet
            cur3 = conn.cursor(cursor_factory=RealDictCursor)
            cur3.execute("""
                SELECT confidence, ai_signal FROM ticker_live
                WHERE symbol = ANY(%s) LIMIT 1
            """, (list(search_variants),))
            live_row3 = cur3.fetchone()
            cur3.close()
            if live_row3:
                probability = float(live_row3['confidence'] or 0.0)
                db_sig = str(live_row3['ai_signal'] or '')
                if 'BUY' in db_sig: signal = 'BUY'
                elif 'SELL' in db_sig: signal = 'SELL'

        print(f"🔍 [Audit] {clean_symbol} | live_price={live_price} | probability={probability}")

        return {
            "symbol": clean_symbol,
            "data": chart_data,
            # ─── Fields the Forge panel reads directly ───────────────
            "price":       round(live_price, 2),
            "probability": round(probability, 1),
            "confidence":  round(probability, 1),
            "signal":      signal,
            "sl_pct":      round(sl_pct, 2),
            "tp_pct":      round(tp_pct, 2),
            "stop_loss":   round(stop_loss, 2),
            "target_price":round(target_price, 2),
            "mc_win_rate": round(mc_win_rate, 1),
            "aiMode":      aiMode,
            "isConviction":isConviction,
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"symbol": clean_symbol, "data": [], "error": f"Failed to fetch audit: {str(e)}"}
    finally:
        if conn:
            conn.close()
            
@app.post("/execute-forge") 
async def execute_forge(trade: ForgeTrade):
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Legacy ticker_live forge logic removed due to schema lock
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
        
        # Legacy ticker_live conviction reset removed due to schema lock
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
        # Legacy ticker_live reset removed due to schema lock
            
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
        # Legacy specific conviction reset removed due to schema lock
            
        conn.commit()
        cursor.close()
        return {"status": "success", "message": f"Trade for {symbol} permanently deleted"}
    except Exception as e:
        if conn: conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        if conn: conn.close()
        
# ============================================================
# IN-MEMORY LIVE FEED BACKGROUND TASK
# Fetches live quotes from Upstox, applies conviction logic,
# and broadcasts directly to all connected WebSocket clients.
# Historical baseline data is read from ticker_master (pre-seeded
# by sync_history.py via the GitHub Actions CRON job).
# ============================================================
async def upstox_live_feed():
    """Background task: poll Upstox REST quotes and broadcast in-memory to all WebSocket clients."""
    import upstox_client

    global in_memory_ticks, AVG_VOL_CACHE, AI_FEATURE_CACHE, active_connections

    print("🚀 [LiveFeed] Starting in-memory Upstox live feed...")

    # --- Step 1: Load token from Neon DB ---
    try:
        conn_init = get_db_connection()
        cur = conn_init.cursor()
        cur.execute("SELECT value FROM system_config WHERE key = 'UPSTOX_TOKEN'")
        row = cur.fetchone()
        if not row:
            print("🔴 [LiveFeed] UPSTOX_TOKEN missing from system_config. Aborting feed.")
            cur.close()
            conn_init.close()
            return
        access_token = row[0]

        # --- Step 2: Pre-load avg volume & AI feature caches from ticker_master ---
        print("📊 [LiveFeed] Loading avg-volume & AI feature caches from ticker_master...")
        cur.execute("SELECT symbol, avg_vol_20d, rsi, volatility, dist_sma20, cluster_id FROM ticker_master")
        for r in cur.fetchall():
            sym = r[0]
            AVG_VOL_CACHE[sym] = float(r[1]) if r[1] else 1.0
            AI_FEATURE_CACHE[sym] = {
                'rsi':         float(r[2]) if r[2] is not None else 50.0,
                'volatility':  float(r[3]) if r[3] is not None else 0.015,
                'dist_sma_20': float(r[4]) if r[4] is not None else 0.0,
                'cluster_id':  int(r[5])   if r[5] is not None else 0,
            }
        cur.close()
        conn_init.close()
        print(f"✅ [LiveFeed] Caches loaded: {len(AVG_VOL_CACHE)} symbols.")
    except Exception as e:
        print(f"❌ [LiveFeed] Initialization error: {e}")
        return

# ---------------------------------------------------------------------------
# Standalone sync helper — runs in a thread pool so the event loop never blocks.
# Returns the raw bytes of the gzip-compressed CSV from Upstox.
# ---------------------------------------------------------------------------
def _fetch_master_csv() -> bytes:
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
    resp.raise_for_status()
    return resp.content


async def upstox_live_feed():
    """Background task: poll Upstox REST quotes and broadcast in-memory to all WebSocket clients."""
    import upstox_client

    global in_memory_ticks, AVG_VOL_CACHE, AI_FEATURE_CACHE, active_connections

    print("🚀 [LiveFeed] Starting in-memory Upstox live feed...")

    # --- Step 1: Load token from Neon DB ---
    try:
        conn_init = get_db_connection()
        cur = conn_init.cursor()
        cur.execute("SELECT value FROM system_config WHERE key = 'UPSTOX_TOKEN'")
        row = cur.fetchone()
        if not row:
            print("🔴 [LiveFeed] UPSTOX_TOKEN missing from system_config. Aborting feed.")
            cur.close()
            conn_init.close()
            return
        access_token = row[0]

        # --- Step 2: Pre-load avg volume & AI feature caches from ticker_master ---
        print("📊 [LiveFeed] Loading avg-volume & AI feature caches from ticker_master...")
        cur.execute("SELECT symbol, avg_vol_20d, rsi, volatility, dist_sma20, cluster_id FROM ticker_master")
        for r in cur.fetchall():
            sym = r[0]
            AVG_VOL_CACHE[sym] = float(r[1]) if r[1] else 1.0
            AI_FEATURE_CACHE[sym] = {
                'rsi':         float(r[2]) if r[2] is not None else 50.0,
                'volatility':  float(r[3]) if r[3] is not None else 0.015,
                'dist_sma_20': float(r[4]) if r[4] is not None else 0.0,
                'cluster_id':  int(r[5])   if r[5] is not None else 0,
            }
        cur.close()
        conn_init.close()
        print(f"✅ [LiveFeed] Caches loaded: {len(AVG_VOL_CACHE)} symbols.")
    except Exception as e:
        print(f"❌ [LiveFeed] Initialization error: {e}")
        return

    # --- Step 3: Download & parse the Upstox master instrument list (async) ---
    # _fetch_master_csv() is a blocking I/O call; asyncio.to_thread() runs it in a
    # thread pool so the event loop is never blocked waiting on the network.
    print("📡 [LiveFeed] Downloading Upstox master instrument list...")
    try:
        raw_csv_bytes = await asyncio.to_thread(_fetch_master_csv)
        print("✅ [LiveFeed] Master list downloaded successfully.")
        csv_content = zlib.decompress(raw_csv_bytes, zlib.MAX_WBITS | 16)
        master_df   = pd.read_csv(io.BytesIO(csv_content))

        for _, row in master_df.iterrows():
            t_symbol = str(row['tradingsymbol']).strip().upper()
            i_key    = str(row['instrument_key']).strip()
            INSTRUMENT_MAP[t_symbol] = i_key
            KEY_TO_SYMBOL[i_key]     = t_symbol
            if '-EQ' in t_symbol:
                clean_sym = t_symbol.replace('-EQ', '')
                INSTRUMENT_MAP[clean_sym] = i_key
                if clean_sym == 'LTM':
                    INSTRUMENT_MAP['LTIM'] = i_key   # corporate action alias

        # Index aliases (Upstox uses space-formatted names in the CSV)
        for alias, canonical in [
            ('NIFTY 50',  'NIFTY_50'),
            ('NIFTY50',   'NIFTY_50'),
            ('NIFTY BANK','NIFTY_BANK'),
            ('NIFTYBANK', 'NIFTY_BANK'),
            ('SENSEX',    'SENSEX'),
        ]:
            if alias in INSTRUMENT_MAP:
                INSTRUMENT_MAP[canonical] = INSTRUMENT_MAP[alias]

        print(f"✅ [LiveFeed] Instrument map built: {len(INSTRUMENT_MAP)} entries, "
              f"{len(KEY_TO_SYMBOL)} reverse entries.")
    except Exception as csv_e:
        print(f"❌ [LiveFeed] Failed to download instrument CSV: {csv_e}. "
              "Key resolution will be limited — retrying on next server restart.")
        # Do NOT return here; the feed can still serve the frontier with partial data.

    # --- Step 4: Build Upstox API client ---
    configuration = upstox_client.Configuration()
    configuration.access_token = access_token
    api_client = upstox_client.ApiClient(configuration)
    market_api = upstox_client.MarketQuoteApi(api_client)

    # --- Step 5: Resolve instrument keys — multi-segment aware ---
    # RAW_SYMBOLS look like "NSE_EQ|HDFCBANK", "NSE_INDEX|Nifty%50", "BSE_INDEX|SENSEX".
    # We need to look up by the FULL instrument_key format used by Upstox: segment|tradingsymbol
    #   NSE_EQ   → key is stored exactly as  "NSE_EQ|HDFCBANK-EQ"  in CSV
    #   NSE_INDEX → key is "NSE_INDEX|Nifty 50", "NSE_INDEX|Nifty Bank"
    #   BSE_INDEX → key is "BSE_INDEX|SENSEX"
    valid_keys       = []
    unmapped_symbols = []
    seen_keys        = set()   # deduplication guard

    for raw_sym in RAW_SYMBOLS:
        segment, right_side = raw_sym.split('|', 1)    # e.g. "NSE_EQ", "HDFCBANK"
        # Decode URL-encoded index names
        right_side = right_side.replace('%50', ' 50').replace('%20', ' ').strip()

        key = None

        # Hardcode explicit overrides to absolutely guarantee pipes instead of colons or other malformations
        OVERRIDE_MAP = {
            "NIFTY 50": "NSE_INDEX|Nifty 50",
            "NIFTY50": "NSE_INDEX|Nifty 50",
            "NIFTY_50": "NSE_INDEX|Nifty 50",
            "BANK NIFTY": "NSE_INDEX|Nifty Bank",
            "NIFTY BANK": "NSE_INDEX|Nifty Bank",
            "NIFTY_BANK": "NSE_INDEX|Nifty Bank",
            "SENSEX": "BSE_INDEX|SENSEX"
        }
        
        if right_side.upper() in OVERRIDE_MAP:
            key = OVERRIDE_MAP[right_side.upper()]
        elif segment == 'NSE_EQ':
            # Upstox NSE equity keys end in "-EQ"  e.g. "NSE_EQ|HDFCBANK-EQ"
            # INSTRUMENT_MAP has both "HDFCBANK" and "HDFCBANK-EQ" entries (we build both)
            key = INSTRUMENT_MAP.get(right_side.upper())
        elif segment == 'NSE_INDEX':
            # Normalise: "Nifty 50" → "NSE_INDEX|Nifty 50"
            key = f"NSE_INDEX|{right_side}"    # literal key used by Upstox API
        elif segment == 'BSE_INDEX':
            key = f"BSE_INDEX|{right_side}"    # e.g. "BSE_INDEX|SENSEX"

        if key and key not in seen_keys:
            valid_keys.append(key)
            seen_keys.add(key)
        else:
            unmapped_symbols.append(f"{segment}|{right_side}")

    # Hardcode the three benchmark indices as guaranteed fallbacks so they
    # are ALWAYS in the feed even if the dynamic lookup produces nothing.
    GUARANTEED_INDICES = [
        "NSE_INDEX|Nifty 50",
        "NSE_INDEX|Nifty Bank",
        "BSE_INDEX|SENSEX",
    ]
    for idx_key in GUARANTEED_INDICES:
        if idx_key not in seen_keys:
            valid_keys.insert(0, idx_key)
            seen_keys.add(idx_key)

    print(f"🎯 [LiveFeed] Successfully resolved {len(valid_keys)} instrument keys.")
    if unmapped_symbols:
        print(f"⚠️  [LiveFeed] Failed to map {len(unmapped_symbols)} symbols. "
              f"Examples: {unmapped_symbols[:10]}")

    BATCH_SIZE = 50
    SLEEP_BETWEEN_BATCHES = 0.5   # seconds — respects Upstox rate limits
    SLEEP_BETWEEN_CYCLES  = 1.0   # seconds — full-cycle cadence

    # --- Step 5: Continuous fetch → process → broadcast loop ---
    while True:
        try:
            from datetime import datetime
            now = datetime.now()
            market_open  = now.replace(hour=9, minute=15, second=0, microsecond=0)
            mins_elapsed = max(1.0, (now - market_open).total_seconds() / 60.0)
            day_fraction = min(1.0, mins_elapsed / 375.0) if now > market_open else 0.01
            day_of_week  = now.weekday()
            time_float   = now.hour + now.minute / 60.0

            all_processed = []
            conn_hist = get_db_connection()

            for i in range(0, len(valid_keys), BATCH_SIZE):
                chunk = valid_keys[i : i + BATCH_SIZE]
                instruments_string = ",".join(c.replace("&", "%26") for c in chunk)

                try:
                    api_response = await asyncio.to_thread(
                        market_api.get_full_market_quote, instruments_string, '2.0'
                    )
                    if not api_response or not api_response.data:
                        continue

                    for ikey, details in api_response.data.items():
                        # Resolve human symbol from KEY_TO_SYMBOL Rosetta Stone
                        t_sym = getattr(details, 'trading_symbol', None)
                        raw_sym = str(t_sym).strip().upper() if t_sym else ikey
                        
                        reverse_map = {
                            "NSE_INDEX|Nifty 50": "NIFTY 50", 
                            "NSE_INDEX|Nifty Bank": "BANK NIFTY", 
                            "BSE_INDEX|SENSEX": "SENSEX"
                        }
                        if ikey in reverse_map:
                            db_symbol = reverse_map[ikey]
                        else:
                            db_symbol = KEY_TO_SYMBOL.get(ikey, raw_sym).replace("-EQ", "")

                        price      = float(getattr(details, 'last_price', 0.0) or 0.0)
                        prev_close = float(getattr(details, 'last_close', 0.0) or 0.0)
                        open_price = 0.0
                        if hasattr(details, 'ohlc') and details.ohlc:
                            open_price = float(getattr(details.ohlc, 'open', 0.0) or 0.0)

                        change_pct = 0.0
                        if open_price > 0:
                            change_pct = (price - open_price) / open_price * 100
                        elif prev_close > 0:
                            change_pct = (price - prev_close) / prev_close * 100

                        current_vol = float(getattr(details, 'volume', 0.0) or 0.0)
                        avg_vol     = AVG_VOL_CACHE.get(db_symbol, 0.0)
                        # RVOL sanity guard: default 1.0 if avg_vol is zero/missing;
                        # cap at 100x to prevent UI overflow from partial-day data.
                        if avg_vol and avg_vol > 0 and day_fraction > 0:
                            raw_rvol = current_vol / (avg_vol * day_fraction)
                            rvol     = round(min(raw_rvol, 100.0), 2)
                        else:
                            rvol = 1.0   # neutral default when no baseline exists

                        features = AI_FEATURE_CACHE.get(db_symbol, {
                            'rsi': 50.0, 'volatility': 0.015, 'dist_sma_20': 0.0, 'cluster_id': 0
                        })

                        stock_dict = {
                            "symbol":      db_symbol,
                            "price":       round(price, 2),
                            "open_price":  round(open_price, 2),
                            "prev_close":  round(prev_close, 2),
                            "pct_change":  round(change_pct, 2),
                            "live_pct":    round(change_pct, 2),
                            "volume":      current_vol,
                            "rvol":        round(rvol, 2),
                            "rsi":         features['rsi'],
                            "volatility":  features['volatility'],
                            "dist_sma20":  features['dist_sma_20'],
                            "cluster_id":  features['cluster_id'],
                            "atr":         features.get('atr', price * 0.015),
                        }

                        processed = apply_conviction_logic(stock_dict, conn=conn_hist)
                        
                        # --- Re-enable DB writes for audit ---
                        # 1. Extract values safely matching the ticker_live schema
                        db_price = float(price)
                        db_pct_change = float(processed.get('pct_change', 0.0)) if processed else 0.0
                        db_rvol = float(processed.get('rvol', 1.0)) if processed else 1.0
                        
                        # Determine AI Signal based on your probability (adjust thresholds as needed)
                        db_confidence = float(processed.get('probability', 0.0)) if processed else 0.0
                        if db_confidence > 0.65:
                            db_ai_signal = 'SNIPER_BUY'
                        elif db_confidence < 0.35:
                            db_ai_signal = 'VOYAGER_SELL'
                        else:
                            db_ai_signal = 'NEUTRAL'
                        
                        # 2. Execute exact schema Upsert
                        try:
                            cursor = conn_hist.cursor()
                            cursor.execute("""
                                INSERT INTO ticker_live (symbol, price, pct_change, rvol, ai_signal, confidence)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (symbol) DO UPDATE SET
                                    price = EXCLUDED.price,
                                    pct_change = EXCLUDED.pct_change,
                                    rvol = EXCLUDED.rvol,
                                    ai_signal = EXCLUDED.ai_signal,
                                    confidence = EXCLUDED.confidence,
                                    timestamp = CURRENT_TIMESTAMP;
                            """, (db_symbol, db_price, db_pct_change, db_rvol, db_ai_signal, db_confidence))
                            conn_hist.commit()
                            cursor.close()
                        except Exception as db_err:
                            conn_hist.rollback()
                            print(f"❌ [DB Error] Failed to upsert {db_symbol}: {db_err}")
                            
                        # --- Task 2: Throttled Insert into History ---
                        import time
                        current_time = time.time()
                        last_write = last_history_write.get(db_symbol, 0)
                        
                        if current_time - last_write > 60:
                            try:
                                cursor = conn_hist.cursor()
                                cursor.execute("""
                                    INSERT INTO ticker_history (symbol, price, ai_signal, confidence)
                                    VALUES (%s, %s, %s, %s);
                                """, (db_symbol, db_price, db_ai_signal, db_confidence))
                                conn_hist.commit()
                                cursor.close()
                                last_history_write[db_symbol] = current_time
                            except Exception as e:
                                conn_hist.rollback()
                                
                        all_processed.append(processed)

                except upstox_client.rest.ApiException as api_e:
                    if api_e.status == 401:
                        print("🚫 [LiveFeed] 401 Unauthorized — token expired. Feed pausing 60s.")
                        await asyncio.sleep(60)
                    else:
                        print(f"⚠️ [LiveFeed] API error on batch {i}: {api_e.status}")
                except Exception as batch_e:
                    print(f"⚠️ [LiveFeed] Batch error: {batch_e}")

                await asyncio.sleep(SLEEP_BETWEEN_BATCHES)

            conn_hist.close()

            # Sort: Indices first, equities by symbol
            def _sort_key(s):
                sym = s.get('symbol', '')
                if sym == 'NIFTY 50':   return (0, sym)
                if sym == 'SENSEX':     return (1, sym)
                if sym == 'BANK NIFTY': return (2, sym)
                return (3, sym)

            all_processed.sort(key=_sort_key)
            in_memory_ticks = all_processed  # atomic assignment

            # Broadcast to every connected WebSocket client
            payload = json.dumps(in_memory_ticks, cls=DecimalEncoder)
            dead = set()
            for ws in active_connections:
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.add(ws)
            active_connections.difference_update(dead)

            if all_processed:
                buys = [s['symbol'] for s in all_processed if s.get('signal') == 'BUY' and s.get('isConviction')]
                log = f"🔄 [LiveFeed] {now.strftime('%H:%M:%S')} | {len(all_processed)} symbols"
                if buys:
                    log += f" | 🔥 CONVICTIONS: {', '.join(buys)}"
                print(log)

        except Exception as cycle_e:
            print(f"❌ [LiveFeed] Cycle error: {cycle_e}")
            await asyncio.sleep(5)
            continue

        await asyncio.sleep(SLEEP_BETWEEN_CYCLES)


# ============================================================
# WEBSOCKET ENDPOINT — Push-based, no DB polling
# Clients connect here and receive broadcasts from upstox_live_feed.
# ============================================================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global active_connections, in_memory_ticks
    await websocket.accept()
    active_connections.add(websocket)
    print(f"🟢 WebSocket Connected. Active clients: {len(active_connections)}")

    try:
        # Immediately send the latest snapshot so the client doesn't wait for the next cycle
        if in_memory_ticks:
            await websocket.send_text(json.dumps(in_memory_ticks, cls=DecimalEncoder))

        # Keep connection alive; data is pushed by upstox_live_feed()
        while True:
            # Receive any client messages (e.g., pings) to keep the socket healthy
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                pass  # expected — no client message needed

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"🔴 WebSocket Error: {e}")
    finally:
        active_connections.discard(websocket)
        print(f"🔌 WebSocket Closed. Active clients: {len(active_connections)}")

if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
