"""
sync_history.py — GitHub Actions CRON: Historical Market Data Sync
===================================================================
Runs once per day (immediately after auto_auth.py) to:
  1. Retrieve today's fresh UPSTOX_TOKEN from the Neon DB (written by auto_auth.py).
  2. Download the Upstox master instrument list and build INSTRUMENT_MAP.
  3. Fetch the last session's OHLCV data for every symbol in the watchlist using yfinance.
  4. Compute technical indicators (RSI, ATR, Volatility, SMA-dist, 52W-high-dist).
  5. Upsert the results into ticker_master on Neon so that server.py has a
     fresh baseline when it starts the in-memory live feed.

Does NOT start any WebSockets or infinite loops.
Exits with sys.exit(0) on success.
"""

import os
import sys
import time
import io
import zlib
import requests
import psycopg2
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

# ──────────────────────────────────────────────
# Database connection (Neon via DATABASE_URL env)
# ──────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("🔴 FATAL: DATABASE_URL not set.")
    sys.exit(1)

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


# ──────────────────────────────────────────────
# Watchlist — mirrors server.py / main.py
# ──────────────────────────────────────────────
YAHOO_SYMBOLS = [
    # --- INDICES ---
    "^NSEI", "^BSESN", "^NSEBANK",

    # --- BANKING & FINANCE ---
    "HDFCBANK.NS", "ICICIBANK.NS", "AXISBANK.NS", "SBIN.NS", "KOTAKBANK.NS",
    "BAJFINANCE.NS", "BAJAJFINSV.NS", "CHOLAFIN.NS", "SHRIRAMFIN.NS", "PFC.NS",
    "RECLTD.NS", "MUTHOOTFIN.NS", "JIOFIN.NS", "HDFCLIFE.NS", "SBILIFE.NS",
    "LICHSGFIN.NS", "BANDHANBNK.NS", "IDFCFIRSTB.NS", "AUBANK.NS", "CANBK.NS",
    "PNB.NS", "BANKBARODA.NS", "IDBI.NS", "FEDERALBNK.NS", "INDUSINDBK.NS",
    "YESBANK.NS", "RBLBANK.NS", "LTF.NS", "M&MFIN.NS",
    "POONAWALLA.NS", "PIRAMALFIN.NS", "IEX.NS", "MCX.NS",

    # --- IT & TECHNOLOGY ---
    "TCS.NS", "INFY.NS", "HCLTECH.NS", "WIPRO.NS", "TECHM.NS", "LTIM.NS",
    "PERSISTENT.NS", "COFORGE.NS", "MPHASIS.NS", "KPITTECH.NS", "TATAELXSI.NS",
    "LTTS.NS", "CYIENT.NS", "BSOFT.NS", "ZENSARTECH.NS", "SONATSOFTW.NS",
    "OFSS.NS", "MASTEK.NS", "TATACOMM.NS", "HFCL.NS",

    # --- OIL, GAS & ENERGY ---
    "RELIANCE.NS", "ONGC.NS", "BPCL.NS", "IOC.NS", "GAIL.NS", "HINDPETRO.NS",
    "PETRONET.NS", "OIL.NS", "COALINDIA.NS", "NTPC.NS", "POWERGRID.NS",
    "ADANIPOWER.NS", "ADANIGREEN.NS", "ADANIENSOL.NS", "TATAPOWER.NS",
    "NHPC.NS", "SJVN.NS", "SUZLON.NS", "IREDA.NS", "CESC.NS",

    # --- AUTOMOBILES ---
    "M&M.NS", "MARUTI.NS", "BAJAJ-AUTO.NS", "EICHERMOT.NS",
    "TVSMOTOR.NS", "HEROMOTOCO.NS", "TIINDIA.NS", "ASHOKLEY.NS", "BALKRISIND.NS",
    "MRF.NS", "BOSCHLTD.NS", "SONACOMS.NS", "MOTHERSON.NS", "APOLLOTYRE.NS",
    "JKTYRE.NS", "CEATLTD.NS", "EXIDEIND.NS",

    # --- CONSUMER & FMCG ---
    "HINDUNILVR.NS", "ITC.NS", "NESTLEIND.NS", "BRITANNIA.NS", "TATACONSUM.NS",
    "VBL.NS", "GODREJCP.NS", "DABUR.NS", "MARICO.NS", "COLPAL.NS",
    "TITAN.NS", "HAVELLS.NS", "DIXON.NS", "VOLTAS.NS", "KAYNES.NS",
    "BLUESTARCO.NS", "POLYCAB.NS", "KEI.NS", "BATAINDIA.NS", "RELAXO.NS",
    "PAGEIND.NS", "TRENT.NS", "DMART.NS", "ABFRL.NS", "NYKAA.NS",

    # --- METALS & MINING ---
    "TATASTEEL.NS", "JSWSTEEL.NS", "HINDALCO.NS", "VEDL.NS", "JSL.NS",
    "NATIONALUM.NS", "NMDC.NS", "SAIL.NS", "HINDZINC.NS", "WELCORP.NS",

    # --- HEALTHCARE ---
    "SUNPHARMA.NS", "CIPLA.NS", "DRREDDY.NS", "DIVISLAB.NS", "ZYDUSLIFE.NS",
    "MANKIND.NS", "TORNTPHARM.NS", "LUPIN.NS", "AUROPHARMA.NS", "ALKEM.NS",
    "APOLLOHOSP.NS", "MAXHEALTH.NS", "FORTIS.NS", "SYNGENE.NS",
    "LAURUSLABS.NS", "GRANULES.NS", "GLAND.NS", "METROPOLIS.NS", "LALPATHLAB.NS",

    # --- CAPITAL GOODS & DEFENCE ---
    "LT.NS", "BEL.NS", "HAL.NS", "BHEL.NS", "ABB.NS", "SIEMENS.NS",
    "CUMMINSIND.NS", "MAZDOCK.NS", "GRASIM.NS", "RVNL.NS", "IRFC.NS",
    "IRCON.NS", "BDL.NS", "COCHINSHIP.NS", "GRSE.NS",

    # --- REAL ESTATE & CONSTRUCTION ---
    "DLF.NS", "LODHA.NS", "GODREJPROP.NS", "OBEROIRLTY.NS", "PHOENIXLTD.NS",
    "PRESTIGE.NS", "BRIGADE.NS", "SOBHA.NS", "KNRCON.NS", "PNCINFRA.NS",

    # --- CEMENT ---
    "ULTRACEMCO.NS", "SHREECEM.NS", "ACC.NS", "AMBUJACEM.NS", "DALBHARAT.NS",
    "JKCEMENT.NS", "RAMCOCEM.NS", "INDIACEM.NS",

    # --- CHEMICALS & SPECIALTY ---
    "SRF.NS", "PIDILITIND.NS", "LINDEINDIA.NS", "SOLARINDS.NS", "GUJGASLTD.NS",
    "TATACHEM.NS", "AARTIIND.NS", "DEEPAKNTR.NS", "ATUL.NS", "NAVINFLUOR.NS",

    # --- LOGISTICS & INTERNET ---
    "ADANIPORTS.NS", "CONCOR.NS", "GMRAIRPORT.NS", "ETERNAL.NS", "PAYTM.NS",
    "POLICYBZR.NS", "DELHIVERY.NS", "INDHOTEL.NS", "EASEMYTRIP.NS", "BLUEDART.NS",

    # --- ADDITIONAL MIDCAPS ---
    "AWL.NS", "PATANJALI.NS", "IGL.NS", "MGL.NS",
    "UBL.NS", "UNITDSPR.NS", "JUBLFOOD.NS", "DEVYANI.NS", "SAPPHIRE.NS",
]


# ──────────────────────────────────────────────
# Technical Indicator Helpers
# ──────────────────────────────────────────────
def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    hl = df['High'] - df['Low']
    hc = (df['High'] - df['Close'].shift()).abs()
    lc = (df['Low'] - df['Close'].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def safe_float(val, default: float = 0.0) -> float:
    try:
        fval = float(val)
        return default if (np.isnan(fval) or np.isinf(fval)) else fval
    except (TypeError, ValueError):
        return default


# ──────────────────────────────────────────────
# Step 1: Retrieve token from Neon DB
# ──────────────────────────────────────────────
def fetch_upstox_token(conn) -> str:
    cur = conn.cursor()
    cur.execute("SELECT value FROM system_config WHERE key = 'UPSTOX_TOKEN'")
    row = cur.fetchone()
    cur.close()
    if not row:
        raise RuntimeError("UPSTOX_TOKEN not found in system_config table.")
    return row[0]


# ──────────────────────────────────────────────
# Step 2: Download & parse Upstox master list
# ──────────────────────────────────────────────
def build_instrument_map() -> dict:
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    print("📡 Downloading Upstox master instrument list...")
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
    response.raise_for_status()
    content = zlib.decompress(response.content, zlib.MAX_WBITS | 16)
    df = pd.read_csv(io.BytesIO(content))

    instrument_map = {}
    for _, row in df.iterrows():
        t_symbol = str(row['tradingsymbol']).strip().upper()
        i_key    = str(row['instrument_key']).strip()
        instrument_map[t_symbol] = i_key
        if '-EQ' in t_symbol:
            clean = t_symbol.replace('-EQ', '')
            instrument_map[clean] = i_key
            if clean == 'LTM':
                instrument_map['LTIM'] = i_key  # Corporate action alias

    print(f"✅ Instrument map built: {len(instrument_map)} entries.")
    return instrument_map


# ──────────────────────────────────────────────
# Step 3 & 4: Fetch history + compute indicators
# ──────────────────────────────────────────────
def fetch_and_compute(symbol: str) -> dict | None:
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=65)  # ~3 months for stable indicators

    df = yf.download(symbol, start=start_date, end=end_date, interval="1d", progress=False)
    if df.empty or len(df) < 20:
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    clean = (symbol
             .replace('.NS', '')
             .replace('^', '')
             .replace('NSEI', 'NIFTY_50')
             .replace('NSEBANK', 'NIFTY_BANK')
             .replace('BSESN', 'SENSEX'))

    close  = df['Close']
    high   = df['High']
    volume = df['Volume']

    sma20      = close.rolling(window=20).mean()
    rsi_series = calculate_rsi(close)
    atr_series = calculate_atr(df)
    high_52w   = high.rolling(window=len(df), min_periods=1).max()
    daily_ret  = close.ffill().pct_change(fill_method=None)
    vol_series = daily_ret.rolling(window=20).std()

    # Extract validated latest closed-day values
    valid_closes = close.dropna()
    past_closes  = valid_closes[valid_closes.index.date < datetime.now().date()]
    if len(past_closes) < 1:
        return None

    last_price  = safe_float(valid_closes.iloc[-1])
    prev_close  = safe_float(past_closes.iloc[-1])

    if prev_close <= 0.0:
        return None

    avg_vol_raw = volume.tail(20).mean()
    avg_vol_20d = int(safe_float(avg_vol_raw.item() if hasattr(avg_vol_raw, 'item') else avg_vol_raw))
    current_rsi = safe_float(rsi_series.iloc[-1], 50.0)
    current_atr = safe_float(atr_series.iloc[-1], last_price * 0.02)
    current_vol = safe_float(vol_series.iloc[-1], 0.015)
    last_sma    = safe_float(sma20.iloc[-1], last_price)
    last_h52    = safe_float(high_52w.iloc[-1], last_price)

    dist_sma20 = round(((last_price - last_sma) / last_sma) * 100, 2) if last_sma > 0 else 0.0
    dist_52wh  = round(((last_price - last_h52) / last_h52) * 100, 2) if last_h52 > 0 else 0.0
    sector     = "Index" if "^" in symbol else "Equity"

    return {
        'symbol':      clean,
        'prev_close':  prev_close,
        'avg_vol_20d': avg_vol_20d,
        'atr':         current_atr,
        'rsi':         current_rsi,
        'dist_sma20':  dist_sma20,
        'dist_52wh':   dist_52wh,
        'sector':      sector,
        'volatility':  current_vol,
    }


# ──────────────────────────────────────────────
# Step 5: Upsert into ticker_master
# ──────────────────────────────────────────────
UPSERT_QUERY = """
    INSERT INTO ticker_master (
        symbol, prev_close, avg_vol_20d, atr, rsi,
        dist_sma20, dist_52wh, sector, volatility, last_updated
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (symbol) DO UPDATE SET
        prev_close   = EXCLUDED.prev_close,
        avg_vol_20d  = EXCLUDED.avg_vol_20d,
        atr          = EXCLUDED.atr,
        rsi          = EXCLUDED.rsi,
        dist_sma20   = EXCLUDED.dist_sma20,
        dist_52wh    = EXCLUDED.dist_52wh,
        sector       = EXCLUDED.sector,
        volatility   = EXCLUDED.volatility,
        last_updated = CURRENT_TIMESTAMP;
"""


def upsert_master(conn, row: dict):
    cur = conn.cursor()
    cur.execute(UPSERT_QUERY, (
        row['symbol'], row['prev_close'], row['avg_vol_20d'],
        row['atr'],    row['rsi'],        row['dist_sma20'],
        row['dist_52wh'], row['sector'],  row['volatility'],
    ))
    conn.commit()
    cur.close()


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    print("=" * 60)
    print("🕐 QuantLens Historical Sync — started at", datetime.now().isoformat())
    print("=" * 60)

    conn = get_db_connection()

    # Step 1 — token (validates auth pipeline ran OK)
    try:
        token = fetch_upstox_token(conn)
        print(f"✅ UPSTOX_TOKEN retrieved from DB (len={len(token)}).")
    except RuntimeError as e:
        print(f"❌ {e}")
        conn.close()
        sys.exit(1)

    # Step 2 — instrument map (used to confirm keys are resolvable)
    try:
        instrument_map = build_instrument_map()
    except Exception as e:
        print(f"⚠️  Instrument map build failed (non-fatal): {e}")
        instrument_map = {}

    # Steps 3–5 — per-symbol fetch, compute, upsert
    ok, skipped, failed = 0, 0, 0
    for yahoo_sym in YAHOO_SYMBOLS:
        try:
            result = fetch_and_compute(yahoo_sym)
            if result is None:
                print(f"⏩ SKIP  {yahoo_sym} — insufficient data")
                skipped += 1
                continue
            upsert_master(conn, result)
            print(f"✅ SYNC  {result['symbol']:15} | prev={result['prev_close']:.2f}  rsi={result['rsi']:.1f}  vol={result['volatility']:.4f}")
            ok += 1
        except Exception as e:
            print(f"❌ ERROR  {yahoo_sym}: {e}")
            conn.rollback()
            failed += 1
        finally:
            time.sleep(0.3)   # polite rate limiting against Yahoo Finance

    conn.close()

    print("=" * 60)
    print(f"🏁 Sync complete: {ok} updated | {skipped} skipped | {failed} failed")
    print("=" * 60)
    sys.exit(0)


if __name__ == "__main__":
    main()
