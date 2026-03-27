import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

# --- CONFIGURATION ---
DB_CONFIG = {
    "dbname": "quantlens",
    "user": "quantadmin",
    "password": "admin123",
    "host": "localhost",
    "port": "5432"
}

# The list remains the same (truncated here for brevity)
SYMBOLS = [
    # --- INDICES (Yahoo Format) ---
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
    "M&M.NS", "MARUTI.NS", "TMPV.NS", "BAJAJ-AUTO.NS", "EICHERMOT.NS", 
    "TVSMOTOR.NS", "HEROMOTOCO.NS", "TIINDIA.NS", "ASHOKLEY.NS", "BALKRISIND.NS", 
    "MRF.NS", "BOSCHLTD.NS", "SONACOMS.NS", "MOTHERSON.NS", "APOLLOTYRE.NS",
    "JKTYRE.NS", "CEATLTD.NS", "EXIDEIND.NS", "ARE&M.NS",

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
    "APOLLOHOSP.NS", "MAXHEALTH.NS", "FORTIS.NS", "GLOBAL.NS", "SYNGENE.NS",
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
    "UBL.NS", "UNITDSPR.NS", "JUBLFOOD.NS", "DEVYANI.NS", "SAPPHIRE.NS"
]

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_atr(df, period=14):
    high_low = df['High'] - df['Low']
    high_cp = np.abs(df['High'] - df['Close'].shift())
    low_cp = np.abs(df['Low'] - df['Close'].shift())
    tr = pd.concat([high_low, high_cp, low_cp], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()

def import_master_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
    except Exception as e:
        print(f"❌ DB Connection Failed: {e}")
        return

    end_date = datetime.now()
    start_date = end_date - timedelta(days=60) 

    print(f"📡 Populating ticker_master for {len(SYMBOLS)} symbols...")

    for symbol in SYMBOLS:
        try:
            # 1. Download data
            df = yf.download(symbol, start=start_date, end=end_date, interval="1d", progress=False)
            
            if df.empty or len(df) < 20:
                print(f"⏩ Skipping {symbol}: Not enough data.")
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # 2. Technical Calculations (EXISTING LOGIC)
            clean_symbol = symbol.replace(".NS", "").replace("^", "").replace("NSEI", "NIFTY_50").replace("NSEBANK", "NIFTY_BANK").replace("BSESN", "SENSEX")
            
            close = df['Close']
            high = df['High']
            volume = df['Volume']

            sma20 = close.rolling(window=20).mean()
            rsi = calculate_rsi(close)
            atr = calculate_atr(df)
            high_52w = high.rolling(window=len(df), min_periods=1).max()
            
            # --- ADDED: VOLATILITY CALCULATION (No logic removed) ---
            daily_returns = close.ffill().pct_change(fill_method=None)
            volatility_series = daily_returns.rolling(window=20).std()
            # --------------------------------------------------------

            # 3. Extract latest snapshot
            try:
                # Drop all NaNs so we aren't looking at empty weekend/holiday rows
                valid_closes = close.dropna()
                
                # Strictly filter out the current day's ongoing partial candle
                past_closes = valid_closes[valid_closes.index.date < datetime.now().date()]
                
                if len(past_closes) < 1:
                    raise ValueError(f"Not enough valid past close prices available.")

                # Use float() directly instead of .item() which can crash on duplicate index Series
                last_price = float(valid_closes.iloc[-1])
                
                # Guarantee we grab the strictly preceding COMPLETED trading day
                prev_close = float(past_closes.iloc[-1])
                
                if prev_close <= 0.0:
                    print(f"⚠️ ALERT: Fetched prev_close for {symbol} is {prev_close} (<= 0.0)")
            except Exception as extraction_err:
                print(f"❌ CRITICAL EXTRACTION ERROR on {symbol}: {extraction_err}")
                import traceback
                traceback.print_exc()
                # Do not proceed with DB update if we literally have no prev_close data
                continue
            
            def safe_float(val, default=0.0):
                try:
                    fval = float(val)
                    if np.isnan(fval) or np.isinf(fval):
                        return default
                    return fval
                except (ValueError, TypeError):
                    return default

            avg_vol_raw = volume.tail(20).mean()
            avg_vol_20d = int(safe_float(avg_vol_raw.item() if hasattr(avg_vol_raw, 'item') else avg_vol_raw))
            
            raw_rsi = rsi.iloc[-1].item() if not rsi.empty else np.nan
            current_rsi = safe_float(raw_rsi, 50.0)
            
            raw_atr = atr.iloc[-1].item() if not atr.empty else np.nan
            current_atr = safe_float(raw_atr, last_price * 0.02)
            
            # Extract Volatility Scalar
            raw_vol = volatility_series.iloc[-1].item() if not volatility_series.empty else np.nan
            current_vol = safe_float(raw_vol, 0.015)

            last_sma = safe_float(sma20.iloc[-1].item() if not sma20.empty else np.nan, last_price)
            last_h52 = safe_float(high_52w.iloc[-1].item() if not high_52w.empty else np.nan, last_price)

            dist_sma20 = round(((last_price - last_sma) / last_sma) * 100, 2) if last_sma > 0 else 0.0
            dist_52wh = round(((last_price - last_h52) / last_h52) * 100, 2) if last_h52 > 0 else 0.0
            
            sector_val = "Index" if "^" in symbol else "Equity"

            # 4. Upsert into ticker_master (PAYLOAD UPDATED)
            master_payload = (
                clean_symbol,
                prev_close,
                avg_vol_20d,
                current_atr,
                current_rsi,
                dist_sma20,
                dist_52wh,
                sector_val,
                current_vol # Added new param
            )

            # UPDATED QUERY: Added volatility column
            query = """
                INSERT INTO ticker_master (
                    symbol, prev_close, avg_vol_20d, atr, rsi, dist_sma20, dist_52wh, sector, volatility, last_updated
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (symbol) DO UPDATE SET
                    prev_close = EXCLUDED.prev_close,
                    avg_vol_20d = EXCLUDED.avg_vol_20d,
                    atr = EXCLUDED.atr,
                    rsi = EXCLUDED.rsi,
                    dist_sma20 = EXCLUDED.dist_sma20,
                    dist_52wh = EXCLUDED.dist_52wh,
                    sector = EXCLUDED.sector,
                    volatility = EXCLUDED.volatility,
                    last_updated = CURRENT_TIMESTAMP;
            """
            
            cur.execute(query, master_payload)
            conn.commit()
            print(f"✅ {clean_symbol:12} | Master Stats + Vol Synced.")
            time.sleep(0.3)

        except Exception as e:
            print(f"❌ Error on {symbol}: {e}")
            conn.rollback()

    cur.close()
    conn.close()
    print("\n🏁 Ticker Master Population Complete.")

if __name__ == "__main__":
    import_master_data()