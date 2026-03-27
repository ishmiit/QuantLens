import yfinance as yf
import psycopg2
import time
from datetime import datetime

# --- SETTINGS ---
TP = 1.5   # Take Profit %
SL = -1.0  # Stop Loss %

def update_live_prices():
    try:
        # Note: Added password/user check from your previous DB_CONFIG
        conn = psycopg2.connect(dbname="quantlens", user="quantadmin", password="admin123", host="127.0.0.1")
        cursor = conn.cursor()

        while True:
            # 1. Only look for 'PENDING' trades
            cursor.execute("SELECT id, symbol, entry_price, signal_type FROM conviction_logs WHERE status = 'PENDING'")
            active_trades = cursor.fetchall()

            if not active_trades:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] No active trades. Scanning...")
            else:
                for trade_id, symbol, entry_p, s_type in active_trades:
                    # 2. Fetch the latest price (Adding .NS for yfinance compatibility)
                    ticker_sym = symbol if symbol.endswith(".NS") or "^" in symbol else f"{symbol}.NS"
                    ticker = yf.Ticker(ticker_sym)
                    
                    try:
                        current_p = ticker.history(period="1d", interval="1m")['Close'].iloc[-1]
                    except:
                        continue # Skip if yfinance throttles
                    
                    # 3. Calculate Unrealized P/L %
                    if s_type == "LONG" or s_type == "STRONG LONG":
                        p_l = ((current_p - entry_p) / entry_p) * 100
                    else: # SHORT
                        p_l = ((entry_p - current_p) / entry_p) * 100

                    # --- 4. THE ACTIVE GUARDIAN LOGIC ---
                    status = 'PENDING'
                    exit_reason = None

                    if p_l >= TP:
                        status = 'CLOSED'
                        exit_reason = 'TAKE_PROFIT'
                    elif p_l <= SL:
                        status = 'CLOSED'
                        exit_reason = 'STOP_LOSS'

                    # 5. Update the DB
                    cursor.execute("""
                        UPDATE conviction_logs 
                        SET exit_price = %s, profit_pct = %s, status = %s, exit_reason = %s
                        WHERE id = %s
                    """, (round(current_p, 2), round(p_l, 2), status, exit_reason, trade_id))
                    
                    if status == 'CLOSED':
                        print(f"🚨 CLOSED {symbol} at {p_l:.2f}% due to {exit_reason}")
                
                conn.commit()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Monitoring {len(active_trades)} positions...")

            time.sleep(30) # Check every 30 seconds for tighter control

    except Exception as e:
        print(f"❌ Guardian Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    update_live_prices()