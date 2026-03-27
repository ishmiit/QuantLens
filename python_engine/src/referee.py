import yfinance as yf
import psycopg2
from datetime import datetime

def run_referee():
    print("🔔 RUNNING POST-MARKET REFEREE...")
    try:
        conn = psycopg2.connect(dbname="quantlens", user="postgres", password="admin123", host="127.0.0.1")
        cursor = conn.cursor()

        # 1. Find all PENDING signals from today
        cursor.execute("SELECT id, symbol, entry_price, signal_type FROM conviction_logs WHERE status = 'PENDING'")
        pending_trades = cursor.fetchall()

        for trade_id, symbol, entry_price, signal_type in pending_trades:
            # 2. Get the closing price from yfinance
            stock = yf.Ticker(symbol)
            # Use '1d' period to get the most recent closing price
            hist = stock.history(period="1d")
            if hist.empty: continue
            
            exit_price = float(hist['Close'].iloc[-1])
            
            # 3. Calculate Profit/Loss %
            if signal_type == "LONG":
                p_l = ((exit_price - entry_price) / entry_price) * 100
            else: # SHORT
                p_l = ((entry_price - exit_price) / entry_price) * 100

            # 4. Determine Win or Loss
            final_status = "SUCCESS" if p_l > 0 else "FAILED"

            # 5. Update the Database
            update_query = """
                UPDATE conviction_logs 
                SET exit_price = %s, profit_pct = %s, status = %s 
                WHERE id = %s
            """
            cursor.execute(update_query, (round(exit_price, 2), round(p_l, 2), final_status, trade_id))
            print(f"📊 {symbol}: Entry {entry_price} -> Exit {round(exit_price, 2)} | {p_l:.2f}% | {final_status}")

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ ALL PENDING TRADES VALIDATED.")

    except Exception as e:
        print(f"❌ Referee Error: {e}")

if __name__ == "__main__":
    run_referee()