import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

def generate_labels_and_upload():
    # 1. Connect to DB
    conn = psycopg2.connect(
        dbname="quantlens", user="postgres", password="admin123", host="127.0.0.1"
    )
    cur = conn.cursor()

    # 2. Fetch raw historical data (assuming you have a table or CSVs)
    # For this example, let's assume you've loaded a CSV into a DataFrame 'df'
    # df = pd.read_csv("your_historical_data.csv")
    
    # --- SIMULATED LABELING LOGIC ---
    # We will iterate through the data to see "what happened next"
    
    def get_target(current_idx, df, tp_pct, sl_pct, lookahead):
        entry_price = df.iloc[current_idx]['close']
        future_data = df.iloc[current_idx + 1 : current_idx + lookahead]
        
        for _, row in future_data.iterrows():
            # Check if we hit Take Profit first
            if row['high'] >= entry_price * (1 + tp_pct):
                return 1
            # Check if we hit Stop Loss first
            if row['low'] <= entry_price * (1 - sl_pct):
                return 0
        return 0 # No target hit within lookahead

    # --- APPLYING RULES ---
    # Sniper: 1% TP, 1% SL, Lookahead 1 day (approx 75 5-min candles or 1 daily candle)
    # Voyager: 5% TP, 3% SL, Lookahead 10 days
    
    print("🏷️ Labeling data... this may take a minute.")
    
    # Note: This logic assumes 'df' has technicals already calculated
    processed_rows = []
    for i in range(len(df) - 20): # Leave room for lookahead
        row = df.iloc[i]
        
        target_intraday = get_target(i, df, 0.01, 0.01, 10) 
        target_swing = get_target(i, df, 0.05, 0.03, 50)
        
        processed_rows.append((
            row['symbol'], row['timestamp'],
            row['rvol'], row['change_percent'], row['cluster_id'],
            row['price'], row['rsi'], row['dist_sma_20'], row['volatility'],
            target_intraday, target_swing
        ))

    # 3. Upload to ml_training_data
    # Change the query line to:
    insert_query = """
    INSERT INTO ml_training_data_v2 (
        symbol, timestamp, rvol, change_percent, cluster_id, 
        price, rsi, dist_sma_20, volatility, target_intraday, target_swing
    ) VALUES %s
"""
    
    execute_values(cur, insert_query, processed_rows)
    conn.commit()
    print(f"✅ Successfully uploaded {len(processed_rows)} labeled rows to PostgreSQL!")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    generate_labels_and_upload()