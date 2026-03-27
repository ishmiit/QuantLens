import pandas as pd
import numpy as np
import psycopg2
import joblib
import warnings
from datetime import time

# Suppress pandas DBAPI warning
warnings.filterwarnings('ignore', category=UserWarning)

# Load Models
voyager_model = joblib.load("swing_model.joblib")

def run_backtest():
    print("📡 Connecting to DB...")
    conn = psycopg2.connect(
        dbname="quantlens", user="postgres", password="admin123", host="127.0.0.1"
    )
    
    # 1. Fetch Market Index Data (NIFTY_50)
    print("📈 Fetching NIFTY_50 for Regime Filter...")
    index_query = "SELECT timestamp, close as price FROM ml_training_data_v3 WHERE symbol = 'NIFTY_50' ORDER BY timestamp ASC"
    index_df = pd.read_sql(index_query, conn)
    index_df['timestamp'] = pd.to_datetime(index_df['timestamp'])
    
    # Calculate 50-day SMA for the market trend
    index_df['index_sma_50'] = index_df['price'].rolling(window=50).mean()
    index_df['market_bullish'] = (index_df['price'] > index_df['index_sma_50']).fillna(True)
    market_status = dict(zip(index_df['timestamp'], index_df['market_bullish']))

    # 2. Get all unique symbols
    print("🔎 Fetching symbol list...")
    symbols_query = "SELECT DISTINCT symbol FROM ml_training_data_v3 WHERE symbol != 'NIFTY_50'"
    symbols = pd.read_sql(symbols_query, conn)['symbol'].tolist()
    
    voyager_trades = []
    
    # The exact 12-feature array matching the new model
    feature_cols = ['rvol', 'change_percent', 'cluster_id', 'rsi', 'dist_sma_20', 'volatility', 'adx', 'obv', 'bb_pb', 'vwap_dist', 'day_of_week', 'time_float']
    
    print(f"🚀 Starting Simulation V6.0 | Threshold: 0.54 | Memory-Optimized Loop")
    
    # Process one symbol at a time to prevent RAM explosion
    for idx, symbol in enumerate(symbols):
        print(f"⚙️ Processing {symbol} ({idx + 1}/{len(symbols)})...", end="\r")
        
        query = f"""
        SELECT timestamp, rvol, change_percent, cluster_id, close as price, 
               rsi, dist_sma_20, volatility, adx, obv, bb_pb, vwap_dist 
        FROM ml_training_data_v3 
        WHERE symbol = '{symbol}'
        ORDER BY timestamp ASC
        """
        
        df = pd.read_sql(query, conn)
        if df.empty:
            continue
            
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['time_float'] = df['timestamp'].dt.hour + (df['timestamp'].dt.minute / 60.0)
        
        # Clean data strictly for this symbol
        df.dropna(subset=feature_cols, inplace=True)
        if df.empty:
            continue

        # AI Inference
        df['voyager_prob'] = voyager_model.predict_proba(df[feature_cols])[:, 1]

        active_trade = None
        last_time = None

        for i, row in df.iterrows():
            curr_price = float(row['price'])
            if curr_price <= 0:
                continue
            curr_time = row['timestamp']
            curr_rsi = float(row['rsi'])
            dist_sma_20 = float(row['dist_sma_20'])
            
            raw_vol_rupees = float(row['volatility'])
            volatility_pct = raw_vol_rupees / curr_price 

            # --- VOYAGER (SWING) LOGIC ---
            if active_trade is not None:
                days_held = (curr_time - active_trade['entry_time']).days
                pnl_pct = ((curr_price - active_trade['entry_price']) / active_trade['entry_price']) * 100
                
                # DATA GAP DETECTOR
                if last_time and (curr_time - last_time).total_seconds() > 345600:
                    exit_trade(voyager_trades, active_trade, active_trade['last_price'], last_time, "DATA_GAP_EXIT", symbol)
                    active_trade = None
                    last_time = curr_time
                    continue

                # 1. STRUCTURAL BREAK GUARD
                if dist_sma_20 < -0.5:
                    exit_trade(voyager_trades, active_trade, curr_price, curr_time, "STRUCTURAL_BREAK", symbol)
                    active_trade = None
                    last_time = curr_time
                    continue

                # 2. PEAK CAPTURE (Profit > 25% or RSI > 80)
                if pnl_pct > 25.0 or curr_rsi > 80:
                    new_sl = curr_price - (0.4 * active_trade['init_vol_rupees'])
                    if new_sl > active_trade['sl']:
                        active_trade['sl'] = new_sl
                        active_trade['exit_reason'] = "PEAK_CAPTURE"

                # 3. VOLATILITY-SCALED MILKING (Profit >= 10%)
                elif pnl_pct >= 10.0:
                    milking_trail_dist = active_trade['init_vol_rupees'] * 0.8
                    new_sl = curr_price - milking_trail_dist
                    if new_sl > active_trade['sl']:
                        active_trade['sl'] = new_sl
                        active_trade['exit_reason'] = "MILKING_TRAIL"

                # 4. STANDARD PHASE (Initial Trailing)
                else:
                    new_sl = curr_price - active_trade['init_vol_rupees']
                    if new_sl > active_trade['sl']:
                        active_trade['sl'] = new_sl

                # CHECK FOR STOP TRIGGER
                if curr_price <= active_trade['sl']:
                    reason = "STOP_LOSS" if pnl_pct < 0 else active_trade['exit_reason']
                    exit_trade(voyager_trades, active_trade, active_trade['sl'], curr_time, reason, symbol)
                    active_trade = None
                    last_time = curr_time
                    continue
                
                # 5. TIME-BASED DE-RISKING
                if days_held >= 5 and pnl_pct > 2.0:
                    active_trade['sl'] = max(active_trade['sl'], active_trade['entry_price'] * 1.005)
                
                active_trade['last_price'] = curr_price 

            # --- ENTRY ---
            elif row['voyager_prob'] >= 0.54:
                is_market_bullish = market_status.get(curr_time, True)
                if is_market_bullish:
                    sl_pct = max(volatility_pct, 0.02)
                    active_trade = {
                        'entry_price': curr_price, 
                        'entry_time': curr_time,
                        'sl': curr_price * (1 - sl_pct), 
                        'last_price': curr_price,
                        'init_vol_rupees': curr_price * sl_pct,
                        'exit_reason': "TRAILING_EXIT"
                    }

            last_time = curr_time

    print("\n✅ Simulation Complete! Saving results...")
    results_df = pd.DataFrame(voyager_trades)
    results_df.to_csv("backtest_apex_v6.csv", index=False)
    print(f"✅ V6.0 Complete! Results saved to backtest_apex_v6.csv")

def exit_trade(trades_list, trade, exit_price, exit_time, reason, symbol):
    pnl_pct = ((exit_price - trade['entry_price']) / trade['entry_price']) * 100
    trades_list.append({
        'Symbol': symbol, 'Entry Time': trade['entry_time'], 'Exit Time': exit_time,
        'Entry Price': round(trade['entry_price'], 2), 'Exit Price': round(exit_price, 2),
        'PnL %': round(pnl_pct, 2), 'Reason': reason
    })

if __name__ == "__main__":
    run_backtest()