import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime

DB_CONFIG = {"dbname": "quantlens", "user": "postgres", "password": "admin123", "host": "127.0.0.1"}

def get_rr_labels(highs, lows, targets, stops, max_lookahead):
    labels = np.zeros(len(highs), dtype=int)
    n = len(highs)
    
    # Fast numpy iterations
    for i in range(n):
        if np.isnan(targets[i]) or np.isnan(stops[i]):
            continue
            
        target = targets[i]
        stop = stops[i]
        
        for j in range(i + 1, min(i + max_lookahead + 1, n)):
            if lows[j] <= stop:
                break
            if highs[j] >= target:
                labels[i] = 1
                break
    return labels

def hydrate_smart_features():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT symbol FROM ml_training_data_v3;")
    symbols = [row[0] for row in cur.fetchall()]
    
    boundary_date = pd.Timestamp("2022-01-01").tz_localize('UTC')

    for symbol in tqdm(symbols, desc="Processing Era-Based Data"):
        query = f"SELECT id, timestamp, open, high, low, close, volume FROM ml_training_data_v3 WHERE symbol = '{symbol}' ORDER BY timestamp ASC"
        df = pd.read_sql(query, conn)
        if len(df) < 50: continue

        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        daily_mask = df['timestamp'] < boundary_date
        intraday_mask = df['timestamp'] >= boundary_date

        close = df['close']
        high = df['high']
        low = df['low']
        volume = df['volume']

        # 1. TR & ATR (14-period)
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=14).mean()

        # 2. RSI (14-period)
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        df['rsi'] = (100 - (100 / (1 + (gain/loss)))).fillna(50)

        # 3. Dist SMA 20
        df['dist_sma_20'] = (close / close.rolling(window=20).mean()).fillna(1.0)
        
        # 4. Historical Volatility (20-day rolling std dev of log returns)
        log_returns = np.log(close / close.shift(1)).replace([np.inf, -np.inf], np.nan)
        df['volatility'] = log_returns.rolling(window=20).std().fillna(0)

        # 5. ADX (14-period)
        up_move = high - high.shift(1)
        down_move = low.shift(1) - low
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
        
        plus_di = 100 * (pd.Series(plus_dm).rolling(window=14).mean() / atr)
        minus_di = 100 * (pd.Series(minus_dm).rolling(window=14).mean() / atr)
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).abs()
        df['adx'] = dx.rolling(window=14).mean().fillna(0)

        # 6. OBV 
        df['obv'] = (np.sign(delta) * volume).fillna(0).cumsum()

        # 7. Bollinger Band %B (20-period, 2-std-dev)
        sma20 = close.rolling(window=20).mean()
        std20 = close.rolling(window=20).std()
        upper_band = sma20 + (2 * std20)
        lower_band = sma20 - (2 * std20)
        df['bb_pb'] = ((close - lower_band) / (upper_band - lower_band)).replace([np.inf, -np.inf], np.nan).fillna(0.5)

        # 8. VWAP Distance (Daily reset)
        df['date'] = df['timestamp'].dt.date
        typical_price = (high + low + close) / 3
        # Group by date to reset cumsum each trading day
        cum_vol = df.groupby('date')['volume'].cumsum()
        temp_df = pd.DataFrame({'tp_vol': typical_price * volume, 'date': df['date']})
        cum_tp_vol = temp_df.groupby('date')['tp_vol'].cumsum()
        vwap = cum_tp_vol / cum_vol
        df['vwap_dist'] = ((close - vwap) / vwap).replace([np.inf, -np.inf], np.nan).fillna(0)

        # 9. strict 1:2 R:R Labels
        # Base distances are using ATR (Intraday spec: 2 * ATR target, 1 * ATR stop)
        df['target_price_intraday'] = close + (2.0 * atr)
        df['stop_price_intraday'] = close - (1.0 * atr)
        
        # We process daily and intraday separately using our fast loop to give labels
        # For daily, look ahead ~20 days max. For intraday, look ahead ~100 candles.
        df['target_intraday'] = 0
        df['target_swing'] = 0
        
        highs_arr = df['high'].values
        lows_arr = df['low'].values
        targets_arr = df['target_price_intraday'].values
        stops_arr = df['stop_price_intraday'].values
        
        df['target_intraday'] = get_rr_labels(highs_arr, lows_arr, targets_arr, stops_arr, max_lookahead=100)
        df['target_swing'] = get_rr_labels(highs_arr, lows_arr, targets_arr, stops_arr, max_lookahead=200)

        # BATCH UPDATE
        update_data = list(zip(
            df['rsi'].tolist(), df['dist_sma_20'].tolist(), df['volatility'].tolist(),
            df['adx'].tolist(), df['obv'].tolist(), df['bb_pb'].tolist(), df['vwap_dist'].tolist(),
            df['target_intraday'].tolist(), df['target_swing'].tolist(),
            df['id'].tolist()
        ))

        execute_values(cur, """
            UPDATE ml_training_data_v3 AS t SET
                rsi = v.rsi, dist_sma_20 = v.dist_sma_20, volatility = v.volatility,
                adx = v.adx, obv = v.obv, bb_pb = v.bb_pb, vwap_dist = v.vwap_dist,
                target_intraday = v.target_intraday, target_swing = v.target_swing
            FROM (VALUES %s) AS v(rsi, dist_sma_20, volatility, adx, obv, bb_pb, vwap_dist, target_intraday, target_swing, id)
            WHERE t.id = v.id
        """, update_data)
        conn.commit()

    cur.close(); conn.close()
    print("✨ Institutional Hybrid Hydration Complete!")

if __name__ == "__main__":
    hydrate_smart_features()