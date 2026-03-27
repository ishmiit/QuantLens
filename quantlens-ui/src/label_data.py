import pandas as pd
import numpy as np
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:admin123@127.0.0.1:5432/quantlens')

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def label_and_engineer_dual():
    print("📂 Reading data from stock_prices...")
    df = pd.read_sql("SELECT * FROM stock_prices", engine)

    if 'timestamp' not in df.columns:
        print("❌ Error: 'timestamp' column not found.")
        return

    df = df.sort_values(['symbol', 'timestamp'])

    print("🛠️ Engineering Multi-Strategy Features...")
    
    # Standard Technicals
    df['sma_20'] = df.groupby('symbol')['price'].transform(lambda x: x.rolling(window=20).mean())
    df['dist_sma_20'] = (df['price'] - df['sma_20']) / df['sma_20']
    df['rsi'] = df.groupby('symbol')['price'].transform(calculate_rsi)
    
    # Volatility Feature: Helps Intraday model see 'energy'
    df['volatility'] = df.groupby('symbol')['price'].transform(lambda x: x.rolling(window=10).std() / x.mean())

    print("🎯 Calculating Dual Targets...")
    
    # TARGET 1: INTRADAY (Sniper)
    # Look at the MAX price reached in the very next day (approx 1 trading cycle ahead)
    df['next_day_max'] = df.groupby('symbol')['price'].shift(-1) 
    df['target_intraday'] = (df['next_day_max'] > (df['price'] * 1.01)).astype(int)

    # TARGET 2: SWING (Voyager)
    # Look at the MAX price reached over the next 15 periods
    df['future_max_15d'] = df.groupby('symbol')['price'].shift(-15).rolling(window=15, min_periods=1).max()
    df['target_swing'] = (df['future_max_15d'] > (df['price'] * 1.05)).astype(int)

    # Cleanup
    df = df.dropna(subset=['rsi', 'sma_20', 'target_intraday', 'target_swing'])
    
    # Remove helper columns before saving
    df = df.drop(columns=['next_day_max', 'future_max_15d'])

    print(f"💾 Saving {len(df)} rows to ml_training_data...")
    df.to_sql('ml_training_data', engine, if_exists='replace', index=False)
    print("✅ Done! Dataset ready for Dual-Model training.")

if __name__ == "__main__":
    label_and_engineer_dual()