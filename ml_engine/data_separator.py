import pandas as pd
from sqlalchemy import create_engine

DB_URL = "postgresql://postgres:admin123@127.0.0.1:5432/quantlens"

def export_with_time_features():
    engine = create_engine(DB_URL)
    
    # 1. Fetch Data - ADDED new institutional features
    query = """
        SELECT 
            symbol, timestamp, close as price, rsi, volatility, dist_sma_20, 
            rvol, change_percent, cluster_id, adx, obv, bb_pb, vwap_dist,
            target_intraday, target_swing
        FROM ml_training_data_v3
        WHERE rsi != 0;
    """
    print("📡 Fetching data from database...")
    df = pd.read_sql(query, engine)
    
    # 2. CONVERT TIMESTAMPS
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # 3. ADD TIME-BASED FEATURES
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['hour'] = df['timestamp'].dt.hour
    df['minute'] = df['timestamp'].dt.minute
    
    # Decimal time (e.g., 9:30 becomes 9.5)
    df['time_float'] = df['hour'] + (df['minute'] / 60)

    # 4. STRICT NAN DROPPING AND SPLIT/SAVE
    print("🧹 Dropping NaNs created by rolling windows...")
    df = df.dropna()
    
    print("💾 Saving CSV files...")
    
    # Intraday (Post-2022 only)
    df_intraday = df[df['timestamp'] >= '2022-01-01'].copy()
    df_intraday.to_csv("train_intraday_30m_v2.csv", index=False)
    
    # Swing (Full History)
    df.to_csv("train_swing_trend_v2.csv", index=False)
    
    print(f"✅ Exported successfully!")
    print(f"📊 Features included: {df.columns.tolist()}")

if __name__ == "__main__":
    export_with_time_features()