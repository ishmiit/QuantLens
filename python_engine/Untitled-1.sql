-- 1. The Master Table (Context & Static Data)
CREATE TABLE ticker_master (
    symbol TEXT PRIMARY KEY,
    prev_close DECIMAL,
    avg_vol_20d BIGINT,
    atr DECIMAL,
    rsi DECIMAL,
    dist_sma20 DECIMAL,
    dist_52wh DECIMAL,
    sector TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. The Live Table (High-Speed Ticks)
CREATE TABLE ticker_live (
    symbol TEXT PRIMARY KEY,
    price DECIMAL,
    pct_change DECIMAL,
    rvol DECIMAL,
    ai_signal TEXT, -- 'SNIPER_BUY', 'VOYAGER_SELL', etc.
    confidence DECIMAL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT 
    l.symbol, 
    l.price AS live_price, 
    m.prev_close, 
    l.rvol,
    m.last_updated -- (If you have a timestamp column, add it to see WHEN it last updated)
FROM ticker_live l
JOIN ticker_master m ON l.symbol = m.symbol
WHERE l.symbol = 'ETERNAL';

SELECT symbol, price FROM ticker_live WHERE symbol = 'RELIANCE';

SELECT symbol, price 
FROM ticker_live 
WHERE symbol IN ('RELIANCE', 'NIFTY_50');

SELECT * FROM ticker_live LIMIT 5;

-- Add columns to store the Forge's custom targets and stop losses
ALTER TABLE ticker_live ADD COLUMN IF NOT EXISTS stop_loss DECIMAL;
ALTER TABLE ticker_live ADD COLUMN IF NOT EXISTS target_price DECIMAL;
ALTER TABLE ticker_live ADD COLUMN IF NOT EXISTS ai_mode TEXT;
ALTER TABLE ticker_live ADD COLUMN IF NOT EXISTS is_manual_forge BOOLEAN DEFAULT FALSE;

-- Optional: Add a flag to distinguish manual trades from AI ones
ALTER TABLE ticker_live ADD COLUMN IF NOT EXISTS is_manual_forge BOOLEAN DEFAULT FALSE;

INSERT INTO ticker_live (symbol, price, rvol, pct_change) 
VALUES ('RELIANCE', 2500.00, 1.5, 2.1) 
ON CONFLICT (symbol) DO UPDATE SET price = 2500.00;

SELECT symbol, stop_loss, target_price, is_manual_forge FROM ticker_live WHERE symbol = 'RELIANCE';

ALTER TABLE ticker_live 
ADD COLUMN open_price DECIMAL DEFAULT 0;

SELECT count(*) FROM ticker_live;

-- Check for the "Zero" or "Null" culprits
SELECT symbol, price, open_price, pct_change 
FROM ticker_live 
WHERE price = 0 OR open_price IS NULL OR open_price = 0;

SELECT 
    l.symbol, 
    l.price, 
    m.rsi, 
    m.atr, 
    m.avg_vol_20d
FROM ticker_live l
LEFT JOIN ticker_master m ON l.symbol = m.symbol
WHERE m.rsi IS NOT NULL;

-- Check the Live table (populated by main.py)
SELECT symbol FROM ticker_live;

-- Check the Master table (populated by refresh_master.py)
SELECT symbol FROM ticker_master;

TRUNCATE ticker_master;

SELECT symbol, price, open_price, pct_change FROM ticker_live ORDER BY pct_change DESC LIMIT 5;

CREATE TABLE ml_training_data_v2 (
    id SERIAL PRIMARY KEY,
    symbol TEXT,
    timestamp TIMESTAMP,
    rvol DECIMAL,
    change_percent DECIMAL,
    cluster_id INTEGER,
    price DECIMAL,
    rsi DECIMAL,
    dist_sma_20 DECIMAL,
    volatility DECIMAL,
    target_intraday INTEGER,
    target_swing INTEGER
);

DELETE FROM ticker_master WHERE symbol LIKE '%&%';
DELETE FROM ticker_live WHERE symbol LIKE '%&%';

CREATE INDEX idx_ml_v2_symbol_time ON ml_training_data_v2 (symbol, timestamp);

TRUNCATE TABLE ml_training_data_v2 RESTART IDENTITY;

SELECT * FROM ml_training_data_v2 LIMIT 5;

SELECT symbol, COUNT(*), MIN(timestamp), MAX(timestamp) 
FROM ml_training_data_v2 
GROUP BY symbol;

SELECT 
    symbol, 
    COUNT(*) AS total_rows, 
    MIN(timestamp) AS start_date, 
    MAX(timestamp) AS end_date,
    -- This calculates how many rows you HAVE vs how many you SHOULD have (approx)
    -- 13,000 is a good benchmark for 5 years of mixed Daily/30min data
    CASE 
        WHEN COUNT(*) < 1000 THEN '🔴 Critical: Missing Data'
        WHEN COUNT(*) < 8000 THEN '🟡 Warning: Partial Data'
        ELSE '🟢 Healthy'
    END AS status
FROM ml_training_data_v2 
GROUP BY symbol 
ORDER BY total_rows ASC;

DELETE FROM ml_training_data_v2 
WHERE symbol IN ('NSE_EQ|INE040A01034', 'NSE_EQ|INE202B01038');

SELECT symbol, COUNT(*) 
FROM ml_training_data_v2 
GROUP BY symbol 
ORDER BY symbol ASC;

SELECT symbol, rsi, target_intraday, target_swing 
FROM ml_training_data_v2 
WHERE rsi > 0 
LIMIT 10;

UPDATE ml_training_data_v2 
SET rsi = 0, 
    volatility = 0, 
    dist_sma_20 = 0, 
    target_intraday = 0, 
    target_swing = 0;

ALTER TABLE ticker_master 
ADD COLUMN IF NOT EXISTS volatility FLOAT DEFAULT 0.0;

SELECT count(*) FROM ml_training_data_v2 
WHERE symbol = 'AARTIIND' 
AND timestamp BETWEEN '2024-06-04' AND '2024-06-14';