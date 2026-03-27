import yfinance as yf
import pandas as pd
import psycopg2
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# ALL 50 NIFTY STOCKS
NIFTY_50 = [
    "DRREDDY.NS", "ONGC.NS", "TECHM.NS", "HINDALCO.NS", "HINDUNILVR.NS", 
    "BAJAJ-AUTO.NS", "INFY.NS", "APOLLOHOSP.NS", "TCS.NS", "HCLTECH.NS", 
    "TITAN.NS", "ICICIBANK.NS", "ASIANPAINT.NS", "SHRIRAMFIN.NS", "SUNPHARMA.NS", 
    "ULTRACEMCO.NS", "HDFCBANK.NS", "KOTAKBANK.NS", "M&M.NS", "ITC.NS", 
    "TMPV.NS", "BHARTIARTL.NS", "MAXHEALTH.NS", "GRASIM.NS", "TATASTEEL.NS", 
    "NESTLEIND.NS", "SBILIFE.NS", "WIPRO.NS", "RELIANCE.NS", "EICHERMOT.NS", 
    "LT.NS", "BEL.NS", "COALINDIA.NS", "BAJFINANCE.NS", "TRENT.NS", 
    "MARUTI.NS", "NTPC.NS", "JSWSTEEL.NS", "TATACONSUM.NS", "HDFCLIFE.NS", 
    "SBIN.NS", "BAJAJFINSV.NS", "POWERGRID.NS", "AXISBANK.NS", "CIPLA.NS", 
    "JIOFIN.NS", "INDIGO.NS", "ETERNAL.NS", "ADANIPORTS.NS", "ADANIENT.NS"
]

def run_correlation_engine():
    print("🧬 Step 1: Fetching 2-Year Market DNA...")
    # Fetching historical daily data
    data = yf.download(NIFTY_50, period="2y", interval="1d")['Close']
    
    # NEW: Drop any columns (stocks) that are completely empty/failed
    data = data.dropna(axis=1, how='all')
    
    # Calculate returns and drop rows with missing values
    returns = data.pct_change().dropna()

    # CHECK: If we have no data left, stop here
    if returns.empty:
        print("❌ Error: No price data was downloaded. Check your internet or symbols.")
        return

    print(f"🤖 Step 2: Training ML Model on {len(returns.columns)} stocks...")
    X = returns.T.values 
    X_scaled = StandardScaler().fit_transform(X)
    
    # Adjust clusters if we have fewer stocks than expected
    n_cl = min(8, len(returns.columns))
    kmeans = KMeans(n_clusters=n_cl, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)

    print("💾 Step 3: Saving Clusters to Postgres...")
    try:
        conn = psycopg2.connect(
            dbname="quantlens", 
            user="quantadmin", 
            password="admin123", 
            host="127.0.0.1"
        )
        cur = conn.cursor()
        
        # --- ADD THIS LINE TO REMOVE GHOST STOCKS ---
        print("🧹 Clearing old cluster data...")
        cur.execute("DELETE FROM stock_clusters;") 
        # --------------------------------------------

        for ticker, cluster_id in zip(returns.columns, clusters):
            cur.execute("""
                INSERT INTO stock_clusters (symbol, cluster_id, last_updated)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
            """, (ticker, int(cluster_id))) # Simplified because table is now empty
            
        conn.commit()
        print(f"✅ Success! {len(returns.columns)} stocks categorized.")
    except Exception as e:
        print(f"❌ Database Error: {e}")
    finally:
        if 'conn' in locals(): cur.close(); conn.close()

if __name__ == "__main__":
    run_correlation_engine()