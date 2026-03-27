import psycopg2
try:
    conn = psycopg2.connect(dbname="quantlens", user="postgres", password="admin123", host="127.0.0.1")
    cur = conn.cursor()
    
    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='ticker_master'")
    print("ticker_master columns:", [row[0] for row in cur.fetchall()])
    
    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='ml_training_data_v3'")
    print("ml_training_data_v3 columns:", [row[0] for row in cur.fetchall()])
    
    # Check what kind of symbols are in ml_training_data_v3
    cur.execute("SELECT DISTINCT symbol FROM ml_training_data_v3 LIMIT 5")
    print("ml_training_data_v3 sample symbols:", cur.fetchall())
    
    # Check what kind of symbols are in ticker_master
    cur.execute("SELECT DISTINCT symbol FROM ticker_master LIMIT 5")
    print("ticker_master sample symbols:", cur.fetchall())
    
except Exception as e:
    print(f"Error: {e}")
