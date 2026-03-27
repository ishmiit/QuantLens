import os
import psycopg2
from dotenv import load_dotenv

# 1. Load the variables from your .env file
load_dotenv()

# 2. Grab the Neon master key
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL is missing from your .env file!")
    exit()

try:
    print("🔌 Attempting to ping the Neon Cloud Database...")
    
    # 3. Connect to the cloud
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # 4. Ask Postgres to identify itself
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    
    print("\n" + "="*50)
    print("✅ CONNECTION SUCCESSFUL!")
    print("="*50)
    print(f"Server Info: {db_version[0]}")
    print("Your database is live and ready for QuantLens.")
    print("="*50 + "\n")
    
    # 5. Clean up
    cursor.close()
    conn.close()

except Exception as e:
    print("\n❌ CONNECTION FAILED!")
    print(f"Error Details: {e}")
    print("Double-check that you copied the exact connection string from Neon.")