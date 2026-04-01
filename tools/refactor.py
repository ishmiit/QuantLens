import os
import re

server_path = 'quantlens-ui/src/server.py'
with open(server_path, 'r', encoding='utf-8') as f:
    server_content = f.read()

globals_block = """
# --- IN-MEMORY PASSTHROUGH STATE ---
active_connections = set()
in_memory_ticks = []
AVG_VOL_CACHE = {}
AI_FEATURE_CACHE = {}
"""
if "active_connections = set()" not in server_content:
    server_content = server_content.replace("HISTORICAL_CACHE = TTLCache(maxsize=1000, ttl=60)", "HISTORICAL_CACHE = TTLCache(maxsize=1000, ttl=60)\n" + globals_block)

task_logic = """
async def upstox_live_feed():
    global in_memory_ticks, AVG_VOL_CACHE, AI_FEATURE_CACHE, active_connections
    print("🚀 Starting In-Memory Upstox Live Feed...")
    import upstox_client
    import time
    from datetime import datetime
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT value FROM system_config WHERE key = 'UPSTOX_TOKEN'")
    row = cur.fetchone()
    if not row:
        print("🔴 Missing UPSTOX_TOKEN from system_config!")
        return
    access_token = row[0]
    
    print("📊 Loading Avg Volume Cache & Features from DB...")
    cur.execute("SELECT symbol, avg_vol_20d, rsi, volatility, dist_sma20 FROM ticker_master")
    rows = cur.fetchall()
    for r in rows:
        sym = r[0]
        # avg vol is [1]
        AVG_VOL_CACHE[sym] = float(r[1]) if r[1] is not None else 1.0
        # AI Features
        AI_FEATURE_CACHE[sym] = {
            'rsi': float(r[2]) if r[2] is not None else 50.0,
            'volatility': float(r[3]) if r[3] is not None else 0.015,
            'dist_sma_20': float(r[4]) if r[4] is not None else 0.0,
            'cluster_id': 0
        }
    cur.close()
    conn.close()

    configuration = upstox_client.Configuration()
    configuration.access_token = access_token
    api_client = upstox_client.ApiClient(configuration)
    market_api = upstox_client.MarketQuoteApi(api_client)

    # valid keys
    valid_keys = []
    for symbol in RAW_SYMBOLS:
        clean_name = symbol.split("|")[-1].replace('%50', ' 50').replace('%20', ' ').strip().upper()
        key = get_instrument_key(clean_name)
        if key: valid_keys.append(key)
    
    batch_size = 50 
    
    while True:
        try:
            now = datetime.now()
            market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
            minutes_passed = max(1, (now - market_open).total_seconds() / 60)
            day_fraction = min(1.0, minutes_passed / 375) if now > market_open else 0.01
            
            all_processed = []
            conn_hist = get_db_connection()
            
            for i in range(0, len(valid_keys), batch_size):
                chunk = valid_keys[i:i + batch_size]
                instruments_string = ",".join(chunk)
                
                try:
                    # Sync call in thread
                    api_response = await asyncio.to_thread(market_api.get_full_market_quote, instruments_string, '2.0')
                    
                    if not api_response or not api_response.data: 
                        continue
                        
                    for key, details in api_response.data.items():
                        db_symbol = KEY_TO_SYMBOL.get(key, key)
                        price = float(getattr(details, 'last_price', 0.0))
                        
                        open_price = 0.0
                        if hasattr(details, 'ohlc') and details.ohlc:
                            open_price = float(getattr(details.ohlc, 'open', 0.0))
                            
                        prev_close = float(getattr(details, 'last_close', 0.0))
                        if open_price > 0:
                            change_pct = ((price - open_price) / open_price * 100)
                        else:
                            change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
                            
                        current_vol = float(getattr(details, 'volume', 0.0))
                        avg_vol = AVG_VOL_CACHE.get(db_symbol, 1.0)
                        rvol = current_vol / (avg_vol * day_fraction) if (avg_vol * day_fraction) > 0 else 1.0
                        
                        features = AI_FEATURE_CACHE.get(db_symbol, {
                            'rsi': 50.0, 'volatility': 0.015, 'dist_sma_20': 0.0, 'cluster_id': 0
                        })
                        
                        stock_dict = {
                            "symbol": db_symbol,
                            "price": round(price, 2),
                            "open_price": round(open_price, 2),
                            "prev_close": round(prev_close, 2),
                            "pct_change": round(change_pct, 2),
                            "live_pct": round(change_pct, 2),
                            "volume": current_vol,
                            "rvol": round(rvol, 2),
                            "rsi": features['rsi'],
                            "volatility": features['volatility'],
                            "dist_sma20": features['dist_sma_20'],
                            "cluster_id": features['cluster_id'],
                            "timestamp": str(pd.Timestamp.now(tz='UTC'))
                        }

                        # Apply all the server logic
                        processed_stock = apply_conviction_logic(stock_dict, conn=conn_hist)
                        all_processed.append(processed_stock)
                        
                except Exception as chunk_e:
                    print(f"Chunk Error: {chunk_e}")
                    
                await asyncio.sleep(0.5) 
                
            conn_hist.close()
            
            # Global Sorting matching existing UX
            def sort_key(s):
                sym = s.get('symbol')
                if sym == 'NIFTY_50': return 0
                if sym == 'SENSEX': return 1
                if sym == 'NIFTY_BANK': return 2
                return 3
            
            all_processed.sort(key=sort_key)
            in_memory_ticks = all_processed
            
            # Broadcast
            dead_connections = set()
            for connection in active_connections:
                try:
                    await connection.send_text(json.dumps(in_memory_ticks, cls=DecimalEncoder))
                except Exception as e:
                    dead_connections.add(connection)
            
            active_connections.difference_update(dead_connections)
            
            await asyncio.sleep(1.0)
            
        except Exception as e:
            print(f"❌ Master Loop Error: {e}")
            await asyncio.sleep(5)

"""
if "async def upstox_live_feed():" not in server_content:
    server_content = server_content.replace('@app.on_event("startup")', task_logic + '\n@app.on_event("startup")')

if "asyncio.create_task(upstox_live_feed())" not in server_content:
    server_content = server_content.replace('print("🚀 Startup complete. Server is ready to accept connections.")', 'print("🚀 Startup complete. Server is ready.")\n    asyncio.create_task(upstox_live_feed())')

new_ws = """@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global active_connections, in_memory_ticks
    await websocket.accept()
    active_connections.add(websocket)
    print(f"🟢 WebSocket Connected. Total: {len(active_connections)}")
    
    try:
        # Push immediate snapshot if available
        if in_memory_ticks:
            await websocket.send_text(json.dumps(in_memory_ticks, cls=DecimalEncoder))
            
        while True:
            # We don't query DB anymore! Just keep connection alive.
            # The async task upstox_live_feed pushes data directly down this socket.
            data = await websocket.receive_text()
            # Handle client pings if needed
    except WebSocketDisconnect:
        active_connections.remove(websocket)
        print(f"ℹ️ WebSocket Disconnected. Total: {len(active_connections)}")
    except Exception as e:
        if websocket in active_connections:
            active_connections.remove(websocket)
        print(f"🔴 WebSocket Error: {e}")"""

server_content = re.sub(r'@app\.websocket\("/ws"\).*?(?=\nif __name__ == "__main__":)', new_ws + "\n\n", server_content, flags=re.DOTALL)

with open(server_path, 'w', encoding='utf-8') as f:
    f.write(server_content)
print("Updated server.py")

main_path = 'python_engine/src/main.py'
placeholder_main = """# main.py
# DEPRECATED:
# Live background fetching has been moved to server.py as 'upstox_live_feed' background task.
# Historical caching has been moved to sync_history.py under GitHub Actions CRON.

import time

if __name__ == "__main__":
    print("🚀 QuantLens Engine: Active in server.py.")
    print("ℹ️ Note: Polling and history fetch removed from main.py per architectural shift.")
    while True:
        time.sleep(60)
"""
with open(main_path, 'w', encoding='utf-8') as f:
    f.write(placeholder_main)
print("Cleaned up main.py")
