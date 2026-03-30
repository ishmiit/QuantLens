#!/bin/bash

echo "🚀 Starting QuantLens Trading Engine (Background)..."
python python_engine/src/main.py &

echo "🌐 Starting FastAPI Web Server (Foreground)..."
python python_engine/src/server.py
