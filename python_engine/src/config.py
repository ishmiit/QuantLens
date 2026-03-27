# config.py
import os

# --- API Credentials from Upstox Developer Portal ---
API_KEY = os.getenv("UPSTOX_API_KEY")
API_SECRET = os.getenv("UPSTOX_API_SECRET")
REDIRECT_URI = os.getenv("UPSTOX_REDIRECT_URI")

# --- Your Personal Login Details ---
MOBILE_NO = os.getenv("UPSTOX_MOBILE_NO")
PIN = os.getenv("UPSTOX_PIN")
TOTP_KEY = os.getenv("UPSTOX_TOTP_KEY")