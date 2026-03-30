# python_engine/src/main.py
#
# ARCHITECTURE NOTE (refactored 2026-03-30):
#
# This file has been intentionally deprecated in the In-Memory Passthrough refactor.
#
#   - Live Upstox tick fetching  →  server.py `upstox_live_feed()` background task
#   - Historical data baseline   →  sync_history.py  (runs via GitHub Actions CRON)
#
# Keeping this file as a stub prevents import errors from any stale references
# and documents the architectural decision for future contributors.

print(
    "ℹ️  main.py is deprecated.\n"
    "   Live ticks  : quantlens-ui/src/server.py  (upstox_live_feed background task)\n"
    "   History sync: python_engine/src/sync_history.py  (GitHub Actions CRON)\n"
)