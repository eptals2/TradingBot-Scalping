# ================================
# config.py
# Stores constants and settings
# ================================

# Your Finnhub API key (from finnhub.io dashboard)
FINNHUB_API_KEY = ""

# Symbol for XAUUSD in Finnhub format
# Make sure this matches the broker's available symbols
SYMBOL = "OANDA:XAUUSD"

# Timeframe: '1' = 1-minute candles
INTERVAL = '1'

# SQLite database file location
DB_FILE = "data/finnhub_data.db"
