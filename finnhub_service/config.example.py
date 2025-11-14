# ================================
# config.py
# Stores constants and settings
# ================================

# Your Finnhub API key (from finnhub.io dashboard)
FINNHUB_API_KEY = "your_key_from_finnhub.io"

# Symbol for XAUUSD in Finnhub format
# Make sure this matches the broker's available symbols
SYMBOL = "OANDA:XAU_USD"

# Timeframe: '1' = 1-minute candles
INTERVAL = '1'

# SQLite database file location
DB_FILE = "data/finnhub_data.db"
