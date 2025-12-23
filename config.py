# config.py (UPDATE)
import os

# MT5 Settings
SYMBOL = "XAUUSD"
TIMEFRAME = "M1"  # 1-minute candles
TIMEFRAME_MAP = {
    "M1": 1,
    "M5": 5,
    "M15": 15,
    "M30": 30,
    "H1": 60
}

# Database
DB_PATH = "data/market_data.db"
os.makedirs("data", exist_ok=True)

# Data Collection
INITIAL_LOOKBACK_DAYS = 30  # For candles
TICK_LOOKBACK_HOURS = 1     # For ticks (they're massive!)

# Model
MODEL_PATH = "models/scalping_model.pkl"
os.makedirs("models", exist_ok=True)

# Logs
os.makedirs("logs", exist_ok=True)