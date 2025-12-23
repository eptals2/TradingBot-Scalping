# config.py

SYMBOL = "XAUUSD"
TIMEFRAME = "M5"        # "M1" | "M5" | "M15"
TIMEFRAME_MAP = {
    "M1": 1,
    "M5": 5,
    "M15": 15
}

INITIAL_LOOKBACK_DAYS = 180
DB_PATH = "./db/market.db"
