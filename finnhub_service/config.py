# ================================
# Secure config loader
# Using .env for sensitive values
# ================================

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Finnhub API Key
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# Trading symbol (Gold)
SYMBOL = os.getenv("SYMBOL", "OANDA:XAUUSD")

# Timeframe interval (1 = 1 minute candles)
INTERVAL = os.getenv("INTERVAL", "1")

# SQLite database location
DB_FILE = os.getenv("DB_FILE", "data/finnhub_data.db")


# ------------ Validation Check ------------
def validate_config():
    """Ensure required environment variables are set."""
    missing = []

    if not FINNHUB_API_KEY:
        missing.append("FINNHUB_API_KEY")

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

# Run validation on import
validate_config()

