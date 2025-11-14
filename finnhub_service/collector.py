# ==================================================
# collector.py
# Fetches candle data from Finnhub and stores it
# ==================================================

import finnhub
import pandas as pd
import time
from datetime import datetime
from config import FINNHUB_API_KEY, SYMBOL, INTERVAL
from storage import save_candles

# create Finnhub API client
client = finnhub.Client(api_key=FINNHUB_API_KEY)

def fetch_recent(minutes=60):
    now = int(time.time())
    past = now - (minutes * 60)

    # Request OHLC candles from Finnhub
    res = client.forex_candles(
        SYMBOL,
        INTERVAL,
        past,
        now
    )

    if res.get('s') != 'ok':
        raise Exception("Failed to fetch  candles from Finnhub")

    df = pd.DataFrame([
        'time' : pd.to_datetime(res['t'], unit='s'),
        'open' : res['o'],
        'high' : res['h'],
        'low' : res['l'],
        'close' : res['c'],
        'volume' : res.get('v', [None] * len(res['t']))
    ])

    return df

def collect(minutes=60):
    df = fetch_recent(minutes)
    save_candles(SYMBOL, INTERVAL, df)
    print(f"[{datetime.now()}] Collected {len(df)} candles for {SYMBOL} {INTERVAL}")
    

