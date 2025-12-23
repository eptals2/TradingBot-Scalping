import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime, timedelta, timezone

from config import SYMBOL, TIMEFRAME, TIMEFRAME_MAP, INITIAL_LOOKBACK_DAYS
from fetcher.sqlite_store import init_db, get_last_time, insert_candles
from fetcher.mt5_connector import connect, shutdown

def fetch_incremental():
    connect()
    init_db()

    last_time = get_last_time()

    if last_time:
        start = pd.to_datetime(last_time) + timedelta(
            minutes=TIMEFRAME_MAP[TIMEFRAME]
        )
    else:
        start = datetime.now(timezone.utc) - timedelta(
            days=INITIAL_LOOKBACK_DAYS
        )

    end = datetime.now(timezone.utc)

    tf = getattr(mt5, f"TIMEFRAME_{TIMEFRAME}")
    rates = mt5.copy_rates_range(SYMBOL, tf, start, end)

    if rates is None or len(rates) == 0:
        print("No new candles")
        shutdown()
        return

    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s').astype(str)

    df = df[['time','open','high','low','close','tick_volume','spread','real_volume']]
    insert_candles(df)

    shutdown()
    print(f"Inserted {len(df)} new candles")
