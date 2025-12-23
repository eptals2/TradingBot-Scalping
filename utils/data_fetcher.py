# utils/data_fetcher.py
import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

from config import SYMBOL, TIMEFRAME, TIMEFRAME_MAP, INITIAL_LOOKBACK_DAYS
from utils.sqlite_store import (
    init_db, 
    get_last_candle_time, 
    get_last_tick_time,
    insert_candles, 
    insert_ticks
)
from utils.mt5_connector import connect, shutdown


def fetch_candles_incremental():
    """Fetch new candles since last stored candle"""
    connect()
    init_db()

    last_time = get_last_candle_time()

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
        return None

    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    
    # Ensure we have bid/ask columns
    if 'bid' not in df.columns:
        # Approximate from close and spread
        df['spread_pips'] = df.get('spread', 0)
        pip_size = 0.01  # for XAU/USD
        df['ask'] = df['close'] + (df['spread_pips'] * pip_size / 2)
        df['bid'] = df['close'] - (df['spread_pips'] * pip_size / 2)
    
    df = df[['time','open','high','low','close','tick_volume','spread','real_volume','bid','ask']]
    insert_candles(df)

    shutdown()
    print(f"✓ Inserted {len(df)} new candles")
    return df


def fetch_ticks_incremental():
    """Fetch new ticks since last stored tick"""
    connect()
    init_db()

    last_time = get_last_tick_time()

    if last_time:
        start = pd.to_datetime(last_time) + timedelta(milliseconds=1)
    else:
        # Start from 1 hour ago for ticks (they're huge!)
        start = datetime.now(timezone.utc) - timedelta(hours=1)

    end = datetime.now(timezone.utc)

    # Fetch ticks
    ticks = mt5.copy_ticks_range(SYMBOL, start, end, mt5.COPY_TICKS_ALL)

    if ticks is None or len(ticks) == 0:
        print("No new ticks")
        shutdown()
        return None

    df = pd.DataFrame(ticks)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df['time_msc'] = pd.to_datetime(df['time_msc'], unit='ms')
    
    # Keep relevant columns
    df = df[['time_msc', 'bid', 'ask', 'last', 'volume', 'flags']]
    df = df.rename(columns={'time_msc': 'time'})
    
    insert_ticks(df)

    shutdown()
    print(f"✓ Inserted {len(df)} new ticks")
    return df


def fetch_all_data():
    """Fetch both candles and ticks"""
    print("Fetching candles...")
    candles = fetch_candles_incremental()
    
    print("Fetching ticks...")
    ticks = fetch_ticks_incremental()
    
    return candles, ticks


if __name__ == "__main__":
    fetch_all_data()