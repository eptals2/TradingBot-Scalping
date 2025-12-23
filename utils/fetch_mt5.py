import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime, timedelta, timezone

from db_utils import init_db, insert_candles, get_last_timestamp

SYMBOL = "XAUUSD"
TIMEFRAME = mt5.TIMEFRAME_M5
BARS_PER_FETCH = 5000

def fetch_incremental():
    if not mt5.initialize():
        raise RuntimeError("MT5 init failed")

    init_db()

    last_time = get_last_timestamp()

    if last_time:
        start = pd.to_datetime(last_time) + timedelta(minutes=5)
    else:
        start = datetime.now(timezone.utc) - timedelta(days=180)

    end = datetime.now(timezone.utc)

    rates = mt5.copy_rates_range(SYMBOL, TIMEFRAME, start, end)
    if rates is None or len(rates) == 0:
        print("No new data")
        return

    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')

    df = df[['time','open','high','low','close','tick_volume','spread','real_volume']]
    df['time'] = df['time'].astype(str)

    insert_candles(df)
    mt5.shutdown()

    print(f"Inserted {len(df)} new candles")

if __name__ == "__main__":
    fetch_incremental()
