# scheduler.py (UPDATED with cleanup)
import schedule
import time
from utils.data_fetcher import fetch_all_data
from utils.sqlite_store import cleanup_old_ticks, get_db_stats

def job():
    print(f"\n{'='*50}")
    print(f"Running data collection at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    fetch_all_data()

def cleanup_job():
    print(f"\n{'='*50}")
    print(f"Running cleanup at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    cleanup_old_ticks(keep_hours=48)  # Keep only last 24 hours of ticks
    stats = get_db_stats()
    print(f"DB Stats: {stats}")

# Fetch every minute
schedule.every(1).minutes.do(job)

# Cleanup every hour
schedule.every(1).days.do(cleanup_job)

print("Data collection scheduler started...")
print("Fetching candles and ticks every 1 minute")
print("Cleaning up old ticks every 1 hour")

# Run first job immediately
job()

while True:
    schedule.run_pending()
    time.sleep(1)