from fastapi import FastAPI
import sqlite3
import requests
import os
import threading
import time
from datetime import datetime

app = FastAPI(
    title="Finnhub Gold Auto Collector",
    description="Automatically collects XAU/USD price every 1 minute",
    version="1.0.0"
)

# ------------------------------------------------------------------
# 1. Database setup
# ------------------------------------------------------------------
os.makedirs("data", exist_ok=True)
DB_PATH = "data/prices.db"

def get_db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            open REAL,
            high REAL,
            low REAL,
            close REAL
        );
    """)
    conn.commit()
    conn.close()

init_db()


# ------------------------------------------------------------------
# 2. Function to fetch gold price from Finnhub
# ------------------------------------------------------------------
def fetch_and_store_gold():
    FINNHUB_KEY = os.getenv("FINNHUB_KEY", "").strip()

    if FINNHUB_KEY == "":
        print("❌ FINNHUB_KEY is missing.")
        return

    symbol = "OANDA:XAU_USD"

    url = (
        f"https://finnhub.io/api/v1/forex/candle"
        f"?symbol={symbol}&resolution=1&count=1&token={FINNHUB_KEY}"
    )

    try:
        response = requests.get(url)
        data = response.json()

        # If Finnhub returns error
        if data.get("s") != "ok":
            print("⚠ Finnhub returned no data:", data)
            return

        # Extract candle
        ts = data["t"][0]
        o = data["o"][0]
        h = data["h"][0]
        l = data["l"][0]
        c = data["c"][0]

        conn = get_db()
        conn.execute(
            "INSERT INTO prices (timestamp, open, high, low, close) VALUES (?, ?, ?, ?, ?)",
            (ts, o, h, l, c)
        )
        conn.commit()
        conn.close()

        print(f"✔ Saved candle @ {datetime.utcfromtimestamp(ts)}")

    except Exception as e:
        print("❌ Error fetching data:", e)


# ------------------------------------------------------------------
# 3. Background thread that runs every 60 seconds
# ------------------------------------------------------------------
def auto_collector():
    while True:
        fetch_and_store_gold()
        time.sleep(60)  # wait 1 minute


# ------------------------------------------------------------------
# 4. Start auto collector on app startup
# ------------------------------------------------------------------
@app.on_event("startup")
def start_background_tasks():
    thread = threading.Thread(target=auto_collector, daemon=True)
    thread.start()
    print("🚀 Auto collector started (fetches every 1 minute)")


# ------------------------------------------------------------------
# 5. Simple endpoints
# ------------------------------------------------------------------
@app.get("/")
def home():
    return {"status": "running", "collector": "active"}


@app.get("/prices/latest/{limit}")
def latest(limit: int = 20):
    conn = get_db()
    rows = conn.execute(
        "SELECT timestamp, open, high, low, close FROM prices ORDER BY id DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()

    return [
        {
            "timestamp": r[0],
            "datetime": datetime.utcfromtimestamp(r[0]).isoformat(),
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
        } for r in rows
    ]
