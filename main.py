from fastapi import FastAPI
import sqlite3
import requests
import threading
import time
from datetime import datetime
import os
from finnhub_service.config import FINNHUB_API_KEY, SYMBOL, INTERVAL, DB_FILE


app = FastAPI(
    title="Finnhub Gold Auto Collector",
    version="1.0.0"
)

# ---------------------------------------------
# Database Setup
# ---------------------------------------------
os.makedirs("data", exist_ok=True)
DB_PATH = DB_FILE

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


# ---------------------------------------------
# Fetch & Save Finnhub Candle
# ---------------------------------------------
def fetch_and_store_gold():
    FINNHUB_KEY = FINNHUB_API_KEY
    symbol = SYMBOL
    interval = INTERVAL

    url = (
        f"https://finnhub.io/api/v1/forex/candle"
        f"?symbol={symbol}&resolution={interval}&count=1&token={FINNHUB_KEY}"
    )

    try:
        response = requests.get(url)
        data = response.json()

        if data.get("s") != "ok":
            print("⚠ Finnhub returned no data:", data)
            return

        ts = data["t"][0]
        o = data["o"][0]
        h = data["h"][0]
        l = data["l"][0]
        c = data["c"][0]

        conn = get_db()
        conn.execute("""
            INSERT INTO prices (timestamp, open, high, low, close)
            VALUES (?, ?, ?, ?, ?)
        """, (ts, o, h, l, c))
        conn.commit()
        conn.close()

        print(f"✔ Saved candle @ {datetime.utcfromtimestamp(ts)}")

    except Exception as e:
        print("❌ Error:", e)


# ---------------------------------------------
# Background Collector (1 minute loop)
# ---------------------------------------------
def auto_collector():
    while True:
        fetch_and_store_gold()
        time.sleep(60)


@app.on_event("startup")
def start_collector():
    threading.Thread(target=auto_collector, daemon=True).start()
    print("🚀 Auto collector started")


# ---------------------------------------------
# Routes
# ---------------------------------------------
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
        }
        for r in rows
    ]
