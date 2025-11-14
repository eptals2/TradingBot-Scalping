# ========================================================
# app.py
# FastAPI app exposing REST endpoints + background jobs
# ========================================================

from fastapi import FastAPI
from collector import collect
from storage import SessionLocal, Candle
from apscheduler.schedulers.background import BackgroundScheduler
from config import SYMBOL, INTERVAL

app = FastAPI("Finnhub Data Collector")

scheduler = BackgroundScheduler()
scheduler.add_job(collect, 'interval', minutes=5, args=[60])
scheduler.start()

@app.get("/fetch")
def fetch_data(minutes: int = 60):
    collect(minutes)
    return {"message": f"Fetched and stored last {minutes} minutes of data."}

@app.get("/data")
def get_data(limit: int = 50):
    session = SessionLocal()
    rows = (
        session.query(Candle)
        .filter_by(symbol=SYMBOL, timeframe=INTERVAL)
        .order_by(Candle.time.desc())
        .limit(limit)
        .all()
    )
    session.close()
    return [
        {
            "time":r.time,
            "open":r.open,
            "high":r.high,
            "low":r.low,
            "close":r.close,
            "volume":r.volume
        }
        for r in rows
    ]