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

