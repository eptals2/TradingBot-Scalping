import sqlite3
import pandas as pd
from config import DB_PATH

def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS candles (
        time TEXT PRIMARY KEY,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        tick_volume INTEGER,
        spread INTEGER,
        real_volume INTEGER
    );
    """)

    conn.commit()
    conn.close()


def get_last_time():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(time) FROM candles")
    last_time = cursor.fetchone()[0]

    conn.close()
    return last_time


def insert_candles(df):
    conn = get_connection()
    df.to_sql("candles", conn, if_exists="append", index=False)
    conn.close()
