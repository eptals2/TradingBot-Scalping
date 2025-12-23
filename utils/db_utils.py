# db_utils.py
import sqlite3
import pandas as pd

DB_PATH = "./db/market.db"

def get_connection():
    return sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)


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


def insert_candles(df):
    conn = get_connection()
    df.to_sql(
        "candles",
        conn,
        if_exists="append",
        index=False
    )
    conn.close()


def get_last_timestamp():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(time) FROM candles")
    result = cursor.fetchone()[0]

    conn.close()
    return result


def load_candles(limit=None):
    conn = get_connection()
    query = "SELECT * FROM candles ORDER BY time"
    if limit:
        query += f" LIMIT {limit}"

    df = pd.read_sql(query, conn, parse_dates=['time'])
    conn.close()
    return df
