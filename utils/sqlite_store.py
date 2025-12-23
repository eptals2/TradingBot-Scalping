# utils/sqlite_store.py (FIXED - Batch Inserts)
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from config import DB_PATH

def init_db():
    """Initialize database with candles and ticks tables"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Candles table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            time TEXT PRIMARY KEY,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            tick_volume INTEGER,
            spread INTEGER,
            real_volume INTEGER,
            bid REAL,
            ask REAL
        )
    """)
    
    # Ticks table - NO PRIMARY KEY on time since multiple ticks can have same timestamp
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT NOT NULL,
            bid REAL,
            ask REAL,
            last REAL,
            volume INTEGER,
            flags INTEGER
        )
    """)
    
    # Create index on time for faster queries
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ticks_time ON ticks(time)
    """)
    
    conn.commit()
    conn.close()


def get_last_candle_time():
    """Get timestamp of last candle"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(time) FROM candles")
    result = cursor.fetchone()[0]
    conn.close()
    return result


def get_last_tick_time():
    """Get timestamp of last tick"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(time) FROM ticks")
    result = cursor.fetchone()[0]
    conn.close()
    return result


def insert_candles(df):
    """Insert candle data into database"""
    if df is None or len(df) == 0:
        return
    
    conn = sqlite3.connect(DB_PATH)
    df = df.copy()
    df['time'] = df['time'].astype(str)
    
    # Batch insert in chunks to avoid "too many variables" error
    chunk_size = 100
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        chunk.to_sql('candles', conn, if_exists='append', index=False)
    
    conn.close()


def insert_ticks(df):
    """Insert tick data into database with batching"""
    if df is None or len(df) == 0:
        return
    
    conn = sqlite3.connect(DB_PATH)
    df = df.copy()
    df['time'] = df['time'].astype(str)
    
    # Drop the id column if it exists (auto-increment will handle it)
    if 'id' in df.columns:
        df = df.drop('id', axis=1)
    
    # SQLite has a limit of 999 variables per query
    # With 6 columns, we can insert ~166 rows at once safely
    # Use 100 to be safe
    chunk_size = 100
    
    total_inserted = 0
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        chunk.to_sql('ticks', conn, if_exists='append', index=False)
        total_inserted += len(chunk)
        
        # Print progress for large inserts
        if len(df) > 1000 and (i + chunk_size) % 1000 == 0:
            print(f"  Inserted {total_inserted}/{len(df)} ticks...")
    
    conn.close()


def get_all_candles():
    """Retrieve all candles from database"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM candles ORDER BY time", conn)
    conn.close()
    return df


def get_all_ticks():
    """Retrieve all ticks from database"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM ticks ORDER BY time, id", conn)
    conn.close()
    return df


def get_candles_range(start_time, end_time):
    """Get candles within time range"""
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM candles WHERE time >= ? AND time <= ? ORDER BY time"
    df = pd.read_sql_query(query, conn, params=(str(start_time), str(end_time)))
    conn.close()
    return df


def get_ticks_range(start_time, end_time):
    """Get ticks within time range"""
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM ticks WHERE time >= ? AND time <= ? ORDER BY time, id"
    df = pd.read_sql_query(query, conn, params=(str(start_time), str(end_time)))
    conn.close()
    return df


def get_recent_candles(n=1000):
    """Get last N candles"""
    conn = sqlite3.connect(DB_PATH)
    query = f"SELECT * FROM candles ORDER BY time DESC LIMIT {n}"
    df = pd.read_sql_query(query, conn)
    df = df.sort_values('time').reset_index(drop=True)
    conn.close()
    return df


def get_recent_ticks(n=10000):
    """Get last N ticks"""
    conn = sqlite3.connect(DB_PATH)
    query = f"SELECT * FROM ticks ORDER BY time DESC, id DESC LIMIT {n}"
    df = pd.read_sql_query(query, conn)
    df = df.sort_values(['time', 'id']).reset_index(drop=True)
    conn.close()
    return df


def cleanup_old_ticks(keep_hours=48):
    """Delete ticks older than X hours to save space"""
    conn = sqlite3.connect(DB_PATH)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=keep_hours)).isoformat()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM ticks WHERE time < ?", (cutoff,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    print(f"Cleaned up {deleted} old ticks")
    return deleted


def get_db_stats():
    """Get database statistics"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM candles")
    candle_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM ticks")
    tick_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(time), MAX(time) FROM candles")
    candle_range = cursor.fetchone()
    
    cursor.execute("SELECT MIN(time), MAX(time) FROM ticks")
    tick_range = cursor.fetchone()
    
    # Get database file size
    cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
    db_size = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'candles': candle_count,
        'ticks': tick_count,
        'candle_range': candle_range,
        'tick_range': tick_range,
        'db_size_mb': round(db_size / (1024 * 1024), 2)
    }