# ====================================================
# storage.py
# Handles SQLite database connection and insertions
# ====================================================

from sqlalchemy import create_engine, Column, Float, String, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import DB_FILE

# Base class for SQLAlchemy ORM models
Base = declarative_base()

# Table structure for candlesticks
class Candle(Base):
    __tablename__ = "candles"

    # Primary key (autoincrement)
    id = Column(Integer, primary_key=True)

    # XAUUSD, EURUSD, etc.
    symbol = Column(String, index=True)

    # Timeframe, e.g. 1, 5, 15 minutes
    timeframe = Column(String)

    # Timestamp of the candle
    time = Column(DateTime, index=True)

    # OHLCV data
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)

# SQLite connection
engine = create_engine(f"sqlite:///{DB_FILE}", echo=False)

# Session factory for DB operations
SessionLocal = sessionmaker(bind=engine)

# Create table if it doesn’t exist
Base.metadata.create_all(bind=engine)


def save_candles(symbol, timeframe, df):
    """
    Saves candle data from a DataFrame into SQLite.
    Avoids duplicates using symbol + timeframe + time.
    """

    session = SessionLocal()

    for _, row in df.iterrows():

        # Convert pandas timestamp to Python datetime
        candle = Candle(
            symbol=symbol,
            timeframe=timeframe,
            time=row['time'].to_pydatetime(),
            open=row['open'],
            high=row['high'],
            low=row['low'],
            close=row['close'],
            volume=row['volume'],
        )

        # Check if this candle already exists (avoid duplicates)
        exists = session.query(Candle).filter_by(
            symbol=symbol,
            timeframe=timeframe,
            time=candle.time
        ).first()

        # Only insert if not already in DB
        if not exists:
            session.add(candle)

    # Commit all inserts at once (faster)
    session.commit()
    session.close()
