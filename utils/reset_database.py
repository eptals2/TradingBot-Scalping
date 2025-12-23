# reset_database.py
import os
from config import DB_PATH
from sqlite_store import init_db

# Delete old database
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print(f"Deleted old database: {DB_PATH}")

# Create new one with correct schema
init_db()
print("Created new database with correct schema")