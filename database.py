import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "database.db")

db = sqlite3.connect(db_path)

# USERS
db.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE,
    password TEXT,
    branch TEXT,
    role TEXT
)
""")

# BOOKINGS
db.execute("""
CREATE TABLE IF NOT EXISTS bookings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT,
    surname TEXT,
    phone TEXT,
    make TEXT,
    model TEXT,
    service TEXT,
    date TEXT,
    branch TEXT,
    status TEXT,
    work_to_be_done TEXT,
    source TEXT,
    quote_declined TEXT
)
""")

# ADMIN
db.execute("""
INSERT OR IGNORE INTO users (username, password, branch, role)
VALUES ('admin', '1234', 'ALL', 'admin')
""")

# SAMPLE USER
db.execute("""
INSERT OR IGNORE INTO users (username, password, branch, role)
VALUES ('silverton', '1234', 'Silverton', 'staff')
""")

db.commit()
db.close()

print("✅ Database ready")
