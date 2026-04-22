import sqlite3

conn = sqlite3.connect("database.db")
cursor = conn.cursor()

# ---------------- SAFE COLUMN ADD FUNCTION ---------------- #

def column_exists(table, column):
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [col[1] for col in cursor.fetchall()]
    return column in columns

# ---------------- CREATE TABLES ---------------- #

cursor.executescript("""
CREATE TABLE IF NOT EXISTS chat_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    phone TEXT,
    branch_id INTEGER,
    state TEXT,
    context TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS invoices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    franchise_id INTEGER,
    month TEXT,
    total_messages INTEGER,
    extra_messages INTEGER,
    amount REAL,
    created_at TEXT
);
""")

# ---------------- SAFE ALTERS ---------------- #

if not column_exists("branches", "daily_capacity"):
    cursor.execute("ALTER TABLE branches ADD COLUMN daily_capacity INTEGER DEFAULT 10")

if not column_exists("bookings", "price"):
    cursor.execute("ALTER TABLE bookings ADD COLUMN price REAL DEFAULT 0")

if not column_exists("service_prices", "cost_price"):
    cursor.execute("ALTER TABLE service_prices ADD COLUMN cost_price REAL DEFAULT 0")

conn.commit()
conn.close()

print("Database updated safely ✅")
