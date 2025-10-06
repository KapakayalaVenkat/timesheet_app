
# timesheet_db_init.py
import os
import sqlite3
from contextlib import closing

DB_PATH = os.environ.get("TS_DB_PATH", "timesheet.db")

schema = """
CREATE TABLE IF NOT EXISTS entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id TEXT NOT NULL,
    date TEXT NOT NULL,
    project_code TEXT NOT NULL,
    man_days REAL NOT NULL,
    activity TEXT NOT NULL,
    remarks TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_date ON entries(date);
CREATE INDEX IF NOT EXISTS idx_employee ON entries(employee_id);
"""

with closing(sqlite3.connect(DB_PATH)) as conn:
    c = conn.cursor()
    for stmt in schema.strip().split(";"):
        if stmt.strip():
            c.execute(stmt)
    conn.commit()
print(f"Initialized DB at {DB_PATH}")
