
# bulk_import.py
import os
import sqlite3
from contextlib import closing
from datetime import datetime
import argparse
import pandas as pd

DB_PATH = os.environ.get("TS_DB_PATH", "timesheet.db")

def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.executescript("""
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
        """)
        conn.commit()

def import_excel(path: str):
    df = pd.read_excel(path)
    required = ["employee_id", "date", "project_code", "man_days", "activity"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["man_days"] = pd.to_numeric(df["man_days"], errors="coerce")
    if df["man_days"].isna().any():
        raise ValueError("Non-numeric values in man_days")

    with closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        rows = 0
        for _, r in df.iterrows():
            c.execute("""
                INSERT INTO entries (employee_id, date, project_code, man_days, activity, remarks, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                str(r["employee_id"]).strip(),
                r["date"],
                str(r["project_code"]).strip(),
                float(r["man_days"]),
                str(r["activity"]).strip(),
                "" if "remarks" not in r or pd.isna(r["remarks"]) else str(r["remarks"]).strip(),
                datetime.utcnow().isoformat()
            ))
            rows += 1
        conn.commit()
    return rows

def main():
    ap = argparse.ArgumentParser(description="Bulk import timesheet rows from Excel")
    ap.add_argument("excel_path", help="Path to Excel file")
    args = ap.parse_args()

    init_db()
    count = import_excel(args.excel_path)
    print(f"Imported {count} rows into {DB_PATH}")

if __name__ == "__main__":
    main()
