
import os
import sqlite3
from contextlib import closing
from datetime import datetime, date
import pandas as pd
import streamlit as st

DB_PATH = os.environ.get("TS_DB_PATH", "timesheet.db")

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    with closing(get_conn()) as conn:
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id TEXT NOT NULL,
            date TEXT NOT NULL,           -- YYYY-MM-DD
            project_code TEXT NOT NULL,
            man_days REAL NOT NULL,
            activity TEXT NOT NULL,
            remarks TEXT,
            created_at TEXT NOT NULL
        )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_date ON entries(date)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_employee ON entries(employee_id)")
        conn.commit()

def insert_entry(row: dict):
    with closing(get_conn()) as conn:
        c = conn.cursor()
        c.execute("""
        INSERT INTO entries (employee_id, date, project_code, man_days, activity, remarks, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            row["employee_id"],
            row["date"],
            row["project_code"],
            float(row["man_days"]),
            row["activity"],
            row.get("remarks", ""),
            datetime.utcnow().isoformat()
        ))
        conn.commit()

def fetch_entries(emp=None, d_from=None, d_to=None, proj=None):
    q = "SELECT id, employee_id, date, project_code, man_days, activity, remarks, created_at FROM entries WHERE 1=1"
    params = []
    if emp:
        q += " AND employee_id = ?"
        params.append(emp)
    if proj:
        q += " AND project_code = ?"
        params.append(proj)
    if d_from:
        q += " AND date >= ?"
        params.append(d_from)
    if d_to:
        q += " AND date <= ?"
        params.append(d_to)
    q += " ORDER BY date DESC, id DESC"
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(q, conn, params=params)
    return df

def delete_entry(entry_id: int):
    with closing(get_conn()) as conn:
        c = conn.cursor()
        c.execute("DELETE FROM entries WHERE id = ?", (entry_id,))
        conn.commit()

def upsert_from_df(df: pd.DataFrame):
    required = ["employee_id", "date", "project_code", "man_days", "activity"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
    # normalize
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["man_days"] = pd.to_numeric(df["man_days"], errors="coerce")
    if df["man_days"].isna().any():
        raise ValueError("Non-numeric values in man_days")
    for _, r in df.iterrows():
        insert_entry(r.to_dict())

st.set_page_config(page_title="Timesheet (Local)", page_icon="🗓️", layout="wide")
st.title("🗓️ Timesheet (Local) — Man-days Utilization")
st.caption("on premise app")

init_db()

with st.sidebar:
    st.header("⚙️ Controls")
    st.write(f"DB: `{DB_PATH}`")
    # Excel uploader for bulk import
    up = st.file_uploader("Bulk import from Excel", type=["xlsx"])
    if up is not None:
        try:
            df_up = pd.read_excel(up)
            upsert_from_df(df_up)
            st.success(f"Imported {len(df_up)} rows.")
        except Exception as e:
            st.error(f"Import failed: {e}")

    st.divider()
    st.subheader("Filters")
    emp = st.text_input("Employee ID (exact)")
    proj = st.text_input("Project Code (exact)")
    col1, col2 = st.columns(2)
    with col1:
        d_from = st.date_input("From date", value=None)
    with col2:
        d_to = st.date_input("To date", value=None)
    if d_from and isinstance(d_from, date):
        d_from = d_from.strftime("%Y-%m-%d")
    else:
        d_from = None
    if d_to and isinstance(d_to, date):
        d_to = d_to.strftime("%Y-%m-%d")
    else:
        d_to = None

st.subheader("➕ Add Entry")
with st.form("add_entry_form", clear_on_submit=True):
    c1, c2, c3 = st.columns(3)
    with c1:
        employee_id = st.text_input("Employee ID", placeholder="E123", max_chars=50)
    with c2:
        dt = st.date_input("Date")
    with c3:
        project_code = st.text_input("Project Code", placeholder="PRJ-1001", max_chars=100)

    c4, c5 = st.columns(2)
    with c4:
        man_days = st.number_input("Man-days", min_value=0.0, step=0.5, value=1.0, format="%.2f")
    with c5:
        activity = st.text_input("Activity", placeholder="Audit - Fieldwork", max_chars=200)
    remarks = st.text_area("Remarks", placeholder="Onsite - Client A", max_chars=500)

    submitted = st.form_submit_button("Save Entry")
    if submitted:
        if not employee_id or not project_code or not activity:
            st.error("Please fill Employee ID, Project Code, and Activity.")
        else:
            row = {
                "employee_id": employee_id.strip(),
                "date": dt.strftime("%Y-%m-%d"),
                "project_code": project_code.strip(),
                "man_days": float(man_days),
                "activity": activity.strip(),
                "remarks": remarks.strip(),
            }
            insert_entry(row)
            st.success("Entry saved.")

st.subheader("📋 Entries")
df = fetch_entries(emp=emp or None, d_from=d_from, d_to=d_to, proj=proj or None)
st.dataframe(df, use_container_width=True, hide_index=True)

st.subheader("📈 Summary")
if df.empty:
    st.info("No data yet.")
else:
    # Summaries
    c1, c2, c3 = st.columns(3)
    with c1:
        by_emp = df.groupby("employee_id")["man_days"].sum().reset_index().rename(columns={"man_days": "total_man_days"})
        st.write("**By Employee**")
        st.dataframe(by_emp, use_container_width=True, hide_index=True)
    with c2:
        by_proj = df.groupby("project_code")["man_days"].sum().reset_index().rename(columns={"man_days": "total_man_days"})
        st.write("**By Project**")
        st.dataframe(by_proj, use_container_width=True, hide_index=True)
    with c3:
        df["month"] = pd.to_datetime(df["date"]).dt.to_period("M").astype(str)
        by_month = df.groupby(["employee_id", "month"])["man_days"].sum().reset_index()
        st.write("**By Employee & Month**")
        st.dataframe(by_month, use_container_width=True, hide_index=True)

    # Export
    st.download_button(
        "⬇️ Export filtered entries as CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="timesheet_export.csv",
        mime="text/csv"
    )
