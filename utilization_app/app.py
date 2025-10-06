
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime, timedelta
import re

st.set_page_config(page_title="Utilization & Availability", layout="wide")

st.title("🧭 Utilization & Availability Dashboard (Localhost)")
st.caption("Upload your Excel and get instant utilization, availability, and project view for each person.")

with st.sidebar:
    st.header("⚙️ Settings")
    mode = st.radio("Data format", ["Risk Projects List Parser (beta)", "Tidy Timesheet (Person/Date/Hours)"])
    work_hours_per_day = st.number_input("Work hours per day (fallback)", min_value=1.0, max_value=24.0, value=8.0, step=0.5)
    week_start = st.selectbox("Week starts on", ["MON", "SUN"])
    st.markdown("---")
    st.markdown("**Export** options will appear after processing.")

uploaded = st.file_uploader("Upload Excel file (.xlsx/.xls)", type=["xlsx", "xls"])

@st.cache_data(show_spinner=False)
def load_excel(file):
    try:
        xl = pd.ExcelFile(file)
        return xl.sheet_names
    except Exception as e:
        st.error(f"Failed to read Excel: {e}")
        return []

def parse_week_label_to_period(week_label: str):
    # Try to parse labels like "Week 4, July 2025"
    if not isinstance(week_label, str):
        return None
    m = re.search(r"Week\s*(\d+)\s*,\s*([A-Za-z]+)\s*(\d{4})", week_label)
    if not m:
        return None
    wnum = int(m.group(1))
    month_str = m.group(2)
    year = int(m.group(3))
    try:
        month = datetime.strptime(month_str, "%B").month
    except ValueError:
        try:
            month = datetime.strptime(month_str, "%b").month
        except ValueError:
            return None
    # Approximate: take the first day of month, find week starts (Mon), pick the wnum-th
    first = datetime(year, month, 1)
    # Align to Monday
    delta = (first.weekday() - 0) % 7  # 0=Mon
    first_mon = first - timedelta(days=delta) if delta != 0 else first
    start = first_mon + timedelta(weeks=wnum-1)
    return start.date()

def detect_person_columns(df: pd.DataFrame):
    # Heuristic: top row contains person names (non-null strings) far to the right
    top = df.iloc[0]
    persons = {}
    for c in df.columns:
        val = top[c]
        if isinstance(val, str) and val.strip() and 'week' not in val.lower() and 'utilization' not in val.lower():
            persons[c] = val.strip()
    return persons  # mapping col -> person_name

def parse_risk_projects_sheet(df: pd.DataFrame):
    # We expect a header row (~0) with person names in some columns.
    person_cols = detect_person_columns(df)
    if not person_cols:
        st.warning("Couldn't detect person columns from the first row. Please check the sheet layout.")
        return pd.DataFrame()

    records = []
    current_week = None

    # We look row by row; when we find a 'Week' label under any person column, we update current_week for that row block.
    for i in range(1, len(df)):
        row = df.iloc[i]
        # If any value in the row contains 'Week', treat it as the current week label
        values = [str(v) for v in row.values if isinstance(v, (str,))]
        week_hits = [v for v in values if 'week' in v.lower()]
        if week_hits:
            # choose the longest/first
            current_week = sorted(week_hits, key=len, reverse=True)[0]
        # For every person column, try to capture metrics
        for col, person in person_cols.items():
            cell = row[col]
            # capture metric names in the same column (strings)
            if isinstance(cell, str):
                cl = cell.lower()
                # Look ahead to next few rows for a numeric value in same column
                def next_numeric(start_idx):
                    for j in range(start_idx+1, min(start_idx+6, len(df))):
                        val = df.iloc[j][col]
                        try:
                            num = float(val)
                            return num
                        except Exception:
                            continue
                    return None

                if 'planned' in cl and 'utilization' in cl:
                    val = next_numeric(i)
                    records.append({"Person": person, "WeekLabel": current_week, "Metric": "PlannedUtilization%", "Value": val})
                elif 'actual' in cl and 'utilization' in cl:
                    val = next_numeric(i)
                    records.append({"Person": person, "WeekLabel": current_week, "Metric": "ActualUtilization%", "Value": val})
                elif 'planned' in cl and 'hours' in cl:
                    val = next_numeric(i)
                    records.append({"Person": person, "WeekLabel": current_week, "Metric": "PlannedHours", "Value": val})
                elif 'actual' in cl and 'hours' in cl:
                    val = next_numeric(i)
                    records.append({"Person": person, "WeekLabel": current_week, "Metric": "ActualHours", "Value": val})

    if not records:
        st.warning("Parsed 0 records. The sheet may have a different layout than expected.")
        return pd.DataFrame()

    tall = pd.DataFrame(records)
    # pivot wider
    wide = tall.pivot_table(index=["Person","WeekLabel"], columns="Metric", values="Value", aggfunc="first").reset_index()
    # derive availability & utilization where possible
    if "ActualHours" in wide.columns and "PlannedHours" in wide.columns:
        wide["AvailabilityHours"] = wide["PlannedHours"] - wide["ActualHours"]
    # Prefer ActualUtilization% if present; else compute from hours if both exist
    if "ActualUtilization%" in wide.columns:
        wide["Utilization%"] = wide["ActualUtilization%"]
    elif "ActualHours" in wide.columns and "PlannedHours" in wide.columns:
        wide["Utilization%"] = (wide["ActualHours"] / wide["PlannedHours"]).replace([np.inf, -np.inf], np.nan) * 100.0

    # Convert WeekLabel to PeriodStart date if possible
    wide["PeriodStart"] = wide["WeekLabel"].apply(parse_week_label_to_period)
    return wide

def tidy_timesheet_pipeline(df: pd.DataFrame, person_col, date_col, hours_col, project_col=None, work_hours_per_day=8.0, week_start="MON"):
    # type conversions
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df[hours_col] = pd.to_numeric(df[hours_col], errors="coerce")
    df = df.dropna(subset=[person_col, date_col, hours_col])
    df[person_col] = df[person_col].astype(str).str.strip()
    # group weekly
    anchor = "W-MON" if week_start=="MON" else "W-SUN"
    df["PeriodStart"] = df[date_col].dt.to_period(anchor).dt.start_time
    agg = df.groupby([person_col, "PeriodStart"])[hours_col].sum().reset_index(name="ActualHours")
    # capacity from workdays * work_hours_per_day per week (5 workdays by default)
    # For each week, assume 5 workdays
    agg["PlannedHours"] = 5 * float(work_hours_per_day)
    agg["AvailabilityHours"] = agg["PlannedHours"] - agg["ActualHours"]
    agg["Utilization%"] = (agg["ActualHours"] / agg["PlannedHours"]) * 100.0
    agg = agg.rename(columns={person_col: "Person"})
    return agg

def df_to_excel_bytes(dfs: dict):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for name, df in dfs.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    return output.getvalue()

if uploaded:
    sheets = load_excel(uploaded)
    if not sheets:
        st.stop()

    default_sheet = None
    for candidate in ["Risk Projects List - 2025", "Utilization Dashboard (in %)", "Sheet1"]:
        if candidate in sheets:
            default_sheet = candidate
            break
    if default_sheet is None: default_sheet = sheets[0]

    st.write("**Detected sheets:**", ", ".join(sheets))
    sel_sheet = st.selectbox("Choose sheet to parse", sheets, index=sheets.index(default_sheet))

    df = pd.read_excel(uploaded, sheet_name=sel_sheet, header=0)

    if mode == "Risk Projects List Parser (beta)":
        st.info("Attempting to parse the wide-format 'Risk Projects List - 2025' layout.")
        parsed = parse_risk_projects_sheet(df)
        if parsed.empty:
            st.stop()

        # Filters
        people = sorted(parsed["Person"].dropna().unique().tolist())
        sel_people = st.multiselect("Filter people", options=people, default=people)
        view = parsed[parsed["Person"].isin(sel_people)].copy()

        # Ensure expected columns exist even if not parsed
        for _col in ["ActualHours","PlannedHours","Utilization%"]:
            if _col not in view.columns:
                view[_col] = np.nan

        # Summary by person (robust to missing columns)
        agg_dict = {}
        if "ActualHours" in view.columns:
            agg_dict["TotalActualHours"] = ("ActualHours","sum")
        if "PlannedHours" in view.columns:
            agg_dict["TotalPlannedHours"] = ("PlannedHours","sum")
        if "Utilization%" in view.columns:
            agg_dict["AvgUtilizationPct"] = ("Utilization%","mean")

        if not agg_dict:
            sum_person = pd.DataFrame({"Person": sorted(view["Person"].unique().tolist())})
        else:
            sum_person = view.groupby("Person").agg(**agg_dict).reset_index()

        # Show tables
        st.subheader("Weekly view")
        st.dataframe(view.sort_values(["Person", "PeriodStart", "WeekLabel"]))

        st.subheader("Summary by person")
        st.dataframe(sum_person.sort_values("Person"))

        # Export
        xbytes = df_to_excel_bytes({"Weekly": view, "Summary": sum_person})
        st.download_button("⬇️ Download report (Excel)", data=xbytes, file_name="utilization_report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    else:
        st.success("Tidy Timesheet mode: map your columns and compute weekly utilization.")
        with st.expander("Column mapping"):
            person_col = st.selectbox("Person column", options=df.columns)
            date_col = st.selectbox("Date column", options=df.columns)
            hours_col = st.selectbox("Hours column", options=df.columns)
            project_col = st.selectbox("Project column (optional)", options=["<none>"] + list(df.columns), index=0)
            if project_col == "<none>":
                project_col = None

        out = tidy_timesheet_pipeline(df, person_col, date_col, hours_col, project_col, work_hours_per_day, week_start)
        st.subheader("Weekly utilization")
        st.dataframe(out.sort_values(["Person", "PeriodStart"]))

        per_person = out.groupby("Person").agg(
            TotalActualHours=("ActualHours", "sum"),
            TotalPlannedHours=("PlannedHours", "sum"),
            AvgUtilizationPct=("Utilization%", "mean")
        ).reset_index()
        st.subheader("Summary by person")
        st.dataframe(per_person.sort_values("Person"))

        # Export
        xbytes = df_to_excel_bytes({"Weekly": out, "Summary": per_person})
        st.download_button("⬇️ Download report (Excel)", data=xbytes, file_name="utilization_report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
else:
    st.info("Upload an Excel file to begin.")
