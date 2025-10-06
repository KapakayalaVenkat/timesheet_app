#!/usr/bin/env python3
"""
Utilization Calculator from Excel
---------------------------------
Reads an Excel sheet of time entries and computes utilization per person over a chosen period.

Expected columns (case-insensitive; flexible names allowed):
- Person (e.g., "Name", "Employee", "Resource")
- Date   (e.g., "Date", "Work Date", "EntryDate")
- Hours  (e.g., "Hours", "Hrs", "Time Spent")
Optional:
- CapacityHours (daily or period capacity per person). If not provided, we estimate using work_days * work_hours_per_day.
- Project (for breakdowns; optional)

Usage:
    python utilization_tool.py --input timesheet.xlsx --sheet "Sheet1" --period monthly --work-hours-per-day 8 --output utilization_report.xlsx

You can also supply a mapping if your column names are different:
    python utilization_tool.py --input timesheet.xlsx --person-col "Employee" --date-col "Work Date" --hours-col "Hrs"

Author: ChatGPT
"""
import argparse
import sys
from pathlib import Path
import pandas as pd
import numpy as np

FALLBACK_PERSON_COLS = ["person", "name", "employee", "resource", "assignee"]
FALLBACK_DATE_COLS   = ["date", "work date", "entrydate", "entry date", "day"]
FALLBACK_HOURS_COLS  = ["hours", "hrs", "time spent", "time_spent"]
FALLBACK_CAP_COLS    = ["capacityhours", "capacity", "dailycapacity", "capacity_hours"]
POSSIBLE_PROJECT_COLS= ["project", "project name", "client", "task", "assignment"]

def find_col(df, preferred, fallbacks):
    if preferred:
        for c in df.columns:
            if c.strip().lower() == preferred.strip().lower():
                return c
    # try soft match
    lowmap = {c.lower(): c for c in df.columns}
    for cand in fallbacks:
        if cand in lowmap:
            return lowmap[cand]
    # try contains match
    for c in df.columns:
        lc = c.lower()
        if any(cand in lc for cand in fallbacks):
            return c
    return None

def parse_args():
    p = argparse.ArgumentParser(description="Compute utilization per person from Excel timesheets.")
    p.add_argument("--input", required=True, help="Input Excel file (.xlsx/.xls)")
    p.add_argument("--sheet", default=None, help="Worksheet name or index (0-based)")
    p.add_argument("--output", default="utilization_report.xlsx", help="Output Excel file")
    p.add_argument("--period", default="monthly", choices=["daily","weekly","monthly","quarterly","yearly"], help="Aggregation period")
    p.add_argument("--timezone", default=None, help="(Optional) Not used; placeholder for future")
    p.add_argument("--person-col", default=None, help="Exact column name for person")
    p.add_argument("--date-col", default=None, help="Exact column name for date")
    p.add_argument("--hours-col", default=None, help="Exact column name for hours")
    p.add_argument("--capacity-col", default=None, help="Exact column name for capacity (per-day or per-period)")
    p.add_argument("--project-col", default=None, help="Exact column name for project")
    p.add_argument("--work-hours-per-day", type=float, default=8.0, help="Working hours per day if capacity not provided")
    p.add_argument("--week-start", default="MON", choices=["SUN","MON"], help="Week start for weekly aggregation")
    p.add_argument("--holidays", default=None, help="(Optional) CSV of holiday dates (YYYY-MM-DD) to exclude from capacity calc")
    p.add_argument("--debug", action="store_true", help="Print extra logs")
    return p.parse_args()

def load_holidays(path):
    if not path:
        return set()
    s = pd.read_csv(path, header=None).iloc[:,0]
    try:
        dates = pd.to_datetime(s, errors="coerce").dropna().dt.normalize()
    except Exception:
        dates = pd.to_datetime(s.astype(str), errors="coerce").dropna().dt.normalize()
    return set(dates.tolist())

def main():
    args = parse_args()
    in_file = Path(args.input)
    if not in_file.exists():
        print(f"ERROR: Input file not found: {in_file}", file=sys.stderr)
        sys.exit(1)

    # Read Excel
    try:
        if args.sheet is None:
            df = pd.read_excel(in_file)
        else:
            try:
                sheet_arg = int(args.sheet)
            except (ValueError, TypeError):
                sheet_arg = args.sheet
            df = pd.read_excel(in_file, sheet_name=sheet_arg)
    except Exception as e:
        print(f"ERROR reading Excel: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(df, pd.DataFrame):
        # If multiple sheets returned, take the first
        df = list(df.values())[0]

    # Trim column names
    df.columns = [str(c).strip() for c in df.columns]

    # Identify columns
    person_col = find_col(df, args.person_col, FALLBACK_PERSON_COLS)
    date_col   = find_col(df, args.date_col,   FALLBACK_DATE_COLS)
    hours_col  = find_col(df, args.hours_col,  FALLBACK_HOURS_COLS)
    cap_col    = find_col(df, args.capacity_col, FALLBACK_CAP_COLS)
    project_col= find_col(df, args.project_col, POSSIBLE_PROJECT_COLS)

    required = {"person": person_col, "date": date_col, "hours": hours_col}
    missing = [k for k,v in required.items() if v is None]
    if missing:
        print(f"ERROR: Could not detect required column(s): {', '.join(missing)}", file=sys.stderr)
        print("Columns in file:", list(df.columns), file=sys.stderr)
        sys.exit(2)

    # Coerce types
    df = df.copy()
    df[date_col]  = pd.to_datetime(df[date_col], errors="coerce")
    df[hours_col] = pd.to_numeric(df[hours_col], errors="coerce")
    df = df.dropna(subset=[date_col, hours_col, person_col])
    df = df[df[hours_col] >= 0]

    if args.debug:
        print("Detected columns:", {"person": person_col, "date": date_col, "hours": hours_col, "capacity": cap_col, "project": project_col})

    # Normalize text columns
    df[person_col] = df[person_col].astype(str).str.strip()

    # Set week anchor
    week_anchor = "W-MON" if args.week_start == "MON" else "W-SUN"

    # Build period key
    if args.period == "daily":
        df["__period"] = df[date_col].dt.to_period("D").dt.start_time
    elif args.period == "weekly":
        # align to chosen week start
        # Convert to week period and then to start time
        # Pandas doesn't have W-SUN/W-MON in to_period, so we floor by week using to_offset
        df["__period"] = df[date_col].dt.to_period(week_anchor).dt.start_time
    elif args.period == "monthly":
        df["__period"] = df[date_col].dt.to_period("M").dt.start_time
    elif args.period == "quarterly":
        df["__period"] = df[date_col].dt.to_period("Q").dt.start_time
    elif args.period == "yearly":
        df["__period"] = df[date_col].dt.to_period("Y").dt.start_time
    else:
        raise ValueError("Unknown period")

    # Aggregate hours per person per period
    grouped = df.groupby([person_col, "__period"])[hours_col].sum().reset_index(name="BookedHours")

    # Capacity calculation
    holidays = load_holidays(args.holidays)
    # Create a calendar of working days per person & period
    # If explicit capacity column exists:
    if cap_col is not None:
        # We assume the capacity column can be per-row (daily or per-entry). We'll sum it per person-period.
        df_cap = df.copy()
        df_cap["__cap"] = pd.to_numeric(df_cap[cap_col], errors="coerce")
        df_cap = df_cap.dropna(subset=["__cap"])
        cap = df_cap.groupby([person_col, "__period"])["__cap"].sum().reset_index(name="CapacityHours")
    else:
        # Derive capacity = (# working days in period) * work_hours_per_day
        # Build a range per person-period to count weekdays minus holidays
        persons = grouped[person_col].unique()
        periods = grouped["__period"].unique()
        rows = []
        for person in persons:
            for pstart in periods:
                # determine p end based on period freq by looking at next period start
                # Estimate end as next period start - 1 day; we'll infer freq from args.period
                if args.period == "daily":
                    pend = pd.Timestamp(pstart) + pd.offsets.Day(1) - pd.offsets.Day(0)
                elif args.period == "weekly":
                    pend = pd.Timestamp(pstart) + pd.offsets.Week(weekday=6) + pd.offsets.Day(1)  # end = start + 7 days
                elif args.period == "monthly":
                    pend = (pd.Timestamp(pstart) + pd.offsets.MonthBegin(1)) + pd.offsets.MonthEnd(0)
                elif args.period == "quarterly":
                    pend = (pd.Timestamp(pstart) + pd.offsets.QuarterBegin(startingMonth=1, n=1)) + pd.offsets.Day(-1)
                elif args.period == "yearly":
                    pend = (pd.Timestamp(pstart) + pd.offsets.YearBegin(1)) + pd.offsets.Day(-1)
                else:
                    pend = pd.Timestamp(pstart)

                # Count business days between pstart and pend inclusive
                rng = pd.date_range(pd.Timestamp(pstart), pend, freq="D")
                workdays = [d for d in rng if d.weekday() < 5 and d.normalize() not in holidays]
                capacity_hours = len(workdays) * float(args.work_hours_per_day)
                rows.append({person_col: person, "__period": pstart, "CapacityHours": capacity_hours})
        cap = pd.DataFrame(rows)

    # Merge and compute utilization
    out = pd.merge(grouped, cap, on=[person_col, "__period"], how="left")
    # avoid divide by zero
    out["CapacityHours"] = out["CapacityHours"].replace(0, np.nan)
    out["Utilization"] = (out["BookedHours"] / out["CapacityHours"]) * 100.0
    out["Utilization"] = out["Utilization"].round(2)

    # Optional project breakdown
    by_project = None
    if project_col is not None:
        gp = df.groupby([person_col, "__period", project_col])[hours_col].sum().reset_index(name="BookedHours")
        by_project = gp.sort_values([person_col, "__period", "BookedHours"], ascending=[True, True, False])

    # Pretty column names
    out = out.rename(columns={person_col: "Person", "__period": "PeriodStart"})
    if by_project is not None:
        by_project = by_project.rename(columns={person_col: "Person", "__period": "PeriodStart", project_col: "Project"})

    # Write to Excel (two sheets)
    with pd.ExcelWriter(args.output, engine="xlsxwriter") as writer:
        out.sort_values(["Person","PeriodStart"]).to_excel(writer, sheet_name="Utilization", index=False)
        if by_project is not None:
            by_project.to_excel(writer, sheet_name="ByProject", index=False)
        # summary pivot
        pivot = out.pivot_table(index="Person", values=["BookedHours","CapacityHours","Utilization"], aggfunc={"BookedHours":"sum","CapacityHours":"sum","Utilization":"mean"}).reset_index()
        pivot = pivot.rename(columns={"Utilization":"AvgUtilization(%)"})
        pivot.to_excel(writer, sheet_name="Summary", index=False)

    print(f"Success! Wrote: {args.output}")
    print("Sheets: Utilization, ByProject (optional), Summary")
    return 0

if __name__ == "__main__":
    sys.exit(main())
