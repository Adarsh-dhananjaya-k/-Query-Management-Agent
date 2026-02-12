# table_db.py
"""
Database utilities for the Query Management Agent.
Handles reading from and writing to QMT Data New.xlsx (multi-sheet Excel file).
"""

import os
import json
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

# Ensure .env (for QMT_EXCEL_PATH) is loaded
load_dotenv()

# Project root relative path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_FILE = os.path.join(BASE_DIR, "data", "QMT Data New.xlsx")
CONFIGURED_FILE = os.getenv("QMT_EXCEL_PATH")

if CONFIGURED_FILE:
    candidate = os.path.expanduser(CONFIGURED_FILE)
    if not os.path.isabs(candidate):
        candidate = os.path.join(BASE_DIR, candidate)
    FILE = os.path.abspath(candidate)
else:
    FILE = os.path.abspath(DEFAULT_FILE)

AUTO_STATUS_AUTO_RESOLVED = "AI Auto-Resolved"
AUTO_STATUS_MANUAL_REVIEW = "AI Attempted - Manual Review"
AUTO_STATUS_MANAGER_REVIEWED = "Manager Reviewed"

if not os.path.exists(FILE):
    raise FileNotFoundError(f"Excel file not found at: {FILE}")


def get_all_tickets_df(sheet_name="Tickets"):
    """
    Load the Tickets sheet into a DataFrame with proper date handling.
    """
    try:
        df = pd.read_excel(FILE, sheet_name=sheet_name, engine="openpyxl")
        
        # Standardize key identifier columns as string
        for col in ["Ticket ID", "User ID", "User Name", "Assigned Team", "Ticket Type"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # Handle Excel serial dates
        date_cols = ["Creation Date", "Ticket Closed Date", "Ticket Updated Date"]
        origin = pd.Timestamp("1899-12-30")

        for col in date_cols:
            if col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = origin + pd.to_timedelta(df[col], unit='D')
                else:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

        return df

    except Exception as e:
        raise RuntimeError(f"Failed to load sheet '{sheet_name}': {str(e)}")


def get_invoices_df():
    """
    Load the Invoice sheet with proper date and string handling.
    """
    try:
        df = pd.read_excel(FILE, sheet_name="Invoice", engine="openpyxl")

        if "Invoice Number" in df.columns:
            df["Invoice Number"] = df["Invoice Number"].astype(str).str.strip()

        # Handle date columns
        date_cols = ["Invoice Date", "Due Date", "Clearing Date", "Posting Date", "Document Date"]
        origin = pd.Timestamp("1899-12-30")

        for col in date_cols:
            if col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = origin + pd.to_timedelta(df[col], unit='D')
                else:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

        return df

    except Exception as e:
        raise RuntimeError(f"Failed to load Invoice sheet: {str(e)}")


def save_tickets_df(df, sheet_name="Tickets"):
    """
    Save DataFrame back to the Excel file (overwrites sheet).
    """
    try:
        with pd.ExcelWriter(FILE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        # print(f"Saved changes to sheet '{sheet_name}' successfully.")
        return True
    except Exception as e:
        print(f"Failed to save DataFrame to '{sheet_name}': {str(e)}")
        return False


def ensure_required_columns(df):
    """
    Ensure important utility columns exist (with defaults).
    """
    defaults = {
        "Ticket Updated Date": pd.NA,
        "Auto Solved": False,
        "AI Response": "",
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


def search_invoices(params: dict):
    """
    Dynamic search in invoices based on provided key-value filters.
    """
    try:
        df = get_invoices_df()
        mask = pd.Series(True, index=df.index)

        for key, value in params.items():
            if value and key in df.columns:
                if isinstance(value, str):
                    mask &= df[key].astype(str).str.contains(value.strip(), case=False, na=False)
                else:
                    mask &= (df[key] == value)

        results = df[mask].copy()

        # Prepare for JSON serialization
        for col in results.columns:
            if pd.api.types.is_datetime64_any_dtype(results[col]):
                results[col] = results[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            elif pd.api.types.is_float_dtype(results[col]):
                results[col] = results[col].where(results[col].notna(), None)

        return results.to_dict(orient='records')

    except Exception as e:
        print(f"Invoice search failed: {str(e)}")
        return []


def update_multiple_fields(ticket_id: str, updates: dict) -> bool:
    """
    Update multiple fields for a given ticket ID.
    """
    try:
        df = get_all_tickets_df()
        df = ensure_required_columns(df)

        # Normalize ticket ID comparison
        df["Ticket ID"] = df["Ticket ID"].astype(str).str.strip()
        ticket_id = str(ticket_id).strip()

        mask = df["Ticket ID"] == ticket_id
        if not mask.any():
            print(f"Ticket {ticket_id} not found.")
            return False

        # Optional field name mapping for compatibility
        field_map = {
            "Team Name": "Assigned Team",
            "Person Name": "User Name",
            "Person ID": "User ID",
            "Ticket Create Date": "Creation Date",
        }

        for orig_field, value in updates.items():
            real_field = field_map.get(orig_field, orig_field)
            if real_field in df.columns:
                col = df[real_field]
                # If column is boolean but we plan to store non-boolean text (e.g. status enums),
                # widen the dtype to object first to avoid FutureWarning/incompatible assignment.
                if pd.api.types.is_bool_dtype(col.dtype) and not isinstance(value, (bool, type(pd.NA))):
                    df[real_field] = col.astype("object")
                df.loc[mask, real_field] = value

        # Always update timestamp
        if "Ticket Updated Date" in df.columns:
            df.loc[mask, "Ticket Updated Date"] = datetime.now().strftime("%Y-%m-%d")

        return save_tickets_df(df)

    except Exception as e:
        print(f"Failed to update ticket {ticket_id}: {str(e)}")
        return False


def update_ticket(ticket_id: str, field: str, value: any) -> bool:
    """Single-field update (backward compatibility)."""
    return update_multiple_fields(ticket_id, {field: value})


def add_auto_solved_flag(ticket_id: str, is_auto: bool = True) -> bool:
    """Convenience function to mark ticket as auto-solved."""
    return update_ticket(ticket_id, "Auto Solved", is_auto)


def get_team_list(team_name=None):
    """
    Get unique teams or employees in a team.
    """
    try:
        df = get_all_tickets_df()
        if team_name:
            if isinstance(team_name, (list, tuple)):
                teams_low = [str(t).strip().lower() for t in team_name]
                mask = df["Assigned Team"].str.lower().isin(teams_low)
            else:
                team_clean = str(team_name).strip().lower()
                mask = df["Assigned Team"].str.lower().str.contains(team_clean, na=False)
            return df[mask]["User Name"].dropna().unique().tolist()
        else:
            return df["Assigned Team"].dropna().unique().tolist()
    except Exception as e:
        print(f"Error in get_team_list: {str(e)}")
        return []


def get_kpi_metrics(team_name=None):
    """
    Calculate key performance indicators.
    """
    try:
        df = get_all_tickets_df()
        df = ensure_required_columns(df)

        if team_name:
            if isinstance(team_name, (list, tuple)):
                teams_low = [str(t).strip().lower() for t in team_name]
                df = df[df["Assigned Team"].str.lower().isin(teams_low)]
            else:
                team_clean = str(team_name).strip().lower()
                df = df[df["Assigned Team"].str.lower().str.contains(team_clean, na=False)]

        metrics = {
            "Total Tickets": len(df),
            "Tickets By Type": df.get("Ticket Type", pd.Series()).value_counts().to_dict(),
            "Auto Solved Count": int(df["Auto Solved"].sum()),
            "Tickets By Employee": df.get("User Name", pd.Series()).value_counts().to_dict(),
            "Status Distribution": df.get("Ticket Status", pd.Series()).value_counts().to_dict(),
        }

        # Avg resolution time in hours
        closed = df[df["Ticket Status"].str.lower() == "closed"].copy()
        if not closed.empty and "Creation Date" in closed and "Ticket Closed Date" in closed:
            closed["Creation Date"] = pd.to_datetime(closed["Creation Date"], errors='coerce')
            closed["Ticket Closed Date"] = pd.to_datetime(closed["Ticket Closed Date"], errors='coerce')
            hours = (closed["Ticket Closed Date"] - closed["Creation Date"]).dt.total_seconds() / 3600
            metrics["Avg Resolution Time (Hours)"] = round(hours.mean(), 2) if not hours.isna().all() else 0
        else:
            metrics["Avg Resolution Time (Hours)"] = 0

        return metrics

    except Exception as e:
        print(f"KPI calculation failed: {str(e)}")
        return {}


def intelligent_assign_tickets(team_name=None):
    """
    Auto-assign open unassigned tickets to employees with lowest current load.
    """
    try:
        df = get_all_tickets_df()
        df = ensure_required_columns(df)

        # Load users
        users_path = os.path.join(BASE_DIR, "user.json")
        with open(users_path, "r", encoding="utf-8") as f:
            users = json.load(f)

        # Collect eligible employees
        employees = []
        for user in users:
            if str(user.get("role", "")).lower() == "employee":
                team = user.get("team", "")
                if not team_name:
                    employees.append(user["name"])
                elif isinstance(team, list):
                    if any(team_name.lower() in str(t).lower() for t in team):
                        employees.append(user["name"])
                elif team_name.lower() in str(team).lower():
                    employees.append(user["name"])

        if not employees:
            return {"status": "error", "message": f"No employees found for team '{team_name or 'ALL'}'"}

        # Find unassigned open tickets
        unassigned_mask = (
            (df["Ticket Status"].str.lower() == "open") &
            (df["User Name"].isna() | df["User Name"].astype(str).str.lower().isin(["", "nan", "none", "unknown", "unassigned"]))
        )

        if team_name:
            unassigned_mask &= df["Assigned Team"].str.lower().str.contains(team_name.lower(), na=False)

        unassigned_indices = df[unassigned_mask].index.tolist()

        if not unassigned_indices:
            return {"status": "success", "message": "No unassigned open tickets.", "assigned_count": 0}

        # Calculate current open ticket load per employee
        workload = {emp: 0 for emp in employees}
        for emp in employees:
            workload[emp] = len(df[(df["User Name"].str.lower() == emp.lower()) & (df["Ticket Status"].str.lower() == "open")])

        # Assign tickets round-robin style (lowest load first)
        assignments = 0
        for idx in unassigned_indices:
            target = min(workload, key=workload.get)
            df.at[idx, "User Name"] = target
            df.at[idx, "Ticket Updated Date"] = datetime.now().strftime("%Y-%m-%d")
            workload[target] += 1
            assignments += 1

        save_tickets_df(df)

        return {
            "status": "success",
            "message": f"Assigned {assignments} tickets.",
            "assigned_count": assignments,
            "new_workload": workload
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"Assignment failed: {str(e)}"}


# Quick test
if __name__ == "__main__":
    print("Testing table_db.py ...")
    try:
        df = get_all_tickets_df()
        print(f"Tickets loaded: {len(df)} rows")
        print("Columns:", df.columns.tolist()[:8], "...")
    except Exception as e:
        print("Test failed:", str(e))
