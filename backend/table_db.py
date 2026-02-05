# table_db.py
"""
Database utilities for the Query Management Agent.
Handles reading from and writing to QMT Data New.xlsx (multi-sheet Excel file).
"""

import pandas as pd
import os
from datetime import datetime

FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "QMT Data New.xlsx")


def get_all_tickets_df(sheet_name="Tickets"):
    """
    Load the Tickets sheet into a DataFrame.
    Properly handles Excel serial dates → datetime conversion only when needed.
    """
    if not os.path.exists(FILE):
        # Check parent dir if not found (optional, but keep it simple for now as it's in the same folder)
        raise FileNotFoundError(f"{FILE} not found. Please place the file in the working directory.")

    try:
        print(f"DEBUG: Loading sheet '{sheet_name}' from {FILE}...")
        df = pd.read_excel(FILE, sheet_name=sheet_name, engine="openpyxl")
        print(f"DEBUG: Successfully loaded {len(df)} rows.")
        
        # Ensure key columns are string type for reliable matching
        for col in ["Ticket ID", "User ID", "User Name", "Assigned Team", "Ticket Type"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        
        print(f"DEBUG: Columns available: {df.columns.tolist()}")
        
        # Handle Excel serial dates → datetime only if column is still numeric
        date_cols = ["Creation Date", "Ticket Closed Date", "Ticket Updated Date"]
        origin = pd.Timestamp("1899-12-30")
        
        for col in date_cols:
            if col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    # Raw Excel serial numbers → apply origin correction
                    df[col] = origin + pd.to_timedelta(df[col], unit='D')
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    # Already parsed as datetime by read_excel → keep as is
                    pass
                else:
                    # Fallback: try to convert (strings, mixed types, etc.)
                    df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
    
    except Exception as e:
        raise RuntimeError(f"Failed to load sheet '{sheet_name}': {str(e)}")


def get_invoices_df():
    """
    Load the Invoice sheet into a DataFrame.
    Properly handles Excel serial dates → datetime conversion only when needed.
    """
    if not os.path.exists(FILE):
        raise FileNotFoundError(f"{FILE} not found.")
    
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
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    pass
                else:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
    
    except Exception as e:
        raise RuntimeError(f"Failed to load Invoice sheet: {str(e)}")


def save_tickets_df(df, sheet_name="Tickets"):
    """
    Save the DataFrame back to the specified sheet in the Excel file.
    Overwrites the sheet if it exists.
    """
    try:
        with pd.ExcelWriter(FILE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"Saved changes to sheet '{sheet_name}' successfully.")
    except Exception as e:
        print(f"Failed to save DataFrame: {str(e)}")


def ensure_required_columns(df):
    """
    Add missing utility columns if they don't exist.
    """
    if "Ticket Updated Date" not in df.columns:
        df["Ticket Updated Date"] = pd.NA
    if "Auto Solved" not in df.columns:
        df["Auto Solved"] = False
    if "AI Response" not in df.columns:
        df["AI Response"] = ""
    return df


def search_invoices(params: dict):
    """
    Search invoices based on dynamic parameters.
    """
    try:
        print(f"DEBUG: search_invoices called with params: {params}")
        df = get_invoices_df()
        for key, value in params.items():
            if key in df.columns:
                print(f"DEBUG: Filtering by {key} = {value}")
                if isinstance(value, str):
                    df = df[df[key].astype(str).str.contains(value, case=False, na=False)]
                else:
                    df = df[df[key] == value]
        # Convert Timestamps to strings for JSON serialization
        results = df.to_dict(orient='records')
        print(f"DEBUG: Found {len(results)} matching invoices.")
        for row in results:
            for k, v in row.items():
                if hasattr(v, 'isoformat'): # Catch Timestamp/datetime
                    row[k] = v.isoformat()
                elif isinstance(v, float) and pd.isna(v):
                    row[k] = None
        return results
    except Exception as e:
        print(f"Invoice search failed: {str(e)}")
        return []


def update_multiple_fields(ticket_id: str, updates: dict) -> bool:
    """
    Update multiple fields for a ticket in one go.
    """
    try:
        df = get_all_tickets_df()
        df = ensure_required_columns(df)
        search_id = str(ticket_id).strip()
        # Robust comparison: handle string IDs and numerical IDs from Excel
        mask = df["Ticket ID"].astype(str).str.strip() == search_id
        
        if not mask.any():
            return False

        # Field mapping for backward compatibility
        field_map = {
            "Team Name":        "Assigned Team",
            "Person Name":      "User Name",
            "Person ID":        "User ID",
            "Ticket Create Date": "Creation Date",
            "Ticket Closed Date": "Ticket Closed Date",
            "Ticket Updated Date": "Ticket Updated Date",
            "Ticket Status":    "Ticket Status",
            "Ticket Priority":  "Priority",
        }

        for field, value in updates.items():
            real_field = field_map.get(field, field)
            if real_field in df.columns:
                df.loc[mask, real_field] = value
        
        df.loc[mask, "Ticket Updated Date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_tickets_df(df)
        return True
    except Exception as e:
        print(f"Multi-update failed: {str(e)}")
        return False


def update_ticket(ticket_id: str, field: str, value: any) -> bool:
    """
    Backward compatible single field update.
    """
    return update_multiple_fields(ticket_id, {field: value})


def add_auto_solved_flag(ticket_id: str, is_auto: bool = True) -> bool:
    return update_ticket(ticket_id, "Auto Solved", is_auto)


def get_team_list(team_name: any = None) -> list:
    """
    Returns a list of unique team names or employees within a team/teams.
    """
    try:
        df = get_all_tickets_df()
        if team_name:
            if isinstance(team_name, list):
                teams_low = [str(t).strip().lower() for t in team_name]
                print(f"DEBUG: Searching for employees in teams: {teams_low}")
                mask = df["Assigned Team"].str.lower().isin(teams_low)
            else:
                team_name_clean = str(team_name).strip().lower()
                print(f"DEBUG: Searching for employees in team matching: '{team_name_clean}'")
                mask = df["Assigned Team"].str.lower().str.contains(team_name_clean, na=False)
            
            matches = df[mask]["User Name"].unique().tolist()
            print(f"DEBUG: Found {len(matches)} employees.")
            return matches
        else:
            teams = df["Assigned Team"].unique().tolist()
            return teams
    except Exception as e:
        print(f"ERROR in get_team_list: {str(e)}")
        return []


def get_kpi_metrics(team_name: str = None) -> dict:
    """
    Calculate KPIs for reports.
    - Total tickets (AP & AR)
    - Auto resolved count
    - Handled by employee
    - Avg resolution time
    - Open vs Closed
    """
    df = get_all_tickets_df()
    df = ensure_required_columns(df)
    
    if team_name:
        if isinstance(team_name, list):
            teams_low = [str(t).strip().lower() for t in team_name]
            print(f"DEBUG: Calculating KPIs for teams: {teams_low}")
            df = df[df["Assigned Team"].str.lower().isin(teams_low)]
        else:
            team_name_clean = str(team_name).strip().lower()
            print(f"DEBUG: Calculating KPIs for team matching: '{team_name_clean}'")
            df = df[df["Assigned Team"].str.lower().str.contains(team_name_clean, na=False)]
    else:
        print("DEBUG: Calculating KPIs for ALL teams.")

    metrics = {}
    
    # 1. Total tickets by type
    type_counts = df["Ticket Type"].value_counts().to_dict()
    metrics["Total Tickets"] = len(df)
    metrics["Tickets By Type"] = type_counts

    # 2. Auto Resolved
    metrics["Auto Solved Count"] = int(df["Auto Solved"].sum())

    # 3. Handled by each employee
    metrics["Tickets By Employee"] = df["User Name"].value_counts().to_dict()

    # 4. Open vs Closed
    metrics["Status Distribution"] = df["Ticket Status"].value_counts().to_dict()

    # 5. Avg Resolution Time (Creation to Closed)
    closed_df = df[df["Ticket Status"] == "Closed"].copy()
    if not closed_df.empty and "Creation Date" in closed_df.columns and "Ticket Closed Date" in closed_df.columns:
        # Ensure dates are datetime
        closed_df["Creation Date"] = pd.to_datetime(closed_df["Creation Date"], errors='coerce')
        closed_df["Ticket Closed Date"] = pd.to_datetime(closed_df["Ticket Closed Date"], errors='coerce')
        
        # Calculate duration in hours
        durations = (closed_df["Ticket Closed Date"] - closed_df["Creation Date"]).dt.total_seconds() / 3600
        metrics["Avg Resolution Time (Hours)"] = round(durations.mean(), 2) if not durations.dropna().empty else 0
    else:
        metrics["Avg Resolution Time (Hours)"] = 0

    return metrics

def intelligent_assign_tickets(team_name: str = None) -> dict:
    """
    Automatically assigns unassigned Open tickets to employees to balance workload.
    1. Finds all Open tickets that are unassigned (User Name is empty/None).
    2. Gets list of employees for the team.
    3. Checks current 'Open' ticket count for each employee.
    4. Assigns unassigned tickets to employees with the least workload.
    """
    try:
        from datetime import datetime
        df = get_all_tickets_df()
        df = ensure_required_columns(df)
        
        # 1. Load users to find available employees
        users_file = os.path.join(os.path.dirname(__file__), "user.json")
        with open(users_file, "r", encoding="utf-8") as f:
            all_users = json.load(f)
        
        # Filter for employees in the specific team
        employees = []
        for u in all_users:
            if str(u.get("role")).lower() == "employee":
                u_team = u.get("team", "")
                if not team_name:
                    employees.append(u["name"])
                elif isinstance(u_team, list):
                    if any(team_name.lower() in str(t).lower() for t in u_team):
                        employees.append(u["name"])
                elif team_name.lower() in str(u_team).lower():
                    employees.append(u["name"])
        
        if not employees:
            return {"status": "error", "message": f"No employees found for team '{team_name or 'ALL'}'"}

        # 2. Filter for Open and Unassigned tickets
        # Unassigned = User Name is empty, "nan", "None", or "Unknown"
        unassigned_mask = (
            (df["Ticket Status"].str.lower() == "open") & 
            (df["User Name"].isna() | (df["User Name"].astype(str).str.lower().isin(["", "nan", "none", "unknown", "unassigned", "default"])))
        )
        
        if team_name:
            unassigned_mask &= df["Assigned Team"].str.lower().str.contains(team_name.lower(), na=False)
            
        unassigned_df = df[unassigned_mask]
        unassigned_indices = unassigned_df.index.tolist()
        
        if not unassigned_indices:
            return {"status": "success", "message": "No unassigned open tickets found.", "assigned_count": 0}

        # 3. Calculate current workload (# of Open tickets) for these employees
        workload = {}
        for emp in employees:
            count = len(df[(df["User Name"].str.lower() == emp.lower()) & (df["Ticket Status"].str.lower() == "open")])
            workload[emp] = count
            
        # 4. Assign tickets
        assignments_made = 0
        for idx in unassigned_indices:
            # Pick employee with minimum workload
            target_emp = min(workload, key=workload.get)
            df.at[idx, "User Name"] = target_emp
            df.at[idx, "Ticket Updated Date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            workload[target_emp] += 1
            assignments_made += 1
            
        save_tickets_df(df)
        
        return {
            "status": "success", 
            "message": f"Successfully assigned {assignments_made} tickets.",
            "assigned_count": assignments_made,
            "new_workload": workload
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"Intelligent assignment failed: {str(e)}"}
