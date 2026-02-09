import json
import os

# Define path to user.json relative to this file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USERS_FILE = os.path.join(BASE_DIR, "user.json")

def load_users():
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Error reading {USERS_FILE}")
            return []
    return []

def get_manager_by_team(team_name):
    """
    Finds a manager for the given team string (e.g. 'AP Team' or 'AR').
    Returns dict with 'name' and 'email' or None.
    """
    users = load_users()
    if not team_name:
        return None
    
    # Normalize query
    team_query = team_name.lower().strip()
    
    for user in users:
        if user.get("role") == "manager":
            user_teams = user.get("team", [])
            
            # team can be a list or a string
            if isinstance(user_teams, list):
                # Check if any team matches
                if any(t.lower() in team_query or team_query in t.lower() for t in user_teams):
                    return user
            else:
                # String comparison
                t_str = str(user_teams).lower()
                if t_str in team_query or team_query in t_str:
                    return user
                    
    return None

def get_user_email_by_name(user_name):
    users = load_users()
    for user in users:
        if user.get("name", "").lower() == str(user_name).lower():
            return user.get("email")
    return None
