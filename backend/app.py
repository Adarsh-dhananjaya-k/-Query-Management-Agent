# app.py
from flask import Flask, render_template, request, session, redirect, url_for, flash, render_template_string
from table_db import get_all_tickets_df, get_invoices_df
from agents.ticket_agent import TicketAIAgent
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
import base64
import traceback
from datetime import datetime
import pandas as pd
import os
import json

app = Flask(__name__, 
            template_folder='../frontend/templates', 
            static_folder='../frontend/static')

app.secret_key = "ey_demo_secret_key_2025_super_secret"

# Users file
USERS_FILE = "user.json"


def load_users():
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            flash("Error reading user.json - file may be corrupted.", "danger")
            return []
    # Default fallback users
    return [
        {"email": "admin@ey.com", "password": "123", "role": "admin", "name": "System Admin"},
        {"email": "manager@ey.com", "password": "123", "role": "manager", "name": "Operations Manager"},
        {"email": "robert@ey.com", "password": "123", "role": "employee", "name": "Robert Brown"}
    ]


def save_users(users):
    with open(USERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(users, f, ensure_ascii=False, indent=2)


def plot_to_img(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', dpi=120)
    buf.seek(0)
    img = base64.b64encode(buf.read()).decode('utf-8')
    buf.close()
    plt.close(fig)
    return img


# ────────────────────────────────────────────────
# Login Page (entry point)
# ────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "").strip()

        users = load_users()
        user = next((u for u in users if u["email"] == email and u["password"] == password), None)

        if user:
            session["user"] = {
                "email": user["email"],
                "name": user["name"],
                "role": user["role"],
                "team": user.get("team")
            }
            flash("Login successful!", "success")
            return redirect(url_for("role_home"))
        else:
            flash("Invalid email or password.", "danger")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out successfully.", "info")
    return redirect(url_for("login"))


@app.before_request
def require_login():
    if request.endpoint in ["login", "static"]:
        return
    if "user" not in session:
        return redirect(url_for("login"))


# ────────────────────────────────────────────────
# Role-based Home Routing
# ────────────────────────────────────────────────

@app.route("/home")
def role_home():
    user = session.get("user", {})
    role = user.get("role")

    if role == "admin":
        return redirect(url_for("admin_dashboard"))
    elif role == "manager":
        return redirect(url_for("chat_home"))
    elif role == "employee":
        return redirect(url_for("employee_home"))
    else:
        flash("Invalid role.", "danger")
        return redirect(url_for("logout"))


# ────────────────────────────────────────────────
# Chat UI (frontend only - no AI backend)
# ────────────────────────────────────────────────

@app.route("/", methods=["GET", "POST"])
@app.route("/chat/<session_id>", methods=["GET", "POST"])
def chat_home(session_id=None):
    # Placeholder for future chat sessions (UI only)
    sessions = []  # No real sessions for now
    current_session = "placeholder"
    chat_history = []  # No real history
    error = None

    if request.method == "POST":
        msg = request.form.get("msg", "").strip()
        if msg:
            # No AI processing - just echo or placeholder response
            flash("Message received (AI backend disconnected for now).", "info")
            # You can add fake responses here later if needed
        else:
            flash("Please type a message.", "warning")

    return render_template("chat.html",
                           sessions=sessions,
                           current_session=current_session,
                           chat_history=chat_history,
                           error=error)


@app.route("/new_session")
def new_session():
    flash("New chat started (AI backend disconnected).", "info")
    return redirect(url_for("chat_home"))


@app.route("/process_tickets")
def process_tickets():
    if session.get("user", {}).get("role") not in ["admin", "manager"]:
        flash("Access denied.", "danger")
        return redirect(url_for("role_home"))
    
    agent = TicketAIAgent()
    results = agent.run_on_all_open_tickets()
    
    flash(f"AI Agent finished processing {len(results)} tickets.", "success")
    return redirect(url_for("dashboard"))


# ────────────────────────────────────────────────
# Admin Dashboard – Add User (no team)
# ────────────────────────────────────────────────

@app.route("/admin_dashboard", methods=["GET", "POST"])
def admin_dashboard():
    if session.get("user", {}).get("role") != "admin":
        flash("Access denied. Admin only.", "danger")
        return redirect(url_for("role_home"))

    users = load_users()

    if request.method == "POST":
        action = request.form.get("action")
        if action == "add_user":
            email = request.form.get("email", "").strip()
            password = request.form.get("password", "").strip()
            name = request.form.get("name", "").strip()
            role = request.form.get("role", "")
            team = request.form.get("team", "").strip()

            if not all([email, password, name, role, team]):
                flash("All fields are required.", "danger")
            elif any(u["email"] == email for u in users):
                flash("Email already exists.", "danger")
            else:
                # Handle team as list for admins, string for others
                if role == "admin":
                    team_val = [t.strip() for t in team.split(",") if t.strip()]
                else:
                    team_val = team

                new_user = {
                    "email": email,
                    "password": password,
                    "name": name,
                    "role": role,
                    "team": team_val
                }

                users.append(new_user)
                save_users(users)
                flash(f"User '{name}' ({role}) added successfully!", "success")

    return render_template("admin_dashboard.html", users=users)


# ────────────────────────────────────────────────
# Manager Home (redirects to dashboard)
# ────────────────────────────────────────────────

@app.route("/manager_home")
def manager_home():
    if session.get("user", {}).get("role") != "manager":
        flash("Access denied.", "danger")
        return redirect(url_for("role_home"))

    return redirect(url_for("dashboard"))


# ────────────────────────────────────────────────
# Employee Home – All open tickets by name (no team filter)
# ────────────────────────────────────────────────

@app.route("/employee_home")
def employee_home():
    if session.get("user", {}).get("role") != "employee":
        flash("Access denied.", "danger")
        return redirect(url_for("role_home"))

    user_name = session["user"]["name"].strip().lower()

    df = get_all_tickets_df()
    # Filter only by name, ignore team
    my_tickets = df[
        (df["User Name"].str.strip().str.lower() == user_name) &
        (df["Ticket Status"] != "Closed")
    ]

    return render_template("employee_dashboard.html", 
                           my_tickets=my_tickets.to_dict(orient='records'),
                           user_name=session["user"]["name"])


# ────────────────────────────────────────────────
# Full Dashboard (manager/admin access only)
# ────────────────────────────────────────────────

@app.route("/dashboard")
def dashboard():
    user = session.get("user", {})
    if user.get("role") not in ["manager", "admin"]:
        flash("Access denied. Dashboard is for managers and admins only.", "danger")
        return redirect(url_for("role_home"))

    try:
        df_tickets = get_all_tickets_df()
        df_invoices = get_invoices_df()

        selected_team = request.args.get("team_filter", "").strip()
        selected_user = request.args.get("user_filter", "").strip()
        selected_type = request.args.get("type_filter", "").strip()

        filtered_tickets = df_tickets.copy()
        if selected_team:
            filtered_tickets = filtered_tickets[filtered_tickets["Assigned Team"].str.upper() == selected_team.upper()]
        if selected_user:
            filtered_tickets = filtered_tickets[filtered_tickets["User Name"].str.title() == selected_user.title()]
        if selected_type in ["Accounts Payable", "Accounts Receivable"]:
            filtered_tickets = filtered_tickets[filtered_tickets["Ticket Type"] == selected_type]

        total_tickets = len(filtered_tickets)
        open_tickets = len(filtered_tickets[filtered_tickets["Ticket Status"] != "Closed"])
        closed_tickets = len(filtered_tickets[filtered_tickets["Ticket Status"] == "Closed"])

        auto_resolved = len(filtered_tickets[filtered_tickets["Auto Resolved"] == True]) if "Auto Resolved" in filtered_tickets.columns else 0

        ap_tickets = len(filtered_tickets[filtered_tickets["Ticket Type"] == "Accounts Payable"])
        ar_tickets = len(filtered_tickets[filtered_tickets["Ticket Type"] == "Accounts Receivable"])

        ticket_rates = {
            "open": round(open_tickets / total_tickets * 100, 1) if total_tickets else 0,
            "closed": round(closed_tickets / total_tickets * 100, 1) if total_tickets else 0,
            "auto_resolved": round(auto_resolved / closed_tickets * 100, 1) if closed_tickets else 0
        }

        total_invoices = len(df_invoices)
        unpaid_invoices = len(df_invoices[df_invoices.get("Payment Status", "") == "Unpaid"])
        paid_invoices = len(df_invoices[df_invoices.get("Payment Status", "") == "Paid"])
        total_amount = df_invoices["Invoice Amount"].sum()
        unpaid_amount = df_invoices[df_invoices["Payment Status"] == "Unpaid"]["Invoice Amount"].sum()

        today = datetime.now()
        overdue = df_invoices[
            (df_invoices["Payment Status"] == "Unpaid") &
            (df_invoices["Due Date"].notna()) &
            (df_invoices["Due Date"] < today)
        ]
        overdue_count = len(overdue)

        teams = sorted(df_tickets["Assigned Team"].dropna().unique())
        users = sorted(df_tickets["User Name"].dropna().unique())
        types = ["Accounts Payable", "Accounts Receivable"]

        status_counts = filtered_tickets["Ticket Status"].value_counts()
        fig1, ax1 = plt.subplots(figsize=(5, 5))
        ax1.pie(status_counts, labels=status_counts.index, autopct='%1.1f%%', startangle=90)
        ax1.set_title("Ticket Status (filtered)")
        pie_img = plot_to_img(fig1)

        team_type = filtered_tickets.groupby(["Assigned Team", "Ticket Type"]).size().unstack(fill_value=0)
        fig2, ax2 = plt.subplots(figsize=(7, 5))
        team_type.plot(kind="bar", ax=ax2, color=["#1f77b4", "#ff7f0e"])
        ax2.set_title("Tickets by Team & Type (filtered)")
        ax2.set_ylabel("Count")
        plt.xticks(rotation=45)
        team_img = plot_to_img(fig2)

        inv_status_counts = df_invoices["Payment Status"].value_counts()
        fig3, ax3 = plt.subplots(figsize=(5, 5))
        ax3.pie(inv_status_counts, labels=inv_status_counts.index, autopct='%1.1f%%', startangle=90)
        ax3.set_title("Invoice Payment Status (all)")
        inv_pie_img = plot_to_img(fig3)

        return render_template("manager_dashboard.html",
                               total_tickets=total_tickets, open_tickets=open_tickets, closed_tickets=closed_tickets,
                               auto_resolved=auto_resolved, ap_tickets=ap_tickets, ar_tickets=ar_tickets,
                               ticket_rates=ticket_rates,
                               total_invoices=total_invoices, paid_invoices=paid_invoices, unpaid_invoices=unpaid_invoices,
                               total_amount=total_amount, unpaid_amount=unpaid_amount, overdue_count=overdue_count,
                               pie_img=pie_img, team_img=team_img, inv_pie_img=inv_pie_img,
                               teams=teams, users=users, types=types,
                               selected_team=selected_team, selected_user=selected_user, selected_type=selected_type)

    except Exception as e:
        return render_template_string("""
        <div class="container my-5 text-center">
            <div class="alert alert-danger">
                <h4>Dashboard Error</h4>
                <p>{{ error }}</p>
                <pre>{{ traceback }}</pre>
                <a href="{{ url_for('chat_home') }}" class="btn btn-outline-light mt-3">← Back to Chat</a>
            </div>
        </div>
        """, error=str(e), traceback=traceback.format_exc())


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
