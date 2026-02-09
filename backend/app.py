# app.py
from flask import Flask, render_template, request, session, redirect, url_for, flash, render_template_string
from table_db import get_all_tickets_df, get_invoices_df, update_multiple_fields
from agents.ticket_agent import TicketAIAgent
from agents.chat_agent import ChatAIAgent
from logger_utils import log_chat_interaction
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
import threading

app = Flask(__name__,
            template_folder='../frontend/templates',
            static_folder='../frontend/static')

app.secret_key = "ey_demo_secret_key_2025_super_secret"

# Users file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USERS_FILE = os.path.join(BASE_DIR, "user.json")


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
# Login / Logout
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

    if role == "admin" or role == "manager":
        return redirect(url_for("chat_home"))
    elif role == "employee":
        return redirect(url_for("employee_home"))
    else:
        flash("Invalid role.", "danger")
        return redirect(url_for("logout"))


# ────────────────────────────────────────────────
# Chat UI with AI Agent Integration
# ────────────────────────────────────────────────

@app.route("/", methods=["GET", "POST"])
@app.route("/chat", methods=["GET", "POST"])
def chat_home():
    user = session.get("user")
    if not user:
        return redirect(url_for("login"))

    if "chat_history" not in session:
        session["chat_history"] = []

    chat_history = session["chat_history"]
    error = None

    if request.method == "POST":
        user_msg = request.form.get("msg", "").strip()
        if user_msg:
            try:
                agent = ChatAIAgent(user)
                ai_response, updated_history, total_tokens = agent.run_chat(user_msg, chat_history)

                log_chat_interaction(user, user_msg, ai_response)

                session["chat_history"] = updated_history
                session["last_token_usage"] = total_tokens
                session.modified = True

            except Exception as e:
                print(f"Chat Error: {str(e)}")
                traceback.print_exc()
                flash("Sorry, I encountered an error. Please check your AI configuration.", "danger")
        else:
            flash("Please type a message.", "warning")

    display_history = [
        msg for msg in session.get("chat_history", [])
        if msg.get("role") in ["user", "assistant"] and msg.get("content") and msg["content"] != "None"
    ]

    return render_template("chat.html",
                           sessions=[],
                           current_session="default",
                           chat_history=display_history,
                           token_usage=session.get("last_token_usage", 0),
                           error=error)


@app.route("/new_session")
def new_session():
    session["chat_history"] = []
    session.modified = True
    flash("Chat history cleared.", "success")
    return redirect(url_for("chat_home"))


@app.route("/process_tickets")
def process_tickets():
    if session.get("user", {}).get("role") not in ["admin", "manager"]:
        flash("Access denied.", "danger")
        return redirect(url_for("role_home"))

    def run_agent_job():
        try:
            print("🚀 Starting background ticket processing...")
            agent = TicketAIAgent()
            results = agent.run_on_all_open_tickets()
            print(f"✅ Background processing complete. Processed {len(results)} tickets.")
        except Exception as e:
            print(f"❌ Background processing failed: {e}")
            traceback.print_exc()

    thread = threading.Thread(target=run_agent_job)
    thread.daemon = True
    thread.start()

    flash("AI Agent started processing tickets in the background. Check logs for progress.", "info")
    return redirect(url_for("dashboard"))


# ────────────────────────────────────────────────
# Auto-Assign Open Tickets (balanced workload + detailed summary)
# ────────────────────────────────────────────────

@app.route("/auto_assign_tickets")
def auto_assign_tickets():
    user = session.get("user", {})
    if user.get("role") not in ["manager", "admin"]:
        flash("Only managers and admins can assign tickets.", "danger")
        return redirect(url_for("dashboard"))

    try:
        df = get_all_tickets_df()

        open_tickets = df[df["Ticket Status"].str.lower() == "open"].copy()

        if open_tickets.empty:
            flash("No open tickets to assign.", "info")
            return redirect(url_for("dashboard"))

        users = df["User Name"].dropna().unique().tolist()

        if not users:
            flash("No users available for assignment.", "danger")
            return redirect(url_for("dashboard"))

        # Track assignments for summary
        assignment_summary = {u: 0 for u in users}

        # Current workload
        workload = {}
        for u in users:
            workload[u] = len(df[(df["User Name"] == u) & (df["Ticket Status"].str.lower() == "open")])

        # Sort users by current workload (lowest first)
        sorted_users = sorted(workload, key=workload.get)

        assigned_count = 0
        for i, (_, row) in enumerate(open_tickets.iterrows()):
            user_index = i % len(sorted_users)
            assigned_user = sorted_users[user_index]

            update_multiple_fields(row["Ticket ID"], {
                "User Name": assigned_user,
                # "User ID": ... (add lookup if needed)
            })

            assignment_summary[assigned_user] += 1
            workload[assigned_user] += 1
            assigned_count += 1

        # Build nice summary message
        summary_lines = [f"{user}: {count} ticket{'s' if count != 1 else ''}"
                         for user, count in assignment_summary.items() if count > 0]

        summary_text = f"Auto-assignment complete! Total open tickets assigned: {assigned_count} - " + \
                       " - ".join(summary_lines)

        flash(summary_text, "success")

    except Exception as e:
        flash(f"Auto-assignment failed: {str(e)}", "danger")

    return redirect(url_for("dashboard"))

def auto_assign_single_ticket(ticket_id):
    """
    Assigns a single open ticket to the user with the lowest open-ticket workload
    """
    df = get_all_tickets_df()

    # Get all users
    users = df["User Name"].dropna().unique().tolist()
    if not users:
        return None

    # Calculate current open-ticket workload
    workload = {
        user: len(
            df[
                (df["User Name"] == user) &
                (df["Ticket Status"].str.lower() == "open")
            ]
        )
        for user in users
    }

    # Pick user with lowest workload
    assigned_user = min(workload, key=workload.get)

    # Update ticket
    update_multiple_fields(ticket_id, {
        "User Name": assigned_user
    })

    return assigned_user


# ────────────────────────────────────────────────
# Handle Reopen / Confirm Closed for AI-closed tickets
# ────────────────────────────────────────────────

@app.route("/review_ticket_action/<ticket_id>", methods=["POST"])
def review_ticket_action(ticket_id):
    user = session.get("user", {})
    if user.get("role") not in ["admin", "manager"]:
        flash("Only admins/managers can review tickets.", "danger")
        return redirect(url_for("dashboard"))

    action = request.form.get("action")

    updates = {}
    if action == "reopen":
        updates["Ticket Status"] = "Open"
        updates["Auto Solved"] = False  # Remove from review list
        flash(f"Ticket {ticket_id} has been reopened.", "success")
    elif action == "confirm_closed":
        updates["Auto Solved"] = False  # Remove from review list
        flash(f"Ticket {ticket_id} confirmed closed – won't show again.", "info")
    else:
        flash("Invalid action.", "danger")
        return redirect(url_for("dashboard"))

    success = update_multiple_fields(ticket_id, updates)
    if success:
        print(f"DEBUG: Ticket {ticket_id} updated successfully.")
    else:
        print(f"ERROR: Failed to update ticket {ticket_id}.")
        flash("Failed to update ticket in Excel.", "danger")

    return redirect(url_for("dashboard"))


# ────────────────────────────────────────────────
# Admin Dashboard – Add User
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
                team_val = [t.strip() for t in team.split(",") if t.strip()] if role == "admin" else team

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
# Employee Home
# ────────────────────────────────────────────────

@app.route("/employee_home")
def employee_home():
    if session.get("user", {}).get("role") != "employee":
        flash("Access denied.", "danger")
        return redirect(url_for("role_home"))

    user_name = session["user"]["name"].strip().lower()

    df = get_all_tickets_df()
    my_tickets = df[
        (df["User Name"].str.strip().str.lower() == user_name) &
        (df["Ticket Status"] != "Closed")
        ]

    return render_template("employee_dashboard.html",
                           my_tickets=my_tickets.to_dict(orient='records'),
                           user_name=session["user"]["name"])


# ────────────────────────────────────────────────
# Manager/Admin Dashboard
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

        auto_resolved = 0
        if "Auto Solved" in filtered_tickets.columns:
            auto_resolved = len(filtered_tickets[filtered_tickets["Auto Solved"] == True])

        ap_tickets = len(filtered_tickets[filtered_tickets["Ticket Type"] == "Accounts Payable"])
        ar_tickets = len(filtered_tickets[filtered_tickets["Ticket Type"] == "Accounts Receivable"])

        ticket_rates = {
            "open": round(open_tickets / total_tickets * 100, 1) if total_tickets else 0,
            "closed": round(closed_tickets / total_tickets * 100, 1) if total_tickets else 0,
            "auto_resolved": round(auto_resolved / closed_tickets * 100, 1) if closed_tickets else 0
        }

        # Review tickets: all where Auto Solved is True
        review_tickets = []
        if "Auto Solved" in df_tickets.columns:
            try:
                review_tickets = df_tickets[
                    df_tickets["Auto Solved"] == True
                    ].sort_values("Ticket Closed Date", ascending=False).to_dict(orient="records")
            except Exception as e:
                print(f"Error filtering auto-solved tickets: {e}")

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
                               total_invoices=total_invoices, paid_invoices=paid_invoices,
                               unpaid_invoices=unpaid_invoices,
                               total_amount=total_amount, unpaid_amount=unpaid_amount, overdue_count=overdue_count,
                               pie_img=pie_img, team_img=team_img, inv_pie_img=inv_pie_img,
                               teams=teams, users=users, types=types,
                               selected_team=selected_team, selected_user=selected_user, selected_type=selected_type,
                               review_tickets=review_tickets)

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

import hashlib
import os

def validate_token(ticket_id, token):
    secret = os.getenv("APPROVAL_SECRET", "ey_approval_secret")
    expected = hashlib.sha256(f"{ticket_id}:{secret}".encode()).hexdigest()
    return token == expected


@app.route("/ticket/approve/<ticket_id>")
def approve_ticket(ticket_id):
    token = request.args.get("token")

    if not validate_token(ticket_id, token):
        return "❌ Invalid or expired approval link.", 403

    success = update_multiple_fields(ticket_id, {
        "Ticket Status": "Closed",
        "Auto Solved": False,
        "Admin Review Needed": "No"
    })

    if success:
        return f"""
        <h2>✅ Ticket {ticket_id} Approved</h2>
        <p>The ticket has been successfully closed.</p>
        """
    else:
        return "❌ Failed to update ticket.", 500

@app.route("/ticket/reject/<ticket_id>")
def reject_ticket(ticket_id):
    token = request.args.get("token")

    if not validate_token(ticket_id, token):
        return "❌ Invalid or expired rejection link.", 403

    # Reopen ticket
    success = update_multiple_fields(ticket_id, {
        "Ticket Status": "Open",
        "Auto Solved": False,
        "Admin Review Needed": "No"
    })

    if not success:
        return "❌ Failed to update ticket.", 500

    # 🔥 NEW: auto-assign immediately
    assigned_user = auto_assign_single_ticket(ticket_id)

    if assigned_user:
        return f"""
        <h2>🔄 Ticket {ticket_id} Reopened</h2>
        <p>The ticket has been reopened and automatically assigned to <b>{assigned_user}</b>.</p>
        """
    else:
        return f"""
        <h2>⚠️ Ticket {ticket_id} Reopened</h2>
        <p>The ticket is open but could not be auto-assigned.</p>
        """


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
