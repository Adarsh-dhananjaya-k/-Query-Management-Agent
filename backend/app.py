
from flask import Flask, render_template, request, session, redirect, url_for, flash, render_template_string
from table_db import (
    AUTO_STATUS_AUTO_RESOLVED,
    AUTO_STATUS_MANAGER_REVIEWED,
    get_all_tickets_df,
    get_invoices_df,
    update_multiple_fields,
)
from agents.ticket_agent import TicketAIAgent, send_requester_resolution_email
from agents.chat_agent import ChatAIAgent
from logger_utils import log_chat_interaction
import matplotlib

matplotlib.use('Agg')  # Non-interactive backend for server
import matplotlib.pyplot as plt
import io
import base64
import traceback
from datetime import datetime
import pandas as pd
import os
import json
import threading
import hashlib

app = Flask(__name__,
            template_folder='../frontend/templates',
            static_folder='../frontend/static')

app.secret_key = "ey_demo_secret_key_2025_super_secret"

# Users file path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USERS_FILE = os.path.join(BASE_DIR, "user.json")
CHAT_HISTORY_FILE = os.path.join(BASE_DIR, "chat_history.json")


def _load_all_histories():
    if os.path.exists(CHAT_HISTORY_FILE):
        try:
            with open(CHAT_HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}


def get_user_chat_history(user_email):
    histories = _load_all_histories()
    return histories.get(user_email, [])


def save_user_chat_history(user_email, history):
    histories = _load_all_histories()
    histories[user_email] = history
    with open(CHAT_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(histories, f, ensure_ascii=False, indent=2)


def load_users():
    """Load users from user.json or return fallback defaults."""
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            flash("Error reading user.json - file may be corrupted.", "danger")
            return []
    return [
        {"email": "admin@ey.com", "password": "123", "role": "admin", "name": "System Admin"},
        {"email": "manager@ey.com", "password": "123", "role": "manager", "name": "Operations Manager"},
        {"email": "robert@ey.com", "password": "123", "role": "employee", "name": "Robert Brown"}
    ]


def save_users(users):
    """Save users list back to user.json."""
    try:
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Failed to save users: {e}")


def fetch_ticket_record(ticket_id: str):
    df = get_all_tickets_df()
    mask = df["Ticket ID"].astype(str).str.strip() == str(ticket_id).strip()
    if not mask.any():
        return None
    return df[mask].iloc[0].to_dict()


def plot_to_img(fig):
    """Convert matplotlib figure to base64 PNG string."""
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
        user = next((u for u in users if u.get("email") == email and u.get("password") == password), None)

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
    role = user.get("role", "").lower()

    if role in ["admin", "manager"]:
        return redirect(url_for("chat_home"))
    elif role == "employee":
        return redirect(url_for("employee_home"))
    else:
        flash("Invalid or missing role.", "danger")
        return redirect(url_for("logout"))


# ────────────────────────────────────────────────
# Chat UI with AI Agent
# ────────────────────────────────────────────────

@app.route("/", methods=["GET", "POST"])
@app.route("/chat", methods=["GET", "POST"])
def chat_home():
    user = session.get("user")
    if not user:
        return redirect(url_for("login"))

    chat_history = get_user_chat_history(user["email"])
    error = None

    if request.method == "POST":
        user_msg = request.form.get("msg", "").strip()
        if user_msg:
            try:
                agent = ChatAIAgent(user)
                ai_response, updated_history, total_tokens = agent.run_chat(user_msg, chat_history)

                log_chat_interaction(user, user_msg, ai_response)

                save_user_chat_history(user["email"], updated_history)
                session["last_token_usage"] = total_tokens
                session.modified = True

            except Exception as e:
                print(f"Chat processing error: {str(e)}")
                traceback.print_exc()
                flash("Sorry, an error occurred while processing your message.", "danger")
        else:
            flash("Please enter a message.", "warning")

    display_history = [
        msg for msg in get_user_chat_history(user["email"])
        if msg.get("role") in ["user", "assistant"]
           and msg.get("content")
           and msg["content"] not in ["None", ""]
    ]

    return render_template("chat.html",
                           sessions=[],
                           current_session="default",
                           chat_history=display_history,
                           token_usage=session.get("last_token_usage", 0),
                           error=error)


@app.route("/new_session")
def new_session():
    user = session.get("user")
    if user:
        save_user_chat_history(user["email"], [])
    flash("New chat session started.", "success")
    return redirect(url_for("chat_home"))


# ────────────────────────────────────────────────
# Background Ticket Processing
# ────────────────────────────────────────────────

@app.route("/process_tickets")
def process_tickets():
    if session.get("user", {}).get("role") not in ["admin", "manager"]:
        flash("Only admins and managers can trigger ticket processing.", "danger")
        return redirect(url_for("role_home"))

    def run_agent_job():
        try:
            print("Starting background AI ticket processing...")
            agent = TicketAIAgent()
            results = agent.run_on_all_open_tickets()
            print(f"Ticket processing complete. Processed {len(results)} tickets.")
        except Exception as e:
            print(f"Background ticket processing failed: {e}")
            traceback.print_exc()

    thread = threading.Thread(target=run_agent_job, daemon=True)
    thread.start()

    flash("AI ticket processing started in the background.", "info")
    return redirect(url_for("dashboard"))


# ────────────────────────────────────────────────
# Auto-Assign Tickets
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
            flash("No open tickets available to assign.", "info")
            return redirect(url_for("dashboard"))

        assignees = df["User Name"].dropna().unique().tolist()
        if not assignees:
            flash("No users available for assignment.", "danger")
            return redirect(url_for("dashboard"))

        workload = {u: len(df[(df["User Name"] == u) & (df["Ticket Status"].str.lower() == "open")]) for u in assignees}
        sorted_users = sorted(workload, key=workload.get)

        assignment_summary = {u: 0 for u in assignees}
        assigned_count = 0

        for i, (_, row) in enumerate(open_tickets.iterrows()):
            assignee = sorted_users[i % len(sorted_users)]
            update_multiple_fields(row["Ticket ID"], {"User Name": assignee})
            assignment_summary[assignee] += 1
            workload[assignee] += 1
            assigned_count += 1

        summary = ", ".join([f"{u}: {c}" for u, c in assignment_summary.items() if c > 0])
        flash(f"Auto-assignment complete! {assigned_count} tickets assigned. ({summary})", "success")

    except Exception as e:
        flash(f"Auto-assignment failed: {str(e)}", "danger")

    return redirect(url_for("dashboard"))


def auto_assign_single_ticket(ticket_id):
    try:
        df = get_all_tickets_df()
        users = df["User Name"].dropna().unique().tolist()
        if not users:
            return None

        workload = {
            user: len(df[(df["User Name"] == user) & (df["Ticket Status"].str.lower() == "open")])
            for user in users
        }

        if not workload:
            return None

        assigned_user = min(workload, key=workload.get)
        success = update_multiple_fields(ticket_id, {"User Name": assigned_user})

        return assigned_user if success else None

    except Exception as e:
        print(f"Single ticket auto-assign failed: {e}")
        return None


# ────────────────────────────────────────────────
# Manager/Admin Review of AI-resolved Tickets (from dashboard)
# ────────────────────────────────────────────────

@app.route("/review_ticket_action/<ticket_id>", methods=["POST"])
def review_ticket_action(ticket_id):
    if session.get("user", {}).get("role") not in ["manager", "admin"]:
        flash("Only managers/admins can review tickets.", "danger")
        return redirect(url_for("dashboard"))

    action = request.form.get("action")

    updates = {}
    message = ""

    if action == "confirm_closed":
        updates["Auto Solved"] = False
        updates["Admin Review Needed"] = "No"
        message = f"Ticket {ticket_id} confirmed closed – no longer pending review."
    elif action == "reopen":
        updates["Ticket Status"] = "Open"
        updates["Auto Solved"] = False
        updates["Admin Review Needed"] = "No"
        message = f"Ticket {ticket_id} has been reopened."
        
        assigned_user = auto_assign_single_ticket(ticket_id)
        if assigned_user:
            message += f" Auto-assigned to {assigned_user}."
        else:
            message += " Could not auto-assign at this time."
    else:
        flash("Invalid action selected.", "danger")
        return redirect(url_for("dashboard"))

    success = update_multiple_fields(ticket_id, updates)

    if success:
        flash(message, "success")
    else:
        flash(f"Failed to update ticket {ticket_id} in the database.", "danger")

    return redirect(url_for("dashboard"))


# ────────────────────────────────────────────────
# Admin Dashboard – User Management
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

            if not all([email, password, name, role]):
                flash("Required fields missing.", "danger")
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
                flash(f"User '{name}' ({role}) added successfully.", "success")

    return render_template("admin_dashboard.html", users=users)


# ────────────────────────────────────────────────
# Employee Dashboard
# ────────────────────────────────────────────────

@app.route("/employee_home")
def employee_home():
    user = session.get("user", {})
    if user.get("role") != "employee":
        flash("Access denied.", "danger")
        return redirect(url_for("role_home"))

    user_name = user["name"].strip().lower()

    df = get_all_tickets_df()
    my_tickets = df[
        (df["User Name"].str.strip().str.lower() == user_name) &
        (df["Ticket Status"] != "Closed")
    ]

    return render_template("employee_dashboard.html",
                           my_tickets=my_tickets.to_dict(orient='records'),
                           user_name=user["name"])


# ────────────────────────────────────────────────
# Manager / Admin Dashboard (Main KPI View)
# ────────────────────────────────────────────────

@app.route("/dashboard")
def dashboard():
    user = session.get("user", {})
    if user.get("role") not in ["manager", "admin"]:
        flash("Dashboard access restricted to managers and admins.", "danger")
        return redirect(url_for("role_home"))

    try:
        df_tickets = get_all_tickets_df()
        df_invoices = get_invoices_df()

        if "Invoice Amount" in df_invoices.columns:
            df_invoices["Invoice Amount"] = pd.to_numeric(df_invoices["Invoice Amount"], errors="coerce")
        else:
            df_invoices["Invoice Amount"] = 0.0

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
            status_series = filtered_tickets["Auto Solved"].astype(str).str.strip()
            auto_resolved = int(status_series.isin([AUTO_STATUS_AUTO_RESOLVED, AUTO_STATUS_MANAGER_REVIEWED]).sum())

        ap_tickets = len(filtered_tickets[filtered_tickets["Ticket Type"] == "Accounts Payable"])
        ar_tickets = len(filtered_tickets[filtered_tickets["Ticket Type"] == "Accounts Receivable"])

        ticket_rates = {
            "open": round(open_tickets / total_tickets * 100, 1) if total_tickets else 0,
            "closed": round(closed_tickets / total_tickets * 100, 1) if total_tickets else 0,
            "auto_resolved": round(auto_resolved / closed_tickets * 100, 1) if closed_tickets else 0
        }

        review_tickets = []
        if "Auto Solved" in df_tickets.columns:
            review_tickets = df_tickets[
                df_tickets["Auto Solved"] == True
            ].sort_values("Ticket Updated Date", ascending=False).to_dict(orient="records")

        total_invoices = len(df_invoices)
        paid_invoices = len(df_invoices[df_invoices.get("Payment Status", "") == "Paid"])
        unpaid_invoices = len(df_invoices[df_invoices.get("Payment Status", "") == "Unpaid"])
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
        ax2.set_title("Tickets by Team & Type")
        ax2.set_ylabel("Count")
        plt.xticks(rotation=45)
        team_img = plot_to_img(fig2)

        # ───── Invoice status – horizontal bar chart (recommended long-term solution) ─────
        inv_status_counts = df_invoices["Payment Status"].value_counts().sort_values(ascending=True)

        fig3, ax3 = plt.subplots(figsize=(7.2, max(4, len(inv_status_counts) * 0.55)))  # adaptive height

        bars = ax3.barh(
            inv_status_counts.index,
            inv_status_counts.values,
            color='#4e79a7',          # professional blue
            edgecolor='white',
            height=0.72
        )

        # Add count labels on the bars
        for bar in bars:
            width = bar.get_width()
            ax3.text(
                width + max(inv_status_counts.max() * 0.015, 1),
                bar.get_y() + bar.get_height()/2,
                f'{int(width):,}' if width >= 10 else f'{width}',
                va='center',
                fontsize=10,
                fontweight='medium'
            )

        # Optional: percentage labels further right
        total = inv_status_counts.sum()
        for i, v in enumerate(inv_status_counts.values):
            pct = v / total * 100
            ax3.text(
                v + max(inv_status_counts.max() * 0.06, 3),
                i,
                f'{pct:.1f}%',
                va='center',
                fontsize=9,
                color='#555',
                fontweight='normal'
            )

        ax3.set_title("Invoice Payment Status", fontsize=13, pad=15, fontweight='semibold')
        ax3.set_xlabel("Number of Invoices", fontsize=11)
        ax3.grid(axis='x', linestyle='--', alpha=0.5, zorder=0)
        ax3.set_axisbelow(True)
        ax3.tick_params(axis='y', labelsize=10)
        plt.tight_layout()

        inv_pie_img = plot_to_img(fig3)

        return render_template("manager_dashboard.html",
                               total_tickets=total_tickets, open_tickets=open_tickets, closed_tickets=closed_tickets,
                               auto_resolved=auto_resolved, ap_tickets=ap_tickets, ar_tickets=ar_tickets,
                               ticket_rates=ticket_rates,
                               total_invoices=total_invoices, paid_invoices=paid_invoices,
                               unpaid_invoices=unpaid_invoices, total_amount=total_amount,
                               unpaid_amount=unpaid_amount, overdue_count=overdue_count,
                               pie_img=pie_img, team_img=team_img, inv_pie_img=inv_pie_img,
                               teams=teams, users=users, types=types,
                               selected_team=selected_team, selected_user=selected_user, selected_type=selected_type,
                               review_tickets=review_tickets)

    except Exception as e:
        print(f"Dashboard rendering error: {str(e)}")
        traceback.print_exc()
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

    ticket_record = fetch_ticket_record(ticket_id)
    if not ticket_record:
        return "❌ Ticket not found.", 404

    success = update_multiple_fields(ticket_id, {
        "Ticket Status": "Closed",
        "Auto Solved": AUTO_STATUS_MANAGER_REVIEWED,
        "Admin Review Needed": "No",
        "Ticket Closed Date": datetime.now().strftime("%Y-%m-%d")
    })

    if success:
        send_requester_resolution_email(ticket_record, ticket_record.get("AI Response", ""))
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
        "Auto Solved": AUTO_STATUS_MANAGER_REVIEWED,
        "Admin Review Needed": "No"
    })

    if not success:
        return "❌ Failed to update ticket.", 500

    # Auto-assign immediately
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
    print("Starting Query Management Flask app...")
    app.run(debug=True, host='0.0.0.0', port=5000)
