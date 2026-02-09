# EY Query Management Tool (QMT) - AI Powered

An intelligent, AI-driven web application for managing and resolving queries (tickets) with automated workflows. Built for EY to streamline operations using Azure OpenAI and Excel-based data management.

## 🚀 Features

*   **AI-Powered Chat Assistant**: Employees can chat with the system to query invoice status, PO details, and ticket updates naturally.
*   **Automated Ticket Resolution**:
    *   The `TicketAIAgent` analyzes open tickets in the background.
    *   It checks invoices/POs in the database.
    *   If a solution is found, it **auto-closes** the ticket and saves the resolution.
    *   It **emails the manager** automatically with the resolution details.
*   **Role-Based Access Control (RBAC)**:
    *   **Employees**: View and create their own tickets.
    *   **Managers**: View team tickets, approve/reject resolutions, view analytics.
    *   **Admins**: Full system access.
*   **Excel as Database**: Seamless integration with `QMT Data New.xlsx` for zero-setup data persistence.
*   **Interactive Dashboards**: Data visualization for ticket status and team performance.

## 🛠️ Tech Stack

*   **Backend**: Python, Flask
*   **AI/LLM**: Azure OpenAI (GPT-4)
*   **Database**: Excel (pandas, openpyxl)
*   **Notifications**: SMTP Email Service (Gmail Integration)
*   **Frontend**: HTML5, Bootstrap, Jinja2 Templates

## 📂 Project Structure

```
EY-Project/
├── backend/
│   ├── app.py                 # Main Flask Application
│   ├── agents/                # AI Agents Logic
│   │   ├── chat_agent.py      # Conversational Agent
│   │   └── ticket_agent.py    # Background Processing Agent
│   ├── email_service.py       # SMTP Email Logic
│   ├── table_db.py            # Excel Database Handler
│   ├── users.json             # User Authentication Data
│   ├── templates/             # HTML Templates
│   ├── requirements.txt       # Python Dependencies
│   └── .env                   # Environment Variables
├── README.md                  # Project Documentation
└── QMT Data New.xlsx         # Main Data File
```

## ⚙️ Setup & Installation

1.  **Clone the repository** (or navigate to the project folder).
2.  **Create a Virtual Environment**:
    ```bash
    python -m venv venv
    # Windows
    venv\Scripts\activate
    # Mac/Linux
    source venv/bin/activate
    ```
3.  **Install Dependencies**:
    ```bash
    pip install -r backend/requirements.txt
    ```
4.  **Configure Environment**:
    *   Copy `backend/.env.example` to `backend/.env`.
    *   Fill in your **Azure OpenAI** keys and **SMTP** credentials.

## ▶️ Running the Application

1.  Navigate to the backend directory:
    ```bash
    cd backend
    ```
2.  Run the Flask app:
    ```bash
    python app.py
    ```
3.  Open your browser and visit: `http://localhost:5000`

## 📧 Email Notifications
The system uses SMTP to send notifications.
*   Ensure `SMTP_EMAIL` and `SMTP_PASSWORD` are set in `.env`.
*   If using Gmail, generate an **App Password** (do not use your login password).

## 🤖 AI Agents
*   **Chat Agent uses**: `backend/agents/chat_agent.py` to answer real-time queries.
*   **Ticket Agent uses**: `backend/agents/ticket_agent.py` to batch process open tickets and resolve them.
