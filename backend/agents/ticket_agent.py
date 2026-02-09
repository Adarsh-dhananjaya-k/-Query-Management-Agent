import hashlib
import secrets
import os
import json
from datetime import datetime
from openai import AzureOpenAI
from utils import get_user_email_by_name, get_manager_by_team
from email_service import send_email
from config import get_azure_client, get_deployment_name
from table_db import get_all_tickets_df, search_invoices, update_multiple_fields

# ────────────────────────────────────────────────
# Approval Token Generator (Email Approval Flow)
# ────────────────────────────────────────────────
def generate_approval_token(ticket_id: str) -> str:
    secret = os.getenv("APPROVAL_SECRET", "ey_approval_secret")
    raw = f"{ticket_id}:{secret}"
    return hashlib.sha256(raw.encode()).hexdigest()


class TicketAIAgent:
    def __init__(self):
        self.client = get_azure_client()
        self.deployment = get_deployment_name()
        self.system_prompt = """
        You are an EY Query Management AI Agent. Your goal is to analyze tickets and resolve them if possible.
        If a ticket involves an invoice (e.g., status check, payment query, PO info), use the 'search_invoices' tool.

        Available Invoice Data includes:
        - Invoice Number, Invoice Date, Invoice Amount
        - Vendor ID, Vendor Name
        - PO Number, PO Status
        - Payment Status, Payment Term, Due Date, Clearing Date
        - Customer ID, Customer Name, Country

        Resolution Criteria:
        - If the info is found: Provide a clear answer (e.g., "Invoice EY-123 is Paid, Cleared on 2023-10-01") and mark as 'auto_solved'.
        - If multiple matches: Ask for clarification or provide all relevant info.
        - If NOT found: Inform the user and mark as 'auto_solved' with 'Data not found in database'.
        - If the query is complex: Provide as much info as possible.

        When resolved, use 'resolve_ticket' to update the spreadsheet.
        """

    def get_tool_definitions(self):
        return [
            {
                "type": "function",
                "function": {
                    "name": "search_invoices",
                    "description": "Search the invoice database for specific details.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "Invoice Number": {"type": "string"},
                            "Customer Name": {"type": "string"},
                            "Vendor Name": {"type": "string"},
                            "Payment Status": {"type": "string"},
                            "PO Number": {"type": "string"},
                            "Vendor ID": {"type": "string"},
                            "Customer ID": {"type": "string"}
                        }
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "resolve_ticket",
                    "description": "Mark a ticket as solved and save the AI response.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "ticket_id": {"type": "string"},
                            "ai_response": {"type": "string"},
                            "auto_solved": {"type": "boolean"}
                        },
                        "required": ["ticket_id", "ai_response", "auto_solved"]
                    }
                }
            }
        ]

    def process_ticket(self, ticket):
        ticket_id = str(ticket.get("Ticket ID"))
        description = str(ticket.get("Description", "No description provided."))
        status = str(ticket.get("Ticket Status", "Open"))

        if status == "Closed":
            print(f"Skipping Ticket {ticket_id}: Status is already Closed.")
            return "Ticket is already closed."

        print(f"\n--- Processing Ticket {ticket_id} ---")
        print(f"Description: {description}")

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": f"Ticket ID: {ticket_id}\nDescription: {description}"}
        ]

        # Max 5 turns to prevent infinite loops
        for turn in range(5):
            response = self.client.chat.completions.create(
                model=self.deployment,
                messages=messages,
                tools=self.get_tool_definitions(),
                tool_choice="auto"
            )

            msg = response.choices[0].message
            messages.append(msg)  # Keep track of assistant's thoughts/calls

            if not msg.tool_calls:
                # No more tools to call, this is the final answer
                print(f"AI Final Response: {msg.content}")
                return msg.content

            for tool_call in msg.tool_calls:
                func_name = tool_call.function.name
                args = json.loads(tool_call.function.arguments)

                if func_name == "search_invoices":
                    print(f"DEBUG: AI is searching invoices with: {args}")
                    results = search_invoices(args)
                    print(f"DEBUG: Found {len(results)} matching invoices.")
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "name": func_name,
                        "content": json.dumps(results, default=str)
                    })


                elif func_name == "resolve_ticket":

                    print(f"DEBUG: AI is calling resolve_ticket...")

                    print(f"   - Auto Solved: {args.get('auto_solved')}")

                    print(f"   - Response: {args.get('ai_response')}")

                    # 1️⃣ Prepare update data

                    update_dict = {

                        "Auto Solved": args['auto_solved'],

                        "AI Response": args['ai_response'],

                    }

                    if args.get('auto_solved', False):
                        update_dict["Ticket Status"] = "Pending Manager Approval"

                        update_dict["Admin Review Needed"] = "Yes"

                    # 2️⃣ Update Excel FIRST

                    success = update_multiple_fields(ticket_id, update_dict)

                    # 3️⃣ Fetch manager AFTER update

                    manager = get_manager_by_team(ticket.get("Assigned Team"))

                    # 4️⃣ Send approval email

                    if manager and args.get('auto_solved', False):
                        token = generate_approval_token(ticket_id)

                        base_url = os.getenv("APP_BASE_URL", "http://localhost:5000")

                        approve_link = f"{base_url}/ticket/approve/{ticket_id}?token={token}"

                        reject_link = f"{base_url}/ticket/reject/{ticket_id}?token={token}"

                        email_body = f"""Hello {manager['name']},


                The AI agent has resolved the following ticket and is requesting your approval.


                Ticket ID: {ticket_id}

                Team: {ticket.get('Assigned Team', 'N/A')}


                AI Resolution:

                {args.get('ai_response', 'No details provided.')}


                --------------------------------------------------

                ✅ APPROVE CLOSURE:

                {approve_link}


                ❌ REJECT & REOPEN:

                {reject_link}

                --------------------------------------------------


                Please click one of the above links to proceed.


                Regards,

                EY Query Management System

                """

                        send_email(

                            to_email=manager['email'],

                            subject=f"Approval Required: Ticket {ticket_id}",

                            body=email_body

                        )

                    # 5️⃣ Logging

                    if success:

                        print(f"SUCCESS: Ticket {ticket_id} updated in Excel.")

                    else:

                        print(f"ERROR: Failed to update ticket {ticket_id} in Excel.")

                    return f"Ticket {ticket_id} resolved: {args['ai_response']}"

        return "Agent reached maximum turns without resolving."

    def run_on_all_open_tickets(self):
        df = get_all_tickets_df()
        open_tickets = df[df["Ticket Status"] != "Closed"]

        results = []
        for index, row in open_tickets.iterrows():
            res = self.process_ticket(row.to_dict())
            results.append(res)
        return results


if __name__ == "__main__":
    agent = TicketAIAgent()
    agent.run_on_all_open_tickets()
