# ticket_agent_enhanced.py
"""
Enhanced Ticket AI Agent with auto-generated invoice document support.
Documents pull structured data from the Invoice sheet to produce shareable PDFs.
"""

import hashlib
import os
import json
import re

import pandas as pd
from datetime import datetime
from openai import AzureOpenAI
from email_service import send_email
from config import get_azure_client, get_deployment_name
from utils import get_user_email_by_name, get_manager_by_team

from table_db import (
    AUTO_STATUS_AUTO_RESOLVED,
    AUTO_STATUS_MANUAL_REVIEW,
    get_all_tickets_df,
    search_invoices,
    update_multiple_fields,
)

APPROVAL_KEYWORDS = {
    "ap": [
        "validate vendor",
        "vendor detail",
        "early payment",
        "invoice on hold",
    ],
    "ar": [
        "refund ticket",
        "raise refund",
        "investigate customer",
        "cancellation reason",
        "block invoice",
    ],
}


def send_requester_resolution_email(ticket: dict, ai_response: str) -> bool:
    """Notify the employee who raised the ticket."""
    
    requester_name = ticket.get("User Name", "there")
    requester_email = get_user_email_by_name(requester_name)
    if not requester_email:
        print(f"INFO: No email found for requester '{requester_name}'")
        return False

    ticket_id = ticket.get("Ticket ID", "N/A")
    body = f"""Hello {requester_name},

{ai_response or 'Your ticket has been processed by the Query Management Agent.'}

Ticket ID: {ticket_id}
Status: Closed

Regards,
Query Management System
"""
    return send_email(
        to_email=requester_email,
        subject=f"Ticket {ticket_id} Resolved",
        body=body,
    )

# Document generators render PDF summaries using invoice table data.
try:
    from document_generator import (
        generate_invoice_copy_pdf,
        generate_payment_confirmation_pdf,
        generate_invoice_details_pdf,
    )
    DOCUMENTS_AVAILABLE = True
    print("Document generator loaded successfully.")
except Exception as exc:
    print(f"WARNING: Document generator unavailable ({exc}).")
    DOCUMENTS_AVAILABLE = False


INVOICE_FIELD_HINTS = [
    "Invoice Number",
    "Invoice",
    "Invoice Reference",
    "Reference Invoice",
    "Invoice ID",
    "Invoice #",
]

INVOICE_REGEX = re.compile(r"\bINV[\s\-#]?[A-Z0-9]+\b", re.IGNORECASE)
GENERIC_INVOICE_REGEX = re.compile(
    r"\bInvoice(?:\s+(?:Number|No\.|#))?\s*[:#-]?\s*([A-Z0-9-]+)\b",
    re.IGNORECASE,
)


def normalize_invoice_reference(value: str | None) -> str | None:
    """Normalize raw invoice references such as 'inv1016' to 'INV-1016'."""
    if not value:
        return None
    cleaned = str(value).strip().upper()
    if not cleaned:
        return None
    cleaned = cleaned.replace("INVOICE", "INV").replace(" ", "").replace("#", "")
    if cleaned.startswith("INV") and not cleaned.startswith("INV-"):
        remainder = cleaned[3:].lstrip("-")
        cleaned = f"INV-{remainder}" if remainder else "INV"
    elif cleaned.isdigit():
        cleaned = f"INV-{cleaned}"
    return cleaned


def extract_invoice_candidates(ticket: dict) -> list[str]:
    """Collect possible invoice numbers from structured fields and free text."""
    candidates: list[str] = []

    for field in INVOICE_FIELD_HINTS:
        value = ticket.get(field)
        normalized = normalize_invoice_reference(value) if value else None
        if normalized:
            candidates.append(normalized)

    description = ticket.get("Description", "") or ""
    for match in INVOICE_REGEX.finditer(description):
        normalized = normalize_invoice_reference(match.group(0))
        if normalized:
            candidates.append(normalized)

    for match in GENERIC_INVOICE_REGEX.finditer(description):
        normalized = normalize_invoice_reference(match.group(1))
        if normalized:
            candidates.append(normalized)

    seen = set()
    ordered: list[str] = []
    for cand in candidates:
        if cand and cand not in seen:
            seen.add(cand)
            ordered.append(cand)
    return ordered

def _get_ticket_field(ticket: dict, target_name: str):
    """Safely fetch a field from ticket dict ignoring casing/extra spaces."""
    if not target_name:
        return None
    normalized = target_name.strip().lower()
    for key, value in ticket.items():
        if key and key.strip().lower() == normalized:
            return value
    return None


def generate_approval_token(ticket_id: str) -> str:
    """Generate SHA256 token for approval links"""
    secret = os.getenv("APPROVAL_SECRET", "ey_approval_secret")
    raw = f"{ticket_id}:{secret}"
    return hashlib.sha256(raw.encode()).hexdigest()


def get_submitter_email(ticket: dict) -> str | None:
    """
    Get requester email with priority: Requestor Email columns → legacy requester fields → mapped employee fallback.
    """
    email_fields = [
        "Requestor Email ID",
        "Requestor Email",
        "Requester Email",
        "Submitter Email",
        "Customer Email",
        "Email",
    ]
    for field in email_fields:
        raw_val = _get_ticket_field(ticket, field)
        if raw_val:
            email = str(raw_val).strip()
            if email and email.lower() not in ["", "nan", "none", "null", "n/a"]:
                return email

    # Fallback to requestor name if present (sometimes listed without email)
    requestor_name = (
        _get_ticket_field(ticket, "Requestor")
        or _get_ticket_field(ticket, "Requestor Name")
        or _get_ticket_field(ticket, "Requester Name")
    )
    if requestor_name:
        req_email = get_user_email_by_name(requestor_name)
        if req_email:
            return req_email

    # Final fallback to internal employee associated with ticket
    employee_name = (
        _get_ticket_field(ticket, "User Name(Ticket Created By)")
        or ticket.get("User Name")
        or ticket.get("Assigned To")
    )
    if employee_name:
        emp_email = get_user_email_by_name(employee_name)
        if emp_email:
            return emp_email

    return None


class TicketAIAgent:
    def __init__(self):
        self.client = get_azure_client()
        self.deployment = get_deployment_name()
        self.system_prompt = """
You are an EY Query Management AI Agent. Analyze tickets and resolve them according to these 4 categories:

═══════════════════════════════════════════════════════════════
CATEGORY 1: "without_document" - Simple Info Response
═══════════════════════════════════════════════════════════════
→ Information requests that DON'T need documents
→ Examples:
  • "What is the payment status?" → Answer: Paid/Unpaid
  • "When is the due date?" → Answer: Date
  • "What is the invoice amount?" → Answer: $X,XXX.XX
  • "Is the invoice paid?" → Answer: Yes/No
→ Action: Email with info → Close ticket
→ NO DOCUMENT NEEDED

═══════════════════════════════════════════════════════════════
CATEGORY 2: "with_document" - Document Request
═══════════════════════════════════════════════════════════════
→ User EXPLICITLY asks for document, copy, proof, PDF, or report
→ Examples:
  • "Send me invoice copy"
  • "I need payment confirmation document"
  • "Provide invoice details in PDF"
  • "Send proof of payment"
  • "Generate invoice report"
→ Action: Generate AI PDF → Attach to email → Close ticket
→ IMPORTANT: AI generates fake/substitute documents clearly marked as AI-generated

Document Types to Generate:
• "invoice_copy" → For invoice copy requests
• "payment_confirmation" → For payment/remittance proof  
• "invoice_details" → For comprehensive details report

═══════════════════════════════════════════════════════════════
CATEGORY 3: "needs_approval" - Manager Approval Required
═══════════════════════════════════════════════════════════════
→ Financial/policy actions requiring manager sign-off
→ AP Examples: validate vendor, early payment request, put on hold
→ AR Examples: raise refund, investigate customer, block invoice
→ Action: Status → "Pending" → Email manager with approval links

═══════════════════════════════════════════════════════════════
CATEGORY 4: "reassign_billing" - Billing Specialist
═══════════════════════════════════════════════════════════════
→ Specialist tasks AI cannot handle
→ AP: reversal request, exchange rate verification
→ AR: credit memo, debit memo, partial credit
→ Action: Reassign to AP/AR team → Email requester + assigned employee → Keep Open

═══════════════════════════════════════════════════════════════

WORKFLOW:
1. If invoice/PO/vendor/customer mentioned → Call search_invoices FIRST
2. Analyze request → Determine category
3. Call appropriate tool (resolve_ticket OR reassign_ticket_and_notify)

KEY RULE: Category 2 produces PDF snapshots sourced from the Invoice sheet.
These support common requests (invoice copy, payment confirmation, invoice details).
Requester emails mention that the attachment is generated from system records.
"""

    def get_tool_definitions(self):
        return [
            {
                "type": "function",
                "function": {
                    "name": "search_invoices",
                    "description": "Search invoice database for matching records.",
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
                    "description": "Resolve ticket (categories 1, 2, or 3).",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "ticket_id": {"type": "string"},
                            "ai_response": {"type": "string", "description": "Resolution summary for email"},
                            "auto_solved": {"type": "boolean"},
                            "closure_type": {
                                "type": "string",
                                "enum": ["without_document", "with_document", "needs_approval"],
                            },
                            "document_type": {
                                "type": "string",
                                "enum": ["invoice_copy", "payment_confirmation", "invoice_details", "none"],
                                "description": "Required for 'with_document'. Use 'none' for others."
                            }
                        },
                        "required": ["ticket_id", "ai_response", "auto_solved", "closure_type"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "reassign_ticket_and_notify",
                    "description": "Reassign to AP/AR billing specialist (category 4).",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "ticket_id": {"type": "string"},
                            "target_team": {"type": "string", "enum": ["AP", "AR"]},
                            "reason": {"type": "string"},
                            "ai_response": {"type": "string"}
                        },
                        "required": ["ticket_id", "target_team", "reason", "ai_response"]
                    }
                }
            }
        ]

    def _resolve_invoice_data_for_document(self, ticket: dict, cached_invoice: dict | None) -> dict | None:
        """
        Ensure we have invoice table data before generating any document attachment.
        Prefers cached search results; otherwise looks for invoice references in the ticket
        and queries the invoice sheet directly.
        """
        if cached_invoice:
            return cached_invoice

        candidates = extract_invoice_candidates(ticket)
        if not candidates:
            print("   ⚠️  No invoice reference detected for document request.")
            return None

        for reference in candidates:
            print(f"   🔎 Fetching invoice row for {reference}")
            results = search_invoices({"Invoice Number": reference})
            if results:
                return results[0]

        print(f"   ⚠️  Invoice data not found for {', '.join(candidates)}")
        return None

    def needs_manager_approval(self, ticket: dict) -> bool:
        team = str(ticket.get("Assigned Team", "")).lower()
        ticket_type = str(ticket.get("Ticket Type", "")).lower()
        description = str(ticket.get("Description", "")).lower()

        def matches(keywords):
            return any(keyword in description for keyword in keywords)

        if "accounts payable" in ticket_type or "ap" in team:
            if matches(APPROVAL_KEYWORDS["ap"]):
                return True
        if "accounts receivable" in ticket_type or "ar" in team:
            if matches(APPROVAL_KEYWORDS["ar"]):
                return True
        return False

    def process_ticket(self, ticket):
        ticket_id = str(ticket.get("Ticket ID"))
        description = str(ticket.get("Description", "No description provided."))
        status = str(ticket.get("Ticket Status", "Open")).lower()

        if status == "closed":
            print(f"⊘ Ticket {ticket_id} already closed. Skipping.")
            return "Ticket is already closed."

        print(f"\n{'='*70}")
        print(f"🎫 Processing: {ticket_id}")
        print(f"{'='*70}")
        print(f"Description: {description[:120]}{'...' if len(description) > 120 else ''}")
        print(f"Team: {ticket.get('Assigned Team', 'N/A')}")

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": f"Ticket ID: {ticket_id}\nDescription: {description}\nTeam: {ticket.get('Assigned Team', 'Unknown')}"}
        ]

        max_turns = 6
        last_invoice_data = None

        for turn in range(max_turns):
            response = self.client.chat.completions.create(
                model=self.deployment,
                messages=messages,
                tools=self.get_tool_definitions(),
                tool_choice="auto",
                max_tokens=500,
                temperature=0.2
            )

            msg = response.choices[0].message
            messages.append(msg)

            if not msg.tool_calls:
                print(f"ℹ️  Final response: {msg.content[:80]}...")
                return msg.content or "No resolution reached."

            for tool_call in msg.tool_calls:
                func_name = tool_call.function.name
                args = json.loads(tool_call.function.arguments)

                # ═══════════════════════════════════════════════════════
                # TOOL 1: search_invoices
                # ═══════════════════════════════════════════════════════
                if func_name == "search_invoices":
                    print(f"🔍 Searching: {args}")
                    results = search_invoices(args)
                    if results:
                        last_invoice_data = results[0]  # Store for document generation
                    print(f"   ↳ Found {len(results)} record(s)")
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "name": func_name,
                        "content": json.dumps(results, default=str)
                    })

                # ═══════════════════════════════════════════════════════
                # TOOL 2: resolve_ticket (Categories 1, 2, 3)
                # ═══════════════════════════════════════════════════════
                elif func_name == "resolve_ticket":
                    closure_type = args["closure_type"]
                    ai_response = args.get("ai_response", "Ticket processed by AI.")
                    auto_solved = args.get("auto_solved", True)
                    document_type = args.get("document_type", "none")

                    print(f"✅ Resolving: {closure_type}")
                    if document_type != "none":
                        print(f"   📄 Document type: {document_type}")

                    update_dict = {
                        "Auto Solved": auto_solved,
                        "AI Response": ai_response,
                        "Ticket Updated Date": datetime.now().strftime("%Y-%m-%d")
                    }

                    email_subject = f"Ticket {ticket_id} - Update"
                    email_body = ai_response
                    recipient_email = None
                    attachment_path = None

                    # ─────────────────────────────────────────────────────
                    # CATEGORY 3: needs_approval
                    # ─────────────────────────────────────────────────────
                    if closure_type == "needs_approval":
                        print(f"   ⏳ Pending manager approval")
                        update_dict["Ticket Status"] = "Pending Manager Approval"
                        update_dict["Admin Review Needed"] = "Yes"
                        manager_note = ai_response
                        user_placeholder = (
                            f"Ticket {ticket_id} is pending manager review. "
                            "We will notify you once a decision is made."
                        )
                        update_dict["AI Response"] = user_placeholder

                        manager = get_manager_by_team(ticket.get("Assigned Team"))
                        if manager:
                            token = generate_approval_token(ticket_id)
                            base_url = os.getenv("APP_BASE_URL", "http://localhost:5000")
                            approve_link = f"{base_url}/ticket/approve/{ticket_id}?token={token}"
                            reject_link = f"{base_url}/ticket/reject/{ticket_id}?token={token}"

                            email_body = f"""Hello {manager['name']},

Ticket {ticket_id} requires your approval.

Team: {ticket.get('Assigned Team', 'N/A')}
Request: {description[:200]}...

AI Analysis:
{manager_note}

Actions:
→ APPROVE: {approve_link}
→ REJECT: {reject_link}

Best regards,
Query Management AI Agent
"""
                            send_email(
                                to_email=manager["email"],
                                subject=f"[APPROVAL] Ticket {ticket_id}",
                                body=email_body
                            )
                            print(f"   📧 Approval sent to {manager['name']}")

                    # ─────────────────────────────────────────────────────
                    # CATEGORY 2: with_document (Invoice PDF attachments)
                    # ─────────────────────────────────────────────────────
                    elif closure_type == "with_document":
                        print(f"   📄 Generating AI document...")
                        update_dict["Ticket Status"] = "Closed"
                        recipient_email = get_submitter_email(ticket)

                        invoice_payload = self._resolve_invoice_data_for_document(ticket, last_invoice_data)
                        if invoice_payload and last_invoice_data is None:
                            last_invoice_data = invoice_payload

                        if DOCUMENTS_AVAILABLE and invoice_payload:
                            if document_type == "payment_confirmation":
                                attachment_path = generate_payment_confirmation_pdf(
                                    invoice_payload,
                                    description,
                                )
                            elif document_type == "invoice_details":
                                attachment_path = generate_invoice_details_pdf(
                                    invoice_payload,
                                    description,
                                )
                            else:  # invoice_copy (default)
                                attachment_path = generate_invoice_copy_pdf(
                                    invoice_payload,
                                    description,
                                )

                            if attachment_path:
                                print(f"   ✓ Document: {os.path.basename(attachment_path)}")
                                email_body = f"""Dear Requester,

Your request for ticket {ticket_id} has been processed.

{ai_response}

We attached the latest invoice snapshot pulled directly from the invoice ledger for your reference.

Best regards,
Query Management Team
"""
                            else:
                                print("   ✗ Document generation failed")
                                payment_status = invoice_payload.get("Payment Status", "Unknown")
                                email_body = f"""Dear Requester,

We attempted to generate a payment confirmation for ticket {ticket_id} (invoice {invoice_payload.get('Invoice Number', 'N/A')}), but the PDF export failed.

Current ledger status: {payment_status}.

If you require an officially stamped confirmation, please reach out to your AP/AR partner and they will provide the formal document.

Best regards,
Query Management Team
"""
                        else:
                            print("   ⚠️  Missing invoice data or generator disabled")
                            fallback_status = invoice_payload.get("Payment Status", "Unknown") if invoice_payload else "Unknown"
                            email_body = f"""Dear Requester,

Your request for ticket {ticket_id} has been reviewed.

{ai_response}

We could not retrieve the invoice details needed to create a PDF automatically (current ledger status: {fallback_status}). Please contact your AP/AR team if you require an official document.

Best regards,
Query Management Team
"""

                    # ─────────────────────────────────────────────────────
                    # CATEGORY 1: without_document (Simple response)
                    # ─────────────────────────────────────────────────────
                    else:  # without_document
                        print(f"   ✉️  Simple email response")
                        update_dict["Ticket Status"] = "Closed"
                        recipient_email = get_submitter_email(ticket)
                        
                        email_body = f"""Dear Requester,

Your inquiry regarding ticket {ticket_id} has been resolved.

{ai_response}

If you need further assistance, please create a new ticket.

Best regards,
Query Management Team
"""

                    # Update database
                    success = update_multiple_fields(ticket_id, update_dict)

                    # Send email to requester (categories 1 & 2)
                    if success and recipient_email and closure_type != "needs_approval":
                        status_text = update_dict.get("Ticket Status", ticket.get("Ticket Status", "Open"))
                        enriched_body = f"{email_body.rstrip()}\n\nTicket status (AI): {status_text}"
                        sent = send_email(
                            to_email=recipient_email,
                            subject=email_subject,
                            body=enriched_body,
                            attachment_path=attachment_path
                        )
                        if sent:
                            print(f"   📧 Email sent to {recipient_email}")
                        else:
                            print(f"   ✗ Email failed")

                    if success:
                        print(f"✓ Ticket {ticket_id} resolved: {closure_type}")
                    else:
                        print(f"✗ Update failed")

                    return f"Ticket {ticket_id}: {closure_type}"

                # ═══════════════════════════════════════════════════════
                # TOOL 3: reassign_ticket_and_notify (Category 4)
                # ═══════════════════════════════════════════════════════
                elif func_name == "reassign_ticket_and_notify":
                    target_team = args["target_team"].upper()
                    reason = args.get("reason", "Billing specialist required")
                    ai_response = args.get("ai_response", f"Reassigned to {target_team}")

                    print(f"🔄 Reassigning to {target_team} billing specialist")

                    update_dict = {
                        "Assigned Team": target_team,
                        "Ticket Status": "Open",
                        "Ticket Updated Date": datetime.now().strftime("%Y-%m-%d"),
                        "AI Response": ai_response,
                        "Auto Solved": False,
                    }

                    success = update_multiple_fields(ticket_id, update_dict)

                    if success:
                        # Email requester
                        requester_email = get_submitter_email(ticket)
                        if requester_email:
                            status_text = update_dict.get("Ticket Status", ticket.get("Ticket Status", "Open"))
                            requester_body = f"""Dear Requester,

Ticket {ticket_id} has been assigned to our {target_team} billing specialist team.

Reason: {reason}

Ticket status (AI): {status_text}

Best regards,
Query Management System
"""
                            send_email(
                                to_email=requester_email,
                                subject=f"Ticket {ticket_id} - Assigned to Specialist",
                                body=requester_body
                            )
                            print(f"   📧 Requester notified")

                        # Email assigned employee (instead of manager)
                        user_name = ticket.get("User Name")
                        if user_name and str(user_name).lower() not in ["nan", "none", "n/a", "null"]:
                            assigned_email = get_user_email_by_name(user_name)
                            if assigned_email:
                                send_email(
                                    to_email=assigned_email,
                                    subject=f"[NEW] Ticket {ticket_id} → {target_team}",
                                    body=f"""Hello {user_name},

Ticket {ticket_id} assigned to {target_team}.

Request: {description[:250]}...

Reason: {reason}

Please review and take necessary action.

Best regards,
AI Agent
"""
                                )
                                print(f"   📧 Assigned employee notified: {user_name}")
                            else:
                                print(f"   ⚠️ No email found for assigned user: {user_name}")
                        else:
                            print(f"   ⚠️ No assigned user name found in ticket")

                        print(f"✓ Reassigned to {target_team}")
                        return f"Ticket {ticket_id} reassigned to {target_team}"
                    else:
                        print(f"✗ Reassignment failed")
                        return "Reassignment failed"

        return "Max turns reached without resolution"

    def run_on_all_open_tickets(self):
        """Process all open tickets"""
        df = get_all_tickets_df()
        open_tickets = df[df["Ticket Status"].str.lower() != "closed"]

        print(f"\n{'='*70}")
        print(f"🚀 BULK TICKET PROCESSING")
        print(f"{'='*70}")
        print(f"Open tickets: {len(open_tickets)}\n")

        results = []
        for idx, row in open_tickets.iterrows():
            res = self.process_ticket(row.to_dict())
            results.append(res)
        
        print(f"\n{'='*70}")
        print(f"✓ Processing complete: {len(results)} tickets")
        print(f"{'='*70}\n")
        
        return results


if __name__ == "__main__":
    print("="*70)
    print("EY Query Management - AI Document Generator Agent")
    print("="*70)
    print("\nProcessing all open tickets...\n")
    
    agent = TicketAIAgent()
    agent.run_on_all_open_tickets()
