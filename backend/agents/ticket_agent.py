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


def _safe_requestor_name(ticket: dict) -> str:
    """
    Safely extract the requestor's display name from the ticket dict.
    The 'Requestor ' column (note trailing space) can be NaN (float) when empty,
    so we must str()-cast before calling .strip().
    """
    raw = ticket.get("Requestor ") or ticket.get("Requestor") or ""
    name = str(raw).strip()
    return name if name and name.lower() not in ("nan", "none", "null") else "Requester"


def send_requester_resolution_email(ticket: dict, ai_response: str) -> bool:
    """
    Called by app.py when a manager APPROVES a ticket (Category 3).
    Sends the resolution update ONLY to the requestor (Requestor Email ID).
    The internal employee is never emailed here.
    """
    requestor_email = get_requestor_email(ticket)
    if not requestor_email:
        print(f"INFO: No requestor email for ticket {ticket.get('Ticket ID', 'N/A')} — skipping approval notification")
        return False

    ticket_id     = ticket.get("Ticket ID", "N/A")
    requestor_name = _safe_requestor_name(ticket)

    body = (
        f"Dear {requestor_name},\n\n"
        f"Your ticket {ticket_id} has been reviewed and approved by the manager.\n\n"
        f"{ai_response or 'Your request has been processed.'}\n\n"
        f"Status: Closed\n\n"
        f"Best regards,\nQuery Management System"
    )
    return send_email(
        to_email=requestor_email,
        subject=f"Ticket {ticket_id} - Approved & Closed",
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


def get_requestor_email(ticket: dict) -> str | None:
    """
    Return the email of the CUSTOMER / VENDOR who raised the ticket.

    Excel column priority:
      1. "Requestor Email ID"  ← primary column in QMT_Data sheet (may have trailing space)
      2. Other legacy email column names
    This function deliberately never touches "User Name" – that is the internal
    employee and must NOT receive Cat 1 / Cat 2 emails.
    """
    # Primary field (strip key to handle trailing-space column names)
    for field in ["Requestor Email ID", "Requestor Email", "Requester Email",
                  "Submitter Email", "Customer Email", "Email"]:
        raw = _get_ticket_field(ticket, field)   # _get_ticket_field already strips + lowercases
        if raw:
            cleaned = str(raw).strip()
            if cleaned and cleaned.lower() not in ["", "nan", "none", "null", "n/a"]:
                return cleaned
    return None


# Keep old name as alias so existing call-sites (e.g. app.py) don't break
get_submitter_email = get_requestor_email


def get_specialist_email(ticket: dict) -> tuple[str | None, str | None]:
    """
    Return (name, email) of the internal employee assigned to handle the ticket.
    Maps to the "User Name" column – used ONLY for Cat 4 reassignment emails.
    """
    name = ticket.get("User Name", "")
    if not name or str(name).strip().lower() in ["", "nan", "none", "n/a", "null", "unassigned"]:
        return None, None
    name = str(name).strip()
    email = get_user_email_by_name(name)
    return name, email


# Keep old name as alias
def get_assigned_employee_email(ticket: dict) -> str | None:
    _, email = get_specialist_email(ticket)
    return email


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
                    closure_type  = args["closure_type"]
                    ai_response   = args.get("ai_response", "Ticket processed by AI.")
                    document_type = args.get("document_type", "none")

                    print(f"✅ Resolving: {closure_type}")
                    if document_type != "none":
                        print(f"   📄 Document type: {document_type}")

                    now_str = datetime.now().strftime("%Y-%m-%d")

                    # ─────────────────────────────────────────────────────
                    # CATEGORY 3: needs_approval
                    # Ticket goes Pending; only the MANAGER gets an email.
                    # Requestor/employee are NOT emailed here.
                    # ─────────────────────────────────────────────────────
                    if closure_type == "needs_approval":
                        print(f"   ⏳ Pending manager approval")

                        update_dict = {
                            "Ticket Status":      "Pending Manager Approval",
                            "Admin Review Needed": "Yes",
                            "Auto Solved":         False,
                            "AI Response":         (
                                f"Ticket {ticket_id} is pending manager review. "
                                "We will notify you once a decision is made."
                            ),
                            "Ticket Updated Date": now_str,
                        }

                        manager = get_manager_by_team(ticket.get("Assigned Team"))
                        if manager:
                            token        = generate_approval_token(ticket_id)
                            base_url     = os.getenv("APP_BASE_URL", "http://localhost:5000")
                            approve_link = f"{base_url}/ticket/approve/{ticket_id}?token={token}"
                            reject_link  = f"{base_url}/ticket/reject/{ticket_id}?token={token}"

                            send_email(
                                to_email=manager["email"],
                                subject=f"[APPROVAL REQUIRED] Ticket {ticket_id}",
                                body=(
                                    f"Hello {manager['name']},\n\n"
                                    f"Ticket {ticket_id} requires your approval.\n\n"
                                    f"Team: {ticket.get('Assigned Team', 'N/A')}\n"
                                    f"Request: {description[:300]}\n\n"
                                    f"AI Analysis:\n{ai_response}\n\n"
                                    f"→ APPROVE: {approve_link}\n"
                                    f"→ REJECT:  {reject_link}\n\n"
                                    f"Best regards,\nQuery Management AI Agent"
                                )
                            )
                            print(f"   📧 Approval email sent → {manager['name']} ({manager['email']})")
                        else:
                            print(f"   ⚠️  No manager found for team: {ticket.get('Assigned Team')}")

                        success = update_multiple_fields(ticket_id, update_dict)
                        if success:
                            print(f"✓ Ticket {ticket_id} set to Pending Manager Approval")
                        else:
                            print(f"✗ DB update failed")
                        return f"Ticket {ticket_id}: needs_approval"

                    # ─────────────────────────────────────────────────────
                    # CATEGORY 2: with_document
                    # Ticket CLOSES. Only the REQUESTOR (Requestor Email ID)
                    # gets an email with the PDF attached.
                    # ─────────────────────────────────────────────────────
                    elif closure_type == "with_document":
                        print(f"   📄 Generating document and closing ticket...")

                        update_dict = {
                            "Ticket Status":      "Closed",
                            "Ticket Closed Date": now_str,
                            "Auto Solved":         AUTO_STATUS_AUTO_RESOLVED,
                            "AI Response":         ai_response,
                            "Ticket Updated Date": now_str,
                        }

                        # Resolve invoice data for PDF
                        invoice_payload = self._resolve_invoice_data_for_document(ticket, last_invoice_data)

                        attachment_path = None
                        if DOCUMENTS_AVAILABLE and invoice_payload:
                            if document_type == "payment_confirmation":
                                attachment_path = generate_payment_confirmation_pdf(invoice_payload, description)
                            elif document_type == "invoice_details":
                                attachment_path = generate_invoice_details_pdf(invoice_payload, description)
                            else:
                                attachment_path = generate_invoice_copy_pdf(invoice_payload, description)

                            if attachment_path:
                                print(f"   ✓ PDF generated: {os.path.basename(attachment_path)}")
                            else:
                                print(f"   ✗ PDF generation failed")
                        else:
                            print(f"   ⚠️  No invoice data – closing without attachment")

                        # Build email body for requestor
                        requestor_email = get_requestor_email(ticket)
                        if requestor_email:
                            if attachment_path:
                                email_body = (
                                    f"Dear {_safe_requestor_name(ticket)},\n\n"
                                    f"Your request for Ticket {ticket_id} has been processed and the ticket is now closed.\n\n"
                                    f"{ai_response}\n\n"
                                    f"Please find the requested document attached. It has been generated directly from the invoice ledger.\n\n"
                                    f"If you need further assistance, please raise a new ticket.\n\n"
                                    f"Best regards,\nQuery Management Team"
                                )
                            else:
                                inv_status = invoice_payload.get("Payment Status", "N/A") if invoice_payload else "N/A"
                                email_body = (
                                    f"Dear {_safe_requestor_name(ticket)},\n\n"
                                    f"Your request for Ticket {ticket_id} has been reviewed and the ticket is now closed.\n\n"
                                    f"{ai_response}\n\n"
                                    f"Note: We were unable to generate the PDF document automatically "
                                    f"(current ledger status: {inv_status}). "
                                    f"Please contact your AP/AR partner for an officially stamped copy.\n\n"
                                    f"Best regards,\nQuery Management Team"
                                )

                            success_db = update_multiple_fields(ticket_id, update_dict)
                            sent = send_email(
                                to_email=requestor_email,
                                subject=f"Ticket {ticket_id} - Closed (Document Attached)" if attachment_path else f"Ticket {ticket_id} - Closed",
                                body=email_body,
                                attachment_path=attachment_path
                            )
                            if sent:
                                print(f"   📧 Requestor notified → {requestor_email}")
                            else:
                                print(f"   ✗ Requestor email failed")
                        else:
                            print(f"   ⚠️  No requestor email found — closing ticket without notification")
                            success_db = update_multiple_fields(ticket_id, update_dict)

                        if success_db:
                            print(f"✓ Ticket {ticket_id} closed: with_document")
                        else:
                            print(f"✗ DB update failed")
                        return f"Ticket {ticket_id}: with_document"

                    # ─────────────────────────────────────────────────────
                    # CATEGORY 1: without_document
                    # Ticket CLOSES. Only the REQUESTOR (Requestor Email ID)
                    # gets a plain-text resolution email. No one else.
                    # ─────────────────────────────────────────────────────
                    else:  # without_document
                        print(f"   ✉️  Closing ticket with simple response...")

                        update_dict = {
                            "Ticket Status":      "Closed",
                            "Ticket Closed Date": now_str,
                            "Auto Solved":         AUTO_STATUS_AUTO_RESOLVED,
                            "AI Response":         ai_response,
                            "Ticket Updated Date": now_str,
                        }

                        requestor_email = get_requestor_email(ticket)
                        if requestor_email:
                            email_body = (
                                f"Dear {_safe_requestor_name(ticket)},\n\n"
                                f"Your inquiry for Ticket {ticket_id} has been resolved and the ticket is now closed.\n\n"
                                f"{ai_response}\n\n"
                                f"If you need further assistance, please raise a new ticket.\n\n"
                                f"Best regards,\nQuery Management Team"
                            )
                            success_db = update_multiple_fields(ticket_id, update_dict)
                            sent = send_email(
                                to_email=requestor_email,
                                subject=f"Ticket {ticket_id} - Resolved",
                                body=email_body
                            )
                            if sent:
                                print(f"   📧 Requestor notified → {requestor_email}")
                            else:
                                print(f"   ✗ Requestor email failed")
                        else:
                            print(f"   ⚠️  No requestor email found — closing ticket without notification")
                            success_db = update_multiple_fields(ticket_id, update_dict)

                        if success_db:
                            print(f"✓ Ticket {ticket_id} closed: without_document")
                        else:
                            print(f"✗ DB update failed")
                        return f"Ticket {ticket_id}: without_document"

                # ═══════════════════════════════════════════════════════
                # TOOL 3: reassign_ticket_and_notify (Category 4)
                # Ticket stays OPEN. Two emails go out:
                #   1. REQUESTOR  (Requestor Email ID) — told their ticket
                #                  is being handled by a specialist
                #   2. SPECIALIST (User Name → email lookup) — told they
                #                  have a new ticket assigned to them
                # Nobody else receives an email.
                # ═══════════════════════════════════════════════════════
                elif func_name == "reassign_ticket_and_notify":
                    raw_team    = args["target_team"].upper()   # "AP" or "AR"
                    reason      = args.get("reason", "Billing specialist required")
                    ai_response = args.get("ai_response", f"Reassigned to {raw_team} specialist")

                    # Normalise team name to match what is stored in Excel ("AP Team" / "AR Team")
                    df_all    = get_all_tickets_df()
                    all_teams = df_all["Assigned Team"].dropna().unique().tolist()
                    matched_team = next(
                        (t for t in all_teams if raw_team in str(t).upper()),
                        raw_team
                    )

                    print(f"🔄 Reassigning to {matched_team} specialist")

                    # Select the specialist with the lowest current open-ticket workload
                    from utils import load_users
                    users_list   = load_users()
                    specialists  = [
                        u["name"] for u in users_list
                        if str(u.get("role", "")).lower() == "employee"
                        and raw_team.lower() in str(u.get("team", "")).lower()
                    ]

                    specialist_name  = None
                    specialist_email = None

                    if specialists:
                        workload = {
                            name: len(df_all[
                                (df_all["User Name"].str.strip().str.lower() == name.lower()) &
                                (df_all["Ticket Status"].str.lower() == "open")
                            ])
                            for name in specialists
                        }
                        specialist_name  = min(workload, key=workload.get)
                        specialist_email = get_user_email_by_name(specialist_name)
                        print(f"   👤 Specialist: {specialist_name} (open load={workload[specialist_name]})")
                    else:
                        print(f"   ⚠️  No employees found for team '{raw_team}'")

                    now_str = datetime.now().strftime("%Y-%m-%d")
                    update_dict = {
                        "Assigned Team":       matched_team,
                        "User Name":           specialist_name if specialist_name else ticket.get("User Name", ""),
                        "Ticket Status":       "Open",
                        "Ticket Updated Date": now_str,
                        "AI Response":         ai_response,
                        "Auto Solved":         False,
                    }

                    success = update_multiple_fields(ticket_id, update_dict)

                    if success:
                        requestor_name = _safe_requestor_name(ticket)

                        # ── EMAIL 1: REQUESTOR ──────────────────────────────
                        requestor_email = get_requestor_email(ticket)
                        if requestor_email:
                            send_email(
                                to_email=requestor_email,
                                subject=f"Ticket {ticket_id} - Assigned to {matched_team} Specialist",
                                body=(
                                    f"Dear {requestor_name},\n\n"
                                    f"Your request for Ticket {ticket_id} requires specialist attention.\n\n"
                                    f"It has been assigned to our {matched_team} billing specialist team who will "
                                    f"review your request and follow up with you directly.\n\n"
                                    f"Reason: {reason}\n\n"
                                    f"Best regards,\nQuery Management System"
                                )
                            )
                            print(f"   📧 Requestor notified → {requestor_email}")
                        else:
                            print(f"   ⚠️  No requestor email — requestor notification skipped")

                        # ── EMAIL 2: SPECIALIST ─────────────────────────────
                        if specialist_name and specialist_email:
                            send_email(
                                to_email=specialist_email,
                                subject=f"[NEW ASSIGNMENT] Ticket {ticket_id} – Action Required",
                                body=(
                                    f"Hello {specialist_name},\n\n"
                                    f"Ticket {ticket_id} has been assigned to you for specialist handling.\n\n"
                                    f"Requestor: {requestor_name}\n"
                                    f"Team: {matched_team}\n\n"
                                    f"Request summary:\n{description[:400]}{'...' if len(description) > 400 else ''}\n\n"
                                    f"Reason for assignment: {reason}\n\n"
                                    f"Please review and take the necessary action.\n\n"
                                    f"Best regards,\nQuery Management System"
                                )
                            )
                            print(f"   📧 Specialist notified → {specialist_name} ({specialist_email})")
                        else:
                            print(f"   ⚠️  No specialist email — specialist notification skipped")

                        print(f"✓ Ticket {ticket_id} reassigned → {matched_team} / {specialist_name or 'unassigned'}")
                        return f"Ticket {ticket_id} reassigned to {matched_team} specialist: {specialist_name or 'unassigned'}"
                    else:
                        print(f"✗ Reassignment DB update failed")
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