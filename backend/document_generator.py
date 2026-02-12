# document_generator.py
"""
Simple PDF builders for ticket attachments using fpdf2.
The PDFs summarize invoice information pulled from the Excel data.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import List, Sequence, Tuple

from fpdf import FPDF

Row = Tuple[str, str]


def _ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _sanitize_invoice_number(invoice_number: str | None) -> str:
    if not invoice_number:
        return "UNKNOWN"
    return str(invoice_number).strip().replace("/", "-").replace(" ", "_") or "UNKNOWN"


def _format_currency(value) -> str:
    try:
        if value in (None, ""):
            return "N/A"
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def _safe_text(value, fallback: str = "N/A") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def _truncate(value: str | None, limit: int = 160) -> str:
    if not value:
        return ""
    return value if len(value) <= limit else value[: limit - 3] + "..."


class SimpleReportPDF(FPDF):
    def header(self) -> None:
        self.set_font("Helvetica", "B", 15)
        self.cell(0, 8, "Query Management System", ln=1, align="C")
        self.set_font("Helvetica", "", 11)
        self.cell(0, 6, "Automated Invoice Snapshot", ln=1, align="C")
        self.ln(4)

    def footer(self) -> None:
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.cell(0, 10, f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}", align="C")


def _effective_width(pdf: FPDF) -> float:
    return pdf.w - pdf.l_margin - pdf.r_margin


def _write_rows(pdf: SimpleReportPDF, rows: Sequence[Row]) -> None:
    width = _effective_width(pdf)
    for label, value in rows:
        pdf.set_font("Helvetica", "B", 11)
        pdf.multi_cell(width, 6, f"{label}:")
        pdf.set_x(pdf.l_margin)
        pdf.set_font("Helvetica", "", 11)
        pdf.multi_cell(width, 6, value)
        pdf.set_x(pdf.l_margin)
        pdf.ln(1)


def _build_pdf(title: str, subtitle: str, rows: Sequence[Row], notes: str, output_path: str) -> str | None:
    pdf = SimpleReportPDF()
    pdf.add_page()
    width = _effective_width(pdf)

    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, title, ln=1)
    pdf.set_font("Helvetica", "", 11)
    pdf.multi_cell(width, 6, subtitle)
    pdf.set_x(pdf.l_margin)
    pdf.ln(4)

    _write_rows(pdf, rows)

    if notes:
        pdf.ln(3)
        pdf.set_font("Helvetica", "I", 10)
        pdf.multi_cell(width, 5, notes)
        pdf.set_x(pdf.l_margin)

    try:
        pdf.output(output_path)
    except Exception as exc:
        print(f"? Failed to write PDF: {exc}")
        return None

    return output_path if os.path.exists(output_path) else None


def _prepare_output_path(prefix: str, invoice_number: str | None, output_dir: str) -> str:
    _ensure_output_dir(output_dir)
    sanitized = _sanitize_invoice_number(invoice_number)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{sanitized}_{timestamp}.pdf"
    return os.path.join(output_dir, filename)


def generate_invoice_copy_pdf(invoice_data: dict, ticket_description: str = "", output_dir: str = "temp_docs") -> str | None:
    try:
        output_path = _prepare_output_path("Invoice_Copy", invoice_data.get("Invoice Number"), output_dir)

        rows: List[Row] = [
            ("Invoice Number", _safe_text(invoice_data.get("Invoice Number"))),
            ("Invoice Date", _safe_text(invoice_data.get("Invoice Date"), "Unknown")),
            ("Due Date", _safe_text(invoice_data.get("Due Date"), "Unknown")),
            ("Invoice Amount", _format_currency(invoice_data.get("Invoice Amount"))),
            (
                "Party",
                _safe_text(
                    invoice_data.get("Vendor Name") or invoice_data.get("Customer Name"),
                    "Not provided",
                ),
            ),
            ("Payment Status", _safe_text(invoice_data.get("Payment Status"), "Unknown")),
            ("PO Number", _safe_text(invoice_data.get("PO Number"), "Not available")),
        ]

        subtitle = _truncate(ticket_description) or "Invoice copy requested by user."
        notes = "Summary generated directly from the invoice ledger for convenience."
        return _build_pdf("Invoice Copy Summary", subtitle, rows, notes, output_path)
    except Exception as exc:
        print(f"? Invoice copy generation failed: {exc}")
        return None


def generate_payment_confirmation_pdf(invoice_data: dict, ticket_description: str = "", output_dir: str = "temp_docs") -> str | None:
    try:
        output_path = _prepare_output_path("Payment_Confirmation", invoice_data.get("Invoice Number"), output_dir)

        payment_status = _safe_text(invoice_data.get("Payment Status"), "Unknown")
        if payment_status.lower() == "paid":
            status_note = "Payment recorded as PAID in the ledger."
        elif payment_status.lower() == "unpaid":
            status_note = "Payment is still pending according to the ledger."
        else:
            status_note = "Payment status reflects the latest ledger update."

        rows: List[Row] = [
            ("Invoice Number", _safe_text(invoice_data.get("Invoice Number"))),
            ("Invoice Amount", _format_currency(invoice_data.get("Invoice Amount"))),
            ("Payment Status", payment_status),
            ("Due Date", _safe_text(invoice_data.get("Due Date"), "Unknown")),
            ("Clearing Date", _safe_text(invoice_data.get("Clearing Date"), "Not available")),
            (
                "Requester",
                _safe_text(
                    invoice_data.get("Vendor Name") or invoice_data.get("Customer Name"),
                    "Not provided",
                ),
            ),
        ]

        subtitle = _truncate(ticket_description) or "Payment confirmation shared with requester."
        return _build_pdf("Payment Confirmation", subtitle, rows, status_note, output_path)
    except Exception as exc:
        print(f"? Payment confirmation generation failed: {exc}")
        return None


def generate_invoice_details_pdf(invoice_data: dict, ticket_description: str = "", output_dir: str = "temp_docs") -> str | None:
    try:
        output_path = _prepare_output_path("Invoice_Details", invoice_data.get("Invoice Number"), output_dir)

        label_map = {
            "Invoice Number": "Invoice Number",
            "Invoice Date": "Invoice Date",
            "Invoice Amount": "Invoice Amount",
            "Vendor Name": "Vendor Name",
            "Vendor ID": "Vendor ID",
            "Customer Name": "Customer Name",
            "Customer ID": "Customer ID",
            "Payment Status": "Payment Status",
            "Due Date": "Due Date",
            "Clearing Date": "Clearing Date",
            "PO Number": "PO Number",
            "PO Status": "PO Status",
            "Payment Term": "Payment Term",
            "Country": "Country",
        }

        rows: List[Row] = []
        for key, label in label_map.items():
            value = invoice_data.get(key)
            if value is None or str(value).strip() == "":
                continue
            if "Amount" in label:
                rows.append((label, _format_currency(value)))
            else:
                rows.append((label, _safe_text(value)))

        if not rows:
            rows.append(("Notice", "No additional invoice attributes were available in the data source."))

        subtitle = _truncate(ticket_description) or "Comprehensive invoice details requested."
        notes = "Values shown above come directly from the latest invoice record in the Excel source."
        return _build_pdf("Invoice Details", subtitle, rows, notes, output_path)
    except Exception as exc:
        print(f"? Invoice details generation failed: {exc}")
        return None


if __name__ == "__main__":
    sample = {
        "Invoice Number": "INV-1001",
        "Invoice Date": "2026-02-10",
        "Invoice Amount": 1234.56,
        "Vendor Name": "ACME Corp",
        "Payment Status": "Paid",
        "Due Date": "2026-02-20",
    }
    print(generate_invoice_copy_pdf(sample, "Need copy"))
    print(generate_payment_confirmation_pdf(sample, "Need payment proof"))
    print(generate_invoice_details_pdf(sample, "Need details"))
