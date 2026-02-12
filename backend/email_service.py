import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

SENDER_EMAIL = os.getenv("SMTP_EMAIL")
SENDER_PASSWORD = os.getenv("SMTP_PASSWORD")


def send_email(to_email: str, subject: str, body: str, attachment_path: str = None) -> bool:
    """
    Send an email with optional attachment.
    
    Args:
        to_email: Recipient email address
        subject: Email subject
        body: Plain text email body
        attachment_path: Optional full path to file to attach
    
    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("❌ ERROR: SMTP credentials missing in .env file")
        return False

    print(f"DEBUG: Preparing to send email to {to_email}")
    if attachment_path:
        print(f"DEBUG: Attachment requested: {attachment_path}")

    # Decide message type
    if attachment_path and os.path.exists(attachment_path):
        msg = MIMEMultipart()
        msg.attach(MIMEText(body, "plain"))
        
        # Attach the file
        try:
            with open(attachment_path, "rb") as attachment_file:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment_file.read())
            
            # Encode file in base64
            encoders.encode_base64(part)
            
            # Add header for attachment
            filename = os.path.basename(attachment_path)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{filename}"'
            )
            
            msg.attach(part)
            print(f"DEBUG: File attached successfully: {filename}")
            
        except Exception as e:
            print(f"WARNING: Failed to attach file {attachment_path}: {e}")
            # Continue without attachment instead of failing completely
            msg = MIMEText(body)
    else:
        msg = MIMEText(body)

    # Common headers
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = to_email

    # Send the email
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        print("DEBUG: Email sent successfully")
        return True
        
    except Exception as e:
        print("ERROR: Email sending failed →", str(e))
        return False


def send_test_email():
    """Quick test function - sends email to sender itself"""
    if not SENDER_EMAIL:
        print("❌ SENDER_EMAIL not set in .env")
        return
    
    print(f"--- Sending test email from {SENDER_EMAIL} ---")
    
    success = send_email(
        to_email=SENDER_EMAIL,
        subject="Test Email - Query Agent",
        body="This is a test message from the Query Management System.\n\n"
             "If you see this email, SMTP configuration is working correctly.",
    )
    
    if success:
        print("✓ Test email sent successfully!")
    else:
        print("✗ Test email failed")


if __name__ == "__main__":
    print("=== Email Service Test ===")
    send_test_email()
