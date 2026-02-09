import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

# Ensure env vars are loaded
load_dotenv()

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

SENDER_EMAIL = os.getenv("SMTP_EMAIL")
SENDER_PASSWORD = os.getenv("SMTP_PASSWORD")

def send_email(to_email, subject, body):
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("❌ ERROR: SMTP Credentials missing in .env")
        return False

    print(f"DEBUG: Sending email to {to_email}")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = to_email

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("DEBUG: Email sent successfully")
        return True
    except Exception as e:
        print("ERROR: Email sending failed ->", e)
        return False

if __name__ == "__main__":
    print("--- Testing Email Service ---")
    if SENDER_EMAIL:
        print(f"Attempting to send test email from {SENDER_EMAIL}...")
        # Send to self for testing
        send_email(SENDER_EMAIL, "Test Email", "This is a test email from the EY Agent system.")
    else:
        print("❌ SENDER_EMAIL not set in .env")
