
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from ml_engine.core.config import env

SMTP_HOST = env("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(env("SMTP_PORT", "587"))
SMTP_USER = env("SMTP_USER")
SMTP_PASSWORD = env("SMTP_PASSWORD")
FROM_EMAIL = env("FROM_EMAIL", SMTP_USER)

def send_email(to_email: str, subject: str, html_content: str) -> bool:
    """
    Send an email via SMTP.
    Returns True if successful, False otherwise.
    """
    if not SMTP_USER or not SMTP_PASSWORD:
        print("⚠️ SMTP credentials not set. Email skipped.")
        return False

    try:
        msg = MIMEMultipart()
        msg["From"] = FROM_EMAIL
        msg["To"] = to_email
        msg["Subject"] = subject

        msg.attach(MIMEText(html_content, "html"))

        print(f"📧 Sending email to {to_email} via {SMTP_HOST}:{SMTP_PORT}...")
        
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
            
        print("✅ Email sent successfully.")
        return True
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        return False
