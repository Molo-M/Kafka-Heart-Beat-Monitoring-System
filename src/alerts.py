"""
Email alerts for pipeline failures.

The producer and consumer call send_alert() when they crash, so you get
an email telling you what failed and why.

Email settings come from .env (see .env.example). With Gmail, use an
App Password, never your normal Google password.

Send a test email to check your settings:
    python src/alerts.py
"""

import smtplib
import socket
from datetime import datetime, timezone
from email.message import EmailMessage

import config


def alerts_configured():
    """Return True if all the email settings needed to send alerts are filled in."""
    return bool(
        config.ALERT_EMAIL_SENDER
        and config.ALERT_EMAIL_PASSWORD
        and config.ALERT_EMAIL_RECIPIENT
    )


def build_message(subject, details):
    """Create the alert email (subject line, addresses and text)."""
    sent_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    message = EmailMessage()
    message["Subject"] = f"[Heartbeat Pipeline] {subject}"
    message["From"] = config.ALERT_EMAIL_SENDER
    message["To"] = config.ALERT_EMAIL_RECIPIENT
    message.set_content(
        f"{subject}\n\n"
        f"Time:    {sent_at}\n"
        f"Machine: {socket.gethostname()}\n\n"
        f"{details}\n"
    )
    return message


def send_alert(subject, details):
    """
    Send an alert email. Returns True if it was sent, False otherwise.

    This function never raises an error: if the email cannot be sent, it
    prints why and returns False, so a problem with alerting can never hide
    the original failure that the alert was about.
    """
    if not alerts_configured():
        print("Alert NOT sent: email settings are missing in .env")
        return False

    message = build_message(subject, details)

    try:
        # Port 465 uses an encrypted (SSL) connection from the start
        with smtplib.SMTP_SSL(config.SMTP_HOST, config.SMTP_PORT, timeout=15) as server:
            server.login(config.ALERT_EMAIL_SENDER, config.ALERT_EMAIL_PASSWORD)
            server.send_message(message)
    except (smtplib.SMTPException, OSError) as error:
        print(f"Alert NOT sent: {error}")
        return False

    print(f"Alert email sent to {config.ALERT_EMAIL_RECIPIENT}")
    return True


if __name__ == "__main__":
    send_alert(
        "Test alert",
        "This is a test email from the heartbeat monitoring pipeline.\n"
        "If you are reading this, email alerts are set up correctly.",
    )