"""
Unit tests for email alerts.

No real email is sent: the Gmail connection is replaced by a fake one
that records what would have been sent.

Run from the project root:
    pytest tests/test_alerts.py -v
"""

import smtplib

import pytest

import alerts
import config


class FakeSMTP:
    """Stands in for smtplib.SMTP_SSL and remembers what it was asked to do."""

    sent_messages = []
    logins = []

    def __init__(self, host, port, timeout):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def login(self, user, password):
        FakeSMTP.logins.append((user, password))

    def send_message(self, message):
        FakeSMTP.sent_messages.append(message)


class FailingSMTP(FakeSMTP):
    """A fake server that rejects the login, like Gmail does with a wrong password."""

    def login(self, user, password):
        raise smtplib.SMTPAuthenticationError(535, b"Username and Password not accepted")


@pytest.fixture(autouse=True)
def email_settings(monkeypatch):
    """Give every test complete email settings and a fresh fake server."""
    monkeypatch.setattr(config, "ALERT_EMAIL_SENDER", "sender@gmail.com")
    monkeypatch.setattr(config, "ALERT_EMAIL_PASSWORD", "abcdefghijklmnop")
    monkeypatch.setattr(config, "ALERT_EMAIL_RECIPIENT", "me@gmail.com")
    monkeypatch.setattr(alerts.smtplib, "SMTP_SSL", FakeSMTP)
    FakeSMTP.sent_messages = []
    FakeSMTP.logins = []


def test_alert_is_sent_with_the_right_content():
    assert alerts.send_alert("Consumer failed", "The database went down") is True

    assert FakeSMTP.logins == [("sender@gmail.com", "abcdefghijklmnop")]
    message = FakeSMTP.sent_messages[0]
    assert message["Subject"] == "[Heartbeat Pipeline] Consumer failed"
    assert message["To"] == "me@gmail.com"
    assert "The database went down" in message.get_content()


def test_no_alert_when_settings_are_missing(monkeypatch):
    monkeypatch.setattr(config, "ALERT_EMAIL_PASSWORD", "")

    assert alerts.send_alert("Consumer failed", "details") is False
    assert FakeSMTP.sent_messages == []


def test_email_problem_does_not_raise(monkeypatch):
    """A failed alert must never crash the script or hide the original error."""
    monkeypatch.setattr(alerts.smtplib, "SMTP_SSL", FailingSMTP)

    assert alerts.send_alert("Consumer failed", "details") is False