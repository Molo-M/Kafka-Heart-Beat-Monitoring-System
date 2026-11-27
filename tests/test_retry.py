"""
Unit tests for the Airflow-style retry logic.

No waiting and no real emails: time.sleep and send_alert are replaced
with fakes that just record how they were called.

Run from the project root:
    pytest tests/test_retry.py -v
"""

import pytest

import config
import retry


@pytest.fixture
def alerts_sent(monkeypatch):
    """Use 2 retries, skip the waiting, and record alerts instead of emailing."""
    monkeypatch.setattr(config, "PIPELINE_RETRIES", 2)
    monkeypatch.setattr(config, "PIPELINE_RETRY_DELAY_SECONDS", 10)
    monkeypatch.setattr(retry.time, "sleep", lambda seconds: None)

    sent = []
    monkeypatch.setattr(retry, "send_alert", lambda subject, details: sent.append(subject))
    return sent


class FlakyTask:
    """A fake pipeline attempt that fails a set number of times, then succeeds."""

    def __init__(self, failures_before_success, makes_progress=False):
        self.failures_left = failures_before_success
        self.makes_progress = makes_progress
        self.attempts = 0
        self.progress = 0

    def attempt(self):
        self.attempts += 1
        if self.makes_progress:
            self.progress += 1  # e.g. handled a message before failing
        if self.failures_left > 0:
            self.failures_left -= 1
            raise ConnectionError("database is down")

    def get_progress(self):
        return self.progress


def test_success_on_first_attempt(alerts_sent):
    task = FlakyTask(failures_before_success=0)

    assert retry.run_with_retries("Consumer", task.attempt, task.get_progress) is True
    assert task.attempts == 1
    assert alerts_sent == []


def test_recovers_within_the_retries_without_alert(alerts_sent):
    task = FlakyTask(failures_before_success=2)  # 2 failures, 2 retries allowed

    assert retry.run_with_retries("Consumer", task.attempt, task.get_progress) is True
    assert task.attempts == 3
    assert alerts_sent == []


def test_alert_only_after_all_retries_fail(alerts_sent):
    task = FlakyTask(failures_before_success=100)  # never recovers

    assert retry.run_with_retries("Consumer", task.attempt, task.get_progress) is False
    assert task.attempts == 3  # 1 attempt + 2 retries
    assert alerts_sent == ["Consumer failed"]  # exactly one alert


def test_progress_resets_the_retry_count(alerts_sent):
    """Failures separated by real work are not 'in a row', so no alert."""
    task = FlakyTask(failures_before_success=5, makes_progress=True)

    assert retry.run_with_retries("Consumer", task.attempt, task.get_progress) is True
    assert task.attempts == 6
    assert alerts_sent == []


def test_ctrl_c_is_not_retried(alerts_sent):
    def stopped_by_user():
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        retry.run_with_retries("Consumer", stopped_by_user, lambda: 0)
    assert alerts_sent == []