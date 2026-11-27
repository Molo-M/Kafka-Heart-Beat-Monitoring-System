"""
Airflow-style retries for the producer and consumer.

If an attempt fails, it is retried up to PIPELINE_RETRIES times, waiting
PIPELINE_RETRY_DELAY_SECONDS between attempts. The alert email is only
sent when every retry has failed.

Because our scripts run forever (unlike an Airflow task), the retry count
resets whenever an attempt makes progress: a failure after hours of normal
running gets a fresh set of retries.
"""

import time
import traceback

import config
from alerts import send_alert


def run_with_retries(name, attempt, get_progress):
    """
    Run attempt() and retry it if it fails.

    name         : shown in messages and the alert, e.g. "Consumer"
    attempt      : function that runs the pipeline until something goes wrong
    get_progress : function returning a number that grows as work is done
                   (e.g. messages handled), used to reset the retry count

    Returns True if attempt() finished normally, or False if it failed
    and all retries were used up (the alert has then been sent).
    Ctrl+C (KeyboardInterrupt) is not an error and is passed straight on.
    """
    retries = config.PIPELINE_RETRIES
    delay = config.PIPELINE_RETRY_DELAY_SECONDS
    failures_in_a_row = 0

    while True:
        progress_before = get_progress()
        try:
            attempt()
            return True
        except Exception as error:
            details = traceback.format_exc()

            # The attempt did some work before failing, so the earlier
            # failures are history: start counting again from zero
            if get_progress() > progress_before:
                failures_in_a_row = 0
            failures_in_a_row += 1

            if failures_in_a_row > retries:
                print(f"\n{name.upper()} FAILED after {failures_in_a_row} attempt(s): {error!r}")
                send_alert(
                    f"{name} failed",
                    f"The {name.lower()} failed {failures_in_a_row} time(s) in a row "
                    f"(1 attempt + {retries} retries, {delay:g} seconds apart) "
                    f"and has stopped.\n\n"
                    f"Last error:\n{error!r}\n\n"
                    f"Details:\n{details}",
                )
                return False

            print(f"\n{name} attempt {failures_in_a_row} failed: {error!r}")
            print(f"Retrying in {delay:g} seconds (retry {failures_in_a_row} of {retries})...")
            time.sleep(delay)