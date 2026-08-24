"""Scheduled automatic billing entry point.

Runs the monthly close-and-charge cycle: for each active location, finalise
the billing period into a single invoice (fixed base price + metered usage
= total) and charge it against the card Paystack saved on that customer's
first payment.

Safe to run more often than monthly. close_billing_period() updates the
existing billing_records row for a period rather than creating duplicates,
charge attempts are skipped once a record is marked paid, and failed
attempts are rate-limited by the backoff in automatic_billing_service. This
matters because Railway cron granularity is coarse and a job that can only
safely run exactly once a month is a job that silently misses a month when
a deploy happens at the wrong moment.
"""
from __future__ import annotations

import logging

from services.automatic_billing_service import run_automatic_billing

logger = logging.getLogger(__name__)


def run_billing_cycle(billing_period: str | None = None):
    summary = run_automatic_billing(billing_period=billing_period)
    logger.info(
        "billing_cycle_complete period=%s closed=%s charged=%s failed=%s",
        summary.get("billing_period"), summary.get("closed"),
        summary.get("charged"), summary.get("failed"),
    )
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_billing_cycle()
