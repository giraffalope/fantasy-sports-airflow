"""
Asset URIs for the yearly ``backfill.*`` Cup scraper DAGs.

Separate from production assets so backfill runs do not trigger prod DAGs (or vice versa).
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import Asset

# Inclusive calendar-year backfill window (one DAG run per year on Dec 31).
BACKFILL_START_DATE = datetime(2010, 12, 31)
BACKFILL_END_DATE = datetime(2025, 12, 31)
BACKFILL_YEARLY_SCHEDULE = "0 0 31 12 *"

# Prod Cup scraper DAGs that must align on logical_date (hourly tick, top of hour UTC).
CUP_SCRAPER_PROD_HOURLY_CRON = "0 * * * *"

BACKFILL_FRCS_CUP_SCHEDULE_ASSET = Asset(
    uri="nascar://backfill/frcs/cup/schedule.csv",
    name="backfill.nascar.cup.scraper.frcs.schedule",
)
BACKFILL_RACING_REFERENCE_CUP_SCHEDULE_ASSET = Asset(
    uri="nascar://backfill/racing-reference/cup/schedule.csv",
    name="backfill.nascar.cup.scraper.racing_reference.schedule",
)
BACKFILL_FRCS_CUP_RACE_WEEK_ASSET = Asset(
    uri="nascar://backfill/frcs/cup/race-week.csv",
    name="backfill.nascar.cup.scraper.frcs.race_week",
)
BACKFILL_RACING_REFERENCE_CUP_RACE_WEEK_ASSET = Asset(
    uri="nascar://backfill/racing-reference/cup/race-week.csv",
    name="backfill.nascar.cup.scraper.racing_reference.race_week",
)
