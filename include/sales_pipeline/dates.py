"""Batch-date resolution.

The batch date labels every stage row (`_load_date`) and is the key of the idempotent
"delete this batch, insert this batch" load. It is derived from the DAG run's logical date,
converted to the business timezone (Asia/Baku, UTC+4, no DST) - never from `now()`.

Why not simply `{{ ds }}`?  `ds` is the logical date in UTC, and what the logical date means for
cron schedules changed between Airflow 2 and 3. Resolving it in Python is explicit, version-proof
and unit-tested. Manual runs may have no logical date at all, hence the fallbacks.
"""
from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def resolve_batch_date(
    dag_run: Any, tz_name: str = "Asia/Baku", now: datetime | None = None
) -> date:
    """Return the business-calendar date of the run (logical_date -> run_after -> now)."""
    moment = getattr(dag_run, "logical_date", None) or getattr(dag_run, "run_after", None)
    if moment is None:
        moment = now or datetime.now(UTC)
        logger.warning("DagRun has no logical_date/run_after; falling back to current time")
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    batch_date = moment.astimezone(ZoneInfo(tz_name)).date()
    logger.info("Resolved batch date %s (source moment %s, tz %s)", batch_date, moment, tz_name)
    return batch_date
