"""Structured failure alerting for every task in this repo.

Wire as `on_failure_callback=alert_callback` on tasks or default_args. The callback
emits a single JSON line that drops into Slack/PagerDuty/OpsGenie webhooks without
further parsing. Alerts fire only on the final try to avoid alert fatigue on retries.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def alert_callback(context: dict[str, Any]) -> None:
    ti = context.get("task_instance") or context.get("ti")
    if ti is None:
        logger.warning("alert_callback invoked without task_instance in context")
        return

    try_number = getattr(ti, "try_number", None)
    max_tries = getattr(ti, "max_tries", None)
    if try_number is not None and max_tries is not None and try_number <= max_tries:
        return

    exception = context.get("exception")
    payload = {
        "event": "airflow_task_failed",
        "dag_id": getattr(context.get("dag"), "dag_id", None),
        "task_id": getattr(context.get("task"), "task_id", None),
        "run_id": getattr(ti, "run_id", None),
        "try_number": try_number,
        "max_tries": max_tries,
        "data_interval_start": _iso(context.get("data_interval_start")),
        "data_interval_end": _iso(context.get("data_interval_end")),
        "exception_type": type(exception).__name__ if exception else None,
        "exception_message": str(exception) if exception else None,
        "log_url": context.get("log_url"),
    }
    logger.error("ALERT %s", json.dumps(payload, default=str))


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        result = isoformat()
        return result if isinstance(result, str) else str(result)
    return str(value)
