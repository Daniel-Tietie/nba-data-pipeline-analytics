"""
Shared helper for logging analytics builder results into data_quality_checks.
Each builder calls log_check() once per rule it verifies after rebuilding
its table. Failures are logged loudly, not swallowed.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from psycopg2.extras import Json

logger = logging.getLogger(__name__)


def log_check(
    conn,
    check_name: str,
    table_name: str,
    passed: bool,
    records_checked: int,
    records_failed: int = 0,
    details: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None,
) -> None:
    """Insert one data_quality_checks row and log the result."""
    failure_rate = round(records_failed / records_checked, 3) if records_checked else 0.0

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_quality_checks (
                check_name, table_name, check_date, passed,
                records_checked, records_failed, failure_rate,
                check_details, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                check_name, table_name, datetime.now(), passed,
                records_checked, records_failed, failure_rate,
                Json(details or {}), error_message,
            ),
        )
    conn.commit()

    if passed:
        logger.info(f"  [PASS] {check_name}: {records_checked} checked, {records_failed} failed")
    else:
        logger.error(f"  [FAIL] {check_name}: {records_checked} checked, {records_failed} failed -- {error_message}")
