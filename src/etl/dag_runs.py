"""
Helpers the Airflow DAG uses to record its own run history and to turn
data_quality_checks failures into an actual task failure, not just a
log line.
"""

import os
from datetime import datetime
from typing import Optional

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        dbname=os.getenv('DB_NAME', 'nba_pipeline'),
        user=os.getenv('DB_USER', 'nba_user'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT', '5432')
    )


def record_dag_run(
    dag_name: str,
    execution_date: datetime,
    start_time: datetime,
    end_time: datetime,
    status: str,
    error_message: Optional[str] = None,
    records_processed: Optional[int] = None,
) -> None:
    """Insert one row into dag_runs summarizing a DAG run."""
    duration_seconds = int((end_time - start_time).total_seconds())

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dag_runs (
                    dag_name, execution_date, start_time, end_time,
                    duration_seconds, status, error_message, records_processed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    dag_name, execution_date, start_time, end_time,
                    duration_seconds, status, error_message, records_processed,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def verify_latest_quality_checks(since: datetime) -> dict:
    """
    Raise if any data_quality_checks row logged since the given timestamp
    failed. Called as its own DAG task after build_analytics, so a failed
    check turns the task red instead of only appearing in a log line.
    """
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT check_name, table_name, records_checked, records_failed, error_message
                FROM data_quality_checks
                WHERE check_date >= %s AND passed = FALSE
                """,
                (since,),
            )
            failures = cur.fetchall()
    finally:
        conn.close()

    if failures:
        summary = "; ".join(f"{row[1]}.{row[0]}: {row[3]} failed of {row[2]}" for row in failures)
        raise RuntimeError(f"Data quality checks failed: {summary}")

    return {'failures': 0}
