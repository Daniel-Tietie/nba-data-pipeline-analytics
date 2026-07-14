"""
Weekly pipeline: pull raw data for the 10 MVP seasons, rebuild the three
analytics tables, then gate on data quality checks. Task functions import
directly from src.ingestion and src.etl -- no shelling out to the CLI
scripts.
"""

from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from src.ingestion.ingest_standings import run as run_ingest_standings
from src.ingestion.ingest_playoff_games import run as run_ingest_playoff_games
from src.ingestion.ingest_player_season_stats import run as run_ingest_player_season_stats
from src.ingestion.ingest_shot_zones import run as run_ingest_shot_zones
from src.etl.build_analytics import run as run_build_analytics
from src.etl.dag_runs import record_dag_run, verify_latest_quality_checks

DAG_NAME = 'nba_pipeline'

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def _record(context, status, error_message=None):
    dag_run = context['dag_run']
    record_dag_run(
        dag_name=DAG_NAME,
        execution_date=dag_run.execution_date,
        start_time=dag_run.start_date or datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc),
        status=status,
        error_message=error_message,
    )


def _on_success(context):
    _record(context, status='success')


def _on_failure(context):
    error = context.get('exception')
    _record(context, status='failed', error_message=str(error) if error else None)


@dag(
    dag_id=DAG_NAME,
    schedule='@weekly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    on_success_callback=_on_success,
    on_failure_callback=_on_failure,
    tags=['nba', 'analytics'],
)
def nba_pipeline():

    @task
    def ingest_standings():
        return run_ingest_standings()

    @task
    def ingest_playoff_games():
        return run_ingest_playoff_games()

    @task
    def ingest_player_season_stats():
        return run_ingest_player_season_stats()

    @task
    def ingest_shot_zones():
        return run_ingest_shot_zones()

    @task
    def build_analytics():
        return run_build_analytics()

    @task
    def quality_checks():
        # build_analytics already logged its own checks into
        # data_quality_checks; this task gates the DAG on whether any of
        # them failed, so a bad check turns the task red, not just a log line.
        since = get_current_context()['dag_run'].start_date
        return verify_latest_quality_checks(since)

    (
        ingest_standings()
        >> ingest_playoff_games()
        >> ingest_player_season_stats()
        >> ingest_shot_zones()
        >> build_analytics()
        >> quality_checks()
    )


nba_pipeline()
