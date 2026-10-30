"""Sales pipeline: Northwind OData API -> PostgreSQL (stage) -> dbt (silver) -> dbt (mart star schema).

Schedule : every day at 09:00 Asia/Baku (cron `0 9 * * *`, timezone-aware start_date).
           Airflow itself keeps working in UTC; 09:00 Baku == 05:00 UTC (Azerbaijan: UTC+4, no DST).
Idempotent: re-running a date never duplicates data (see README, "Idempotency").

TG1 api_to_stage : 3 parallel extract+load tasks (one per API entity) -> data-quality gate
TG2 build_silver : dbt run (silver) -> dbt test (silver)
TG3 build_mart   : dbt run (mart)   -> dbt test (mart)
Dependency       : TG1 >> TG2 >> TG3
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TaskGroup, task

from sales_pipeline.config import get_settings
from sales_pipeline.dates import resolve_batch_date
from sales_pipeline.odata_client import ODataClient
from sales_pipeline.quality import assert_no_errors, run_stage_checks
from sales_pipeline.stage_load import ENTITIES, load_entity

DBT_BIN = "/opt/airflow/dbt_venv/bin/dbt"
DBT_DIRS = "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"


def _connect():
    return PostgresHook(postgres_conn_id=get_settings().dwh_conn_id).get_conn()


def _make_load_task(entity_name: str):
    """One extract+load task per API entity. Returns only a row count through XCom (never data)."""

    @task(task_id=f"load_{entity_name}")
    def load(**context) -> int:
        settings = get_settings()
        batch_date = resolve_batch_date(context["dag_run"], settings.business_timezone)
        client = ODataClient(
            settings.source_base_url,
            timeout=(settings.http_connect_timeout, settings.http_read_timeout),
        )
        conn = _connect()
        try:
            return load_entity(ENTITIES[entity_name], client, conn, batch_date)
        finally:
            conn.close()

    return load()


@task(task_id="check_stage")
def check_stage(**context) -> None:
    """Data-quality gate: stop the pipeline before bad raw data reaches silver/mart."""
    settings = get_settings()
    batch_date = resolve_batch_date(context["dag_run"], settings.business_timezone)
    conn = _connect()
    try:
        assert_no_errors(run_stage_checks(conn, batch_date))
    finally:
        conn.close()


with DAG(
    dag_id="sales_pipeline",
    description="Northwind API -> stage -> silver -> mart (star schema), daily 09:00 Baku",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Baku"),
    catchup=False,
    max_active_runs=1,  # never two overlapping runs fighting over the same tables
    dagrun_timeout=timedelta(hours=1),
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["sales", "dbt", "postgres"],
    doc_md=__doc__,
) as dag:
    with TaskGroup(group_id="api_to_stage") as api_to_stage:
        loads = [_make_load_task(name) for name in ENTITIES]
        loads >> check_stage()

    with TaskGroup(group_id="build_silver") as build_silver:
        dbt_run_silver = BashOperator(
            task_id="dbt_run_silver",
            bash_command=f"{DBT_BIN} run {DBT_DIRS} --select path:models/silver",
        )
        # "cautious": only tests whose parents are ALL silver (mart does not exist yet on 1st run).
        dbt_test_silver = BashOperator(
            task_id="dbt_test_silver",
            bash_command=(
                f"{DBT_BIN} test {DBT_DIRS} --select path:models/silver "
                "--indirect-selection cautious"
            ),
        )
        dbt_run_silver >> dbt_test_silver

    with TaskGroup(group_id="build_mart") as build_mart:
        dbt_run_mart = BashOperator(
            task_id="dbt_run_mart",
            bash_command=f"{DBT_BIN} run {DBT_DIRS} --select path:models/mart",
        )
        # Default (eager) selection also runs the fact-vs-silver reconciliation tests.
        dbt_test_mart = BashOperator(
            task_id="dbt_test_mart",
            bash_command=f"{DBT_BIN} test {DBT_DIRS} --select path:models/mart",
        )
        dbt_run_mart >> dbt_test_mart

    api_to_stage >> build_silver >> build_mart
