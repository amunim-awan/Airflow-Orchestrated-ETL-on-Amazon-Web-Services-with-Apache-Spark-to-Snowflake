"""Airflow DAG: extract -> transform (Spark) -> load (Snowflake) -> notify.

This matches the approach described in the slide deck:
- Move from cron (Phase 0) to Airflow orchestration (Phase 1)
- Use Spark for the load/transform phase and QC checks (Phase 1/2)
- Add alerts + clear monitoring (Phase 2) fileciteturn1file0L7-L17

Note: keep providers minimal. This DAG uses BashOperator for spark-submit to stay portable.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from src.config import load_config
from src.extract_media_usage import extract_media_usage
from src.snowflake_loader import SnowflakeTarget, copy_into_table
from src.alerts import notify_slack


def _extract(**context) -> str:
    config = load_config()
    run_date = context["data_interval_start"].date()
    s3_url = extract_media_usage(config, run_date=run_date)
    # Push for downstream tasks
    context["ti"].xcom_push(key="raw_s3_url", value=s3_url)
    return s3_url


def _load_to_snowflake(**context) -> None:
    config = load_config()
    run_date = context["data_interval_start"].date().isoformat()

    # Your stage path should map to your curated bucket prefix in Snowflake external stage.
    # Example assumes your stage points at s3://<curated-bucket>/curated/
    stage_path = f"{config.snowflake_stage}/media_usage/dt={run_date}/"

    target = SnowflakeTarget(
        account=config.snowflake_account,
        user=config.snowflake_user,
        password=config.snowflake_password,
        warehouse=config.snowflake_warehouse,
        database=config.snowflake_database,
        schema=config.snowflake_schema,
        role=config.snowflake_role,
        stage=config.snowflake_stage,
        table=config.snowflake_table,
    )
    copy_into_table(target, stage_path=stage_path)


def _notify_success(**context) -> None:
    config = load_config()
    run_date = context["data_interval_start"].date().isoformat()
    notify_slack(config.slack_webhook_url, f"✅ Media usage ETL succeeded for {run_date}")


def _notify_failure(context) -> None:
    try:
        config = load_config()
    except Exception:
        config = None
    run_date = context.get("data_interval_start")
    msg = f"❌ Media usage ETL failed for {run_date}. See Airflow logs for details."
    if config:
        notify_slack(config.slack_webhook_url, msg)


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="media_usage_etl_to_snowflake",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",  # daily at 02:00
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=6),
    on_failure_callback=_notify_failure,
    tags=["etl", "spark", "snowflake"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_api_to_s3_raw",
        python_callable=_extract,
    )

    # Use xcom in bash via templating if you want. Here we build deterministic paths by run_date.
    transform_task = BashOperator(
        task_id="spark_transform_raw_to_curated",
        bash_command=(
            "spark-submit /opt/airflow/spark_jobs/transform_media_usage.py "
            "--input s3://{{ var.value.S3_BUCKET_RAW }}/raw/media_usage/dt={{ ds }}/media_usage.jsonl "
            "--output s3://{{ var.value.S3_BUCKET_CURATED }}/curated/media_usage/dt={{ ds }}/ "
            "--run_date {{ ds }}"
        ),
    )

    load_task = PythonOperator(
        task_id="copy_curated_into_snowflake",
        python_callable=_load_to_snowflake,
    )

    notify_task = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
        trigger_rule="all_success",
    )

    extract_task >> transform_task >> load_task >> notify_task
