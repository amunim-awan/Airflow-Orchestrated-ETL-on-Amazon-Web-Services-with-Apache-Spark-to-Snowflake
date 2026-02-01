"""Centralized configuration for the ETL system.

Keep secrets in environment variables or your Airflow Connections, not in git.
"""
from __future__ import annotations

from dataclasses import dataclass
import os


def _get_env(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return str(val) if val is not None else ""


@dataclass(frozen=True)
class AppConfig:
    # Data lake
    s3_bucket_raw: str
    s3_bucket_curated: str
    aws_region: str

    # Source API (example)
    source_api_base_url: str
    source_api_token: str

    # Snowflake
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_role: str
    snowflake_stage: str  # e.g. @MY_S3_STAGE
    snowflake_table: str  # e.g. MEDIA_USAGE_FACT

    # Ops
    slack_webhook_url: str


def load_config() -> AppConfig:
    """Load config from environment variables.

    In Airflow, set these as Variables/Connections and pass them into the container.
    """
    return AppConfig(
        s3_bucket_raw=_get_env("S3_BUCKET_RAW", required=True),
        s3_bucket_curated=_get_env("S3_BUCKET_CURATED", required=True),
        aws_region=_get_env("AWS_REGION", "us-east-1"),

        source_api_base_url=_get_env("SOURCE_API_BASE_URL", required=True),
        source_api_token=_get_env("SOURCE_API_TOKEN", required=True),

        snowflake_account=_get_env("SNOWFLAKE_ACCOUNT", required=True),
        snowflake_user=_get_env("SNOWFLAKE_USER", required=True),
        snowflake_password=_get_env("SNOWFLAKE_PASSWORD", required=True),
        snowflake_warehouse=_get_env("SNOWFLAKE_WAREHOUSE", required=True),
        snowflake_database=_get_env("SNOWFLAKE_DATABASE", required=True),
        snowflake_schema=_get_env("SNOWFLAKE_SCHEMA", required=True),
        snowflake_role=_get_env("SNOWFLAKE_ROLE", ""),
        snowflake_stage=_get_env("SNOWFLAKE_STAGE", required=True),
        snowflake_table=_get_env("SNOWFLAKE_TABLE", required=True),

        slack_webhook_url=_get_env("SLACK_WEBHOOK_URL", ""),
    )
