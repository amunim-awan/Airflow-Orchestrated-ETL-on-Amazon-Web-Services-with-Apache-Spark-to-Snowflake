"""Load curated data into Snowflake.

This uses Snowflake's COPY INTO pattern from an external stage (commonly S3).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import snowflake.connector


@dataclass(frozen=True)
class SnowflakeTarget:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str
    stage: str      # e.g. @MY_S3_STAGE
    table: str      # e.g. MEDIA_USAGE_FACT


def copy_into_table(
    target: SnowflakeTarget,
    stage_path: str,
    file_format: str = "TYPE=PARQUET",
    pattern: Optional[str] = None,
) -> None:
    """Copy parquet files from stage_path into target.table.

    stage_path example: @MY_S3_STAGE/curated/media_usage/dt=2026-01-01/
    """
    conn = snowflake.connector.connect(
        account=target.account,
        user=target.user,
        password=target.password,
        warehouse=target.warehouse,
        database=target.database,
        schema=target.schema,
        role=target.role or None,
    )
    try:
        with conn.cursor() as cur:
            pat = f" PATTERN='{pattern}'" if pattern else ""
            sql = f"""
            COPY INTO {target.database}.{target.schema}.{target.table}
            FROM {stage_path}
            FILE_FORMAT=({file_format})
            MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
            {pat}
            ON_ERROR='ABORT_STATEMENT'
            """
            cur.execute(sql)
    finally:
        conn.close()
