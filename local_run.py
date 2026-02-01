"""Run the pipeline locally without Airflow (useful for debugging).

You can point input/output to local paths or S3 paths if your AWS creds are set.
"""
from __future__ import annotations

import os
from datetime import date

from src.config import load_config
from src.extract_media_usage import extract_media_usage


def main():
    config = load_config()
    run_date = date.fromisoformat(os.getenv("RUN_DATE", date.today().isoformat()))
    raw_url = extract_media_usage(config, run_date=run_date)
    print(f"Wrote raw data to: {raw_url}")
    print("Next: run spark-submit to transform, then load to Snowflake.")


if __name__ == "__main__":
    main()
