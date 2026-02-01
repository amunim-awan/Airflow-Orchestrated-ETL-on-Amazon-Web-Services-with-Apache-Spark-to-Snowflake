"""Example extraction step.

In your real project, replace the endpoint + pagination logic to match your source system.
"""
from __future__ import annotations

from datetime import date
from typing import Any
import requests

from .s3_utils import put_json_lines
from .config import AppConfig


def extract_media_usage(config: AppConfig, run_date: date) -> str:
    """Pull media usage records from a source API and store them in S3 raw zone.

    Returns the S3 URL of the raw file.
    """
    # Example endpoint. Adapt to your API.
    url = f"{config.source_api_base_url.rstrip('/')}/media-usage"
    headers = {"Authorization": f"Bearer {config.source_api_token}"}

    params: dict[str, Any] = {"date": run_date.isoformat(), "page": 1, "page_size": 500}
    rows: list[dict[str, Any]] = []

    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        items = payload.get("items", [])
        rows.extend(items)

        # Basic pagination pattern. Adjust as needed.
        if not payload.get("has_more"):
            break
        params["page"] = int(params["page"]) + 1

    key = f"raw/media_usage/dt={run_date.isoformat()}/media_usage.jsonl"
    return put_json_lines(config.s3_bucket_raw, key, rows, region=config.aws_region)
