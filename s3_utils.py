"""Small S3 helpers used by extraction and loading steps."""
from __future__ import annotations

import json
from typing import Any, Iterable
import boto3


def s3_client(region: str):
    return boto3.client("s3", region_name=region)


def put_json_lines(bucket: str, key: str, rows: Iterable[dict[str, Any]], region: str) -> str:
    """Write JSON Lines into S3. Returns s3:// url."""
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
    client = s3_client(region)
    client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    return f"s3://{bucket}/{key}"
