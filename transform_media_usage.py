"""Spark transformation job: Raw JSONL -> Curated Parquet

Run (example):
spark-submit spark_jobs/transform_media_usage.py \
  --input s3://my-raw-bucket/raw/media_usage/dt=2026-01-31/media_usage.jsonl \
  --output s3://my-curated-bucket/curated/media_usage/dt=2026-01-31/
"""
from __future__ import annotations

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit


REQUIRED_COLUMNS = ["event_id", "user_id", "event_time", "product", "region", "facility", "metric_name", "metric_value"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="S3/local path to JSON lines")
    p.add_argument("--output", required=True, help="S3/local output folder for parquet")
    p.add_argument("--run_date", default="", help="Optional run date (YYYY-MM-DD) to stamp into data")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("transform_media_usage").getOrCreate()

    df = spark.read.json(args.input)

    # Basic schema normalization. Adjust to match your actual payload.
    df2 = (
        df.select(
            col("event_id").cast("string"),
            col("user_id").cast("string"),
            to_timestamp(col("event_time")).alias("event_time"),
            col("product").cast("string"),
            col("region").cast("string"),
            col("facility").cast("string"),
            col("metric_name").cast("string"),
            col("metric_value").cast("double"),
        )
        .withColumn("run_date", lit(args.run_date or None))
    )

    # Lightweight QC inside the job (fail fast)
    missing = [c for c in REQUIRED_COLUMNS if c not in df2.columns]
    if missing:
        raise RuntimeError(f"Missing required columns after transform: {missing}")

    if df2.count() == 0:
        raise RuntimeError("Transform produced 0 rows")

    (
        df2.repartition(8)
           .write.mode("overwrite")
           .parquet(args.output)
    )

    spark.stop()


if __name__ == "__main__":
    main()
