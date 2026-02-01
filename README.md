Manual ETL setup (cron jobs, no log UI, weak access control) and evolving it into a monitored, fault-tolerant pipeline using orchestration + Spark, with QC checks, alerts, and a containerized CI/CD workflow. 

End to end ETL using AWS snowfl…

 It also supports reporting needs like revenue by product/region/facility and media usage/video visit analytics, plus forecasting and category comparisons. 

Python code files I created for your repository

I generated a ready-to-push starter repo that matches your deck’s architecture (extract → transform with Spark → load into Snowflake → QC/alerts), and is structured so you can extend it per dataset/product line:

dags/media_usage_etl_dag.py (Airflow DAG: extract → spark-submit → Snowflake load → Slack notify)

spark_jobs/transform_media_usage.py (PySpark transform job: JSONL → Parquet curated)

src/extract_media_usage.py (API extraction → S3 raw JSONL)

src/snowflake_loader.py (Snowflake COPY INTO loader)

src/qc_checks.py (lightweight QC framework)

src/alerts.py (Slack webhook notifier)

src/config.py, src/s3_utils.py (config + S3 helpers)

scripts/local_run.py (run extraction locally for debugging)

tests/test_qc_checks.py (small unit tests)
