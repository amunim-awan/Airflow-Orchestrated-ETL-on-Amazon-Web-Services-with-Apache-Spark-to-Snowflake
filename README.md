# End-to-End ETL: Airflow + Spark on AWS → Snowflake

## Overview
This project demonstrates how we modernized a legacy, manual ETL setup into a reliable, production-grade data pipeline using orchestration, distributed processing, data quality checks, and automated monitoring.

The client needed dependable reporting across multiple business dimensions:
- Revenue reporting across products, regions, and facilities
- Media usage and video-visit analytics
- Forecasting views
- Category-level comparisons

To meet these needs, we designed and implemented a set of optimized ETL pipelines and a maintainable warehouse workflow that teams can trust daily.

## What This System Solves
### Phase 0 (Legacy State)
The initial ETL was scheduled with crontab and had typical operational gaps:
- No UI to monitor dependencies or job health
- No UI for logs (debugging was slow and painful)
- No separation between DEV and PROD
- Broad server access (weak governance / security)
- Failed jobs were rerun manually, creating risk and inconsistency

This created frequent delays, limited visibility, and high operational overhead. :contentReference[oaicite:1]{index=1}

## Solution Approach
We rebuilt the system as a fault-tolerant and scalable pipeline environment with:
- Reusable orchestration (DAG-based) pipelines
- Strong monitoring and easier troubleshooting
- Data QC checks with alerting
- Secure handling of sensitive data
- Dockerized services and CI/CD for repeatable deployments

The goal was not just “ETL that runs”, but ETL that is observable, reliable, and maintainable. :contentReference[oaicite:2]{index=2}

## Architecture (High-Level)
**Extract → Land (Raw) → Transform (Curated) → Load (Warehouse) → QC + Alerts → Reporting**
- Orchestration handles dependencies, retries, and scheduling
- Spark handles scalable transformations and heavy ingestion work
- Warehouse loading enables downstream dashboards and executive reporting
- QC checks protect trust in metrics (row counts, freshness, schema drift, null thresholds)
- Alerts notify stakeholders when failures or anomalies occur

## Implementation Phases

### Phase 1 (Stabilize & Orchestrate)
We introduced an orchestration layer and standardized pipeline execution:
- Implemented Airflow as the scheduler/orchestrator
- Provisioned EMR for distributed processing and a separate EC2 instance for Airflow
- Replaced CLI-based dependencies with standard Python libraries
- Streamlined existing workflows and expanded pipeline coverage
- Introduced Spark for the load phase

Outcome: major stability and visibility improvements, plus a foundation for scaling. :contentReference[oaicite:3]{index=3}

#### Phase 1 Challenges
As volume grew, new bottlenecks appeared:
- Main pipeline runtime ~4 hours
- EMR cost was high (2 clusters)
- Deployments were painful
- Single repo mixed orchestration + Spark code (hard to maintain)
- No partial loads
- Ingestion queries were slow

These issues drove the next optimization phase. :contentReference[oaicite:4]{index=4}

### Phase 2 (Optimize Performance & Cost)
We re-architected processing for speed, cost efficiency, and maintainability:
- Shifted processing to a Spark standalone cluster
- Achieved a significant speed boost
- Reduced cost by ~55%
- Separated storage and compute engines
- Dockerized both Airflow and the Spark cluster
- Split into separate repositories (Airflow vs Spark jobs)
- Added CI/CD (GitLab) for predictable deployments
- Added multiple Data QC checks

Outcome: faster pipelines, lower cost, cleaner deployments, and better long-term maintainability. :contentReference[oaicite:5]{index=5}

## Key Features
- **Fault tolerance & high availability:** retry logic, managed scheduling, consistent re-runs
- **Monitoring & logs:** centralized visibility into dependencies and failures
- **Data Quality Checks (QC):** freshness + completeness validation, schema/volume checks
- **Alerting:** notifications when jobs fail or data fails QC
- **Secure patterns:** controlled access and clearer environment separation
- **Docker + CI/CD:** repeatable deployments and faster iteration

## Who This Repo Is For
- Data engineers building production ETL
- Teams migrating from cron-based scripts to orchestrated pipelines
- Organizations needing reliable metrics across marketing, ops, and leadership reporting
- Anyone designing a “single source of truth” warehouse workflow with strong QC

## Next Improvements (Optional)
- Add more granular QC (distribution checks, anomaly detection, reconciliation vs source totals)
- Implement incremental/partial loads across all tables
- Add backfills and idempotent load patterns
- Add warehouse performance optimizations (clustering/partition strategies, query tuning)
- Expand alert routing (Slack + email + incident workflow)
