# AWS Loyalty Tier Data Pipeline

An end-to-end AWS data pipeline that ingests raw retail transaction data, cleans it, and builds a **Slowly Changing Dimension (Type 2) loyalty tier history table** — tracking each customer's tier (Bronze / Silver / Gold) over time based on cumulative spend.

Built as a self-directed project to move from data analyst to data engineer.

## What it does

Raw transaction CSV → cleaned Parquet → loyalty tier history table, fully in AWS.

1. **Ingest & clean** — A Glue (PySpark) job reads raw transaction CSVs from S3, fixes embedded-newline issues in quoted fields (which break Athena's row-splitting), and writes typed Parquet to S3.
2. **Build tier history** — A second Glue job calculates each customer's running cumulative spend, assigns a tier when thresholds are crossed, and produces an SCD Type 2 table with `valid_from` / `valid_to` validity windows and an `is_current` flag.
3. **Orchestration** — Triggered via Lambda on S3 upload; designed to run on a schedule via Airflow (see docs).

## Tech used

- **AWS Glue** (PySpark / Spark) — ETL jobs
- **Amazon S3** — data lake storage (raw, cleaned, output)
- **AWS Lambda** — event-driven job triggering
- **AWS IAM** — least-privilege scoped roles per job
- **Parquet** — columnar storage format
- **Apache Airflow** — orchestration (design stage)

## Repository structure

- `Scripts/` — Glue ETL jobs (cleaning, tier pipeline) and the Lambda trigger
- `IAM Roles/` — scoped IAM policy JSONs + role-to-policy mapping
- `Documentation/` — project specification, design decisions, and results
- `Datasets/` — sample transaction data

## Key design decisions

- **SCD Type 2** so a customer's tier at the time of any past transaction is preserved, not overwritten.
- **Least-privilege IAM** — each job's role is scoped to only the S3 prefixes and actions it needs; CloudWatch logging is a shared reusable policy.
- **Idempotent writes** — overwrite mode / dynamic partitioning so reruns don't duplicate data.
- **Cumulative spend based on actual amount paid** (quantity × price), not unit price — caught during testing when tiers weren't populating.

## Verified result

Example — customer 340516 progressing through all three tiers, with correctly chained validity windows (no gaps) and the current tier flagged:

| CustomerID | tier | valid_from | valid_to | is_current |
|---|---|---|---|---|
| 340516 | Bronze | 2023-10-06 | 2024-02-05 | N |
| 340516 | Silver | 2024-02-06 | 2024-04-26 | N |
| 340516 | Gold | 2024-04-27 | 9999-12-31 | Y |

Verified on the full ~200k-row dataset.

## Notes

- Account IDs in policy files are the author's development account.
- The tier ETL role includes interactive-session permissions used during development; a production batch version would not need these.
