# AWS E-Commerce Data Lakehouse

## What is this?

This is a **data engineering** project. Data engineering sits between your backend (the app, the database) and the people who need to analyze that data (analysts, dashboards, ML models). The job is to move data reliably, transform it into something useful, and make sure it's always fresh and queryable.

You see this kind of system at any company with real data needs — Amazon tracking every order and click, Swiggy understanding drop-off in the checkout funnel, Zepto knowing which SKUs are flying off shelves. The tools differ, the pattern is the same: get data out of the database → clean it → serve it for analysis.

This project simulates that entire pipeline using AWS-native services on an e-commerce dataset (users, orders, clickstream events).

---

## Architecture

> *(diagram coming — will be added here)*

---

## Tech Stack

| Service | What it does in this project |
|---|---|
| **Terraform** | Provisions all AWS infrastructure as code — VPC, RDS, S3, IAM roles. One `terraform apply`, everything is up. |
| **RDS (PostgreSQL)** | The source-of-truth database. Holds users and orders tables, like any e-commerce backend would. |
| **AWS DMS** | Watches the Postgres write-ahead log and ships every INSERT / UPDATE / DELETE to S3 as CSV files. No polling, no cron — it reacts to changes as they happen. |
| **Kinesis + Firehose** | Handles clickstream events (views, clicks, add-to-carts, purchases). Events are streamed in real time, Firehose buffers them and lands GZIP files in S3 every 60 seconds. |
| **AWS Glue (PySpark)** | Three ETL jobs that transform raw S3 data into clean, queryable Iceberg tables. See [Glue Jobs](#glue-jobs) below. |
| **Apache Iceberg** | Table format on top of S3. Gives you UPDATE, DELETE, time-travel queries, and ACID guarantees — things plain Parquet files can't do. |
| **Athena** | Serverless SQL directly over the Iceberg tables. Used to create views for revenue, churn, and conversion funnel. |
| **Redshift Spectrum** | Lets Redshift query S3/Iceberg tables without loading data in. Used for BI workloads and materialized views. |
| **QuickSight** | Dashboards connected to Athena — daily revenue, conversion funnel by referrer, user churn KPIs. |
| **Step Functions** | Orchestrates the three Glue jobs in the right order. If any job fails, the pipeline stops and logs the failure. |
| **Glue Data Quality** | Ruleset on `fact_orders` — checks completeness, uniqueness, row count. Failures publish to CloudWatch. |
| **CloudWatch + SNS** | Alarms on pipeline failures and DQ failures. Sends email via SNS when something breaks. |

---

## Glue Jobs

Three PySpark jobs, each in `glue_jobs/`:

**`cdc-scd2-users-job.py`**
Reads DMS CDC files (LOAD + delta) and applies SCD Type 2 logic to `users_scd2`. When a user's address or status changes, the old row is closed (`is_current = false`, `expiration_date = now`) and a new row is inserted. Full history, never overwritten.

**`clickstream_process.py`**
Reads raw GZIP JSON from Firehose, deduplicates on `event_id`, and sessionizes — aggregating individual events into session-level metrics (clicks, views, add-to-carts, purchases per session).

**`fact-order-job.py`**
Reads CDC orders, deduplicates by keeping the latest version per `order_id`, filters deletes, and joins to the current snapshot of `users_scd2` to enrich with user name, email, and status.

---

# Project Proof

Screenshots follow the data as it moves through the pipeline.

## Phase 0 — Infrastructure
Terraform provisions everything. RDS is up, S3 bucket created, outputs show the endpoint.

### Terraform apply complete
![Terraform apply complete](screenshots/01_terraform_apply.png)

### S3 Endpoint
![Terraform apply complete](screenshots/s3_endpoint.png)
## Phase 1 — CDC with DMS
A real UPDATE runs in Postgres. DMS captures it and writes both the full load file and the delta to S3.

### DMS Endpoints in console
![DMS task in console](screenshots/05_dms_endpoint_console.png)
### DMS task in Cli
![DMS task in console](screenshots/02_dms_console.png)

### DB update running
![DB update running](screenshots/03_db_update.png)

### DMS output in S3 — LOAD + delta files
![DMS output in S3 — LOAD + delta files](screenshots/04_dms_s3_output.png)


## Phase 2 — Clickstream via Kinesis
Kinesis stream active, Python producer sending events, Firehose delivering GZIP files to S3.

### Kinesis stream active
![Kinesis stream active](screenshots/06_kinesis_stream.png)

### Glue databases created
![Glue databases created](screenshots/07_glue_databases.png)

### Python producer sending events
![Python producer sending events](screenshots/08_producer_sending.png)

### Clickstream files landing in S3
![Clickstream files landing in S3](screenshots/09_clickstream_s3.png)

## Phase 3 & 4 — Glue ETL + Iceberg Tables
All three Glue jobs succeed. Iceberg tables are created and populated.

### SCD TYPE 2 for DMS CDC
![Glue job runs — DMS SCD2](screenshots/10_scd_type_2.png)

### Clickstream Session Table 
![Glue job runs — clickstream](screenshots/11_clickstream_session.png)

### Fact Table
![Glue job runs — fact orders](screenshots/12_enriched_fact.png)

<!-- ### Clickstream files landing in S3
![users_scd2 Iceberg table](screenshots/12_users_scd2.png)

### Clickstream files landing in S3
![sessions Iceberg table](screenshots/13_sessions.png)

### Clickstream files landing in S3
![fact_orders Iceberg table](screenshots/14_fact_orders.png) -->

## Phase 5 — Analytics (Athena + Redshift + QuickSight)
Three Athena views created and queried. Redshift Spectrum reads the Iceberg tables. QuickSight dashboards published.

### Daily Revenue View - Athena Query
![Athena view 1 — daily revenue](screenshots/13_athena_daily_revenue.png)
### User Churn View - Athena Query
![Athena view 2 — user churn](screenshots/14_athena_user_churn.png)
### Conversion Funnel View - Athena Query
![Athena view 3 — conversion funnel](screenshots/15_athena_conversion_funnel.png)

### Daily Revenue - Quicksight Dashboard
![QuickSight — daily revenue dashboard](screenshots/16_qs_daily_revenue.png)

### User Churn - Quicksight Dashboard
![QuickSight — user churn](screenshots/17_qs_churn.png)

### Conversion Funnel - Quicksight Dashboard
![QuickSight — conversion funnel](screenshots/18_qs_funnel.png)


## Phase 6 — Orchestration + Monitoring
Step Functions runs all three jobs in sequence, all green. Glue DQ ruleset runs. CloudWatch alarm is intentionally triggered and recovers.

### Step Functions — execution graph
![Step Functions — execution graph](screenshots/19_step_functions_graph.png)

### Step Functions — all green execution
![Step Functions — all green execution](screenshots/20_step_functions_green.png)

### Glue Data Quality — Ruleset Results
![Glue Data Quality — ruleset results](screenshots/21_glue_dq.png)

### Testing with CloudWatch alarm + sns: (Setting an alarm for data quality):
![CloudWatch — alarm triggered (IN ALARM)](screenshots/22_cloudwatch_setting_alarm.png)

### Data Quality Test:
![CloudWatch — alarm recovered (OK)](screenshots/23_dq_test_result.png)

### CloudWatch Alarm Triggered:
![S3 — final curated data](screenshots/24_alarm_triggered.png)

---

## Cost

Total spend for the full project end-to-end: **under $3**.