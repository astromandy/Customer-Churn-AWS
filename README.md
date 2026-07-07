# Customer Churn AWS ETL Pipeline

This project shows an end-to-end customer churn analytics pipeline built with AWS services and orchestrated with Apache Airflow. The goal is to move raw churn data into a cleaned analytics layer that can be queried in Athena and visualized in Power BI.

## What the pipeline does

1. Stores raw churn data in Amazon S3
2. Uses AWS Glue Catalog and crawler metadata
3. Runs a Glue ETL job to clean and deduplicate the dataset
4. Writes the transformed data back to S3 in Parquet format
5. Refreshes Athena metadata
6. Exposes the curated data for BI dashboards

## Repository structure

```text
Customer-Churn-AWS/
├── Dag/
│   └── cust_churn_glue_etl.py
├── Glue/
│   └── glue_auto_etl.py
├── Dataset/
│   └── Telco_customer_churn.csv
├── Customer Churn Data Analytics Dashboard/
└── README.md
```

## AWS resources expected

- Raw data bucket: `cust-churn-raw-data`
- Processed data bucket: `cust-churn-processed-data`
- Athena results bucket: `cust-churn-athena-results`
- Glue database: `churn_db`
- Glue source table: `cust_churn_raw`
- Athena target table: `cleaned_churn_data`
- Default AWS region in code: `us-east-1`

You can override these values with environment variables when deploying the DAG.

## Airflow DAG behavior

The DAG in `Dag/cust_churn_glue_etl.py`:

- checks whether new files exist in the raw S3 bucket
- triggers the Glue job
- passes the source database, source table, and processed output path as Glue script arguments
- runs `MSCK REPAIR TABLE` in Athena after the ETL step

### Environment variables supported by the DAG

- `RAW_BUCKET_NAME`
- `PROCESSED_BUCKET_NAME`
- `ATHENA_RESULTS_BUCKET_NAME`
- `AWS_REGION`
- `GLUE_JOB_NAME`
- `GLUE_DATABASE`
- `ATHENA_TABLE`

## Glue job behavior

The Glue script in `Glue/glue_auto_etl.py`:

- reads the source table from the Glue Catalog
- drops the `Zip Code` column if present
- removes duplicate rows
- writes the cleaned output to S3 as Parquet

Optional Glue job arguments:

- `--SOURCE_DATABASE`
- `--SOURCE_TABLE`
- `--OUTPUT_PATH`

## Suggested setup flow

1. Upload `Dataset/Telco_customer_churn.csv` to the raw S3 bucket.
2. Create a Glue crawler pointed at the raw bucket.
3. Confirm the crawler creates the `cust_churn_raw` table in `churn_db`.
4. Create a Glue job using `Glue/glue_auto_etl.py`.
5. Point your Airflow or MWAA DAG to `Dag/cust_churn_glue_etl.py`.
6. Create or refresh an Athena table over `s3://cust-churn-processed-data/cleaned_data/`.
7. Connect Power BI to Athena for dashboarding.

## Portfolio talking points

- Serverless data engineering workflow using core AWS analytics services
- Clear separation between raw, processed, and query-results storage
- Practical orchestration example with Airflow and Glue integration
- Business-facing output through Athena and Power BI

## Current limitations

- The repository does not include IAM policies, Terraform, or CloudFormation
- Athena DDL is not versioned here yet
- The dashboard artifact is included, but refresh instructions are still manual

## Recommended next improvements

- Add infrastructure-as-code for buckets, roles, and Glue resources
- Version the Athena DDL used to expose the curated table
- Add data quality checks before the Glue write step
- Document the Power BI connection setup with screenshots
