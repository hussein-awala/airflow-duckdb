"""
This is an example of how to use the DuckDBPodOperator to append data to a table in DuckDB.

In this example, we load a CSV file from a S3 bucket contains information about chauffeur licenses,
we filter the expired licenses last month, and we append the result to a parquet table partitioned
by year and month. Finally, we select the count of expired licenses to push it to XCom.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow_duckdb.operators.duckdb import DuckDBPodOperator, S3FSConfig

with DAG(
    "duckdb_example2",
    start_date=datetime(2024, 2, 1),
    schedule="@monthly",
    catchup=False,
) as dag:
    DuckDBPodOperator(
        task_id="duckdb_task",
        query="expired_licenses.sql",
        do_xcom_push=True,
        s3_fs_config=S3FSConfig(
            endpoint="minio.default.svc.cluster.local:443",
            access_key_id="{{ conn.duckdb_s3.login }}",
            secret_access_key="{{ conn.duckdb_s3.password }}",
        ),
    )
