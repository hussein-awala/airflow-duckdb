"""
This is a simple example of how to use the DuckDBPodOperator with a S3FSConfig.

In this example, we load the S3 access key and secret from a connection, and the sql query from a file.
We set `do_xcom_push` to `True` to push the result of the query to XCom.
"""
from datetime import datetime
from airflow import DAG
from airflow_duckdb.operators.duckdb import DuckDBPodOperator, S3FSConfig


with DAG(
    "duckdb_example1",
    start_date=datetime(2024, 2, 1),
    schedule=None,
    catchup=False,
) as dag:
    DuckDBPodOperator(
        task_id="duckdb_task",
        query="flights.sql",
        do_xcom_push=True,
        s3_fs_config=S3FSConfig(
            endpoint="minio.default.svc.cluster.local:443",
            access_key_id="{{ conn.duckdb_s3.login }}",
            secret_access_key="{{ conn.duckdb_s3.password }}",
        ),
        # For local development with kind or minikube, set the image_pull_policy to Never
        image_pull_policy="Never",
    )
