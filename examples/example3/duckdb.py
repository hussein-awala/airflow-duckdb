"""
This is an example of how to configure the resources for the DuckDBPodOperator task.

In this example, we use the same query as in the previous example, but we set the resources
for the task to request 2 CPU and 8Gi of memory.
"""
from __future__ import annotations

from datetime import datetime

import kubernetes.client as k8s
from airflow import DAG
from airflow_duckdb.operators.duckdb import DuckDBPodOperator, S3FSConfig

with DAG(
    "duckdb_example3",
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
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "2", "memory": "8Gi"},
            limits={"cpu": "2", "memory": "8Gi"},
        ),
    )
