# Airflow DuckDB on Kubernetes

[DuckDB](https://duckdb.org/) is an in-memory analytical database to run analytical queries on large data sets.

[Apache Airflow](https://airflow.apache.org/) is an open-source platform for developing, scheduling, and monitoring
batch-oriented workflows.

Apache Airflow is not an ETL tool, but more of a workflow scheduler that can be used to schedule and monitor ETL jobs.
Airflow users create DAGs to schedule Spark, Hive, Athena, Trino, BigQuery, and other ETL jobs to process their data.

By using DuckDB with Airflow, the users can run analytical queries on local or remote large data sets and store the
results without the need to use these ETL tools.

To use DuckDB with Airflow, the users can use the PythonOperator with the DuckDB Python library, the BashOperator with
the DuckDB CLI, or one of the available Airflow operators that support DuckDB (e.g.
[airflow-provider-duckdb](https://github.com/astronomer/airflow-provider-duckdb) developed by Astronomer). All of these
operators will be running in the worker pod and limited by its resources, for that reason, some users use the
Kubernetes Executor to run the tasks in a dedicated Kubernetes pod to request more resources when needed.

Setting up Kubernetes Executor could be a bit challenging for some users, especially maintaining the workers docker
image. This project provides an alternative solution to run DuckDB with Airflow using the KubernetesPodOperator.

## How to use

The developed operator is completely based on the KubernetesPodOperator, so it needs cncf-kubernetes provider to be
installed in the Airflow environment (preferably the latest version to profit from all the features).

### Install the package

To use the operator, you need to install the package in your Airflow environment. You can install the package using pip:

```bash
pip install airflow-duckdb
```

### Use the operator

The operators supports all the parameters of the KubernetesPodOperator, and it has some additional parameters to
simplify the usage of DuckDB.

Here is an example of how to use the operator:

```python
with DAG("duckdb_dag", ...) as dag:
    DuckDBPodOperator(
        task_id="duckdb_task",
        query="SELECT MAX(col1) AS  FROM READ_PARQUET('s3://my_bucket/data.parquet');",
        do_xcom_push=True,
        s3_fs_config=S3FSConfig(
            access_key_id="{{ conn.duckdb_s3.login }}",
            secret_access_key="{{ conn.duckdb_s3.password }}",
        ),
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "1", "memory": "8Gi"},
            limits={"cpu": "1", "memory": "8Gi"},
        ),
    )
```

## Features

The current version of the operator supports the following features:
- Running one or more DuckDB queries in a Kubernetes pod
- Configuring the pod resources (requests and limits) to run the queries
- Configuring the S3 credentials securely with a Kubernetes secret to read and write data from/to S3
(AWS S3, MinIO or GCS with S3 compatibility)
- Using Jinja templating to configure the query
- Loading the queries from a file
- Pushing the query result to XCom

The project also provides a Docker image with DuckDB CLI and some extensions to use it with Airflow.
