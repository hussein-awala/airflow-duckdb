"""
This is an example of how to use the DuckDBPodOperator to extract data from a parquet table
and use the result to create new tasks dynamically.
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow_duckdb.operators.duckdb import DuckDBPodOperator, S3FSConfig

if TYPE_CHECKING:
    from airflow.utils.context import Context


class FlightProcessor(BaseOperator):
    def __init__(
        self,
        flight_date: str,
        distance: float,
        departure_time: float,
        arrival_time: float,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.flight_date = flight_date
        self.distance = distance
        self.departure_time = departure_time
        self.arrival_time = arrival_time

    def execute(self, context: Context) -> Any:
        print(f"Processing flight {self.flight_date} with distance {self.distance}")
        print(f"Departure time: {self.departure_time}, Arrival time: {self.arrival_time}")
        return self.arrival_time - self.departure_time


with DAG(
    "duckdb_example4",
    start_date=datetime(2024, 2, 1),
    schedule=None,
    catchup=False,
) as dag:
    duckdb_flights = DuckDBPodOperator(
        task_id="duckdb_task",
        query="flights.sql",
        do_xcom_push=True,
        s3_fs_config=S3FSConfig(
            endpoint="minio.default.svc.cluster.local:443",
            access_key_id="{{ conn.duckdb_s3.login }}",
            secret_access_key="{{ conn.duckdb_s3.password }}",
        ),
    )
    FlightProcessor.partial(
        task_id="process_flights",
    ).expand_kwargs(
        duckdb_flights.output,
    )
