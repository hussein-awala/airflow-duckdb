#!/bin/bash

mkdir -p /airflow/xcom
duckdb "$@"
