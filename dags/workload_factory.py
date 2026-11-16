"""Six benchmark DAGs with stable names and fixed topology."""

from datetime import datetime, timezone

from airflow.sdk import DAG, chain
from airflow.providers.standard.operators.python import PythonOperator

from bench_tasks import (
    build_mart, copy_to_postgres, extract_csv, fetch_pages, normalize_csv,
    normalize_json, run_cpu, run_io, run_noop, validate_mart,
)


def make_dag(scenario, stages):
    with DAG(
        dag_id=f"bench_{scenario}",
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        schedule=None,
        catchup=False,
        max_active_runs=128,
        max_active_tasks=128,
        default_args={"retries": 0},
        tags=["benchmark"],
    ) as dag:
        tasks = [
            PythonOperator(task_id=name, python_callable=function, op_kwargs=kwargs)
            for name, function, kwargs in stages
        ]
        if len(tasks) > 1:
            chain(*tasks)
    return dag


noop = make_dag("noop", [("work", run_noop, {})])
cpu = make_dag("cpu", [("work", run_cpu, {})])
io_http = make_dag("io_http", [("work", run_io, {})])
etl_csv = make_dag("etl_csv", [
    ("extract", extract_csv, {}),
    ("transform", normalize_csv, {}),
    ("load", copy_to_postgres, {"upstream": "transform"}),
])
api_ingest = make_dag("api_ingest", [
    ("fetch", fetch_pages, {}),
    ("normalize", normalize_json, {}),
    ("load", copy_to_postgres, {"upstream": "normalize"}),
])
clickhouse_mart = make_dag("clickhouse_mart", [
    ("build_mart", build_mart, {}),
    ("validate", validate_mart, {}),
])
