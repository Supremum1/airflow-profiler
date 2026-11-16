"""Work performed inside Airflow tasks."""

import csv
import hashlib
import json
import os
import shutil
import time
from pathlib import Path

from airflow.sdk import get_current_context
from bench_trace import operation, trace_task


def task_conf():
    return get_current_context()["dag_run"].conf


def work_dir():
    context = get_current_context()
    key = f"{context['ti'].dag_id}/{context['run_id']}"
    path = Path(os.environ["BENCH_DATA"]) / "runs" / hashlib.sha256(key.encode()).hexdigest()
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_noop():
    with trace_task() as steps, operation(steps, "run_noop.empty_callable"):
        pass


def run_cpu():
    iterations = task_conf()["cpu_iterations"]
    with trace_task() as steps, operation(steps, "run_cpu.sha256_loop", iterations=iterations):
        payload = b"airflow-resource-benchmark"
        for _ in range(iterations):
            payload = hashlib.sha256(payload).digest()


def run_io():
    conf = task_conf()
    received = 0
    with trace_task() as steps, http_client(steps) as client:
        request = client.build_request("GET", os.environ["BENCH_HTTP"] + "/stream",
                                       params={"size": conf["io_bytes"], "delay": conf["delay"]})
        with operation(steps, "run_io.response_headers", delay=conf["delay"]):
            response = client.send(request, stream=True)
        try:
            response.raise_for_status()
            with operation(steps, "run_io.read_body", bytes=conf["io_bytes"]):
                for chunk in response.iter_bytes():
                    received += len(chunk)
        finally:
            response.close()
    if received != conf["io_bytes"]:
        raise ValueError(f"Expected {conf['io_bytes']} bytes, received {received}")


def http_client(steps, **kwargs):
    with operation(steps, "httpx.import_and_client"):
        import httpx

        return httpx.Client(timeout=60, **kwargs)


def extract_csv():
    conf = task_conf()
    source = Path(os.environ["BENCH_DATA"]) / "fixtures" / f"{conf['rows']}-{conf['cols']}.csv"
    path = work_dir() / "raw.csv"
    with trace_task() as steps, operation(steps, "extract_csv.copyfile", bytes=source.stat().st_size):
        shutil.copyfile(source, path)
    return {"path": str(path), "bytes": path.stat().st_size}


def normalize_csv():
    with trace_task() as steps:
        with operation(steps, "airflow.xcom_pull"):
            source = get_current_context()["ti"].xcom_pull(task_ids="extract")
        path = work_dir() / "normalized.csv"
        with operation(steps, "normalize_csv.read_convert_write", bytes=source["bytes"]):
            with open(source["path"], newline="", encoding="utf-8") as input_file:
                reader = csv.reader(input_file)
                columns = next(reader)
                with path.open("w", newline="", encoding="utf-8") as output_file:
                    writer = csv.writer(output_file)
                    rows = 0
                    for row in reader:
                        writer.writerow(int(value.strip()) for value in row)
                        rows += 1
    return {"path": str(path), "columns": columns, "rows": rows, "bytes": path.stat().st_size}


def fetch_pages():
    conf = task_conf()
    path = work_dir() / "raw.ndjson"
    started = time.monotonic()
    response_seconds = 0.0
    with trace_task() as steps, http_client(steps, base_url=os.environ["BENCH_HTTP"]) as client:
        with path.open("w", encoding="utf-8") as output_file:
            for page in range(conf["pages"]):
                with operation(steps, "fetch_pages.http_get", page=page):
                    response = client.get("/page", params={
                        "page": page, "rows": conf["page_rows"], "delay": conf["delay"],
                    })
                    response.raise_for_status()
                    response_seconds += response.elapsed.total_seconds()
                with operation(steps, "fetch_pages.decode_write", page=page):
                    for record in response.json():
                        output_file.write(json.dumps(record) + "\n")
    elapsed = time.monotonic() - started
    print(f"API pages={conf['pages']} response_seconds={response_seconds:.3f} elapsed={elapsed:.3f}")
    return {"path": str(path), "bytes": path.stat().st_size}


def normalize_json():
    from itertools import islice

    with trace_task() as steps:
        with operation(steps, "normalize_json.import_pandas"):
            import pandas as pd

        with operation(steps, "airflow.xcom_pull"):
            source = get_current_context()["ti"].xcom_pull(task_ids="fetch")
        path = work_dir() / "normalized.csv"
        rows = 0
        with open(source["path"], encoding="utf-8") as input_file:
            with path.open("w", newline="", encoding="utf-8") as output_file:
                while batch := list(islice(input_file, 1000)):
                    with operation(steps, "normalize_json.decode_flatten", rows=len(batch)):
                        frame = pd.json_normalize([json.loads(line) for line in batch])
                    with operation(steps, "normalize_json.to_csv", rows=len(batch)):
                        frame[["id", "customer.region", "amount"]].to_csv(output_file, index=False, header=False)
                    rows += len(frame)
    return {"path": str(path), "columns": ["id", "region", "amount"],
            "rows": rows, "bytes": path.stat().st_size}


def copy_to_postgres(upstream):
    with trace_task() as steps:
        return load_postgres(steps, upstream)


def load_postgres(steps, upstream):
    with operation(steps, "load_postgres.import_psycopg"):
        import psycopg
        from psycopg import sql
    context = get_current_context()
    with operation(steps, "airflow.xcom_pull"):
        manifest = context["ti"].xcom_pull(task_ids=upstream)
    table = sql.Identifier(task_conf()["target_table"])
    run_id = context["run_id"]
    columns = [sql.Identifier("run_id")] + [sql.Identifier(name) for name in manifest["columns"]]
    statement = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV)").format(
        table, sql.SQL(", ").join(columns),
    )
    # The runner generates run IDs from ASCII letters, digits and underscores.
    if not run_id.replace("_", "").isalnum():
        raise ValueError("Load requires a runner-generated run_id")
    prefix = run_id.encode() + b","
    with operation(steps, "load_postgres.connect"):
        connection = psycopg.connect(os.environ["BENCH_TARGET_DB"])
    with connection:
        with connection.cursor() as cursor:
            with operation(steps, "load_postgres.delete_previous"):
                cursor.execute(sql.SQL("DELETE FROM {} WHERE run_id = %s").format(table), (run_id,))
            with operation(steps, "load_postgres.copy_stream", rows=manifest["rows"], bytes=manifest["bytes"]):
                with cursor.copy(statement) as copy, open(manifest["path"], "rb") as source:
                    while lines := source.readlines(1024 * 1024):
                        copy.write(b"".join(prefix + line for line in lines))
            if cursor.rowcount != manifest["rows"]:
                raise ValueError(f"Expected {manifest['rows']} rows, loaded {cursor.rowcount}")
        with operation(steps, "load_postgres.commit"):
            connection.commit()
    return {"rows": manifest["rows"], "bytes": manifest["bytes"]}


def clickhouse(statement, **params):
    import httpx

    response = httpx.post(
        os.environ["BENCH_CLICKHOUSE"],
        auth=("bench", "bench"),
        params={"wait_end_of_query": 1, **params},
        content=statement,
        timeout=300,
    )
    response.raise_for_status()
    return response.text.strip()


def build_mart():
    context = get_current_context()
    run_id = context["run_id"]
    query_id = f"{run_id}_build_{context['ti'].try_number}"
    with trace_task() as steps, operation(steps, "build_mart.clickhouse_insert", query_id=query_id):
        clickhouse("""
        INSERT INTO sales_mart
        SELECT {build_id:String}, day, region, category, count(), sum(amount)
        FROM sales
        WHERE day < toDate('2025-01-01') + {days:UInt16}
        GROUP BY day, region, category
        """, query_id=query_id, param_build_id=query_id, param_days=task_conf()["days"])
    print(f"ClickHouse query_id={query_id}")
    return query_id


def validate_mart():
    with trace_task() as steps:
        with operation(steps, "airflow.xcom_pull"):
            build_id = get_current_context()["ti"].xcom_pull(task_ids="build_mart")
        query_id = build_id + "_validate"
        with operation(steps, "validate_mart.clickhouse_select", query_id=query_id):
            actual = clickhouse("""
                SELECT count(), sum(orders), sum(revenue) FROM sales_mart
                WHERE build_id = {build_id:String}
            """, query_id=query_id, param_build_id=build_id)
    if actual.split("\t") != [str(value) for value in task_conf()["mart_expected"]]:
        raise ValueError(f"Unexpected mart totals: {actual}")
