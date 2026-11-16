"""Prepare a scenario, release batches and save results from Airflow and Prometheus."""

import argparse
import csv
import json
import os
import time
import uuid
from bisect import bisect_left
from datetime import datetime, timezone
from pathlib import Path
from statistics import median, quantiles

import httpx
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from trace_report import SERVICES, collect_traces, collect_timeline, collect_airflow_logs, clickhouse_profiles, write_report
from process_report import collect_process_profile
from resource_report import collect_resources, preflight, verify_prometheus, verify_process_profile


SCENARIOS = ("noop", "cpu", "io_http", "etl_csv", "api_ingest", "clickhouse_mart")


def utc_now():
    return datetime.now(timezone.utc)


def prepare(scenario, conf, experiment):
    if scenario in {"etl_csv", "api_ingest"}:
        columns = ["id", "region", "amount"]
        if scenario == "etl_csv":
            columns = [f"c{i}" for i in range(conf["cols"])]
            folder = Path(os.environ["BENCH_DATA"]) / "fixtures"
            folder.mkdir(exist_ok=True, mode=0o775)
            fixture = folder / f"{conf['rows']}-{conf['cols']}.csv"
            temporary = fixture.with_suffix(f".{experiment}.tmp")
            with temporary.open("w", newline="", encoding="utf-8") as output:
                writer = csv.writer(output)
                writer.writerow(columns)
                for row in range(conf["rows"]):
                    writer.writerow((row + col * 17) % 1000000 for col in range(conf["cols"]))
            temporary.replace(fixture)
        conf["target_table"] = experiment
        definition = sql.SQL(", ").join(
            sql.SQL("{} bigint NOT NULL").format(sql.Identifier(name)) for name in columns
        )
        with psycopg.connect(os.environ["BENCH_TARGET_DB"]) as connection:
            connection.execute(
                sql.SQL("CREATE TABLE {} (run_id text NOT NULL, {})").format(
                    sql.Identifier(experiment), definition,
                )
            )
            connection.execute(sql.SQL("CREATE INDEX ON {} (run_id)").format(sql.Identifier(experiment)))
    elif scenario == "clickhouse_mart":
        response = httpx.post(
            os.environ["BENCH_CLICKHOUSE"], auth=("bench", "bench"), timeout=60,
            params={"param_days": conf["days"]},
            content="""
                SELECT uniqExact(day, region, category), count(), sum(amount)
                FROM sales
                WHERE day < toDate('2025-01-01') + {days:UInt16}
            """,
        )
        response.raise_for_status()
        conf["mart_expected"] = [int(value) for value in response.text.strip().split("\t")]
        if not conf["mart_expected"][1]:
            raise ValueError("ClickHouse sales fixture is empty")


def api_request(client, method, path, **kwargs):
    try:
        response = client.request(method, path, **kwargs)
    except httpx.ConnectError as error:
        raise RuntimeError(
            f"Cannot connect to Airflow at {client.base_url}. "
            "Check 'docker compose ps -a' and 'docker compose logs api'."
        ) from error
    if response.is_error:
        raise RuntimeError(f"Airflow {method} {path}: {response.status_code} {response.text}")
    return response.json()


def run_batch(client, db, dag_id, conf, run_ids, timeout, pending_path):
    token = api_request(client, "GET", "/auth/token")["access_token"]
    client.headers["Authorization"] = f"Bearer {token}"
    dag = f"/api/v2/dags/{dag_id}"
    for _ in range(30):
        if client.get(dag).status_code != 404:
            break
        time.sleep(1)
    api_request(client, "PATCH", dag, json={"is_paused": True})
    for run_id in run_ids:
        api_request(client, "POST", dag + "/dagRuns", json={
            "dag_run_id": run_id, "logical_date": None, "conf": conf,
        })
    released = utc_now()
    pending_path.write_text(json.dumps({"dag_id": dag_id, "run_ids": run_ids,
                                       "released_at": released.isoformat()}), encoding="utf-8")
    api_request(client, "PATCH", dag, json={"is_paused": False})
    deadline = time.monotonic() + timeout
    while True:
        runs = db.execute("""
            SELECT run_id, state, start_date, end_date
            FROM dag_run WHERE dag_id = %s AND run_id = ANY(%s)
            ORDER BY run_id
        """, (dag_id, run_ids)).fetchall()
        if len(runs) == len(run_ids) and all(row["state"] in {"success", "failed"} for row in runs):
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Batch {run_ids[0]} did not finish within {timeout}s")
        time.sleep(2)
    tasks = db.execute("""
        SELECT id::text AS ti_id, run_id, task_id, state, scheduled_dttm, queued_dttm, start_date, end_date
        FROM task_instance WHERE dag_id = %s AND run_id = ANY(%s)
        ORDER BY run_id, task_id
    """, (dag_id, run_ids)).fetchall()
    finished = max(row["end_date"] for row in runs)
    return released, finished, runs, tasks


def task_times(tasks):
    for task in tasks:
        for name, first, last in [
            ("scheduled_to_queued_s", "scheduled_dttm", "queued_dttm"),
            ("queued_to_running_s", "queued_dttm", "start_date"),
            ("running_s", "start_date", "end_date"),
        ]:
            task[name] = (
                (task[last] - task[first]).total_seconds() if task[first] and task[last] else None
            )
    summary = {}
    for stage in sorted({task["task_id"] for task in tasks}):
        summary[stage] = {}
        for name in ("scheduled_to_queued_s", "queued_to_running_s", "running_s"):
            samples = [task[name] for task in tasks if task["task_id"] == stage and task[name] is not None]
            summary[stage][name] = {
                "n": len(samples),
                "p50": median(samples) if samples else None,
                "p95": quantiles(samples, n=100, method="inclusive")[94] if len(samples) >= 20 else None,
            }
    return summary


def prometheus_query(client, query, at):
    response = client.get("/api/v1/query", params={"query": query, "time": at})
    response.raise_for_status()
    return response.json()["data"]["result"]


def estimate_counter(samples, start, end):
    """Interpolate only when real samples cover both ends without a reset."""
    left = bisect_left(samples, start, key=lambda point: point[0])
    right = bisect_left(samples, end, key=lambda point: point[0])
    if right == len(samples) or (left == 0 and samples[left][0] > start):
        return None
    first = left if samples[left][0] == start else left - 1
    interval = samples[first:right + 1]
    gaps = [right[0] - left[0] for left, right in zip(interval, interval[1:])]
    if not gaps or max(gaps) > 8 or any(
        left[1] > right[1] for left, right in zip(interval, interval[1:])
    ):
        return None

    def at(stamp):
        index = bisect_left(interval, stamp, key=lambda point: point[0])
        right_time, right_value = interval[index]
        if right_time == stamp:
            return right_value
        left_time, left_value = interval[index - 1]
        return left_value + (right_value - left_value) * (stamp - left_time) / (right_time - left_time)

    return at(end) - at(start), max(gaps)


def fill_short_windows(client, metrics, start, end):
    pad = 8
    window = f"{round((end - start + 2 * pad) * 1000)}ms"
    pending = {service for service in SERVICES if "cpu_seconds" not in metrics[service]}
    for attempt in range(5):
        if not pending:
            break
        services = "|".join(sorted(pending))
        selector = f'{{job="{os.getenv("BENCH_RESOURCE_JOB", "airflow-cgroups")}",service=~"{services}"}}[{window}]'
        series = prometheus_query(client, "container_cpu_usage_seconds_total" + selector, end + pad)
        containers = {}
        for item in series:
            labels = item["metric"]
            key = (labels["service"], labels["id"])
            samples = [(float(stamp), float(value)) for stamp, value in item["values"]]
            containers.setdefault(key, []).append(estimate_counter(samples, start, end))
        for service in sorted(pending):
            valid = [parts for (name, _), parts in containers.items()
                     if name == service and all(part is not None for part in parts)]
            if len(valid) == 1:
                metrics[service].update(
                    cpu_seconds=sum(part[0] for part in valid[0]),
                    cpu_measurement="interpolated",
                    cpu_bracket_max_gap_seconds=max(part[1] for part in valid[0]),
                )
                pending.remove(service)
        if time.time() >= end + pad:
            break
        if pending and attempt < 4:
            time.sleep(0.75)

    missing_memory = {service for service in SERVICES if "memory_peak_bytes" not in metrics[service]}
    if missing_memory:
        services = "|".join(sorted(missing_memory))
        selector = f'{{job="{os.getenv("BENCH_RESOURCE_JOB", "airflow-cgroups")}",service=~"{services}"}}[{window}]'
        series = prometheus_query(client, "container_memory_working_set_bytes" + selector, end + pad)
        for service in missing_memory:
            samples = [(float(stamp), float(value)) for item in series if item["metric"]["service"] == service
                       for stamp, value in item["values"]]
            if samples:
                stamp, value = min(samples, key=lambda point: max(start - point[0], point[0] - end, 0))
                metrics[service].update(memory_near_window_bytes=value,
                                        memory_near_window_at=datetime.fromtimestamp(stamp, timezone.utc).isoformat())


def resource_metrics(start, end):
    window_ms = int((end - start).total_seconds() * 1000)
    if window_ms < 1:
        return {}, "Measurement window shorter than 1 ms"
    window = f"{window_ms}ms"
    metrics = {service: {} for service in SERVICES}
    queries = {
        "cpu_seconds": f"sum by (service) (increase(container_cpu_usage_seconds_total[{window}]))",
        "memory_peak_bytes": f"max by (service) (max_over_time(container_memory_working_set_bytes[{window}]))",
        "memory_samples": f"max by (service) (count_over_time(container_memory_working_set_bytes[{window}]))",
        "disk_read_bytes": f"sum by (service) (increase(container_fs_reads_bytes_total[{window}]))",
        "disk_write_bytes": f"sum by (service) (increase(container_fs_writes_bytes_total[{window}]))",
        "network_rx_bytes": f"sum by (service) (increase(container_network_receive_bytes_total[{window}]))",
        "network_tx_bytes": f"sum by (service) (increase(container_network_transmit_bytes_total[{window}]))",
        "cpu_throttled_seconds": f"sum by (service) (increase(container_cpu_cfs_throttled_seconds_total[{window}]))",
    }
    try:
        with httpx.Client(base_url=os.environ["BENCH_PROMETHEUS"], timeout=10) as client:
            for name, query in queries.items():
                query = query.replace('[', f'{{job="{os.getenv("BENCH_RESOURCE_JOB", "airflow-cgroups")}"}}[')
                for result in prometheus_query(client, query, end.timestamp()):
                    metrics[result["metric"]["service"]][name] = float(result["value"][1])
            fill_short_windows(client, metrics, start.timestamp(), end.timestamp())
    except (httpx.HTTPError, KeyError, ValueError) as error:
        return metrics, f"Prometheus: {error}"
    missing = [service for service, values in metrics.items() if "cpu_seconds" not in values]
    return metrics, f"Too few samples or missing container metrics: {', '.join(missing)}" if missing else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=SCENARIOS)
    parser.add_argument("--series", default="1,4,8,16", help="batch sizes, e.g. 1,4,8")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=600, help="maximum seconds for a batch to finish")
    parser.add_argument("--continue-on-failure", action="store_true", help="Continue after terminal failed DAGs")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--cols", type=int, default=12)
    parser.add_argument("--pages", type=int, default=8)
    parser.add_argument("--page-rows", type=int, default=1000)
    parser.add_argument("--delay", type=float, default=0.25)
    parser.add_argument("--io-bytes", type=int, default=1024**2)
    parser.add_argument("--cpu-iterations", type=int, default=5000000)
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    try:
        sizes = [int(size) for size in args.series.split(",")]
    except ValueError:
        parser.error("--series must contain comma-separated integers")
    if sizes != sorted(set(sizes)) or not all(1 <= size <= 128 for size in sizes):
        parser.error("--series must contain unique ascending sizes between 1 and 128")
    if min(args.repeats, args.timeout, args.rows, args.pages, args.cpu_iterations) < 1:
        parser.error("repeats, timeout, rows, pages and cpu-iterations must be positive")
    if not (1 <= args.cols <= 200 and 1 <= args.page_rows <= 10000
            and 0 <= args.delay <= 30 and 1 <= args.io_bytes <= 64 * 1024**2 and 1 <= args.days <= 30):
        parser.error("limits: cols 1..200, page-rows 1..10000, delay 0..30, io-bytes 1..64MiB, days 1..30")

    experiment = utc_now().strftime("b%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]
    output = Path(os.getenv("BENCH_RESULTS", "/bench/results")) / experiment
    output.mkdir(parents=True)
    conf = vars(args).copy()
    dag_id = f"bench_{args.scenario}"
    db_url = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].replace("postgresql+psycopg2:", "postgresql:", 1)
    schedule = [(0, 1)]
    for repeat in range(1, args.repeats + 1):
        schedule.extend((repeat, size) for size in (sizes if repeat % 2 else sizes[::-1]))
    with httpx.Client(base_url=os.environ["BENCH_AIRFLOW_API"], timeout=30) as client:
        with psycopg.connect(db_url, row_factory=dict_row, autocommit=True) as db:
            if not db.execute("SELECT pg_try_advisory_lock(83520) AS locked").fetchone()["locked"]:
                raise SystemExit("Another benchmark is running on this Airflow instance")
            prepare(args.scenario, conf, experiment)
            config = {"experiment": experiment, "parallelism": int(os.environ["AIRFLOW__CORE__PARALLELISM"]),
                      "resource_job": os.getenv("BENCH_RESOURCE_JOB", "airflow-cgroups"),
                      "executor": os.environ["AIRFLOW__CORE__EXECUTOR"],
                      "celery_operation_timeout": float(os.getenv("AIRFLOW__CELERY__OPERATION_TIMEOUT", "1.0")),
                      "worker_concurrency": int(os.environ["AIRFLOW__CELERY__WORKER_CONCURRENCY"]), **conf}
            (output / "config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")
            failed = False
            for repeat, size in schedule:
                ids = [f"{experiment}_r{repeat}_n{size}_{index}" for index in range(size)]
                print(f"{dag_id}: repeat={repeat}, runs={size}", flush=True)
                pending_path = output / "pending_batch.json"
                try:
                    if config["resource_job"] == "airflow-cgroups":
                        preflight(os.environ["BENCH_RESOURCE_URL"], os.environ["BENCH_PROMETHEUS"])
                    start, end, runs, tasks = run_batch(client, db, dag_id, conf, ids, args.timeout, pending_path)
                except (psycopg.Error, httpx.HTTPError, RuntimeError, TimeoutError) as error:
                    (output / "error.json").write_text(json.dumps({"at": utc_now().isoformat(),
                        "repeat": repeat, "size": size, "type": type(error).__name__, "message": str(error)}), encoding="utf-8")
                    print(f"Infrastructure error: {error}", flush=True)
                    failed = True
                    break
                successful = sum(run["state"] == "success" for run in runs)
                elapsed = (end - start).total_seconds()
                stages = task_times(tasks)
                task_running_seconds = sum(task["running_s"] or 0 for task in tasks)
                resources, warning = resource_metrics(start, end) if repeat else ({}, None)
                record = {
                    "repeat": repeat, "warmup": repeat == 0, "size": size, "released_at": start,
                    "finished_at": end, "duration_seconds": elapsed, "successful": successful,
                    "successful_dags_per_min": 60 * successful / elapsed if elapsed > 0 else None,
                    "task_running_seconds": task_running_seconds,
                    "running_slot_utilization": (
                        task_running_seconds / (elapsed * config["parallelism"]) if elapsed > 0 else None
                    ),
                    "stages": stages, "resources": resources, "metrics_warning": warning,
                    "runs": runs, "tasks": tasks,
                    "resource_job": config["resource_job"],
                }
                if repeat and config["resource_job"] == "airflow-cgroups":
                    collect_resources(record, Path(os.environ["BENCH_DATA"]), output)
                    verify_prometheus(record, os.environ["BENCH_PROMETHEUS"])
                    if not record["measurement_quality"]["valid"]:
                        failed = True
                        print("INVALID MEASUREMENT: " + record["metrics_warning"], flush=True)
                with (output / "series.jsonl").open("a", encoding="utf-8") as file:
                    file.write(json.dumps(record, default=str) + "\n")
                pending_path.unlink(missing_ok=True)
                print(f"  success={successful}/{size}, duration={elapsed:.2f}s", flush=True)
                if warning:
                    print(f"  {warning}", flush=True)
                estimates = [name for name, values in resources.items() if values.get("cpu_measurement") == "interpolated"]
                if estimates:
                    print(f"  CPU estimated from adjacent samples: {', '.join(estimates)}", flush=True)
                if successful != size:
                    failed = True
                    if not args.continue_on_failure or any(task["state"] in {"running", "queued"} for task in tasks):
                        break
                if repeat and record.get("measurement_quality", {}).get("valid") is False:
                    break
            series = output / "series.jsonl"
            records = [json.loads(line) for line in series.read_text().splitlines()] if series.exists() else []
            # Allow Alloy to send the last buffered lines after the measured window.
            time.sleep(3)
            for record in records:
                if not record["warmup"]:
                    collect_traces(record, dag_id, Path(os.environ["BENCH_DATA"]))
                    collect_process_profile(record, Path(os.environ["BENCH_DATA"]), output)
                    collect_timeline(record, os.environ["BENCH_PROMETHEUS"])
                    if config["resource_job"] == "airflow-cgroups":
                        collect_resources(record, Path(os.environ["BENCH_DATA"]), output)
                        verify_prometheus(record, os.environ["BENCH_PROMETHEUS"])
                        verify_process_profile(record)
                        failed = failed or not record["measurement_quality"]["valid"]
                    collect_airflow_logs(record, os.environ["BENCH_LOKI"], dag_id)
            if args.scenario == "clickhouse_mart":
                clickhouse_profiles(records, os.environ["BENCH_CLICKHOUSE"], experiment)
            write_report(output, config, records)
            print(f"Report: results/{experiment}/trace.html", flush=True)
    print(f"Results: results/{experiment}")
    if failed:
        raise SystemExit(f"Execution or resource measurement failed. See {output} and Airflow task logs.")


if __name__ == "__main__":
    main()
