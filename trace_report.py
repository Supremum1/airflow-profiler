"""Join task code intervals with measured Airflow component activity."""

import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

SERVICES = ("scheduler", "worker", "api", "dag-processor", "metadata", "redis")
SERVICE_PATTERN = "|".join(SERVICES)
TASK_ID_IN_LOG = re.compile(r"\btask_id=['\"]?([\w.-]+)")
CELERY_ID_IN_LOG = re.compile(r"(?:execute_workload)?\[([0-9a-f-]{36})\]")
LOG_CONTEXT_SECONDS = 2


def stamp(value):
    return datetime.fromisoformat(value).timestamp() if value else None


def collect_traces(record, dag_id, data_dir):
    operations = []
    boundaries = []
    missing = []
    for task in record["tasks"]:
        folder = data_dir / "traces" / dag_id / task["run_id"]
        paths = list(folder.glob(f"{task['task_id']}.*.json"))
        if not paths:
            missing.append(f"{task['run_id']}/{task['task_id']}")
            continue
        for path in sorted(paths):
            trace = json.loads(path.read_text(encoding="utf-8"))
            for key in ("memory_before", "memory_after"):
                if key in trace:
                    point = trace[key]
                    boundaries.append({**point, "run_id": trace["run_id"], "task_id": trace["task_id"],
                                       "pid": trace["pid"], "boundary": key})
            for step in trace["operations"]:
                operations.append({**step, "run_id": trace["run_id"], "task_id": trace["task_id"],
                                   "pid": trace["pid"], "try_number": trace["try_number"]})
    record["operations"] = sorted(operations, key=lambda step: step["start"])
    record["task_boundaries"] = boundaries
    record["missing_traces"] = missing


def overlaps(start, end, left, right):
    return max(0, min(end, right) - max(start, left))


def collect_timeline(record, url):
    import httpx

    start = stamp(record["released_at"])
    end = stamp(record["finished_at"])
    # Read actual scrape timestamps, including the samples just outside the batch.
    pad = 5
    window = f"{round((end - start + 2 * pad) * 1000)}ms"
    record["timeline"] = {}
    try:
        with httpx.Client(base_url=url, timeout=15) as client:
            for key, metric in (
                ("cpu", "container_cpu_usage_seconds_total"),
                ("memory", "container_memory_working_set_bytes"),
            ):
                response = client.get("/api/v1/query", params={
                    "query": f'{metric}{{job="{record.get("resource_job", os.getenv("BENCH_RESOURCE_JOB", "airflow-cgroups"))}",service=~"{SERVICE_PATTERN}"}}[{window}]',
                    "time": end + pad,
                })
                response.raise_for_status()
                samples = response.json()["data"]["result"]
                record["timeline"][key] = [
                    {"labels": series["metric"], "values": [[t, float(v)] for t, v in series["values"]]}
                    for series in samples
                ]
            record["timeline_warning"] = None
            response = client.get("/api/v1/query", params={
                "query": f'pg_stat_database_xact_commit{{job="metadata-postgres",datname="bench"}}[{window}]',
                "time": end + pad,
            })
            response.raise_for_status()
            record["timeline"]["commits"] = [point for series in response.json()["data"]["result"]
                                             for point in series["values"]]
    except (httpx.HTTPError, KeyError, ValueError) as error:
        record["timeline_warning"] = str(error)


def file_log_entries(path, start, end, **labels):
    with path.open(encoding="utf-8", errors="replace") as file:
        for line in file:
            try:
                at = stamp(json.loads(line)["timestamp"])
            except (ValueError, KeyError, TypeError):
                continue
            if at is not None and start <= at <= end:
                yield {"at": at, "line": line.rstrip("\r\n"), **labels}


def task_logs(record, dag_id, start, end):
    root = Path(os.getenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs")) / f"dag_id={dag_id}"
    entries, missing = [], 0
    for task in record["tasks"]:
        folder = root / f"run_id={task['run_id']}" / f"task_id={task['task_id']}"
        paths = sorted(folder.glob("attempt=*.log"))
        if not paths and task.get("start_date"):
            missing += 1
        for path in paths:
            entries.extend(file_log_entries(path, start, end, run_id=task["run_id"],
                                            task_id=task["task_id"], source="task"))
    return entries, missing


def dag_processor_logs(start, end):
    root = Path(os.getenv("AIRFLOW__LOGGING__DAG_PROCESSOR_CHILD_PROCESS_LOG_DIRECTORY",
                          "/opt/airflow/logs/dag_processor"))
    day = datetime.fromtimestamp(start, timezone.utc).date()
    last = datetime.fromtimestamp(end, timezone.utc).date()
    while day <= last:
        for path in (root / day.isoformat()).rglob("*.log"):
            yield from file_log_entries(path, start, end, source="dag-file")
        day += timedelta(days=1)


def collect_airflow_logs(record, url, dag_id):
    import httpx

    start = stamp(record["released_at"])
    end = stamp(record["finished_at"])
    left, right = start - LOG_CONTEXT_SECONDS, end + LOG_CONTEXT_SECONDS
    record["airflow_events"] = {}
    record["logs"] = logs = {service: [] for service in SERVICES}
    record["log_context_seconds"] = LOG_CONTEXT_SECONDS
    record["log_warnings"] = warnings = {}
    tasks = {task["ti_id"]: task for task in record["tasks"] if task.get("ti_id")}
    runs = {run["run_id"] for run in record["runs"]}
    ids = re.compile(r"(?<![\w-])(?:" + "|".join(map(re.escape, (*tasks, *sorted(runs)))) + r")(?![\w-])")
    limit = 5000
    with httpx.Client(base_url=url, timeout=10) as client:
        for service in SERVICES:
            try:
                response = client.get("/loki/api/v1/query_range", params={
                    "query": f'{{service="{service}"}}', "start": str(round(left * 1e9)),
                    "end": str(round(right * 1e9)), "direction": "forward", "limit": limit,
                })
                response.raise_for_status()
                for stream in response.json()["data"]["result"]:
                    for at, raw in stream["values"]:
                        line = re.sub(r"\x1b\[[0-9;]*m", "", raw)
                        matches = set(ids.findall(line))
                        matched_tasks = [tasks[key] for key in matches if key in tasks]
                        run_ids = sorted((matches & runs) | {task["run_id"] for task in matched_tasks})
                        task_names = {task["task_id"] for task in matched_tasks} or set(TASK_ID_IN_LOG.findall(line))
                        logs[service].append({"at": int(at) / 1e9, "line": line, "source": "container",
                                              "run_ids": run_ids,
                                              "task_id": next(iter(task_names)) if len(task_names) == 1 else None})
                if len(logs[service]) == limit:
                    warnings[service] = f"Loki returned the first {limit} lines."
            except (httpx.HTTPError, KeyError, ValueError) as error:
                warnings[service] = f"Loki: {error}"
            logs[service].sort(key=lambda entry: entry["at"])
            if service == "worker":
                link_celery_logs(logs[service])
    try:
        entries, missing = task_logs(record, dag_id, left, right)
        logs["worker"].extend(entries)
        logs["worker"].sort(key=lambda entry: entry["at"])
        if missing:
            warnings["worker-task"] = f"Task log files missing for {missing} task instances."
    except OSError as error:
        warnings["worker-task"] = f"Task logs: {error}"
    try:
        logs["dag-processor"].extend(dag_processor_logs(left, right))
        logs["dag-processor"].sort(key=lambda entry: entry["at"])
    except OSError as error:
        warnings["dag-processor-file"] = f"DAG processor logs: {error}"
    for service, entries in logs.items():
        record["airflow_events"][service] = [event for entry in entries if (event := airflow_event(service, entry))]


def link_celery_logs(entries):
    # Celery receive/result messages have only the workload UUID. The start line has both IDs.
    by_celery_id = {}
    for entry in entries:
        if entry["run_ids"] and "Executing workload in Celery" in entry["line"]:
            match = CELERY_ID_IN_LOG.search(entry["line"])
            if match:
                by_celery_id[match[1]] = entry
    for entry in entries:
        if "Task execute_workload[" not in entry["line"]:
            continue
        match = CELERY_ID_IN_LOG.search(entry["line"])
        if match and match[1] in by_celery_id:
            linked = by_celery_id[match[1]]
            entry.update(run_ids=linked["run_ids"], task_id=linked["task_id"])


def airflow_event(service, entry):
    line = entry["line"]
    label, duration = None, None
    path = re.search(r"\bpath=['\"]?(/execution/[^\s?'\"]+)", line) if service == "api" else None
    if path:
        label = re.sub(r"(/task-instances/)[^/]+", r"\1{task_instance_id}", path[1])
        if label.startswith("/execution/xcoms/"):
            label = "/execution/xcoms"
        duration = re.search(r"\bduration_us=(\d+)", line)
    else:
        patterns = {
            "scheduler": ("Setting the following tasks to queued state", "Trying to enqueue tasks", "Sending TaskInstance",
                          "Received executor event", "TaskInstance Finished", "DagRun Finished"),
            "worker": ("Executing workload in Celery", "Task finished", "received", "succeeded in", "failed", "exited with"),
            "dag-processor": ("DAG File Processing Stats", "Bulk-writing dags", "Processing file", "Filling up the DagBag"),
        }
        label = next((value for value in patterns.get(service, ()) if value in line), None)
    if label:
        return {"at": entry["at"], "label": label, "duration_s": int(duration[1]) / 1e6 if duration else None}


def clickhouse_profiles(records, url, experiment):
    import httpx

    # query_log is buffered. Read it after all measured windows.
    try:
        with httpx.Client(base_url=url, auth=("bench", "bench"), timeout=20) as client:
            client.post("/", content="SYSTEM FLUSH LOGS").raise_for_status()
            result = client.post("/", params={"param_prefix": experiment + "_"}, content="""
                SELECT query_id, query_duration_ms, read_rows, read_bytes, memory_usage,
                       (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'])
                           / 1000000 AS cpu_seconds
                FROM system.query_log
                WHERE type = 'QueryFinish' AND startsWith(query_id, {prefix:String})
                FORMAT JSONEachRow
            """)
            result.raise_for_status()
            rows = [json.loads(line) for line in result.text.splitlines()]
    except (httpx.HTTPError, ValueError) as error:
        for record in records:
            record["clickhouse_warning"] = str(error)
        return
    by_id = {row["query_id"]: row for row in rows}
    for record in records:
        for step in record.get("operations", []):
            query_id = step["details"].get("query_id")
            if query_id in by_id:
                step["server"] = by_id[query_id]


def cpu_intervals(record):
    """Derive cores between consecutive samples of the same counter."""
    rows = []
    limit = record.get("measurement_quality", {}).get("allowed_gap_seconds", 8)
    for series in record["timeline"].get("cpu", []):
        labels = series["labels"]
        values = series["values"]
        for (left, before), (right, after) in zip(values, values[1:]):
            if 0 < right - left <= limit and after >= before:
                rows.append({"service": labels["service"], "start": left, "end": right,
                             "cores": (after - before) / (right - left)})
    return rows


def timeline_gaps(record):
    """Report raw resource sampling gaps, including gauges held stale in Grafana."""
    start, end = stamp(record["released_at"]), stamp(record["finished_at"])
    gaps = {}
    limit = record.get("measurement_quality", {}).get("allowed_gap_seconds", 8)
    for metric in ("cpu", "memory"):
        for series in record.get("timeline", {}).get(metric, []):
            for left, right in zip(series["values"], series["values"][1:]):
                if right[0] - left[0] > limit and overlaps(left[0], right[0], start, end):
                    key = series["labels"]["service"], left[0], right[0]
                    row = gaps.setdefault(key, {"service": key[0], "start": key[1], "end": key[2],
                                                "gap_seconds": right[0] - left[0], "metrics": []})
                    row["metrics"].append(metric)
    return sorted(gaps.values(), key=lambda row: row["start"])


def correlate(record):
    start = stamp(record["released_at"])
    end = stamp(record["finished_at"])
    operations = record["operations"]
    peaks, changes = {}, []
    for service in SERVICES:
        candidates = [row for row in record["cpu_intervals"]
                      if row["service"] == service and overlaps(row["start"], row["end"], start, end)]
        inside = [row for row in candidates if start <= row["start"] < row["end"] <= end]
        intervals = inside or candidates
        if not intervals:
            continue
        peak = max(intervals, key=lambda row: row["cores"])
        peaks[service] = describe_interval(record, operations, peak, start, end)
        ordered = sorted(inside, key=lambda row: row["start"])
        adjacent = [(a, b) for a, b in zip(ordered, ordered[1:]) if a["end"] == b["start"]]
        if adjacent:
            delta = lambda pair: pair[1]["cores"] - pair[0]["cores"]
            for direction, pair in (("rise", max(adjacent, key=delta)), ("fall", min(adjacent, key=delta))):
                change = delta(pair)
                if (direction == "rise" and change > 0) or (direction == "fall" and change < 0):
                    before, after = pair
                    changes.append({
                        **describe_interval(record, operations, after, start, end),
                        "direction": direction, "previous_cores": before["cores"],
                        "previous_operations": describe_interval(record, operations, before, start, end)["operations"],
                    })
    return peaks, changes


def peak_running(tasks):
    changes = [(stamp(task["start_date"]), 1) for task in tasks if task["start_date"]]
    changes += [(stamp(task["end_date"]), -1) for task in tasks if task["end_date"]]
    running = peak = 0
    for _, delta in sorted(changes):
        running += delta
        peak = max(peak, running)
    return peak


def log_diagnostics(record):
    rules = (
        ("celery-send", "scheduler", "Error sending Celery workload", "Не удалось отправить задачу в Celery",
         "Scheduler сообщил об ошибке отправки. Проверьте исходную строку и состояние задачи; она могла не дойти до worker."),
        ("celery-operation", "scheduler", "Process timed out", "Таймаут операции Scheduler",
         "Сработал таймаут. Это событие само по себе не доказывает медленную сеть или нехватку CPU."),
        ("task-exception", "worker", '"event":"Task failed with exception"', "Исключение в процессе задачи",
         "Ошибка записана в файле конкретной задачи Airflow. Исходная строка содержит тип исключения и стек."),
        ("external-state", "scheduler", "task-state-changed-externally", "Расхождение состояния executor и TaskInstance",
         "Scheduler получил результат executor, когда состояние TaskInstance ещё отличалось. Ищите более раннюю ошибку отправки или исполнения."),
    )
    result = []
    for code, service, pattern, title, explanation in rules:
        entries = [entry for entry in record.get("logs", {}).get(service, []) if pattern in entry["line"]]
        if entries:
            first = min(entries, key=lambda entry: entry["at"])
            result.append({"code": code, "service": service, "at": first["at"], "count": len(entries),
                           "title": title, "explanation": explanation})
    return result


def describe_interval(record, operations, interval, start, end):
    left, right = max(start, interval["start"]), min(end, interval["end"])
    active = defaultdict(float)
    for step in operations:
        active[step["operation"]] += overlaps(step["start"], step["end"], left, right)
    transitions = {
        field: sum(left <= stamp(task[field]) < right for task in record["tasks"] if task[field])
        for field in ("scheduled_dttm", "queued_dttm", "start_date", "end_date")
    }
    return {**interval, "operations": sorted(
        [(name, seconds) for name, seconds in active.items() if seconds > 0],
        key=lambda item: item[1], reverse=True,
    )[:3], "transitions": transitions}


def write_report(output, config, records):
    measured = [row for row in records if not row["warmup"]]
    source_file = Path(__file__).with_name("dags") / "bench_tasks.py"
    if not source_file.is_file():
        source_file = Path("/opt/airflow/dags/bench_tasks.py")
    locations = {}
    if source_file.is_file():
        for number, line in enumerate(source_file.read_text(encoding="utf-8").splitlines(), 1):
            match = re.search(r'operation\(steps, "([^"]+)"', line)
            if match:
                locations.setdefault(match[1], []).append(f"dags/bench_tasks.py:{number}")
    for row in measured:
        not_started = {f"{task['run_id']}/{task['task_id']}" for task in row["tasks"] if not task["start_date"]}
        row["not_started_tasks"] = sorted(not_started)
        row["missing_traces"] = [key for key in row["missing_traces"] if key not in not_started]
        row["running_peak"] = peak_running(row["tasks"])
        row["diagnostics"] = log_diagnostics(row)
        for step in row["operations"]:
            matches = locations.get(step["operation"], [])
            if len(matches) == 1:
                step.setdefault("source", matches[0])
        row["cpu_intervals"] = cpu_intervals(row)
        row["timeline_gaps"] = timeline_gaps(row)
        row["peaks"], row["cpu_changes"] = correlate(row)
    error_file = output / "error.json"
    error = json.loads(error_file.read_text(encoding="utf-8")) if error_file.exists() else None
    document = {"config": config, "batches": measured, "error": error}
    (output / "trace.json").write_text(json.dumps(document), encoding="utf-8")
    render_html(output, document)
    from process_report import write_profile_index

    write_profile_index(output.parent)


def render_html(output, document):
    template = Path(__file__).with_name("trace_template.html").read_text(encoding="utf-8")
    data = json.dumps(document, ensure_ascii=False).replace("<", "\\u003c")
    (output / "trace.html").write_text(template.replace("__TRACE_DATA__", data), encoding="utf-8")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folders", nargs="+", type=Path)
    parser.add_argument("--baseline", type=Path, help="Compare with a saved noop trace.json of matching concurrency")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--refresh-events", action="store_true", help="Reload resource timeline and logs")
    mode.add_argument("--refresh-logs", action="store_true", help="Reload logs only")
    mode.add_argument("--refresh-processes", action="store_true", help="Join saved process samples to tasks")
    mode.add_argument("--html-only", action="store_true", help="Render HTML from saved trace.json")
    args = parser.parse_args()
    for output in args.folders:
        trace = output / "trace.json"
        if args.html_only:
            document = json.loads(trace.read_text(encoding="utf-8"))
            if args.baseline:
                from process_report import attach_baseline

                attach_baseline(document, args.baseline)
            render_html(output, document)
            continue
        if trace.exists():
            document = json.loads(trace.read_text(encoding="utf-8"))
        else:
            config = json.loads((output / "config.json").read_text(encoding="utf-8"))
            rows = (output / "series.jsonl").read_text(encoding="utf-8").splitlines()
            document = {"config": config, "batches": [row for line in rows if not (row := json.loads(line))["warmup"]]}
        for batch in document["batches"]:
            if "operations" not in batch:
                collect_traces(batch, f"bench_{document['config']['scenario']}", Path(os.environ["BENCH_DATA"]))
            if args.refresh_processes:
                from process_report import collect_process_profile

                collect_process_profile(batch, Path(os.getenv("BENCH_DATA", ".")), output)
            if args.refresh_events or "timeline" not in batch:
                collect_timeline(batch, os.environ["BENCH_PROMETHEUS"])
            if args.refresh_events or args.refresh_logs or "logs" not in batch:
                collect_airflow_logs(batch, os.environ["BENCH_LOKI"], f"bench_{document['config']['scenario']}")
        if args.baseline:
            from process_report import attach_baseline

            attach_baseline(document, args.baseline)
        write_report(output, document["config"], document["batches"])
