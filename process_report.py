"""Join external /proc samples to TaskInstance UUIDs and operation intervals."""

import json
from html import escape
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median


GROUPS = ("service", "tasks", "other_tasks")
SCENARIOS = ("noop", "cpu", "io_http", "etl_csv", "api_ingest", "clickhouse_mart")


def stamp(value):
    return datetime.fromisoformat(value).timestamp()


def read_samples(folder, start, end):
    hour = datetime.fromtimestamp(start, timezone.utc).replace(minute=0, second=0, microsecond=0)
    last = datetime.fromtimestamp(end, timezone.utc)
    rows, damaged = [], 0
    while hour <= last:
        for path in folder.glob(hour.strftime("%Y%m%d%H") + "-*.jsonl"):
            with path.open(encoding="utf-8") as file:
                for line in file:
                    try:
                        row = json.loads(line)
                        if start <= row["at"] <= end:
                            rows.append(row)
                    except (ValueError, KeyError):
                        damaged += 1  # The sampler may still be appending the last line.
        hour += timedelta(hours=1)
    return sorted(rows, key=lambda row: row["at"]), damaged


def task_owners(rows, record):
    owners, parents = {}, {}
    for sample in rows:
        for process in sample["processes"]:
            parents[process["key"]] = process.get("parent_key")
            if process.get("task_instance_id"):
                owners[process["key"]] = process["task_instance_id"]
    bound_by_pid = set()
    # The task trace also knows its PID. This can identify bootstrap samples
    # taken before Airflow changes the process title to its TaskInstance UUID.
    for task in record["tasks"]:
        ti_id = task.get("ti_id")
        if not ti_id or ti_id in owners.values() or not task.get("start_date") or not task.get("end_date"):
            continue
        pids = {op["pid"] for op in record.get("operations", [])
                if op["run_id"] == task["run_id"] and op["task_id"] == task["task_id"]}
        if len(pids) != 1:
            continue
        left, right = stamp(task["start_date"]), stamp(task["end_date"])
        candidates = {process["key"] for sample in rows for process in sample["processes"]
                      if process["pid"] in pids
                      and (left <= sample["at"] <= right
                           or left <= process.get("memory_end", sample["at"]) <= right)}
        # Multiple process lifetimes with the same PID are ambiguous: leave them unbound.
        if len(candidates) == 1:
            key = next(iter(candidates))
            if key not in owners:
                owners[key] = ti_id
                bound_by_pid.add(ti_id)
    # A child may be observed before the parent gets its Airflow process title.
    while True:
        inherited = {key: owners[parent] for key, parent in parents.items()
                     if key not in owners and parent in owners}
        if not inherited:
            return owners, bound_by_pid
        owners.update(inherited)


def total(processes, field):
    values = [process.get(field) for process in processes]
    return sum(values) if all(value is not None for value in values) else None


def peak(points, field):
    values = [row[field] for row in points if row.get(field) is not None]
    return max(values) if values else None


def build_profile(record, rows):
    start, end = stamp(record["released_at"]), stamp(record["finished_at"])
    owners, bound_by_pid = task_owners(rows, record)
    tasks = {task["ti_id"]: task for task in record["tasks"] if task.get("ti_id")}
    series, previous, task_points = [], {}, {}
    task_cpu = dict.fromkeys(tasks, 0.)
    task_cpu_intervals = dict.fromkeys(tasks, 0)
    seen_tasks, process_counts = set(), {}
    max_gap = 0
    previous_at = None
    inside = [row for row in rows if start <= row["at"] <= end]
    for sample in rows:
        at = sample["at"]
        interval = sample.get("interval_seconds", .5)
        buckets = {key: [] for key in GROUPS}
        cpu = dict.fromkeys(GROUPS, 0.)
        cpu_count = dict.fromkeys(GROUPS, 0)
        per_task = {}
        per_task_cores = {}
        for process in sample["processes"]:
            owner = owners.get(process["key"])
            group = "tasks" if owner in tasks else "other_tasks" if owner else "service"
            buckets[group].append(process)
            if owner in tasks:
                per_task.setdefault(owner, []).append(process)
            old = previous.get(process["key"])
            if old:
                before, old_cpu = old
                elapsed = at - before
                delta = process["cpu_seconds"] - old_cpu
                if start <= before < at <= end and elapsed <= max(2, interval * 3) and delta >= 0:
                    cpu[group] += delta / elapsed
                    cpu_count[group] += 1
                    if owner in tasks:
                        task_cpu[owner] += delta
                        task_cpu_intervals[owner] += 1
                        per_task_cores[owner] = per_task_cores.get(owner, 0) + delta / elapsed
            previous[process["key"]] = at, process["cpu_seconds"]
        for ti_id, processes in per_task.items():
            # Root-process PSS is kept separately for code-operation comparisons.
            roots = [p for p in processes if owners.get(p.get("parent_key")) != ti_id]
            root = min(roots, key=lambda p: p["start_ticks"]) if roots else None
            point = {"at": at, "pss_bytes": total(processes, "pss_bytes"),
                     "count": len(processes),
                     "private_bytes": total(processes, "private_bytes"),
                     "root_pss_bytes": root["pss_bytes"] if root else None,
                     "root_pid": root["pid"] if root else None,
                     "root_sample_start": root.get("memory_start", at) if root else None,
                     "root_sample_end": root.get("memory_end", at + sample.get("collection_seconds", 0)) if root else None,
                     "cores": per_task_cores.get(ti_id)}
            task_points.setdefault(ti_id, []).append(point)
            if start <= at <= end:
                seen_tasks.add(ti_id)
                process_counts.setdefault(ti_id, set()).update(p["key"] for p in processes)
        if not start <= at <= end:
            continue
        if previous_at is not None:
            max_gap = max(max_gap, at - previous_at)
        previous_at = at
        series.append({"at": at, "groups": {
            group: {"pss_bytes": total(processes, "pss_bytes"), "count": len(processes),
                    "cores": cpu[group] if cpu_count[group] else 0 if not processes else None}
            for group, processes in buckets.items()}, "worker_memory": sample.get("worker_memory", {})})
    periodic_matched = len(seen_tasks)
    for point in record.get("task_boundaries", []):
        ti_id = next((key for key, task in tasks.items()
                      if task["run_id"] == point["run_id"] and task["task_id"] == point["task_id"]), None)
        if ti_id and point.get("pss_bytes") is not None:
            task_points.setdefault(ti_id, []).append({**point, "root_pid": point["pid"], "count": 1,
                "root_pss_bytes": point["pss_bytes"], "root_sample_start": point["memory_start"],
                "root_sample_end": point["at"], "cores": None, "source": "code_boundary"})
            seen_tasks.add(ti_id)
    for points in task_points.values():
        points.sort(key=lambda point: point["at"])
    summaries = []
    for ti_id, task in tasks.items():
        points = [p for p in task_points.get(ti_id, []) if start <= p["at"] <= end]
        summaries.append({"ti_id": ti_id, "run_id": task["run_id"], "task_id": task["task_id"],
                          "samples": len(points), "process_count": max(len(process_counts.get(ti_id, [])),1 if points else 0),
                          "pss_peak_bytes": peak(points, "pss_bytes"),
                          "private_peak_bytes": peak(points, "private_bytes"),
                          "cpu_observed_seconds": task_cpu[ti_id] if task_cpu_intervals[ti_id] else None,
                          "binding": "trace_pid" if ti_id in bound_by_pid else "process_title" if ti_id in owners.values() else "code_boundary",
                          "points": points})
    by_task = {(task["run_id"], task["task_id"]): task["ti_id"] for task in summaries}
    for operation in record.get("operations", []):
        ti_id = by_task.get((operation["run_id"], operation["task_id"]))
        points = [p for p in task_points.get(ti_id, [])
                  if p["root_pid"] == operation["pid"] and p["root_pss_bytes"] is not None]
        left, right = operation["start"], operation["end"]
        during = [p for p in points if left <= p["root_sample_start"] <= p["root_sample_end"] <= right]
        # Boundary samples are nearby observations, not exact before/after allocations.
        before = next((p for p in reversed(points) if left - 1 <= p["root_sample_end"] <= left), None)
        after = next((p for p in points if right <= p["root_sample_start"] <= right + 1), None)
        operation["process_memory"] = {
            "samples": len(during), "pss_peak_bytes": peak(during, "root_pss_bytes"),
            "before": before, "after": after,
        }
    warnings = []
    started = [task for task in tasks.values() if task.get("start_date")]
    missing = len([task for task in started if task["ti_id"] not in seen_tasks])
    if missing:
        warnings.append(f"Нет отсчётов процессов для {missing} запущенных задач; короткие задачи могли пройти между замерами.")
    if inside:
        expected = max(row.get("interval_seconds", .5) for row in inside)
        if max_gap > max(2, expected * 3):
            warnings.append(f"Разрыв сбора процессов до {max_gap:.1f} с.")
        if inside[0]["at"] - start > max(2, expected * 3) or end - inside[-1]["at"] > max(2, expected * 3):
            warnings.append("Сбор процессов покрывает не всё окно пакета.")
        if any(p.get("pss_bytes") is None for row in inside for p in row["processes"]):
            warnings.append("PSS отдельных отсчётов недоступен: возможны выход процесса или ограничения доступа.")
        if any("error" in row.get("worker_memory", {}) for row in inside):
            warnings.append("Состав памяти cgroup недоступен; PSS процессов собирается независимо.")
    observer_cpu, observer_wall = 0., 0.
    for left, right in zip(inside, inside[1:]):
        if left.get("session") == right.get("session") and "observer_cpu_seconds" in left and "observer_cpu_seconds" in right:
            elapsed = right["at"] - left["at"]
            delta = right["observer_cpu_seconds"] - left["observer_cpu_seconds"]
            if elapsed > 0 and delta >= 0:
                observer_cpu += delta
                observer_wall += elapsed
    return {"available": bool(inside), "interval_seconds": rows[0].get("interval_seconds") if rows else None,
            "samples": series, "tasks": summaries, "warnings": warnings,
            "matched_tasks": len(seen_tasks), "started_tasks": len(started),
            "periodic_matched_tasks": periodic_matched,
            "trace_pid_bindings": len(bound_by_pid),
            "max_gap_seconds": max_gap,
            "observer_avg_cores": observer_cpu / observer_wall if observer_wall else None,
            "max_collection_seconds": max((row.get("collection_seconds", 0) for row in inside), default=0)}


def attach_baseline(document, path):
    baseline = json.loads(Path(path).read_text(encoding="utf-8"))
    config, reference = document["config"], baseline["config"]
    if reference["scenario"] != "noop":
        raise ValueError("Baseline must be a noop experiment")
    for key in ("parallelism", "worker_concurrency", "executor", "celery_operation_timeout"):
        if config.get(key) != reference.get(key):
            raise ValueError(f"Baseline has a different {key}")
    for batch in document["batches"]:
        candidates = [row for row in baseline["batches"] if row["size"] == batch["size"]
                      and row["successful"] == row["size"] and row.get("process_profile", {}).get("available")
                      and row.get("measurement_quality", {}).get("valid") is not False]
        current = batch.get("process_profile", {})
        if not candidates or not current.get("available"):
            batch["noop_reference"] = {"warning": "Нет успешного noop с отсчётами процессов и таким размером пакета."}
            continue
        if any(row["process_profile"]["interval_seconds"] != current["interval_seconds"] for row in candidates):
            raise ValueError("Baseline uses a different process sampling interval")
        running = [task["running_s"] for row in candidates for task in row["tasks"] if task.get("running_s") is not None]
        memory = [task["pss_peak_bytes"] for row in candidates for task in row["process_profile"]["tasks"]
                  if task["pss_peak_bytes"] is not None]
        batch["noop_reference"] = {"experiment": reference["experiment"], "packages": len(candidates),
                                   "matched_tasks": sum(row["process_profile"]["matched_tasks"] for row in candidates),
                                   "started_tasks": sum(row["process_profile"]["started_tasks"] for row in candidates),
                                   "running_p50_seconds": median(running) if running else None,
                                   "task_pss_peak_p50_bytes": median(memory) if memory else None}


def collect_process_profile(record, data_dir, output):
    saved = output / f"processes-r{record['repeat']}-n{record['size']}.jsonl"
    if saved.exists():
        rows = [json.loads(line) for line in saved.read_text(encoding="utf-8").splitlines()]
        damaged = 0
    else:
        rows, damaged = read_samples(data_dir / "process-profiles", stamp(record["released_at"]) - 2,
                                     stamp(record["finished_at"]) + 2)
        if rows:
            saved.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    record["process_profile"] = build_profile(record, rows)
    if damaged:
        record["process_profile"]["warnings"].append(f"Пропущено неполных строк сборщика: {damaged}.")


def write_profile_index(results):
    """Keep one entry per mode so reports can be found without knowing run IDs."""
    latest = {}
    for folder in sorted(results.glob("b*"), reverse=True):
        path = folder / "trace.json"
        if not path.is_file():
            continue
        document = json.loads(path.read_text(encoding="utf-8"))
        mode = document["config"]["scenario"]
        if mode in latest:
            continue
        measured = [batch for batch in document["batches"] if batch.get("process_profile", {}).get("available")]
        if measured:
            latest[mode] = folder.name, max(measured, key=lambda batch: batch["size"])
        if len(latest) == len(SCENARIOS):
            break
    rows = []
    for mode in SCENARIOS:
        if mode not in latest:
            rows.append(f'<tr><td>{mode}</td><td colspan="6">Нет новых замеров</td></tr>')
            continue
        folder, batch = latest[mode]
        profile = batch["process_profile"]
        memory = []
        for group in ("service", "tasks"):
            values = [point["groups"][group]["pss_bytes"] for point in profile["samples"]
                      if point["groups"][group]["pss_bytes"] is not None]
            memory.append(f"{max(values) / 2**20:.1f}" if values else "—")
        link = f'../{escape(folder)}/trace.html?batch={batch["size"]}'
        status = f'{batch["successful"]}/{batch["size"]}'
        rows.append('<tr>' + ''.join(f'<td>{escape(str(value))}</td>' for value in
                    (mode, batch["size"], status, f'{batch["duration_seconds"]:.2f}', *memory)) +
                    f'<td><a href="{link}">Открыть →</a></td></tr>')
    output = results / "process-profiles"
    output.mkdir(exist_ok=True)
    campaigns = sorted((path for path in results.glob("celery-*") if (path / "index.html").exists()),
                       key=lambda path: (path / "index.html").stat().st_mtime, reverse=True)
    campaign_link = (f'<p><a href="../{escape(campaigns[0].name)}/index.html">Матрица прогонов →</a></p>'
                     if campaigns else '')
    flow_link = ('<p><a href="../flow-profiles/index.html">Нагрузочные тесты</a></p>'
                 if (results / "flow-profiles/index.html").is_file() else '')
    (output / "index.html").write_text('''<!doctype html><html lang="ru"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Airflow · Режимы профилирования</title>
<style>:root{color-scheme:dark;font:14px/1.5 system-ui;background:#0d131b;color:#e6edf6}
body{max-width:1100px;margin:32px auto;padding:0 16px}h1{font-size:25px;margin:0}p{color:#a3b4c9}
.scroll{overflow:auto;background:#151e2a;border:1px solid #2c3a4a;border-radius:8px}
table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}th,td{text-align:right;padding:14px 12px;white-space:nowrap;border-bottom:1px solid #2c3a4a}
th{font-size:12px;color:#a3b4c9}td:first-child,th:first-child{text-align:left}a{color:#8bd7f7;text-decoration:none}
</style><h1>Airflow · Нагрузки</h1>
''' + campaign_link + flow_link + '''<div class="scroll"><table><thead><tr><th>Режим</th><th>DAG</th><th>Успех</th><th>Время, с</th>
<th>Celery и непривязанные<br>пик PSS, МиБ</th><th>Задачи вместе<br>пик PSS, МиБ</th><th></th></tr></thead><tbody>''' +
                    ''.join(rows) + '</tbody></table></div>', encoding="utf-8")
