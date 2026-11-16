"""Read durable cgroup measurements and reject incomplete resource windows."""

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SERVICES = ("scheduler", "worker", "api", "dag-processor", "metadata", "redis")
MAX_GAP = 3.0


def stamp(value):
    return value.timestamp() if isinstance(value, datetime) else datetime.fromisoformat(value).timestamp()


def read_rows(folder, start, end):
    hour = datetime.fromtimestamp(start, timezone.utc).replace(minute=0, second=0, microsecond=0)
    rows = []
    while hour.timestamp() <= end:
        path = folder / (hour.strftime("%Y%m%d%H") + ".jsonl")
        if path.exists():
            with path.open(encoding="utf-8") as file:
                for line in file:
                    try:
                        row = json.loads(line)
                        if start <= row["at"] <= end:
                            rows.append(row)
                    except (ValueError, KeyError):
                        continue  # The current writer may not have finished its last line.
        hour += timedelta(hours=1)
    return sorted(rows, key=lambda row: row["at"])


def interval_values(points, start, end):
    before = [point for point in points if point["at"] <= start]
    after = [point for point in points if point["at"] >= end]
    if not before or not after:
        return [], "Нет отсчётов по обе стороны измерения"
    values = [point for point in points if before[-1]["at"] <= point["at"] <= after[0]["at"]]
    if len({point["id"] for point in values}) != 1:
        return values, "Компонент пересоздан во время измерения"
    if any(b["at"] - a["at"] > MAX_GAP for a, b in zip(values, values[1:])):
        return values, "Разрыв сбора больше 3 с"
    if any(b["cpu_seconds"] < a["cpu_seconds"] for a, b in zip(values, values[1:])):
        return values, "Сброс счётчика CPU"
    return values, None


def counter_at(points, at):
    for left, right in zip(points, points[1:]):
        if left["at"] <= at <= right["at"]:
            fraction = (at - left["at"]) / (right["at"] - left["at"])
            return left["cpu_seconds"] + fraction * (right["cpu_seconds"] - left["cpu_seconds"])
    raise ValueError("No counter bracket")


def apply_resources(record, rows):
    start, end = stamp(record["released_at"]), stamp(record["finished_at"])
    timeline, resources, issues, gaps = {"cpu": [], "memory": []}, {}, [], {}
    if any(row.get("errors",{}).get("mapping") for row in rows if start<=row["at"]<=end):
        issues.append("Недопустимая конфигурация сопоставления cgroup")
    for service in SERVICES:
        points = sorted((row["services"][service] for row in rows if service in row["services"]), key=lambda p: p["at"])
        values, error = interval_values(points, start, end)
        if any(service in row.get("errors", {}) for row in rows if start <= row["at"] <= end):
            error = error or "Ошибка чтения cgroup внутри измерения"
        if error:
            issues.append(f"{service}: {error}")
        # Keep partial data visible, but never turn it into a valid aggregate.
        for identity in {p["id"] for p in points}:
            own = [p for p in points if p["id"] == identity]
            labels = {"service": service, "id": identity, "job": "airflow-cgroups"}
            timeline["cpu"].append({"labels": labels, "values": [[p["at"], p["cpu_seconds"]] for p in own]})
            timeline["memory"].append({"labels": labels, "values": [[p["at"], p["working_set_bytes"]] for p in own]})
        gaps[service] = max((b["at"]-a["at"] for a,b in zip(values,values[1:])), default=None)
        inside = [p for p in points if start <= p["at"] <= end]
        resources[service] = {"source": "cgroup", "memory_samples": len(inside)}
        if inside:
            resources[service]["memory_peak_bytes"] = max(p["working_set_bytes"] for p in inside)
        elif not error:
            issues.append(f"{service}: Нет снимка памяти внутри измерения")
        if not error and len(values) >= 2:
            resources[service].update(cpu_seconds=counter_at(values,end)-counter_at(values,start),
                                      cpu_measurement="cgroup_interpolated", cpu_bracket_max_gap_seconds=gaps[service])
    record.setdefault("timeline", {}).update(timeline)
    record["resources"] = resources
    record["metrics_warning"] = "; ".join(issues) or None
    record["measurement_quality"] = {"valid": not issues, "source": "cgroup", "issues": issues,
                                      "max_gap_seconds": gaps, "allowed_gap_seconds": MAX_GAP}


def collect_resources(record, data_dir, output):
    saved = output / f"resources-r{record['repeat']}-n{record['size']}.jsonl"
    if saved.exists():
        rows = [json.loads(line) for line in saved.read_text().splitlines()]
    else:
        deadline = time.monotonic() + 4
        while True:
            rows = read_rows(data_dir / "component-metrics", stamp(record["released_at"])-4,
                             stamp(record["finished_at"])+4)
            if all(any(row["services"].get(service, {}).get("at", 0) >= stamp(record["finished_at"])
                       for row in rows) for service in SERVICES) or time.monotonic() >= deadline:
                break
            time.sleep(.25)
        saved.write_text("".join(json.dumps(row)+"\n" for row in rows), encoding="utf-8")
    apply_resources(record, rows)


def verify_prometheus(record, url):
    import httpx

    quality = record["measurement_quality"]
    start, end = stamp(record["released_at"]), stamp(record["finished_at"])
    try:
        response = httpx.get(url + "/api/v1/query", params={
            "query": f'up{{job="airflow-cgroups"}}[{round((end-start+4)*1000)}ms]', "time": end+2}, timeout=5)
        response.raise_for_status()
        values = [p for series in response.json()["data"]["result"] for p in series["values"]
                  if start <= p[0] <= end]
        record["prometheus_scrapes"] = {"job": "airflow-cgroups", "values": values}
        if not values or any(float(point[1]) != 1 for point in values):
            quality["issues"].append("Prometheus: пропущены опросы resource-metrics")
        times = [start, *sorted(p[0] for p in values), end]
        if any(b-a > MAX_GAP for a,b in zip(times,times[1:])):
            quality["issues"].append("Prometheus: разрыв опросов больше 3 с")
    except (httpx.HTTPError, KeyError, ValueError) as error:
        quality["issues"].append(f"Prometheus: {error}")
    quality["valid"] = not quality["issues"]
    record["metrics_warning"] = "; ".join(quality["issues"]) or None


def verify_process_profile(record):
    quality=record["measurement_quality"]
    profile=record.get("process_profile",{})
    times=[stamp(record["released_at"]),*(p["at"] for p in profile.get("samples",[])),stamp(record["finished_at"])]
    profile["max_window_gap_seconds"]=max((b-a for a,b in zip(times,times[1:])),default=0)
    if not profile.get("available") or profile["max_window_gap_seconds"]>MAX_GAP:
        quality["issues"].append("PSS: нет непрерывного наблюдения worker")
    if profile.get("matched_tasks",0)<profile.get("started_tasks",0):
        quality["issues"].append("PSS: у части задач нет ни периодических, ни граничных наблюдений")
    started={task["ti_id"] for task in record.get("tasks",[]) if task.get("start_date")}
    observed={task["ti_id"] for task in profile.get("tasks",[]) if task.get("pss_peak_bytes") is not None}
    if started-observed:
        quality["issues"].append("PSS: нет наблюдений памяти части запущенных задач")
    quality["valid"]=not quality["issues"]
    record["metrics_warning"]="; ".join(quality["issues"]) or None


def preflight(url, prometheus=None, timeout=30):
    import httpx

    deadline = time.monotonic() + timeout
    with httpx.Client(timeout=2) as client:
        while time.monotonic() < deadline:
            try:
                if client.get(url + "/ready").status_code == 200:
                    if prometheus:
                        result = client.get(prometheus + "/api/v1/query", params={"query": 'up{job="airflow-cgroups"}'}).json()
                        if not result.get("data", {}).get("result") or any(float(s["value"][1]) != 1 for s in result["data"]["result"]):
                            time.sleep(.5)
                            continue
                    return
            except httpx.HTTPError:
                pass
            time.sleep(.5)
    raise RuntimeError("Resource/process collectors or Prometheus are not ready")
