"""Record wall time, process CPU and RSS at code boundaries."""

import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from airflow.sdk import get_current_context


@contextmanager
def trace_task():
    context = get_current_context()
    task = context["ti"]
    record = {
        "dag_id": task.dag_id, "run_id": context["run_id"], "task_id": task.task_id,
        "try_number": task.try_number, "pid": os.getpid(), "operations": [],
        "memory_before": memory_snapshot(),
    }
    try:
        yield record["operations"]
    finally:
        record["memory_after"] = memory_snapshot()
        folder = Path(os.environ["BENCH_DATA"]) / "traces" / task.dag_id / context["run_id"]
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{task.task_id}.{task.try_number}.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(record), encoding="utf-8")
        temporary.replace(path)


def rss_bytes():
    return int(Path("/proc/self/statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")


def memory_snapshot():
    """Two synchronous observations also cover tasks shorter than the poll interval."""
    start = time.time()
    memory = {}
    for line in Path("/proc/self/smaps_rollup").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in {"Pss", "Private_Clean", "Private_Dirty"}:
            memory[key] = int(value.split()[0]) * 1024
    return {"at": time.time(), "memory_start": start, "pss_bytes": memory.get("Pss"),
            "private_bytes": memory.get("Private_Clean", 0)+memory.get("Private_Dirty", 0)}


@contextmanager
def operation(operations, name, **details):
    frame = sys._getframe(2)
    source = f"dags/{Path(frame.f_code.co_filename).name}:{frame.f_lineno}"
    rss_before = rss_bytes()
    cpu_start = time.process_time()
    start = time.time()
    try:
        yield details
    finally:
        end = time.time()
        cpu_seconds = time.process_time() - cpu_start
        operations.append({
            "operation": name, "source": source, "start": start, "end": end, "cpu_seconds": cpu_seconds,
            "rss_before": rss_before, "rss_after": rss_bytes(), "details": details,
        })
