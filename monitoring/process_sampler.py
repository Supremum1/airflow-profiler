"""Observe a worker PID namespace. No imports or threads in task processes."""

import argparse
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

TASK_TITLE = re.compile(r"airflow worker -- ([0-9a-f-]{36})")


def read_process(pid, proc=Path("/proc")):
    ticks = os.sysconf("SC_CLK_TCK")
    folder = proc / str(pid)
    raw = (folder / "stat").read_text()
    # The command in parentheses can itself contain spaces or parentheses.
    fields = raw[raw.rindex(")") + 2:].split()
    if fields[0] in {"Z", "X"}:
        raise ProcessLookupError("Process has exited")
    command = (folder / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
    memory = {}
    memory_start = time.time()
    try:
        for line in (folder / "smaps_rollup").read_text().splitlines():
            key, _, value = line.partition(":")
            if key in {"Pss", "Rss", "Private_Clean", "Private_Dirty"}:
                memory[key] = int(value.split()[0]) * 1024
    except (PermissionError, FileNotFoundError, ProcessLookupError):
        pass
    if not memory:
        current = (folder / "stat").read_text()
        if current[current.rindex(")") + 2:].split()[0] in {"Z", "X"}:
            raise ProcessLookupError("Process exited while sampling memory")
    match = TASK_TITLE.search(command)
    return {
        "pid": pid, "ppid": int(fields[1]), "start_ticks": int(fields[19]),
        "cpu_seconds": (int(fields[11]) + int(fields[12])) / ticks,
        "pss_bytes": memory.get("Pss"), "rss_bytes": memory.get("Rss"),
        "private_bytes": (memory["Private_Clean"] + memory["Private_Dirty"]
                          if "Private_Clean" in memory and "Private_Dirty" in memory else None),
        "task_instance_id": match[1] if match else None,
        "memory_start": memory_start, "memory_end": time.time(),
    }


def descendants(processes, root):
    selected = {root} if root in processes else set()
    while True:
        children = {pid for pid, row in processes.items() if row["ppid"] in selected}
        added = children - selected
        if not added:
            return selected
        selected.update(added)


def cgroup_memory(root, mount):
    try:
        relative = next(line[3:] for line in Path(f"/proc/{root}/cgroup").read_text().splitlines()
                        if line.startswith("0::"))
        # In Compose this collector uses the host cgroup namespace and a read-only mount.
        path = mount / relative.lstrip("/")
        stats = dict(line.split() for line in (path / "memory.stat").read_text().splitlines())
        return {"current": int((path / "memory.current").read_text()),
                "anon": int(stats["anon"]), "file": int(stats["file"]),
                "kernel": int(stats["kernel"]) if "kernel" in stats else None,
                "swap": int((path / "memory.swap.current").read_text())}
    except (OSError, ValueError, KeyError, StopIteration) as error:
        return {"error": str(error)}


def snapshot(root, session, known, mount):
    started = time.time()
    rows = {}
    for folder in Path("/proc").iterdir():
        if folder.name.isdigit() and int(folder.name) != os.getpid():
            try:
                row = read_process(int(folder.name))
                rows[row["pid"]] = row
            except (OSError, ValueError, ProcessLookupError):
                continue  # A process may exit between directory enumeration and reading stat.
    selected = descendants(rows, root)
    for pid in selected:
        row = rows[pid]
        key = f"{session}:{pid}:{row['start_ticks']}"
        row["key"] = key
        row["task_instance_id"] = row["task_instance_id"] or known.get(key)
        if row["task_instance_id"]:
            known[key] = row["task_instance_id"]
        parent = rows.get(row["ppid"])
        row["parent_key"] = (f"{session}:{parent['pid']}:{parent['start_ticks']}" if parent else None)
    return {"at": started, "session": session, "interval_seconds": None,
            "processes": [rows[pid] for pid in sorted(selected)],
            "worker_memory": cgroup_memory(root, mount),
            "observer_cpu_seconds": time.process_time(),
            "collection_seconds": time.time() - started}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root-pid", type=int, default=1)
    parser.add_argument("--interval", type=float, default=.5)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cgroup-mount", type=Path, default=Path("/host-cgroup"))
    args = parser.parse_args()
    if args.interval < .1:
        parser.error("interval must be at least 0.1 seconds")
    os.umask(0o002)
    args.output.mkdir(parents=True, exist_ok=True)
    session, known = uuid.uuid4().hex, {}
    print(f"Profiling worker PID {args.root_pid} every {args.interval}s, session={session}", flush=True)
    while True:
        tick = time.monotonic()
        row = snapshot(args.root_pid, session, known, args.cgroup_mount)
        row["interval_seconds"] = args.interval
        hour = datetime.fromtimestamp(row["at"], timezone.utc).strftime("%Y%m%d%H")
        with (args.output / f"{hour}-{session}.jsonl").open("a", encoding="utf-8") as file:
            file.write(json.dumps(row) + "\n")
        health = {"at": time.time(), "root_present": any(p["pid"]==args.root_pid for p in row["processes"]),
                  "worker_cgroup": Path(f"/proc/{args.root_pid}/cgroup").read_text()}
        temporary=args.output/"health.tmp"
        temporary.write_text(json.dumps(health),encoding="utf-8")
        temporary.replace(args.output/"health.json")
        time.sleep(max(0, args.interval - (time.monotonic() - tick)))


if __name__ == "__main__":
    main()
