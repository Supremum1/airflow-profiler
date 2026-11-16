"""Small cgroup collector. Docker discovery and HTTP never block sampling."""

import argparse
import http.client
import json
import os
import re
import socket
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlencode

SERVICES = ("scheduler", "worker", "api", "dag-processor", "metadata", "redis")


class DockerConnection(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect("/var/run/docker.sock")


def discover(mount, project):
    relative = next(line[3:] for line in Path("/proc/self/cgroup").read_text().splitlines()
                    if line.startswith("0::"))
    if not re.search(r"[0-9a-f]{64}", relative):
        raise ValueError("Cannot identify Docker cgroup path")
    query = urlencode({"filters": json.dumps({"label": [f"com.docker.compose.project={project}"]})})
    connection = DockerConnection("localhost", timeout=2)
    try:
        connection.request("GET", "/containers/json?" + query)
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(f"Docker discovery HTTP {response.status}")
        containers = json.load(response)
    finally:
        connection.close()
    found = {}
    for container in containers:
        service = container["Labels"].get("com.docker.compose.service")
        if service in SERVICES:
            if service in found:
                raise ValueError(f"Multiple {service} containers: this collector requires one container per service")
            identity = container["Id"]
            path = mount / re.sub(r"[0-9a-f]{64}", identity, relative).lstrip("/")
            if not (path / "cpu.stat").is_file():
                raise ValueError(f"Cgroup not found for {service}: {path}")
            found[service] = identity, path
    return found


def read_cgroup(identity, path):
    cpu = dict(line.split() for line in (path / "cpu.stat").read_text().splitlines())
    memory = dict(line.split() for line in (path / "memory.stat").read_text().splitlines())
    current = int((path / "memory.current").read_text())
    return {"id": identity, "at": time.time(), "cpu_seconds": int(cpu["usage_usec"]) / 1e6,
            "memory_bytes": current, "working_set_bytes": max(0, current - int(memory.get("inactive_file", 0))),
            "anon_bytes": int(memory["anon"]), "file_bytes": int(memory["file"]),
            "kernel_bytes": int(memory.get("kernel", 0)),
            "throttled_seconds": int(cpu.get("throttled_usec", 0)) / 1e6}


def exposition(samples, max_age=3):
    names = {"cpu_seconds": "container_cpu_usage_seconds_total",
             "working_set_bytes": "container_memory_working_set_bytes",
             "memory_bytes": "container_memory_usage_bytes",
             "throttled_seconds": "container_cpu_cfs_throttled_seconds_total"}
    lines = []
    for service in SERVICES:
        row = samples.get(service)
        fresh = bool(row and time.time() - row["at"] <= max_age)
        lines.append(f'bench_resource_up{{service="{service}"}} {int(fresh)}')
        if not row:
            continue
        labels = f'service="{service}",id="{row["id"]}"'
        lines.append(f'bench_resource_last_sample_seconds{{{labels}}} {row["at"]}')
        if fresh:
            for field, name in names.items():
                lines.append(f'{name}{{{labels}}} {row[field]} {round(row["at"] * 1000)}')
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mount", type=Path, default=Path("/host-cgroup"))
    parser.add_argument("--project", default="airflow-profiler")
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--port", type=int, default=9199)
    parser.add_argument("--process-status", type=Path)
    args = parser.parse_args()
    os.umask(0o002)
    args.output.mkdir(parents=True, exist_ok=True)
    state = {"paths": {}, "samples": {}, "discovery_error": None, "mapping_invalid": False, "write_error": None}

    def mapping_loop():
        while True:
            try:
                state["paths"] = discover(args.mount, args.project)
                state["discovery_error"] = None
                state["mapping_invalid"] = False
            except ValueError as error:
                state["mapping_invalid"] = True
                state["discovery_error"] = str(error)
            except (OSError, RuntimeError, http.client.HTTPException) as error:
                state["discovery_error"] = str(error)
            time.sleep(5)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            samples = state["samples"]
            if self.path == "/metrics":
                body, code = exposition(samples), 200
            elif self.path == "/ready":
                ready = (not state["write_error"] and not state["mapping_invalid"] and all(service in samples and time.time() - samples[service]["at"] < 3
                                                         for service in SERVICES))
                if args.process_status:
                    try:
                        status=json.loads(args.process_status.read_text())
                        ready=ready and status["root_present"] and time.time()-status["at"]<3 and samples.get("worker",{}).get("id","missing") in status["worker_cgroup"]
                    except (OSError,ValueError,KeyError):
                        ready=False
                body, code = json.dumps({"ready": ready, "discovery_error": state["discovery_error"],
                                         "write_error": state["write_error"]}), 200 if ready else 503
            else:
                body, code = "Not found", 404
            self.send_response(code)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(body.encode())

        def log_message(self, *_):
            pass

    threading.Thread(target=mapping_loop, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"cgroup sampling every {args.interval}s; HTTP port {args.port}", flush=True)
    while True:
        tick = time.monotonic()
        samples, errors = {}, {}
        for service, (identity, path) in state["paths"].copy().items():
            try:
                samples[service] = read_cgroup(identity, path)
            except (OSError, ValueError, KeyError) as error:
                errors[service] = str(error)
        row = {"at": time.time(), "interval_seconds": args.interval, "services": samples, "errors": errors}
        if state["mapping_invalid"]:
            row["errors"]["mapping"] = state["discovery_error"]
        hour = datetime.fromtimestamp(row["at"], timezone.utc).strftime("%Y%m%d%H")
        try:
            with (args.output / f"{hour}.jsonl").open("a", encoding="utf-8") as file:
                file.write(json.dumps(row) + "\n")
            state["write_error"] = None
        except OSError as error:
            state["write_error"] = str(error)
        state["samples"] = samples
        time.sleep(max(0, args.interval - (time.monotonic() - tick)))


if __name__ == "__main__":
    main()
