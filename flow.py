"""Arrival-rate experiments; the existing batch benchmark remains available."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import threading
import time
import uuid

import httpx
import psycopg
from psycopg.rows import dict_row

from benchmark import SCENARIOS, api_request, prepare, task_times, utc_now
from process_report import collect_process_profile
from resource_report import collect_resources, preflight, verify_prometheus, verify_process_profile
from trace_report import collect_traces, collect_timeline, collect_airflow_logs, clickhouse_profiles, write_report
from flow_report import render_flow


def make_plan(profile, rate, rates, seconds, burst):
    stages, arrivals, offset = [], [], 0.
    for level in rates if profile == "steps" else [rate]:
        index = len(stages)
        stages.append({"name": f"{level:g} DAG/мин", "rate": level, "start": offset, "end": offset+seconds})
        count = 0
        while count*60/level < seconds:
            arrivals.append((offset+count*60/level, index))
            count += 1
        offset += seconds
    if profile == "burst":
        stages.append({"name": f"{burst} DAG одновременно", "rate": None, "start": offset, "end": offset})
        arrivals.extend((offset, 1) for _ in range(burst))
        stages.append({"name": "Восстановление", "rate": rate, "start": offset, "end": offset+seconds})
        count = 0
        while count*60/rate < seconds:
            arrivals.append((offset+count*60/rate, 2))
            count += 1
        offset += seconds
    return stages, arrivals, offset


def poll(db, dag_id, ids):
    runs = db.execute("SELECT run_id,state,start_date,end_date FROM dag_run WHERE dag_id=%s AND run_id=ANY(%s)",
                      (dag_id, ids)).fetchall()
    tasks = db.execute("""SELECT id::text AS ti_id,run_id,task_id,state,scheduled_dttm,queued_dttm,start_date,end_date
                          FROM task_instance WHERE dag_id=%s AND run_id=ANY(%s) ORDER BY run_id,task_id""",
                       (dag_id, ids)).fetchall()
    now = utc_now()
    queued = [t for t in tasks if t["state"] == "queued"]
    scheduled = [t for t in tasks if t["state"] == "scheduled"]
    oldest = lambda items, field: max(((now-t[field]).total_seconds() for t in items if t[field]), default=0)
    completed = sum(r["state"] in {"success", "failed"} for r in runs)
    frame = {"at": now.timestamp(), "accepted": len(ids), "completed": completed,
             "successful": sum(r["state"] == "success" for r in runs), "pending": len(ids)-completed,
             "queued_tasks": len(queued), "scheduled_tasks": len(scheduled),
             "running_tasks": sum(t["state"] == "running" for t in tasks),
             "oldest_queued_seconds": oldest(queued, "queued_dttm"),
             "oldest_scheduled_seconds": oldest(scheduled, "scheduled_dttm")}
    return runs, tasks, frame


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=SCENARIOS)
    parser.add_argument("--profile", choices=("steady", "steps", "burst"), default="steady")
    parser.add_argument("--rate", type=float, default=12, help="DagRuns/minute")
    parser.add_argument("--rates", default="6,12,24")
    parser.add_argument("--stage-seconds", type=int, default=300)
    parser.add_argument("--burst-runs", type=int, default=32)
    parser.add_argument("--drain-timeout", type=int, default=600)
    parser.add_argument("--max-pending", type=int, default=128)
    parser.add_argument("--queue-limit", type=float, default=5)
    for flag, default in (("rows",100000),("cols",12),("pages",8),("page-rows",1000),
                          ("cpu-iterations",5000000),("io-bytes",1048576),("days",28)):
        parser.add_argument("--"+flag,type=int,default=default)
    parser.add_argument("--delay",type=float,default=.25)
    args = parser.parse_args()
    try:
        rates = [float(v) for v in args.rates.split(",")]
    except ValueError:
        parser.error("rates must be comma-separated numbers")
    if not all(math.isfinite(v) for v in [args.rate,*rates]) or min([args.rate,*rates]) <= 0 or max([args.rate,*rates]) > 600 or not 5<=args.stage_seconds<=3600:
        parser.error("rates: 0..600/min, stage-seconds >=5")
    if not 1<=args.burst_runs<=128 or not 1<=args.max_pending<=1024 or args.drain_timeout<1:
        parser.error("burst 1..128, max-pending 1..1024, positive drain-timeout")
    if not (args.rows>0 and 1<=args.cols<=200 and 1<=args.pages<=1000 and 1<=args.page_rows<=10000
            and args.cpu_iterations>0 and 1<=args.io_bytes<=64*1024**2 and 1<=args.days<=30 and 0<=args.delay<=30):
        parser.error("Invalid workload size")
    experiment = utc_now().strftime("flow-%Y%m%d-%H%M%S-")+uuid.uuid4().hex[:8]
    output = Path(os.getenv("BENCH_RESULTS", "/bench/results"))/experiment
    output.mkdir(parents=True)
    conf = vars(args).copy()
    config = {**conf, "experiment":experiment, "executor":os.environ["AIRFLOW__CORE__EXECUTOR"],
              "parallelism":int(os.environ["AIRFLOW__CORE__PARALLELISM"]),
              "worker_concurrency":int(os.environ["AIRFLOW__CELERY__WORKER_CONCURRENCY"]),
              "celery_operation_timeout":float(os.getenv("AIRFLOW__CELERY__OPERATION_TIMEOUT","10")),
              "resource_job":"airflow-cgroups"}
    config["run_prefix"] = experiment.replace('-','_')
    (output/"config.json").write_text(json.dumps(config,indent=2),encoding="utf-8")
    stages, arrivals, total = make_plan(args.profile,args.rate,rates,args.stage_seconds,args.burst_runs)
    dag_id = "bench_"+args.scenario
    db_url = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].replace("postgresql+psycopg2:","postgresql:",1)
    stop, generated = threading.Event(), threading.Event()
    futures, submitted, errors, frames = [], [], [], []
    runs, tasks = [], []
    generation_ended, stop_reason = [], None
    with httpx.Client(base_url=os.environ["BENCH_AIRFLOW_API"],timeout=15) as client, \
         psycopg.connect(db_url,row_factory=dict_row,autocommit=True) as db:
        if not db.execute("SELECT pg_try_advisory_lock(83520) AS locked").fetchone()["locked"]:
            raise SystemExit("Another experiment is running")
        active = db.execute("SELECT count(*) AS n FROM dag_run WHERE dag_id LIKE 'bench_%' AND state IN ('queued','running')").fetchone()["n"]
        if active:
            raise SystemExit("Previous benchmark DAGs must finish before a new experiment")
        preflight(os.environ["BENCH_RESOURCE_URL"],os.environ["BENCH_PROMETHEUS"])
        prepare(args.scenario,conf,experiment)
        client.headers["Authorization"] = "Bearer "+api_request(client,"GET","/auth/token")["access_token"]
        api_request(client,"PATCH",f"/api/v2/dags/{dag_id}",json={"is_paused":False})
        released = utc_now()
        clock = time.monotonic()
        auth_at = clock
        print(f"{args.profile}: {args.scenario}, parallelism={config['parallelism']}, worker={config['worker_concurrency']}",flush=True)

        def submit(index, offset, stage):
            row = {"run_id":f"{experiment.replace('-','_')}_{index}","stage":stage,
                   "planned_at":released.timestamp()+offset,"sent_at":time.time()}
            try:
                api_request(client,"POST",f"/api/v2/dags/{dag_id}/dagRuns",json={
                    "dag_run_id":row["run_id"],"logical_date":None,"conf":conf})
                row["accepted_at"] = time.time()
            except (httpx.HTTPError,RuntimeError) as error:
                row["error"] = str(error)
            return row

        with ThreadPoolExecutor(max_workers=8) as executor:
            def generate():
                try:
                    for index,(offset,stage) in enumerate(arrivals):
                        if stop.wait(max(0,clock+offset-time.monotonic())):
                            break
                        if sum(not f.done() for f in futures)>=max(32,args.burst_runs+8):
                            errors.append("Генератор: слишком много незавершённых API-запросов")
                            stop.set()
                            break
                        futures.append(executor.submit(submit,index,offset,stage))
                    stop.wait(max(0,clock+total-time.monotonic()))
                finally:
                    generation_ended.append(time.time())
                    generated.set()
            thread = threading.Thread(target=generate,daemon=True)
            thread.start()
            seen, drain_deadline, progress_at = set(), None, 0.
            try:
                while True:
                    for future in futures.copy():
                        if future.done() and future not in seen:
                            row = future.result()
                            seen.add(future)
                            submitted.append(row)
                            with (output/"arrivals.jsonl").open("a",encoding="utf-8") as file:
                                file.write(json.dumps(row,ensure_ascii=False)+"\n")
                            if row.get("error"):
                                errors.append(row["error"])
                                stop.set()
                    ids = [r["run_id"] for r in submitted if "accepted_at" in r]
                    runs,tasks,frame = poll(db,dag_id,ids)
                    if time.monotonic()-progress_at>=10:
                        print(f"accepted={len(ids)}, completed={frame['completed']}, pending={frame['pending']}, queued_tasks={frame['queued_tasks']}",flush=True)
                        progress_at=time.monotonic()
                    if time.monotonic()-auth_at>=600:
                        client.headers["Authorization"]="Bearer "+api_request(client,"GET","/auth/token")["access_token"]
                        auth_at=time.monotonic()
                    frames.append(frame)
                    with (output/"queue.jsonl").open("a",encoding="utf-8") as file:
                        file.write(json.dumps(frame)+"\n")
                    if len(frames)>1 and frames[-1]["at"]-frames[-2]["at"]>3:
                        errors.append("Разрыв наблюдения очереди больше 3 с")
                        stop.set()
                    if frame["pending"]>=args.max_pending:
                        stop_reason="Достигнут предел незавершённых DagRun"
                        stop.set()
                    try:
                        preflight(os.environ["BENCH_RESOURCE_URL"],os.environ["BENCH_PROMETHEUS"],timeout=1)
                    except RuntimeError as error:
                        if not stop.is_set(): errors.append(str(error))
                        stop.set()
                    if generated.is_set():
                        drain_deadline = drain_deadline or time.monotonic()+args.drain_timeout
                        if len(seen)==len(futures) and frame["completed"]==len(ids):
                            break
                        if time.monotonic()>drain_deadline:
                            errors.append("Очередь не завершилась за drain-timeout")
                            stop.set()
                            break
                    time.sleep(1)
            except (psycopg.Error,httpx.HTTPError,RuntimeError) as error:
                errors.append(str(error))
                stop.set()
            finally:
                stop.set()
                thread.join(timeout=2)
        finished = utc_now()
        successful = sum(r["state"]=="success" for r in runs)
        stages_summary = task_times(tasks)
        record = {"repeat":1,"warmup":False,"size":len(runs),"released_at":released.isoformat(),
                  "finished_at":finished.isoformat(),"duration_seconds":(finished-released).total_seconds(),
                  "successful":successful,"successful_dags_per_min":60*successful/(finished-released).total_seconds(),
                  "tasks":json.loads(json.dumps(tasks,default=str)),"runs":json.loads(json.dumps(runs,default=str)),
                  "stages":stages_summary,"resources":{},"resource_job":"airflow-cgroups"}
        time.sleep(3)
        collect_traces(record,dag_id,Path(os.environ["BENCH_DATA"]))
        collect_process_profile(record,Path(os.environ["BENCH_DATA"]),output)
        collect_timeline(record,os.environ["BENCH_PROMETHEUS"])
        collect_resources(record,Path(os.environ["BENCH_DATA"]),output)
        verify_prometheus(record,os.environ["BENCH_PROMETHEUS"])
        verify_process_profile(record)
        collect_airflow_logs(record,os.environ["BENCH_LOKI"],dag_id)
        if args.scenario=="clickhouse_mart": clickhouse_profiles([record],os.environ["BENCH_CLICKHOUSE"],config["run_prefix"])
        document = {"config":config,"stages":stages,"arrivals":submitted,"queue":frames,"errors":errors,
                    "stop_reason":stop_reason,
                    "generated_seconds":generation_ended[0]-released.timestamp() if generation_ended else 0}
        (output/"series.jsonl").write_text(json.dumps(record)+"\n",encoding="utf-8")
        write_report(output,config,[record])
        document["measurement_quality"] = record["measurement_quality"]
        (output/"flow.json").write_text(json.dumps(document,ensure_ascii=False),encoding="utf-8")
        render_flow(output,document,record)
        print(f"Flow report: results/{experiment}/flow.html",flush=True)
        if errors or not record["measurement_quality"]["valid"] or successful!=len(runs):
            raise SystemExit("Experiment failed execution/measurement requirements; see report")


if __name__=="__main__": main()
