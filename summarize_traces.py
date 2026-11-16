"""Make a comparison page for completed benchmark experiments."""

import csv
import json
from collections import defaultdict
from html import escape
from pathlib import Path
from statistics import median
import sys


def resource_cells(batch, service):
    resources = batch["resources"].get(service, {})
    cpu = resources.get("cpu_seconds")
    average = f"{cpu / batch['duration_seconds']:.2f}" if cpu is not None else "—"
    if resources.get("cpu_measurement") in {"interpolated", "cgroup_interpolated"}:
        average += " ≈"
    memory = resources.get("memory_peak_bytes")
    return [average, f"{memory / 2**20:.0f}" if memory is not None else "—"]


def summarize(document):
    config = document["config"]
    for batch in document["batches"]:
        calls = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0, 0.0]))
        for step in batch["operations"]:
            values = calls[step["operation"]][step["run_id"]]
            values[0] += step["end"] - step["start"]
            values[1] += step["cpu_seconds"]
            values[2] += step.get("server", {}).get("query_duration_ms", 0) / 1000
        operations = sorted(
            [(name, *map(median, zip(*runs.values()))) for name, runs in calls.items()],
            key=lambda row: row[1], reverse=True,
        )
        yield config, batch, operations


def main():
    output = Path(sys.argv[1])
    experiments = [Path(path) for path in sys.argv[2:]]
    output.mkdir(parents=True, exist_ok=True)
    table = []
    resources = []
    comparisons = defaultdict(lambda: defaultdict(list))
    outcomes = defaultdict(dict)
    interruptions = []
    process_rows = []
    process_csv = []
    with (output / "operations.csv").open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["experiment", "scenario", "parallelism", "celery_operation_timeout", "size", "repeat", "operation",
                         "median_wall_s_per_dag", "median_process_cpu_s_per_dag", "median_clickhouse_s_per_dag"])
        for folder in experiments:
            document = json.loads((folder / "trace.json").read_text(encoding="utf-8"))
            if document.get("error"):
                error = document["error"]
                config = document["config"]
                interruptions.append(f'<p>{escape(config["scenario"])} · parallelism={config["parallelism"]} · '
                    f'{error["size"]} DAG: {escape(error["message"])} '
                    f'<a href="../{escape(folder.name)}/trace.html">Диагностика</a></p>')
            for config, batch, operations in summarize(document):
                quality = batch.get("measurement_quality", {})
                measured_ok = quality.get("valid") is not False
                complete = batch["successful"] == batch["size"] and measured_ok
                measurement = "Полное" if quality.get("valid") is True else "Недостоверно" if not measured_ok else "Без проверки"
                timeout = config.get("celery_operation_timeout", 1)
                profile = batch.get("process_profile", {})
                peaks = []
                for group in ("service", "tasks"):
                    values = [point["groups"][group]["pss_bytes"] for point in profile.get("samples", [])
                              if point["groups"][group]["pss_bytes"] is not None]
                    peaks.append(max(values) if values else None)
                process_csv.append([folder.name, config["scenario"], config["parallelism"], batch["size"],
                                    *peaks, profile.get("matched_tasks"), profile.get("started_tasks"),
                                    profile.get("interval_seconds"), profile.get("max_gap_seconds"),
                                    profile.get("observer_avg_cores")])
                process_rows.append(
                    f'<tr data-scenario="{escape(config["scenario"])}" data-parallelism="{config["parallelism"]}" '
                    f'data-size="{batch["size"]}" data-ok="{complete}">'
                    + ''.join(f'<td>{escape(str(value))}</td>' for value in
                              [config["scenario"], config["parallelism"], batch["size"],
                               *(f'{value / 2**20:.0f}' if value is not None else '—' for value in peaks)])
                    + f'<td><a href="../{escape(folder.name)}/trace.html?batch={batch["size"]}">Открыть ↗</a></td></tr>')
                key = (config["scenario"], batch["size"], timeout)
                outcomes[key][config["parallelism"]] = batch["successful"]
                if complete:
                    comparisons[key][config["parallelism"]].append(batch["duration_seconds"])
                for service, values in batch["resources"].items():
                    resources.append([folder.name, config["scenario"], config["parallelism"], timeout, batch["size"], batch["repeat"],
                                      batch["duration_seconds"], batch["successful"], batch.get("running_peak"), service,
                                      values.get("cpu_seconds"), values.get("cpu_measurement"), values.get("memory_peak_bytes"),
                                      values.get("memory_samples"), len(batch.get("logs", {}).get(service, [])),
                                      quality.get("valid"), "; ".join(quality.get("issues", []))])
                for name, wall, cpu, server in operations:
                    writer.writerow([folder.name, config["scenario"], config["parallelism"], timeout, batch["size"], batch["repeat"],
                                     name, wall, cpu, server if server else ""])
                leaders = "<br>".join(f"{escape(name)} · {wall:.3f} с / {cpu:.3f} CPU·с"
                                      for name, wall, cpu, _ in operations[:3])
                waits = [s["queued_to_running_s"]["p50"] for s in batch["stages"].values()
                         if s["queued_to_running_s"]["p50"] is not None]
                metrics = "".join(f"<td>{value}</td>" for service in ("scheduler", "worker")
                                  for value in resource_cells(batch, service))
                wait = f"{max(waits):.3f}" if waits else "—"
                table.append(
                    f'<tr data-scenario="{escape(config["scenario"])}" data-parallelism="{config["parallelism"]}" data-size="{batch["size"]}" data-ok="{complete}">'
                    f"<td>{escape(config['scenario'])}</td>"
                    f"<td>{config['parallelism']}</td><td>{timeout:g}</td><td>{batch['size']}</td>"
                    f'<td class="{"ok" if batch["successful"] == batch["size"] else "bad"}">{batch["successful"]}/{batch["size"]}</td>'
                    f'<td class="{"bad" if not measured_ok else ""}" title="{escape("; ".join(quality.get("issues", [])))}">{measurement}</td>'
                    f"<td>{batch['duration_seconds']:.2f}</td><td>{batch.get('running_peak', '—')}</td>"
                    f"<td>{batch['successful_dags_per_min']:.1f}</td>"
                    f"<td>{wait}</td>{metrics}<td>{leaders}</td>"
                    f'<td><a title="{escape(folder.name)}" href="../{escape(folder.name)}/trace.html?batch={batch["size"]}">Открыть ↗</a></td></tr>'
                )
    with (output / "resources.csv").open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["experiment", "scenario", "parallelism", "celery_operation_timeout", "size", "repeat", "duration_s", "successful",
                         "running_peak", "service", "cpu_seconds", "cpu_measurement", "memory_peak_bytes", "memory_samples", "log_lines",
                         "measurement_valid", "measurement_issues"])
        writer.writerows(resources)
    with (output / "processes.csv").open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["experiment", "scenario", "parallelism", "size", "service_pss_peak_bytes", "tasks_pss_peak_bytes",
                         "matched_tasks", "started_tasks", "interval_seconds", "max_gap_seconds", "observer_avg_cores"])
        writer.writerows(process_csv)
    comparison = []
    for (scenario, size, timeout), counts in outcomes.items():
        if not {16, 32} <= counts.keys():
            continue
        series = comparisons[(scenario, size, timeout)]
        cells = [f"{median(series[p]):.2f}" if series[p] else f"нет полного замера ({counts[p]}/{size} DAG)" for p in (16, 32)]
        change = f"{(median(series[32]) / median(series[16]) - 1) * 100:+.1f}%" if all(series[p] for p in (16, 32)) else "—"
        color = "ok" if change.startswith("-") else "warn" if change.startswith("+") else ""
        comparison.append(f'<tr data-scenario="{escape(scenario)}" data-size="{size}"><td>{escape(scenario)}</td>'
                          f'<td>{timeout:g}</td><td>{size}</td><td>{cells[0]}</td>'
                          f'<td>{cells[1]}</td><td class="{color}">{change}</td></tr>')
    comparison_table = ('<h2>Время пакета при смене лимита</h2><div class="scroll"><table><thead><tr>'
                        '<th>Режим</th><th>Celery timeout, с</th><th>DAG</th><th>16, с</th><th>32, с</th><th>Изменение</th>'
                        '</tr></thead><tbody id="comparison">' + ''.join(comparison) + '</tbody></table></div>'
                        '<p class="muted hint">Медиана успешных пакетов · сравнение при одинаковом Celery timeout</p>') if comparison else ""
    materials = " · ".join(f'<a href="{name}">{label}</a>' for name, label in (
        ("incident.md", "Разбор сбоев CSV"), ("grafana-csv-16.jpg", "Скриншот Grafana"),
        ("notes.md", "Разбор результатов"), ("validation.json", "Проверка матрицы"),
        ("grafana-csv-32.jpg", "Grafana CSV 32"),
    ) if (output / name).is_file())
    (output / "index.html").write_text("""<!doctype html><html lang="ru"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Airflow · Сравнение прогонов</title>
<style>
:root{color-scheme:dark;background:#0d131b;color:#e6edf6;font:14px/1.45 Inter,system-ui,sans-serif}
body{margin:0}main{max-width:1500px;margin:auto;padding:20px clamp(12px,3vw,40px)}
h1,h2,p{margin:0}h1{font-size:25px}h2{font-size:16px;margin-bottom:8px}
.toolbar{display:flex;gap:8px 16px;align-items:center;flex-wrap:wrap;margin:10px 0}
section{background:#151e2a;border:1px solid #2c3a4a;border-radius:8px;padding:12px;margin-top:12px}
select{font:inherit;color:inherit;background:#1b2735;border:1px solid #2c3a4a;border-radius:5px;padding:4px 8px}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}
td,th{text-align:right;border-bottom:1px solid #2c3a4a;padding:6px 8px;vertical-align:top;white-space:nowrap}
td:first-child,th:first-child,td:nth-last-child(-n+2),th:nth-last-child(-n+2){text-align:left}
th{color:#a3b4c9;font-size:12px}a{color:#8bd7f7;text-underline-offset:3px}.scroll{overflow:auto}
.muted{color:#a3b4c9}.hint{font-size:12px;margin-top:6px}.ok{color:#70ddb0}.warn{color:#ffca80}.bad{color:#ff9b9b}
details{margin-top:12px}summary{cursor:pointer}[hidden]{display:none!important}
</style><main><h1>Airflow · Сравнение прогонов</h1>
<div class="toolbar hint"><a href="operations.csv">Операции CSV</a><a href="resources.csv">Ресурсы CSV</a>
<a href="processes.csv">Процессы CSV</a><a href="../process-profiles/index.html">Все режимы</a>
""" + materials + """</div>
<div class="toolbar" aria-label="Фильтры пакетов">
<label>Режим <select id="scenario"><option value="">Все</option></select></label>
<label>DAG <select id="size"><option value="">Все</option></select></label>
<label>Parallelism <select id="parallelism"><option value="">Все</option></select></label>
<label><input type="checkbox" id="failed"> Только со сбоями</label><span id="count" class="muted hint" aria-live="polite"></span>
</div>
""" + ('<details><summary>Прерванные измерения · ' + str(len(interruptions)) + '</summary>' +
        ''.join(interruptions) + '</details>' if interruptions else '') + (
        '<section id="comparison-panel">' + comparison_table + '</section>' if comparison_table else '') + """
<section><h2>Память процессов</h2><div class="scroll" style="max-height:480px"><table><thead><tr>
<th>Режим</th><th>Parallelism</th><th>DAG</th><th>Celery и непривязанные, МиБ PSS</th><th>Задачи вместе, МиБ PSS</th><th>Трасса</th>
</tr></thead><tbody id="processes">""" + ''.join(process_rows) + """</tbody></table></div></section>
<section><h2>Пакеты</h2>
<div class="scroll"><table><thead><tr><th>Режим</th><th>Parallelism</th><th>Celery timeout, с</th><th>DAG</th><th>Успех</th><th>Измерение</th><th>Пакет, с</th><th>Пик running</th><th>DAG/мин</th>
<th>Очередь, p50 с</th><th>Scheduler, ядра</th><th>Scheduler, МиБ</th>
<th>Worker, ядра</th><th>Worker, МиБ</th><th>Операции · wall / CPU</th><th>Трасса</th>
</tr></thead><tbody id="packages">""" + "".join(table) + """</tbody></table></div>
<p class="muted hint">Очередь — наибольший p50 по стадиям · ≈ CPU по соседним отсчётам</p></section></main>
<script>
const rows=[...document.querySelectorAll('#packages tr')];
const processRows=[...document.querySelectorAll('#processes tr')];
const compare=[...document.querySelectorAll('#comparison tr')];
const filters=['scenario','size','parallelism'].map(id=>document.getElementById(id));
const failed=document.getElementById('failed');
for(const field of filters){
  const values=[...new Set(rows.map(row=>row.dataset[field.id]))];
  values.sort((a,b)=>a.localeCompare(b,undefined,{numeric:true}));
  for(const value of values) field.add(new Option(value,value));
}
function applyFilters(){
  for(const row of [...rows,...processRows]) row.hidden=filters.some(field=>field.value && row.dataset[field.id]!==field.value) ||
    (failed.checked && row.dataset.ok==='True');
  for(const row of compare) row.hidden=filters.slice(0,2).some(field=>field.value && row.dataset[field.id]!==field.value);
  const comparison=document.getElementById('comparison-panel');
  if(comparison) comparison.hidden=failed.checked || !compare.some(row=>!row.hidden);
  document.getElementById('count').textContent='Пакеты '+rows.filter(row=>!row.hidden).length+' / '+rows.length;
}
for(const field of [...filters,failed]) field.addEventListener('change',applyFilters);
applyFilters();
</script></html>""", encoding="utf-8")


if __name__ == "__main__":
    main()
