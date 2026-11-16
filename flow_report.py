"""Compact DevOps report for steady traffic, steps and bursts."""

from datetime import datetime
from html import escape
import json
from pathlib import Path

PROFILES = {"steady": "Постоянная нагрузка", "steps": "Рост нагрузки", "burst": "Пиковая нагрузка"}


def stage_name(stage, document):
    if stage["rate"] is None:
        return f'{document["config"]["burst_runs"]} DAG одновременно'
    if document["config"]["profile"] == "burst" and stage["start"] > 0:
        return "Восстановление"
    return f'{stage["rate"]:g} DAG/мин'


def percentile(values, p=.95):
    if len(values)<20:
        return None
    values=sorted(values)
    index=(len(values)-1)*p
    left=int(index)
    return values[left]+(values[min(left+1,len(values)-1)]-values[left])*(index-left)


def stage_results(document, record):
    start=datetime.fromisoformat(record["released_at"]).timestamp()
    results=[]
    for index, stage in enumerate(document["stages"]):
        arrivals=[r for r in document["arrivals"] if r["stage"]==index and "accepted_at" in r]
        ids={r["run_id"] for r in arrivals}
        tasks=[t for t in record["tasks"] if t["run_id"] in ids]
        waits=[(datetime.fromisoformat(t["start_date"])-datetime.fromisoformat(t["scheduled_dttm"])).total_seconds()
               for t in tasks if t.get("start_date") and t.get("scheduled_dttm")]
        p95=percentile(waits)
        left,right=start+stage["start"],start+stage["end"]
        frames=[f for f in document["queue"] if left<=f["at"]<=right]
        steady=[f for f in frames if f["at"]>=left+(right-left)/3]
        slope=None
        if len(steady)>=2:
            xs=[f["at"]-steady[0]["at"] for f in steady]
            ys=[f["pending"] for f in steady]
            mx,my=sum(xs)/len(xs),sum(ys)/len(ys)
            denominator=sum((x-mx)**2 for x in xs)
            slope=60*sum((x-mx)*(y-my) for x,y in zip(xs,ys))/denominator if denominator else None
        oldest=max((max(f["oldest_queued_seconds"],f["oldest_scheduled_seconds"]) for f in frames),default=0)
        lag=max([0,*(r["sent_at"]-r["planned_at"] for r in arrivals)])
        failures=sum(t["state"]=="failed" for t in tasks)
        actual=60*len(arrivals)/(right-left) if right>left else None
        completion=sum(bool(r.get("end_date")) and left<=datetime.fromisoformat(r["end_date"]).timestamp()<=right
                       for r in record["runs"])
        enough=right-left>=120 and p95 is not None and len(steady)>=20
        if not document["measurement_quality"]["valid"]:
            verdict="Измерение недостоверно"
        elif document["errors"]:
            verdict="Прогон прерван"
        elif document.get("stop_reason"):
            verdict="Остановлено по очереди"
        elif lag>2:
            verdict="Генератор не успевает"
        elif stage["rate"] is None:
            verdict="Пиковая нагрузка"
        elif failures:
            verdict="Ошибки задач"
        elif oldest>document["config"]["queue_limit"] or (p95 is not None and p95>document["config"]["queue_limit"]):
            verdict="Долгое ожидание запуска"
        elif not enough:
            verdict="Недостаточно данных"
        elif slope is not None and slope>max(1,stage["rate"]*.1):
            verdict="Очередь растёт"
        elif actual<stage["rate"]*.95 or 60*completion/(right-left)<stage["rate"]*.95:
            verdict="Обработка не успевает"
        else:
            verdict="Очередь стабильна"
        results.append({**stage,"accepted":len(arrivals),"actual_rate":actual,
                        "completed_rate":60*completion/(right-left) if right>left else None,
                        "p95_start_seconds":p95,"pending_slope_per_minute":slope,"max_submit_lag":lag,
                        "oldest_wait_seconds":oldest,"verdict":verdict})
    return results


def charts(document, record):
    start=datetime.fromisoformat(record["released_at"]).timestamp()
    end=datetime.fromisoformat(record["finished_at"]).timestamp()
    span=max(1,end-start)
    x=lambda t:64+780*(t-start)/span
    output=[]
    def text(left,top,value):
        output.append(f'<text x="{left}" y="{top}" fill="#a3b4c9" font-size="12">{escape(str(value))}</text>')
    for stage in document["stages"]:
        left=x(start+stage["start"])
        width=780*(stage["end"]-stage["start"])/span
        output.append(f'<rect x="{left}" y="20" width="{width}" height="250" fill="#263747" opacity=".35"/>')
        text(left+3,14,stage_name(stage,document))
    accepted=[r["accepted_at"] for r in document["arrivals"] if "accepted_at" in r]
    completed=[datetime.fromisoformat(r["end_date"]).timestamp() for r in record["runs"] if r.get("end_date")]
    frames=document["queue"]
    rates=[{"at":f["at"],"arrival":2*sum(f["at"]-30<t<=f["at"] for t in accepted),
            "finished":2*sum(f["at"]-30<t<=f["at"] for t in completed)} for f in frames]
    for points, fields, top, bottom, label in (
        (rates,[("arrival","#76bfff","Принято"),("finished","#70ddb0","Завершено")],38,126,"DAG/мин · окно 30 с"),
        (frames,[("pending","#ffc26a","Незавершённые DAG"),("queued_tasks","#b5a1ff","Queued задачи"),
                 ("scheduled_tasks","#f29175","Scheduled задачи"),
                 ("running_tasks","#70cdec","Running задачи")],177,265,"Очередь и выполнение")):
        maximum=max([1,*(p[key] for p in points for key,_,_ in fields)])
        text(64,top-6,label)
        for i in range(3):
            y=bottom-(bottom-top)*i/2
            output.append(f'<line x1="64" x2="844" y1="{y}" y2="{y}" stroke="#2c3a4a"/>')
            text(8,y+4,f"{maximum*i/2:.0f}")
        for key,color,name in fields:
            coords=" ".join(f'{x(p["at"]):.2f},{bottom-(bottom-top)*p[key]/maximum:.2f}' for p in points)
            output.append(f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{coords}"><title>{name}</title></polyline>')
    for i in range(5): text(64+780*i/4,288,f"{span*i/4:.0f} с")
    return '<svg viewBox="0 0 880 300">'+''.join(output)+'</svg>'


def recovery_seconds(document, record):
    if not document["measurement_quality"]["valid"] or document["errors"]:
        return None
    start=datetime.fromisoformat(record["released_at"]).timestamp()
    burst=next((i for i,s in enumerate(document["stages"]) if s["rate"] is None),None)
    if burst is None:
        return None
    begin=start+document["stages"][burst]["start"]
    end=start+document["stages"][burst+1]["end"]
    baseline=[f["pending"] for f in document["queue"] if f["at"]<begin]
    accepted=[r["accepted_at"] for r in document["arrivals"] if r["stage"]==burst and "accepted_at" in r]
    if not baseline or not accepted:
        return None
    consecutive=[]
    for frame in document["queue"]:
        if not max(accepted)<=frame["at"]<=end:
            continue
        normal=frame["pending"]<=max(baseline)+1 and max(frame["oldest_queued_seconds"],frame["oldest_scheduled_seconds"])<=document["config"]["queue_limit"]
        consecutive=consecutive+[frame] if normal else []
        if len(consecutive)>=3:
            return consecutive[0]["at"]-begin
    return None


def render_flow(output, document, record):
    marker=output/"fault-injection.json"
    if marker.exists(): document["injected_fault"]=json.loads(marker.read_text(encoding="utf-8"))
    results=stage_results(document,record)
    document["stage_results"]=results
    document["recovery_seconds"]=recovery_seconds(document,record)
    (output/"flow.json").write_text(json.dumps(document,ensure_ascii=False),encoding="utf-8")
    fmt=lambda value: "—" if value is None else f"{value:.2f}"
    rows=[]
    for row in results:
        cells=[stage_name(row,document),fmt(row["actual_rate"]),fmt(row["completed_rate"]),fmt(row["p95_start_seconds"]),
               fmt(row["pending_slope_per_minute"]),row["verdict"]]
        rows.append('<tr>'+''.join('<td>'+escape(str(v))+'</td>' for v in cells)+'</tr>')
    proven=[r["rate"] for r in results if r["verdict"]=="Очередь стабильна"]
    slow=[r["rate"] for r in results if r["verdict"]=="Долгое ожидание запуска" and r["rate"] is not None]
    config=document["config"]
    conclusion=(f'Ожидание запуска > {config["queue_limit"]:g} с при {min(slow):g} DAG/мин' if slow else
                f"Выдержано {max(proven):g} DAG/мин" if proven else "Результаты")
    status=("Измерение недостоверно" if not document["measurement_quality"]["valid"] else
            "Прогон прерван" if document["errors"] else document.get("stop_reason") or "")
    quality=[*document["measurement_quality"]["issues"],*document["errors"]]
    fault_note='<p>Тест потери метрик</p>' if document.get("injected_fault") else ''
    recovery_note=('<p>Восстановление очереди: '+
                   (fmt(document["recovery_seconds"])+" с" if document["recovery_seconds"] is not None else "не завершилось за время теста")+'</p>') if config["profile"]=="burst" else ''
    legend=[("#76bfff","Новые DAG"),("#70ddb0","Завершённые DAG"),("#ffc26a","Незавершённые DAG"),
            ("#f29175","Scheduled"),("#b5a1ff","Queued"),("#70cdec","Running")]
    (output/"flow.html").write_text('''<!doctype html><html lang="ru"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Airflow · Нагрузочный тест</title>
<style>:root{color-scheme:dark;background:#0d131b;color:#e6edf6;font:14px/1.5 system-ui}body{max-width:1250px;margin:24px auto;padding:0 16px}
h1{font-size:25px;margin:0}h2{font-size:17px}a{color:#8bd7f7}p,small{color:#a3b4c9}.cards,.legend{display:flex;gap:12px;flex-wrap:wrap}.legend{font-size:12px}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:5px}
.card,section{background:#151e2a;border:1px solid #2c3a4a;border-radius:8px;padding:14px;margin:12px 0}.card strong{display:block;font-size:25px}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}td,th{border-bottom:1px solid #2c3a4a;padding:10px;text-align:right;white-space:nowrap}td:first-child,th:first-child,td:last-child,th:last-child{text-align:left}.scroll{overflow:auto}svg{width:100%;min-width:560px}summary{cursor:pointer}
@media(max-width:850px){td,th{font-size:12px;padding:8px 5px;white-space:normal}}
</style>''' + f'<h1>{escape(config["scenario"])} · {PROFILES[config["profile"]]}</h1>'+('<p>'+escape(status)+'</p>' if status else '')+fault_note+
      '<div class="cards">'+''.join(f'<div class="card"><small>{escape(label)}</small><strong>{escape(value)}</strong></div>' for label,value in [
          ("Завершено успешно",f'{record["successful"]}/{record["size"]}'),
          ("Лимит Airflow / Celery",f'{config["parallelism"]} / {config["worker_concurrency"]}'),
          ("Пик незавершённых DAG",str(max((f["pending"] for f in document["queue"]),default=0))),
          ("Обработка остатка",f'{max(0,record["duration_seconds"]-document["generated_seconds"]):.1f} с')])+'</div>'+
      '<p><a href="trace.html">Ресурсы и логи</a>　<a href="../flow-profiles/index.html">История запусков</a></p>'+
      '<section><div class="scroll">'+charts(document,record)+'</div><div class="legend">'+''.join(f'<span><i class="dot" style="background:{color}"></i>{name}</span>' for color,name in legend)+'</div></section>'+
      '<section><h2>'+escape(conclusion)+'</h2>'+recovery_note+'<div class="scroll"><table><thead><tr><th>Нагрузка</th><th>Новые DAG/мин</th><th>Завершено DAG/мин</th><th>Ожидание p95, с</th><th>Рост очереди DAG/мин</th><th>Вывод</th></tr></thead><tbody>'+''.join(rows)+'</tbody></table></div></section>'+
      '<details><summary>Параметры и диагностика</summary><p>Для оценки устойчивой работы: минимум 120 с и 20 стартовавших задач на каждом уровне нагрузки. Ожидание — от scheduled до старта; также проверяется возраст задач в очереди. Частота замеров ресурсов — 1 с, допустимый разрыв — 3 с.</p><pre>'+escape(json.dumps({"quality":quality,"config":config},ensure_ascii=False,indent=2))+'</pre></details>',encoding="utf-8")
    index=output.parent/"flow-profiles"
    index.mkdir(exist_ok=True)
    links=[]
    for path in sorted(output.parent.glob("flow-*/flow.json"),reverse=True):
        flow=json.loads(path.read_text(encoding="utf-8"))
        c=flow["config"]
        state='Тест потери метрик' if flow.get('injected_fault') else 'Полные метрики' if flow['measurement_quality']['valid'] else 'Неполные метрики'
        links.append(f'<tr><td>{escape(c["scenario"])}</td><td>{PROFILES[c["profile"]]}</td><td>{c["parallelism"]} / {c["worker_concurrency"]}</td><td>{state}</td><td><a href="../{escape(path.parent.name)}/flow.html">Открыть</a></td></tr>')
    (index/"index.html").write_text('<!doctype html><meta charset="utf-8"><title>Нагрузочные тесты</title><style>body{background:#0d131b;color:#e6edf6;font:16px system-ui;max-width:1100px;margin:30px auto}a{color:#8bd7f7}td,th{padding:12px;border-bottom:1px solid #2c3a4a;text-align:left}table{width:100%;border-collapse:collapse}</style><h1>Нагрузочные тесты</h1><table><tr><th>Задачи</th><th>Нагрузка</th><th>Лимит Airflow / Celery</th><th>Метрики</th><th></th></tr>'+''.join(links)+'</table>',encoding="utf-8")
