import json
import os
import sys, re
from collections import defaultdict
from pathlib import Path
from flask import Flask, redirect, render_template, request, session, url_for
from flask import abort

sys.path.insert(0, os.getcwd())

from operations.runner.Runner import Runner

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "summarys-dev-secret")

TOP_N = None
BUCKET = "tpch-100"
BASE_FOLDER = "./history-10-zstd/"
run = Runner('./benchmarks/tpch/q15.yml.j2')


def get_base_folder():
    return session.get("base_folder", BASE_FOLDER)


def set_base_folder(base_folder):
    base_folder = (base_folder or "").strip()
    if not base_folder:
        base_folder = BASE_FOLDER
    session["base_folder"] = base_folder

def run_path(base_folder, query_name, folder):
    """A run id is "<run>" (base/<query>/<run>) or "<plan>/<run>" (base/<plan>/<query>/<run>, history-plans layout)."""
    if "/" in folder:
        plan, folder = folder.split("/", 1)
        return os.path.join(base_folder, plan, query_name, folder)
    return os.path.join(base_folder, query_name, folder)


def list_runs(base_folder, query_name):
    """Runs of a query: base/<query>/<run>, plus base/<plan>/<query>/<run> when base is e.g. ./history-plans/."""
    runs = []
    query_folder = os.path.join(base_folder, query_name)
    if os.path.isdir(query_folder):
        runs += [f for f in os.listdir(query_folder) if os.path.isdir(os.path.join(query_folder, f))]
    if os.path.isdir(base_folder):
        for plan in os.listdir(base_folder):
            plan_query = os.path.join(base_folder, plan, query_name)
            if plan != query_name and os.path.isdir(plan_query):
                runs += [f"{plan}/{f}" for f in os.listdir(plan_query) if os.path.isdir(os.path.join(plan_query, f))]
    return sorted(runs)


def group_history_folders(folders):
    groups = defaultdict(list)
    for f in folders:
        plan, _, name = f.rpartition("/")
        if "--" in name:
            _, right = name.split("--", 1)
            system, _, tag = right.partition("--")      # <time>--bloomfaas-s3-bloomfaas--knee
            group = "-".join(system.split("-")[:2])
            tag = plan or tag
            if tag:
                group = f"{group} · {tag}"
        else:
            group = f"{plan} · OTHER" if plan else "OTHER"
        groups[group].append(f)
    for g in groups:
        groups[g].sort()
    return dict(sorted(groups.items()))


def get_history_folders():
    base_folder = get_base_folder()
    return sorted([
        f for f in os.listdir(base_folder)
        if os.path.isdir(os.path.join(base_folder, f))
    ])

def summary(history_logs:str):

    if history_logs is None:
        history_logs = history_logs

    folder = Path(history_logs)
    summary = {
        'total_pipelines': 0,
        'total_execution_time_s': 0,
        'total_tasks_time_s': 0,
        'total_tasks': 0,
        'total_successful_tasks': 0,
        'total_failed_tasks': 0,
        'total_output_size_mb': 0,
        'pipelines': []
    }
    for json_file in folder.glob('*.json'):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            file_total_size = 0
            task_start_times = []
            task_end_times = []

            for task in data.get("tasks", []):
                try:
                    file_total_size += task.get("response", {}) \
                        .get("data", {}) \
                        .get("size_mb", 0)
                except Exception:
                    pass

                if task.get("start_time"):
                    task_start_times.append(task["start_time"])
                if task.get("end_time"):
                    task_end_times.append(task["end_time"])

            pipeline_start = min(task_start_times) if task_start_times else None
            pipeline_end = max(task_end_times) if task_end_times else None
            summary['total_output_size_mb'] += file_total_size
            summary['total_pipelines'] += 1
            summary['total_execution_time_s'] += data.get('pipeline_execution_time_s', 0)
            summary['total_tasks_time_s'] += data.get('total_tasks_time_s', 0)
            summary['total_tasks'] += data.get('total_task', 0)
            summary['total_successful_tasks'] += data.get('successful_task', 0)
            summary['total_failed_tasks'] += data.get('failed_task', 0)

            summary['pipelines'].append({
                'file': json_file.name,
                'execution_time_s': data.get('pipeline_execution_time_s', 0),
                'total_tasks_time_s': data.get('total_tasks_time_s', 0),
                'total_task': data.get('total_task', 0),
                'successful_task': data.get('successful_task', 0),
                'failed_task': data.get('failed_task', 0),
                'mtime': json_file.stat().st_mtime,
                'output_size_mb': round(file_total_size, 5),
                'start_time': pipeline_start,
                'end_time': pipeline_end,
            })

        except Exception as e:
            print(f"Error loading {json_file}: {e}")

    summary['total_execution_time_s'] = round(summary['total_execution_time_s'], 3)
    summary['total_tasks_time_s'] = round(summary['total_tasks_time_s'], 3)
    summary['avg_execution_time_s'] = round(summary['total_execution_time_s'] / summary['total_pipelines'], 3) if summary['total_pipelines'] > 0 else 0
    summary['pipelines'] = sorted(
        summary['pipelines'],
        key=lambda x: x['mtime']
    )
    summary['total_output_size_mb'] = round(
        summary['total_output_size_mb'], 5
    )
    return summary

def parse(file):
    metrics = {}
    print(file)
    with open(file) as f:
        for line in f:
            m = re.match(r'minio_bucket_requests_total\{api="([^"]+)",bucket="([^"]+)"[^}]*\}\s+([\d.e+]+)', line)
            if m:
                metrics[(m.group(1), m.group(2))] = int(float(m.group(3)))
    return metrics


def diff(before_file, after_file, bucket_filter=None):
    before = parse(before_file)
    after = parse(after_file)

    result = []

    buckets = sorted(
        set(k[1] for k in before.keys()) |
        set(k[1] for k in after.keys())
    )

    for bucket in buckets:
        if bucket_filter and bucket != bucket_filter:
            continue

        apis = sorted(
            set(k[0] for k in before.keys() if k[1] == bucket) |
            set(k[0] for k in after.keys() if k[1] == bucket)
        )

        categories = {
            'READ': ['getobject'],
            'WRITE': ['putobject', 'putobjectpart', 'newmultipartupload', 'completemultipartupload'],
            'DELETE': ['deleteobject'],
            'METADATA': ['headobject'],
            'OTHER': []
        }

        for api in apis:
            if not any(api in v for v in categories.values()):
                categories['OTHER'].append(api)

        bucket_data = {
            "bucket": bucket,
            "categories": [],
            "totals": {}
        }

        for category, cat_apis in categories.items():
            rows = []
            category_total = 0

            for api in sorted(cat_apis):
                key = (api, bucket)
                b = before.get(key, 0)
                a = after.get(key, 0)
                d = a - b

                rows.append({
                    "api": api,
                    "before": b,
                    "after": a,
                    "delta": d
                })

                category_total += d

            if rows:
                bucket_data["categories"].append({
                    "name": category,
                    "rows": rows
                })

            if category_total != 0:
                bucket_data["totals"][category] = category_total

        result.append(bucket_data)

    return result


def render_summary_page():
    base_folder = get_base_folder()
    query_name = request.form.get("query_name") or request.args.get("query_name") or "q11"
    folders = list_runs(base_folder, query_name)

    grouped_folders = group_history_folders(folders)
    bucket_filter = BUCKET
    results = []
    global_summary = []
    minio = None
    selected = []

    if request.method == "POST":
        selected = request.form.getlist("folders")

        if len(selected) > 1:
            before_file = os.path.join(run_path(base_folder, query_name, selected[0]), 'minio.txt')
            after_file = os.path.join(run_path(base_folder, query_name, selected[1]), 'minio.txt')
            if os.path.exists(before_file) and os.path.exists(after_file):
                minio = diff(before_file, after_file, bucket_filter)

        for folder in selected:
            tmp = summary(run_path(base_folder, query_name, folder))
            pipeline_start_times = [p["start_time"] for p in tmp["pipelines"] if p.get("start_time")]
            pipeline_end_times = [p["end_time"] for p in tmp["pipelines"] if p.get("end_time")]
            results.append({
                "folder": folder,
                "summary": tmp,
                "start_time": min(pipeline_start_times) if pipeline_start_times else None,
                "end_time": max(pipeline_end_times) if pipeline_end_times else None,
            })

    if results:
        exec_times = [r["summary"]["total_execution_time_s"] for r in results]
        task_times = [r["summary"]["total_tasks_time_s"] for r in results]
        total_tasks = [r["summary"]["total_tasks"] for r in results]
        success_tasks = [r["summary"]["total_successful_tasks"] for r in results]
        failed_tasks = [r["summary"]["total_failed_tasks"] for r in results]
        start_times = [r["start_time"] for r in results if r.get("start_time")]
        end_times = [r["end_time"] for r in results if r.get("end_time")]

        def avg(arr):
            return round(sum(arr) / len(arr), 3) if arr else 0

        def delta(v, avg):
            return round(v - avg, 3)

        global_summary = {
            "start_time": min(start_times) if start_times else None,
            "end_time": max(end_times) if end_times else None,
            "avg": {
                "execution": avg(exec_times),
                "tasks_time": avg(task_times),
                "total_tasks": avg(total_tasks),
                "success": avg(success_tasks),
                "failed": avg(failed_tasks),
            }
        }

        global_summary["min"] = {
            "execution": min(exec_times),
            "tasks_time": min(task_times),
            "total_tasks": min(total_tasks),
            "success": min(success_tasks),
            "failed": min(failed_tasks),
            "execution_delta": delta(min(exec_times), global_summary["avg"]["execution"]),
            "tasks_time_delta": delta(min(task_times), global_summary["avg"]["tasks_time"]),
            "total_tasks_delta": delta(min(total_tasks), global_summary["avg"]["total_tasks"]),
            "success_delta": delta(min(success_tasks), global_summary["avg"]["success"]),
            "failed_delta": delta(min(failed_tasks), global_summary["avg"]["failed"]),
        }

        global_summary["max"] = {
            "execution": max(exec_times),
            "tasks_time": max(task_times),
            "total_tasks": max(total_tasks),
            "success": max(success_tasks),
            "failed": max(failed_tasks),
            "execution_delta": delta(max(exec_times), global_summary["avg"]["execution"]),
            "tasks_time_delta": delta(max(task_times), global_summary["avg"]["tasks_time"]),
            "total_tasks_delta": delta(max(total_tasks), global_summary["avg"]["total_tasks"]),
            "success_delta": delta(max(success_tasks), global_summary["avg"]["success"]),
            "failed_delta": delta(max(failed_tasks), global_summary["avg"]["failed"]),
        }

        if TOP_N is not None:
            results = sorted(results, key=lambda r: r["summary"]["total_tasks_time_s"])[:TOP_N]

    return render_template(
        "index.html",
        folders=folders,
        results=results,
        global_summary=global_summary,
        data=minio,
        selected=selected,
        grouped_folders=grouped_folders,
        query_name=query_name,
        base_folder=base_folder,
    )


@app.route("/", methods=["GET", "POST"])
def index():
    return render_summary_page()


@app.route("/history20zstd", methods=["GET", "POST"])
def history20zstd():
    set_base_folder("./history20zstd/")
    return render_summary_page()


@app.route("/settings/base-folder", methods=["GET", "POST"])
def base_folder_settings():
    if request.method == "POST":
        set_base_folder(request.form.get("base_folder"))
        query_name = request.form.get("query_name") or request.args.get("query_name") or "q11"
        return redirect(url_for("index", query_name=query_name))

    return render_template(
        "base_folder.html",
        base_folder=get_base_folder(),
        default_base_folder=BASE_FOLDER,
        query_name=request.args.get("query_name") or "q11",
    )


@app.route("/log")
def log():
    import html as html_lib

    query_name = request.args.get("query_name")
    folder = request.args.get("folder")
    file = request.args.get("file")
    base_folder = get_base_folder()

    path = os.path.join(run_path(base_folder, query_name, folder), file)
    path = os.path.abspath(path)
    base_path = os.path.abspath(base_folder)
    if os.path.commonpath([base_path, path]) != base_path:
        abort(403)
    if not os.path.exists(path):
        abort(404)

    with open(path) as f:
        data = json.load(f)

    pretty = html_lib.escape(json.dumps(data, indent=2, ensure_ascii=False))
    return f"""<!DOCTYPE html>
            <html>
            <head>
            <meta charset="utf-8">
            <title>{query_name}/{folder}/{file}</title>
            <link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11/build/styles/1c-light.min.css">
            <script src="https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11/build/highlight.min.js"></script>
            </head>
            <body>
            <pre><code class="language-json">{pretty}</code></pre>
            <script>hljs.highlightAll();</script>
            </body>
            </html>"""

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
