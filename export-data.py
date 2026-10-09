import json
import os
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import lru_cache
from pathlib import Path

FNAME = "50-zstd-02"
BASE_FOLDER = f"./history-{FNAME}/"
BUCKET_FILTER = "tpch-100"
SORT_LIMIT = -1
MAX_WORKERS = min(32, (os.cpu_count() or 1) + 4)


REQUEST_PATH_TO_FUNC = {
    "/scan-starling": "scan_starling",
    "/scan-bloomfaas": "scan_bloomfaas",
    "/join": "broadcast_join",
    "/join-pushdown": "broadcast_join_pushdown",
    "/merge-bf": "merge_bf",
    "/aggregate": "aggregate",
    "/hash-partition": "hash_partition",
    "/range-join": "range_join",
    "/summary-clean": "summary_clean",
}

def _iter_run_json_files(run_folder: Path):
    for file in sorted(run_folder.glob("*.json")):
        if "statistics" in file.name:
            continue
        yield file


def _load_json(file: Path) -> dict | None:
    try:
        with open(file) as f:
            return json.load(f)
    except Exception:
        return None


def _iso_to_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _group_name(folder_name: str) -> str:
    if "--" in folder_name:
        _, right = folder_name.split("--", 1)
        return right
    return "OTHER"


def _request_path_to_func(path: str | None) -> str | None:
    if not path:
        return None
    return REQUEST_PATH_TO_FUNC.get(path, path.strip("/").replace("-", "_"))


def _task_to_func(task: dict) -> str | None:
    task_name = task.get("task_name")
    if isinstance(task_name, str) and "combine" in task_name.lower():
        return "combine"
    func_name = _request_path_to_func(task.get("request_path"))
    if func_name and func_name.startswith("scan_"):
        return "scan"
    return func_name


def _has_bf_path_value(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() not in {"", "none", "null", "false"}
    return bool(value)


def _combine_builds_bf(task: dict) -> bool:
    params = task.get("params")
    if isinstance(params, dict) and _has_bf_path_value(params.get("bf_path")):
        return True

    response_data = task.get("response", {}).get("data", {})
    return isinstance(response_data, dict) and _has_bf_path_value(response_data.get("bf_path"))


def _task_func_times(task: dict) -> list[tuple[str, float]]:
    func_name = _task_to_func(task)
    if not func_name:
        return []

    execution_time_s = task.get("execution_time_s", 0) or 0
    if func_name == "combine" and _combine_builds_bf(task):
        return [
            ("combine", execution_time_s * 0.8),
            ("merge_bf", execution_time_s * 0.2),
        ]
    return [(func_name, execution_time_s)]


def _stats(values: list[float]) -> dict:
    if not values:
        return {"avg": 0, "min": 0, "max": 0}
    return {
        "avg": round(sum(values) / len(values), 3),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
    }


def _single(value) -> dict:
    return {"value": value}


@lru_cache(maxsize=None)
def _parse_minio(file: Path) -> dict:
    metrics = {}
    with open(file) as f:
        for line in f:
            match = re.match(
                r'minio_bucket_requests_total\{api="([^"]+)",bucket="([^"]+)"[^}]*\}\s+([\d.e+]+)',
                line,
            )
            if match:
                metrics[(match.group(1), match.group(2))] = int(float(match.group(3)))
    return metrics


def _collect_run_metrics(run_folder: Path) -> dict:
    processing_time = 0.0
    task_processing_time = 0.0
    intermediate_mb = 0.0
    total_invocations = 0
    start_times = []
    end_times = []
    all_delays_ms = []
    func_times = defaultdict(float)
    func_invocations = defaultdict(int)
    func_delays_ms = defaultdict(list)

    for file in _iter_run_json_files(run_folder):
        data = _load_json(file)
        if not data:
            continue

        processing_time += data.get("pipeline_execution_time_s", 0)
        task_processing_time += data.get("total_tasks_time_s", 0)
        total_invocations += data.get("total_task", 0)

        for task in data.get("tasks", []):
            intermediate_mb += task.get("response", {}).get("data", {}).get("size_mb", 0)

            start_time = task.get("start_time")
            end_time = task.get("end_time")
            if start_time:
                start_times.append(start_time)
            if end_time:
                end_times.append(end_time)

            func_time_items = _task_func_times(task)
            for func_name, execution_time_s in func_time_items:
                func_times[func_name] += execution_time_s
                func_invocations[func_name] += 1

            sent_at = _iso_to_datetime(task.get("request_sent_at"))
            server_start = _iso_to_datetime(task.get("server_start_time"))
            if not sent_at or not server_start:
                continue

            delay_ms = (server_start - sent_at).total_seconds() * 1000
            all_delays_ms.append(delay_ms)
            for func_name, _ in func_time_items:
                func_delays_ms[func_name].append(delay_ms)

    return {
        "processing_time": processing_time,
        "task_processing_time": task_processing_time,
        "intermediate_mb": round(intermediate_mb, 5),
        "total_invocations": total_invocations,
        "start": min(start_times) if start_times else None,
        "end": max(end_times) if end_times else None,
        "latency_ms": _stats(all_delays_ms),
        "funcs": {
            name: {
                "time_s": time_s,
                "invocations": func_invocations[name],
                "latency_ms": _stats(func_delays_ms.get(name, [])),
            }
            for name, time_s in sorted(func_times.items())
        },
    }


def _collect_bf(run_folder: Path) -> float:
    def _legacy_size(item) -> float:
        if isinstance(item, dict):
            return float(item.get("total_size_mb", item.get("size_mb", item.get("size_m", 0))) or 0)
        return 0.0

    def _bf_size_from_obj(obj, seen_paths: set[str]) -> float:
        if isinstance(obj, list):
            return sum(_bf_size_from_obj(item, seen_paths) for item in obj)
        if not isinstance(obj, dict):
            return 0.0

        total = 0.0
        bf_path = obj.get("bf_path")
        if isinstance(bf_path, dict):
            path = str(bf_path.get("path", ""))
            size = bf_path.get("size_m", bf_path.get("size_mb", bf_path.get("total_size_mb", 0)))
            if size and (not path or path not in seen_paths):
                total += float(size)
                if path:
                    seen_paths.add(path)

        for value in obj.values():
            total += _bf_size_from_obj(value, seen_paths)
        return total

    def _load_json(path: Path):
        try:
            return json.loads(path.read_text())
        except Exception:
            return None

    bf_file = run_folder / "statistics-bf.json"
    task_names = []
    legacy_total = 0.0
    stats = _load_json(bf_file) if bf_file.exists() else None
    if isinstance(stats, list):
        task_names = [item.get("task_name") for item in stats if isinstance(item, dict) and item.get("task_name")]
        legacy_total = sum(_legacy_size(item) for item in stats)
    elif isinstance(stats, dict):
        task_name = stats.get("task_name")
        task_names = [task_name] if task_name else []
        legacy_total = _legacy_size(stats)

    seen_paths = set()
    total = 0.0
    for task_name in task_names:
        total += _bf_size_from_obj(_load_json(run_folder / f"{task_name}.json"), seen_paths)

    if total == 0.0:
        for path in run_folder.glob("*.json"):
            if path.name == "statistics-bf.json":
                continue
            total += _bf_size_from_obj(_load_json(path), seen_paths)

    return round(total if total else legacy_total, 5)



def _collect_minio_diff(run_folder: Path, prev_folder: Path, bucket: str) -> dict[str, int]:
    after_file = run_folder / "minio.txt"
    before_file = prev_folder / "minio.txt"

    if not after_file.exists() or not before_file.exists():
        return {"read": 0, "write": 0, "meta": 0}

    read_apis = {"getobject"}
    write_apis = {"putobject", "putobjectpart", "newmultipartupload", "completemultipartupload"}
    meta_apis = {"headobject"}

    after = _parse_minio(after_file)
    before = _parse_minio(before_file)

    read = write = meta = 0
    for (api, bkt), value in after.items():
        if bkt != bucket:
            continue
        diff = value - before.get((api, bkt), 0)
        if api in read_apis:
            read += diff
        elif api in write_apis:
            write += diff
        elif api in meta_apis:
            meta += diff
    return {"read": read, "write": write, "meta": meta}


def _empty_query_metrics() -> dict:
    return {
        "run_count": 0,
        "proc_times": [],
        "task_times": [],
        "intermediates": [],
        "bf_sizes": [],
        "reads": [],
        "writes": [],
        "metas": [],
        "invocations": [],
        "start_times": [],
        "end_times": [],
        "run_ranges": [],
        "latencies_ms": [],
        "func_times": defaultdict(list),
        "func_invocations": defaultdict(list),
        "func_latencies_ms": defaultdict(list),
    }


def _collect_run_export(payload: tuple[str, str, Path, Path, str]) -> dict:
    query, system, prev_run_folder, run_folder, bucket = payload
    run_metrics = _collect_run_metrics(run_folder)
    minio = _collect_minio_diff(run_folder, prev_run_folder, bucket)
    return {
        "query": query,
        "system": system,
        "run_metrics": run_metrics,
        "bf_size": _collect_bf(run_folder),
        "minio": minio,
    }


def _merge_run_export(systems: dict[str, dict[str, dict]], item: dict) -> None:
    query = item["query"]
    system = item["system"]
    run_metrics = item["run_metrics"]

    systems.setdefault(system, {})
    systems[system].setdefault(query, _empty_query_metrics())
    query_metrics = systems[system][query]
    run_index = query_metrics["run_count"]

    query_metrics["proc_times"].append(run_metrics["processing_time"])
    query_metrics["task_times"].append(run_metrics["task_processing_time"])
    query_metrics["intermediates"].append(run_metrics["intermediate_mb"])
    query_metrics["bf_sizes"].append(item["bf_size"])

    minio = item["minio"]
    query_metrics["reads"].append(minio["read"])
    query_metrics["writes"].append(minio["write"])
    query_metrics["metas"].append(minio["meta"])
    query_metrics["invocations"].append(run_metrics["total_invocations"])

    if run_metrics["start"]:
        query_metrics["start_times"].append(run_metrics["start"])
    if run_metrics["end"]:
        query_metrics["end_times"].append(run_metrics["end"])
    if run_metrics["start"] and run_metrics["end"]:
        query_metrics["run_ranges"].append({
            "start": run_metrics["start"],
            "end": run_metrics["end"],
        })

    if run_metrics["latency_ms"]["avg"] > 0:
        query_metrics["latencies_ms"].append(run_metrics["latency_ms"]["avg"])

    current_funcs = run_metrics["funcs"]
    existing_func_names = set(query_metrics["func_times"].keys())
    incoming_func_names = set(current_funcs.keys())

    for func_name in existing_func_names - incoming_func_names:
        query_metrics["func_times"][func_name].append(0)
        query_metrics["func_invocations"][func_name].append(0)
        query_metrics["func_latencies_ms"][func_name].append(0)

    for func_name in incoming_func_names - existing_func_names:
        query_metrics["func_times"][func_name].extend([0] * run_index)
        query_metrics["func_invocations"][func_name].extend([0] * run_index)
        query_metrics["func_latencies_ms"][func_name].extend([0] * run_index)

    for func_name, values in run_metrics["funcs"].items():
        query_metrics["func_times"][func_name].append(values["time_s"])
        query_metrics["func_invocations"][func_name].append(values["invocations"])
        query_metrics["func_latencies_ms"][func_name].append(values["latency_ms"]["avg"])

    query_metrics["run_count"] += 1


def export(
    history_folder: str = BASE_FOLDER,
    sort_limit: int = SORT_LIMIT,
    bucket: str = BUCKET_FILTER,
    max_workers: int = MAX_WORKERS,
) -> list:
    base = Path(history_folder)
    queries = sorted(d.name for d in base.iterdir() if d.is_dir())

    systems: dict[str, dict[str, dict]] = {}
    jobs = []

    for query in queries:
        query_path = base / query
        all_folders = sorted(d for d in query_path.iterdir() if d.is_dir())

        system_folders = defaultdict(list)
        for run_folder in all_folders:
            system_folders[_group_name(run_folder.name)].append(run_folder)

        for _, folders in system_folders.items():
            folders_sorted = sorted(folders, key=lambda rf: rf.name)
            if sort_limit > 0:
                folders_sorted = folders_sorted[:sort_limit + 1]

            for i in range(1, len(folders_sorted)):
                prev_run_folder = folders_sorted[i - 1]
                run_folder = folders_sorted[i]
                system = _group_name(run_folder.name)
                jobs.append((query, system, prev_run_folder, run_folder, bucket))

    if max_workers <= 1 or len(jobs) <= 1:
        for job in jobs:
            _merge_run_export(systems, _collect_run_export(job))
    else:
        workers = min(max_workers, len(jobs))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for item in executor.map(_collect_run_export, jobs):
                _merge_run_export(systems, item)

    result = []
    for system, queries_data in sorted(systems.items()):
        entry = {"system": system, "data": {}}
        for query, data in sorted(queries_data.items(), key=lambda x: int(re.sub(r"\D", "", x[0]) or 0)):
            funcs = {}
            all_func_names = sorted(
                set(data["func_times"].keys()) |
                set(data["func_invocations"].keys()) |
                set(data["func_latencies_ms"].keys())
            )
            for func_name in all_func_names:
                funcs[func_name] = {
                    "time_s": _stats(data["func_times"].get(func_name, [])),
                    "invocations": _stats(data["func_invocations"].get(func_name, [])),
                    "latency_ms": _stats(data["func_latencies_ms"].get(func_name, [])),
                }

            entry["data"][query] = {
                "processing_time": _stats(data["proc_times"]),
                "task_processing_time": _stats(data["task_times"]),
                "intermediate": _stats(data["intermediates"]),
                "bf": _stats(data["bf_sizes"]),
                "read": _stats(data["reads"]),
                "write": _stats(data["writes"]),
                "meta": _stats(data["metas"]),
                "total_invocation": _stats(data["invocations"]),
                "latency_ms": _stats(data["latencies_ms"]),
                "funcs": funcs,
                "run_ranges": data["run_ranges"],
            }
        result.append(entry)

    return result


if __name__ == "__main__":
    import sys
    
    folder = sys.argv[1] if len(sys.argv) > 1 else BASE_FOLDER
    data = export(folder)
    out = f"./notebooks/data/25 Apr/experiment-export-tpch-100-5seek-history-{FNAME}.json"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Exported to {out}")
