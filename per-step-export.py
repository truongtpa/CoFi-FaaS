"""
Export experiment data with PER-STEP granularity, with correct averaging.

KEY UPDATE in v3:
- Track "BF apply time" separately = sum of all BF-specific work:
  * merge_bf stage time (100%)
  * 20% of combine/aggregate steps that BUILD a Bloom filter

- Track baseline Fixed and Optimizable.
- For BF system, only need: total time + BF apply time + BF reduction metrics.

Classification rules (per-step):
  scan from tpch100/tpch-100     -> FIXED
  scan from s3 benchmark paths   -> OPTIMIZABLE
  aggregate (no BF build)       -> OPTIMIZABLE  (combine intermediate data)
  aggregate (builds BF)         -> 80% OPTIMIZABLE + 20% BF apply
  aggregate (final)             -> FIXED
  merge_bf                      -> 100% BF apply  (BF construction overhead)
  broadcast_join                -> OPTIMIZABLE
  hash_join                     -> OPTIMIZABLE
  hash_partition                -> OPTIMIZABLE
  range_join                    -> OPTIMIZABLE
  summary_clean                 -> FIXED
"""

import json
import os
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

FNAME = "50-zstd"
BASE_FOLDER = f"./history-{FNAME}/"
MAX_WORKERS = min(32, (os.cpu_count() or 1) + 4)


REQUEST_PATH_TO_FUNC = {
    "/scan-starling": "scan",
    "/scan-bloomfaas": "scan",
    "/join": "broadcast_join",
    "/join-pushdown": "broadcast_join",
    "/merge-bf": "merge_bf",
    "/aggregate": "aggregate",
    "/hash-partition": "hash_partition",
    "/range-join": "range_join",
    "/summary-clean": "summary_clean",
}


def _iter_run_json_files(run_folder: Path):
    for file in sorted(run_folder.glob("*.json")):
        if "statistics" in file.name or file.name == "minio.txt":
            continue
        yield file


def _load_json(file: Path):
    try:
        with open(file) as f:
            return json.load(f)
    except Exception:
        return None


def _group_name(folder_name: str) -> str:
    if "--" in folder_name:
        _, right = folder_name.split("--", 1)
        return right
    return "OTHER"


def _request_path_to_func(path):
    if not path:
        return None
    return REQUEST_PATH_TO_FUNC.get(path, path.strip("/").replace("-", "_"))


def _has_bf_path_value(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() not in {"", "none", "null", "false"}
    return bool(value)


def _scan_source_type(location) -> str:
    if not isinstance(location, str):
        return "unknown"

    value = location.strip().lower().replace("s3://", "")
    if re.search(r"(^|/)(tpch100|tpch-100)(/|$)", value):
        return "tpch"
    if re.search(r"(^|/)(s3-)?benchmarks?(/|$)", value):
        return "benchmark"
    return "unknown"


def _classify_task(task: dict) -> dict:
    task_name = task.get("task_name", "")
    func = _request_path_to_func(task.get("request_path"))

    if func == "aggregate":
        is_combine = "--combine" in task_name.lower()
        is_reduce = "--reduce-" in task_name.lower()
        is_final_agg = not is_combine and not is_reduce and "agg" in task_name.lower()
    else:
        is_final_agg = False

    params = task.get("params") or {}
    response_data = (task.get("response") or {}).get("data") or {}

    builds_bf = False
    if isinstance(params, dict) and _has_bf_path_value(params.get("bf_path")):
        builds_bf = True
    if isinstance(response_data, dict) and _has_bf_path_value(response_data.get("bf_path")):
        builds_bf = True

    uses_bf = False
    if isinstance(params, dict):
        if params.get("filter_by_bf") is True or params.get("filter_by_bf") == "true":
            uses_bf = True

    scan_source_type = None
    if func == "scan" and isinstance(params, dict):
        scan_source_type = _scan_source_type(params.get("location"))

    return {
        "task_name": task_name,
        "func": func,
        "builds_bf": builds_bf,
        "uses_bf": uses_bf,
        "is_final_agg": is_final_agg,
        "scan_source_type": scan_source_type,
    }


def _collect_run_steps(run_folder: Path) -> dict:
    """
    Collect per-step metrics for a SINGLE run.
    """
    step_data = {}
    pipeline_time_s = 0.0
    total_task_time_s = 0.0

    for file in _iter_run_json_files(run_folder):
        data = _load_json(file)
        if not data:
            continue

        step_name = file.stem
        step_pipeline_time = data.get("pipeline_execution_time_s", 0) or 0
        step_total_task = data.get("total_tasks_time_s", 0) or 0
        total_tasks = data.get("total_task", 0) or 0

        pipeline_time_s += step_pipeline_time
        total_task_time_s += step_total_task

        tasks = data.get("tasks") or []
        if not tasks:
            continue

        cls = _classify_task(tasks[0])
        if not cls["func"]:
            for t in tasks[1:]:
                c2 = _classify_task(t)
                if c2["func"]:
                    cls = c2
                    break

        for t in tasks:
            c = _classify_task(t)
            if c["builds_bf"]:
                cls["builds_bf"] = True
            if c["uses_bf"]:
                cls["uses_bf"] = True
            if c["scan_source_type"] == "benchmark":
                cls["scan_source_type"] = "benchmark"
            elif c["scan_source_type"] == "tpch" and cls["scan_source_type"] != "benchmark":
                cls["scan_source_type"] = "tpch"

        step_data[step_name] = {
            "func": cls["func"],
            "time_s": step_total_task,
            "invocations": total_tasks,
            "builds_bf": cls["builds_bf"],
            "uses_bf": cls["uses_bf"],
            "is_final_agg": cls["is_final_agg"],
            "scan_source_type": cls["scan_source_type"],
        }

    return {
        "pipeline_time_s": pipeline_time_s,
        "total_task_time_s": total_task_time_s,
        "steps": step_data,
    }


def _stats(values):
    if not values:
        return {"avg": 0, "min": 0, "max": 0, "n": 0}
    return {
        "avg": round(sum(values) / len(values), 3),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
        "n": len(values),
    }


def _empty_query_metrics():
    return {
        "run_count": 0,
        "pipeline_times": [],
        "total_task_times": [],
        "steps_runs": defaultdict(list),
        "steps_meta": {},
        "steps_invocations": defaultdict(list),
    }


def _collect_run_export(payload):
    query, system, run_folder = payload
    run_data = _collect_run_steps(run_folder)
    return {
        "query": query,
        "system": system,
        "run_data": run_data,
    }


def _merge_run_export(systems, item):
    query = item["query"]
    system = item["system"]
    run_data = item["run_data"]

    systems.setdefault(system, {})
    systems[system].setdefault(query, _empty_query_metrics())
    qm = systems[system][query]

    qm["run_count"] += 1
    qm["pipeline_times"].append(run_data["pipeline_time_s"])
    qm["total_task_times"].append(run_data["total_task_time_s"])

    for step_name, step_info in run_data["steps"].items():
        qm["steps_runs"][step_name].append(step_info["time_s"])
        qm["steps_invocations"][step_name].append(step_info["invocations"])

        if step_name not in qm["steps_meta"]:
            qm["steps_meta"][step_name] = {
                "func": step_info["func"],
                "builds_bf": step_info["builds_bf"],
                "uses_bf": step_info["uses_bf"],
                "is_final_agg": step_info["is_final_agg"],
                "scan_source_type": step_info["scan_source_type"],
            }
        else:
            meta = qm["steps_meta"][step_name]
            meta["builds_bf"] = meta["builds_bf"] or step_info["builds_bf"]
            meta["uses_bf"] = meta["uses_bf"] or step_info["uses_bf"]
            meta["is_final_agg"] = meta["is_final_agg"] or step_info["is_final_agg"]
            if step_info["scan_source_type"] == "benchmark":
                meta["scan_source_type"] = "benchmark"
            elif step_info["scan_source_type"] == "tpch" and meta.get("scan_source_type") != "benchmark":
                meta["scan_source_type"] = "tpch"


def export(history_folder=BASE_FOLDER, max_workers=MAX_WORKERS, skip_first_run=False):
    base = Path(history_folder)
    queries = sorted(d.name for d in base.iterdir() if d.is_dir())

    systems = {}
    jobs = []

    for query in queries:
        query_path = base / query
        all_folders = sorted(d for d in query_path.iterdir() if d.is_dir())

        system_folders = defaultdict(list)
        for run_folder in all_folders:
            system_folders[_group_name(run_folder.name)].append(run_folder)

        for _, folders in system_folders.items():
            folders_sorted = sorted(folders, key=lambda rf: rf.name)
            runs_to_use = folders_sorted[1:] if skip_first_run and len(folders_sorted) > 1 else folders_sorted

            for run_folder in runs_to_use:
                system = _group_name(run_folder.name)
                jobs.append((query, system, run_folder))

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
            steps_out = {}
            for step_name, times_per_run in data["steps_runs"].items():
                meta = data["steps_meta"][step_name]
                steps_out[step_name] = {
                    "func": meta["func"],
                    "time_s": _stats(times_per_run),
                    "invocations": _stats(data["steps_invocations"][step_name]),
                    "builds_bf": meta["builds_bf"],
                    "uses_bf": meta["uses_bf"],
                    "is_final_agg": meta["is_final_agg"],
                    "scan_source_type": meta.get("scan_source_type"),
                }

            entry["data"][query] = {
                "run_count": data["run_count"],
                "pipeline_time_s": _stats(data["pipeline_times"]),
                "total_task_time_s": _stats(data["total_task_times"]),
                "steps": steps_out,
            }
        result.append(entry)

    return result


def amdahl_decompose(query_data: dict) -> dict:
    """
    Compute Amdahl decomposition with BF apply time tracked separately.

    Returns:
      fixed_s        : time in stages BF cannot reduce (Fixed for Amdahl)
      optimizable_s  : time in stages BF can reduce (Optimizable for Amdahl)
      bf_apply_s     : BF-specific overhead = merge_bf + 20% of BF-building combines
                       (this is reported SEPARATELY for the BF system)

    Note: For BASELINE system, bf_apply_s is always 0 (no merge_bf, no BF builds).
    For BF system, bf_apply_s captures the cost of building BFs.
    """
    fixed_s = 0.0
    optimizable_s = 0.0
    bf_apply_s = 0.0

    for step_name, info in query_data.get("steps", {}).items():
        func = info.get("func")
        time_s = info.get("time_s", {}).get("avg", 0) if isinstance(info.get("time_s"), dict) else info.get("time_s", 0)
        builds_bf = info.get("builds_bf", False)
        is_final_agg = info.get("is_final_agg", False)

        if func == "scan":
            if info.get("scan_source_type") == "benchmark":
                optimizable_s += time_s
            else:
                fixed_s += time_s
        elif func == "aggregate":
            if is_final_agg:
                fixed_s += time_s
            elif builds_bf:
                # 80% optimizable (combine work) + 20% BF apply (build BF)
                optimizable_s += 0.8 * time_s
                bf_apply_s += 0.2 * time_s
            else:
                optimizable_s += time_s
        elif func == "merge_bf":
            # 100% BF apply
            bf_apply_s += time_s
        elif func in ("broadcast_join", "hash_join", "range_join", "hash_partition"):
            optimizable_s += time_s
        elif func == "summary_clean":
            fixed_s += time_s
        else:
            fixed_s += time_s

    total_s = fixed_s + optimizable_s + bf_apply_s
    return {
        "fixed_s": round(fixed_s, 3),
        "optimizable_s": round(optimizable_s, 3),
        "bf_apply_s": round(bf_apply_s, 3),
        "total_s": round(total_s, 3),
    }


def make_summary_table(data, baseline_system="starling-base", bf_system="bloomfaas-s3-bloomfaas"):
    """Generate paper-ready summary table per query."""
    base_entry = next(e for e in data if e["system"] == baseline_system)
    bf_entry = next(e for e in data if e["system"] == bf_system)

    rows = []
    queries = sorted(base_entry["data"].keys(), key=lambda x: int(re.sub(r"\D", "", x) or 0))

    for q in queries:
        if q not in bf_entry["data"]:
            continue

        bd = base_entry["data"][q]
        fd = bf_entry["data"][q]

        base_decomp = amdahl_decompose(bd)
        bf_decomp = amdahl_decompose(fd)

        # Baseline
        fixed = base_decomp["fixed_s"]
        optim = base_decomp["optimizable_s"]
        # NOTE: baseline should have bf_apply_s = 0; if not, it means some
        # baseline aggregates have bf_path metadata (unusual). Add to fixed.
        if base_decomp["bf_apply_s"] > 0.01:
            fixed += base_decomp["bf_apply_s"]
        total_b = fixed + optim
        p_pct = optim / total_b * 100 if total_b else 0

        # BF system
        bf_apply = bf_decomp["bf_apply_s"]
        total_f = bf_decomp["total_s"]

        # TR
        tr = (total_b - total_f) / total_b * 100 if total_b else 0

        rows.append({
            "q": q,
            "fixed_b": fixed,
            "optim_b": optim,
            "p_pct": p_pct,
            "bf_apply_s": bf_apply,
            "total_bf": total_f,
            "tr": tr,
        })
    return rows


def print_summary_table(rows):
    print(f"{'Q':<5} {'Fix_b':>7} {'Opt_b':>7} {'p%':>6} | {'BFapply':>8} {'Tot_BF':>8} {'TR%':>6}")
    print("-" * 60)
    for r in rows:
        print(f"{r['q']:<5} {r['fixed_b']:>6.1f}s {r['optim_b']:>6.1f}s {r['p_pct']:>5.1f}% | "
              f"{r['bf_apply_s']:>7.1f}s {r['total_bf']:>7.1f}s {r['tr']:>+5.1f}%")


if __name__ == "__main__":
    import sys

    folder = sys.argv[1] if len(sys.argv) > 1 else BASE_FOLDER
    skip_first = "--skip-first" in sys.argv

    data = export(folder, skip_first_run=skip_first)
    out = f"./notebooks/data/25 Apr/per-step-export-v3-{FNAME}.json"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Exported to {out}")

    # Print verification + summary
    print("\n=== Decomposition with BF apply tracked separately ===\n")
    for entry in data:
        print(f"\nSystem: {entry['system']}")
        print(f"{'Query':<6} {'Runs':>5} {'Total':>9} {'Fixed':>9} {'Optim':>9} {'BFapply':>9} {'p%':>6}")
        print("-" * 65)
        for q, qdata in entry["data"].items():
            decomp = amdahl_decompose(qdata)
            measured = qdata["total_task_time_s"]["avg"]
            decomposed = decomp["total_s"]
            p_pct = decomp["optimizable_s"] / decomposed * 100 if decomposed else 0
            tag = "" if abs(decomposed - measured) < 0.5 else " *MISMATCH*"
            print(f"{q:<6} {qdata['run_count']:>5} {decomposed:>8.1f}s "
                  f"{decomp['fixed_s']:>8.1f}s {decomp['optimizable_s']:>8.1f}s "
                  f"{decomp['bf_apply_s']:>8.2f}s {p_pct:>5.1f}%{tag}")

    # Summary
    print("\n=== Paper-ready summary (baseline vs BloomFaaS S3) ===\n")
    print_summary_table(make_summary_table(data, bf_system="bloomfaas-s3-bloomfaas"))

    print("\n=== Paper-ready summary (baseline vs BloomFaaS NFS) ===\n")
    print_summary_table(make_summary_table(data, bf_system="bloomfaas-nfs-bloomfaas"))
