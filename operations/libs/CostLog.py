import json
import threading

_lock = threading.Lock()


def _deps(value):
    if not value or (isinstance(value, str) and value.strip().lower() in ("none", "null")):
        return []
    return [value] if isinstance(value, str) else list(value)


def dag(folder, steps, variables, max_parallel):
    try:
        _dag(folder, steps, variables, max_parallel)
    except Exception as e:
        print(f"[CostLog] dag: {e!r}")


def stage(folder, name, result):
    try:
        _stage(folder, name, result)
    except Exception as e:
        print(f"[CostLog] stage {name}: {e!r}")


def _dag(folder, steps, variables, max_parallel):
    keys = ("bf_column", "filter_by_bf", "est_elements", "error_rate", "join_type", "num_buckets")
    data = {
        "enable_bf": int(variables.get("enable_bf") or 0),
        "enable_nfs_shuffle": int(variables.get("enable_nfs_shuffle") or 0),
        "max_parallel": max_parallel,
        "steps": [{
            "name": s["name"],
            "func": s["func"],
            "deps": _deps(s.get("from_previous_tasks")),
            "params": {k: (s.get("params") or {}).get(k) for k in keys if (s.get("params") or {}).get(k) is not None},
            **({"max_parallel": s["max_parallel"]} if s.get("max_parallel") else {}),   # per-stage override
            **({"max_size_mb": s["max_size_mb"]} if s.get("max_size_mb") else {}),
        } for s in steps],
    }
    with open(f"{folder}/cost-dag.json", "w") as f:
        json.dump(data, f, indent=2, default=str)


def _stage(folder, name, result):
    tasks = []
    for t in result.get("tasks", []):
        p = (t.get("response") or {}).get("cost_probe") if isinstance(t.get("response"), dict) else None
        if t.get("status") != "success" or not p:
            continue
        tasks.append({
            "lat": t.get("execution_time_s"), "retries": t.get("retries"),
            "start": t.get("start_time"), "end": t.get("end_time"),
            "disp": max((t.get("execution_time_s") or 0) - p["t_total"], 0.0),
            **{k: p.get(k) for k in ("t0", "t1", "t_total", "t_read", "t_comp", "t_write", "cpu_s", "cold", "uptime_s",
                                     "host", "cpu", "mem", "endpoint", "in", "out", "rows_in", "rows_out",
                                     "rows_build", "rows_probe", "bf", "bf_read", "bf_write", "probe_error")
               if p.get(k) is not None},
        })
    if not tasks:
        return
    row = {"stage": name, "p": len(tasks), "wall_s": result.get("pipeline_execution_time_s"),
           "failed": result.get("failed_task", 0), "tasks": tasks}
    with _lock, open(f"{folder}/cost-stages.jsonl", "a") as f:
        f.write(json.dumps(row, default=str) + "\n")
