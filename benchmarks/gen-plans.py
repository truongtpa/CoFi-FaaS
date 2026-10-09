"""Generate (and optionally run) the plans chosen by notebooks/plan_opt.py (Section 3: performance of the optimized plans).

Each plan in plans.json is the BF-on S3 pipeline with
  * bf_off:       merge_bf steps removed, their builders stop writing the BF, their appliers stop filtering,
  * max_size_mb:  per-step partition size, which sets the number of functions p_v.
History goes to history-plans/<plan>/<query>/..., so cost_model_batch.py can read each plan like a normal history.

    .venv/bin/python benchmarks/gen-plans.py notebooks/cost-report/tpch100-v5/opt/plans.json --plans default knee
    .venv/bin/python benchmarks/gen-plans.py ... --dry-run      # only write the plan pipelines and check them
    .venv/bin/python benchmarks/gen-plans.py ... --emit-j2      # only write one template per plan and query, in the
        # tpch-parms format ($parm*, $bucket, $base_path kept), to be run by run-seek15.py like any other query
"""
import os
import sys
import json
import time
import argparse
import importlib.util
from datetime import datetime

import yaml

sys.path.insert(0, os.getcwd())
from operations.runner.FlowParser import FlowParser, load_and_render_yaml
from operations.runner.Runner import Runner
from operations.runner.J2FileEditor import J2FileEditor

spec = importlib.util.spec_from_file_location("rs", os.path.join(os.path.dirname(__file__), "run-seek15.py"))
rs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rs)

HISTORY_ROOT = "./history-plans/"
PLAN_DIR = "./benchmarks/plans/"
J2_DIR = "./benchmarks/plans-j2/"         # <plan>/<query>.yml.j2
CFG = rs.CONFIGS["bloom_faas_v2"]          # BF-capable S3 system; the plan decides which BFs are used


def as_list(x):
    if x is None or x == {} or (isinstance(x, str) and x.strip().lower() in ("", "none")):   # q14 writes `none`
        return []
    return [x] if isinstance(x, str) else list(x)


def apply_plan(raw, plan):
    """Edit a rendered pipeline dict in place."""
    steps = raw["steps"]
    by_name = {s["name"]: s for s in steps}
    for m in plan["bf_off"]:
        merge = by_name.get(m)
        if not merge:
            raise ValueError(f"{plan['query']}: no step {m}")
        for b in as_list(merge.get("from_previous_task")):
            for k in ("bf_column", "bf_path", "est_elements"):
                by_name[b].get("params", {}).pop(k, None)
        for s in steps:
            if m in as_list(s.get("from_previous_task")):
                rest = [x for x in as_list(s["from_previous_task"]) if x != m]
                if rest:
                    s["from_previous_task"] = rest if len(rest) > 1 else rest[0]
                else:
                    s.pop("from_previous_task")
                for k in ("filter_by_bf", "bf_column"):
                    s.get("params", {}).pop(k, None)
        steps.remove(merge)
    for name, mb in plan["max_size_mb"].items():
        by_name[name]["max_size_mb"] = mb
    for name, mp in plan.get("max_parallel", {}).items():   # per-stage functions at once (Runner override)
        by_name[name]["max_parallel"] = mp
    return raw


def render_plan(plan, seed, all_params):
    q = plan["query"]
    rs.TMP_YAML = os.path.join(PLAN_DIR, f"_{q}.yml.j2")
    rs.render_tmp_yaml(f"./benchmarks/tpch-parms/{q}.yml.j2", CFG, rs.get_tpch_parms(all_params, q, seed))
    raw = apply_plan(load_and_render_yaml(rs.TMP_YAML), plan)
    os.remove(rs.TMP_YAML)
    path = os.path.join(PLAN_DIR, f"{q}-{plan['plan']}-{seed}.yml")
    with open(path, "w") as f:
        yaml.safe_dump(raw, f, sort_keys=False, allow_unicode=True)
    return path


def emit_j2(plan, all_params, j2_dir=J2_DIR):
    """Template of the plan with the run-time placeholders kept. Only $enable_bf is fixed (to 1): it is the one
    variable the Jinja conditions read, and the plan is defined on the BF-on pipeline."""
    q = plan["query"]
    os.makedirs(j2_dir, exist_ok=True)
    tmp = os.path.join(j2_dir, f"_{q}.yml.j2")
    J2FileEditor(f"./benchmarks/tpch-parms/{q}.yml.j2").replace("$enable_bf", 1).save(tmp)
    raw = apply_plan(load_and_render_yaml(tmp), plan)
    os.remove(tmp)
    path = os.path.join(j2_dir, plan["plan"], f"{q}.yml.j2")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump(raw, f, sort_keys=False, allow_unicode=True)
    # check: render it the way run-seek15.py does (seed1) and verify the plan survived
    rs.TMP_YAML = tmp
    import contextlib, io
    with contextlib.redirect_stdout(io.StringIO()):
        rs.render_tmp_yaml(path, CFG, rs.get_tpch_parms(all_params, q, "seed1"))
    n = check(tmp, plan)
    left = [l for l in open(tmp) if "$parm" in l or "$bucket" in l or "$base_path" in l]
    os.remove(tmp)
    assert not left, f"{path}: placeholders left after rendering: {left[0].strip()}"
    return path, n


def emit_no_bf(q, all_params, j2_dir=J2_DIR):
    """<j2_dir>/no_bf/<q>.yml.j2: the same pipeline without Bloom filters ($enable_bf = 0), run in the same session
    as the plans; the runner names these runs starling-base (scans with 1 worker, as in the accuracy runs)."""
    os.makedirs(j2_dir, exist_ok=True)
    tmp = os.path.join(j2_dir, f"_{q}.yml.j2")
    J2FileEditor(f"./benchmarks/tpch-parms/{q}.yml.j2").replace("$enable_bf", 0).save(tmp)
    raw = load_and_render_yaml(tmp)
    os.remove(tmp)
    path = os.path.join(j2_dir, "no_bf", f"{q}.yml.j2")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump(raw, f, sort_keys=False, allow_unicode=True)
    rs.TMP_YAML = tmp
    import contextlib, io
    with contextlib.redirect_stdout(io.StringIO()):
        rs.render_tmp_yaml(path, CFG, rs.get_tpch_parms(all_params, q, "seed1"))
    steps = FlowParser(tmp).get_steps()
    left = [l for l in open(tmp) if "$parm" in l or "$bucket" in l or "$base_path" in l]
    os.remove(tmp)
    assert not left, f"{path}: placeholders left after rendering: {left[0].strip()}"
    assert not any(s["func"] == "merge_bf" for s in steps), f"{path}: a merge_bf step is left"
    return path, len(steps)


def check(path, plan):
    steps = {s["name"]: s for s in FlowParser(path).get_steps()}
    assert not any(m in steps for m in plan["bf_off"]), "a removed merge_bf step is still there"
    for n, mb in plan["max_size_mb"].items():
        assert steps[n]["max_size_mb"] == mb, f"max_size_mb of {n} not applied"
    for n, mp in plan.get("max_parallel", {}).items():
        assert steps[n].get("max_parallel") == mp, f"max_parallel of {n} not applied"
    for s in steps.values():
        for d in as_list(s["from_previous_tasks"]):
            assert d in steps, f"{s['name']} depends on missing step {d}"
    return len(steps)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("plans_json")
    ap.add_argument("--plans", nargs="*", default=["default", "knee", "min_T", "min_C"])
    ap.add_argument("--queries", nargs="*")
    ap.add_argument("--seeds", nargs="*", default=rs.SEEDS)
    ap.add_argument("--runs", type=int, default=rs.TOTAL_RUNS)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--emit-j2", action="store_true", help=f"only write {J2_DIR}<plan>/<query>.yml.j2 templates")
    ap.add_argument("--with-no-bf", action="store_true",
                    help="--emit-j2: also write <j2-dir>/no_bf/<query>.yml.j2 (pipeline without Bloom filters)")
    ap.add_argument("--j2-dir", default=J2_DIR,
                    help="where --emit-j2 writes; benchmarks/plans-j2-<x>/ is run into history-plans-<x>/ by run-seek15.py")
    a = ap.parse_args()

    os.makedirs(PLAN_DIR, exist_ok=True)
    all_params = rs.load_params()
    plans = [p for p in json.load(open(a.plans_json))
             if p["plan"] in a.plans and (not a.queries or p["query"] in a.queries)]
    if a.emit_j2:
        for plan in plans:
            path, n = emit_j2(plan, all_params, a.j2_dir)
            print(f"{plan['query']:4} {plan['plan']:8} {n} steps, bf_off={len(plan['bf_off'])}, "
                  f"resized={len(plan['max_size_mb'])} -> {path}")
        if a.with_no_bf:
            for q in sorted({p["query"] for p in plans}, key=lambda q: int(q[1:])):
                path, n = emit_no_bf(q, all_params, a.j2_dir)
                print(f"{q:4} no_bf    {n} steps -> {path}")
        return
    for plan in plans:
        hist = os.path.join(HISTORY_ROOT, plan["plan"])
        os.makedirs(hist, exist_ok=True)
        for seed in a.seeds:
            path = render_plan(plan, seed, all_params)
            n = check(path, plan)
            print(f"{plan['query']:4} {plan['plan']:8} {seed}: {n} steps, bf_off={len(plan['bf_off'])}, "
                  f"resized={len(plan['max_size_mb'])} -> {path}")
            if a.dry_run:
                continue
            for i in range(1, a.runs + 1):
                start = datetime.now().isoformat()
                result = Runner(path).execute(history_logs=hist, knative_func=rs.KNATIVE_FUNC, k8s=rs.K8S_ENDPOINT,
                                              max_parallel=rs.TASK_PARALLELISM, func_ver=CFG["function_version"])
                end = datetime.now().isoformat()
                failed = (not result) or isinstance(result, dict)
                print(f"    run {i}: {'FAILED ' + str(result) if failed else 'ok'} "
                      f"{(datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds():.1f}s")
                if failed:
                    break
                time.sleep(rs.RUN_DELAY_S)


if __name__ == "__main__":
    main()
