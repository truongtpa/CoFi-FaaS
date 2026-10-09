"""Per-stage max_size_mb chosen from the measured U curves of the size sweep (history-sweep/, from
benchmarks/run-seek15-max-size-series.py), instead of from the cost model.

The runner runs stages one after another, so the query time is the sum of the stage wall times plus a small gap, and
the cost is the sum of the stage costs. Each stage has its own U curve (wall time vs size; cost falls with size), so
for a weight lambda every stage picks independently the size minimising
    lambda * wall / T0 + (1 - lambda) * cost / C0          (T0, C0: the default 50 MB plan)
lambda = 1 gives min_T, lambda = 0 gives min_C, the knee is the front point closest to (T_min, C_min) after
normalisation (as in plan_opt.py). best_global is the single size for every stage with the lowest measured T.

Assumption to check by running the plans: a stage's time depends mostly on its own size, not on the sizes upstream.

    .venv/bin/python notebooks/plan_sweep.py history-sweep --out notebooks/cost-report/tpch100-v5/opt-sweep
    .venv/bin/python benchmarks/gen-plans.py notebooks/cost-report/tpch100-v5/opt-sweep/plans.json --emit-j2 \\
        --j2-dir ./benchmarks/plans-j2-sweep/
Output plans.json has the format of plan_opt.py (read by gen-plans.py and plan_compare.py), plus stages.csv with the
per-stage curves and choices.
"""
import os
import re
import sys
import csv
import glob
import json
import math
import argparse
import statistics as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cost_model as cm

DEFAULT_MB = 50
TUNABLE = ("scan", "aggregate", "broadcast_join")
LAMBDAS = [i / 20 for i in range(21)]
TOL = 0.05   # keep a stage at 50 MB unless another size is at least 5% better for it (medians of a few runs are noisy)


def load(root, P):
    """{query: {size: [run, ...]}}"""
    out = {}
    for d in sorted(glob.glob(os.path.join(root, "size-*", "q*", "*"))):
        if os.path.exists(os.path.join(d, "cost-stages.jsonl")):
            size = int(re.search(r"size-(\d+)", d).group(1))
            out.setdefault(d.split(os.sep)[-2], {}).setdefault(size, []).append(cm.load(d, P))
    return out


def curves(runs_by_size, P):
    """Per size: median wall and measured cost of every stage, the query T and C, and the overhead outside stages."""
    out = {}
    for size, runs in runs_by_size.items():
        ev = [cm.evaluate(r, cm.calibrate([r], P), P) for r in runs]
        stages = {}
        for name in runs[0]["m"]:
            w = [r["m"][name]["d_wall"] for r in runs if name in r["m"]]
            c = [row["meas_C"] for rows, _ in ev for row in rows if row["stage"] == name]
            stages[name] = (st.median(w), st.median(c), runs[0]["m"][name]["p"])
        T = st.median(t["T_meas"] for _, t in ev)
        C = st.median(t["C_meas"] for _, t in ev)
        out[size] = {"stages": stages, "T": T, "C": C, "runs": len(runs),
                     "gap": T - sum(v[0] for v in stages.values()), "fixed_C": C - sum(v[1] for v in stages.values())}
    return out


def pareto(points):
    pts = sorted(points, key=lambda p: (p["T_est"], p["C_est"]))
    front, best_c = [], math.inf
    for p in pts:
        if p["C_est"] < best_c - 1e-12:
            front.append(p)
            best_c = p["C_est"]
    return front


def plan(q, name, sizes, cur, base):
    """sizes: {stage: MB} for the tunable stages; others keep their 50 MB measurement."""
    T = base["gap"] + sum(cur[sizes.get(n, DEFAULT_MB)]["stages"][n][0] for n in base["stages"])
    C = base["fixed_C"] + sum(cur[sizes.get(n, DEFAULT_MB)]["stages"][n][1] for n in base["stages"])
    return {"query": q, "plan": name, "T_est": T, "C_est": C, "max_fn_mb": None, "bf_off": [],
            "max_size_mb": {n: x for n, x in sizes.items() if x != DEFAULT_MB}, "p": {}}


def optimize(q, runs_by_size, P, a_tol=TOL):
    full = st.median(len(r) for r in runs_by_size.values())
    cur = curves(runs_by_size, P)
    skipped = sorted(s for s in cur if cur[s]["runs"] < full and s != DEFAULT_MB)   # runs failed at that size
    for s in skipped:
        del cur[s]
    base = cur[DEFAULT_MB]
    func = {s["name"]: s["func"] for s in runs_by_size[DEFAULT_MB][0]["steps"]}
    tun = [n for n in base["stages"] if func.get(n) in TUNABLE and all(n in cur[s]["stages"] for s in cur)]
    T0, C0 = base["T"], base["C"]

    cands = []
    for lam in LAMBDAS:
        pick = {}
        for n in tun:
            f = lambda s: lam * cur[s]["stages"][n][0] / T0 + (1 - lam) * cur[s]["stages"][n][1] / C0
            best = min(cur, key=f)
            pick[n] = DEFAULT_MB if f(DEFAULT_MB) <= f(best) * (1 + a_tol) else best
        cands.append(plan(q, f"lambda={lam:.2f}", pick, cur, base))
    front = pareto(cands)
    tmin, cmin = min(p["T_est"] for p in front), min(p["C_est"] for p in front)
    knee = min(front, key=lambda p: math.hypot(p["T_est"] / tmin - 1, p["C_est"] / cmin - 1))
    g = min(cur, key=lambda s: cur[s]["T"])
    plans = [plan(q, "default", {}, cur, base),
             {**plan(q, "best_global", {n: g for n in tun}, cur, base), "T_est": cur[g]["T"], "C_est": cur[g]["C"]},
             {**front[0], "plan": "min_T"}, {**knee, "plan": "knee"}, {**front[-1], "plan": "min_C"}]
    rows = [{"query": q, "stage": n, "func": func.get(n), "tunable": n in tun,
             **{f"wall_{s}": round(cur[s]["stages"][n][0], 3) for s in sorted(cur) if n in cur[s]["stages"]},
             **{f"pick_{p['plan']}": p["max_size_mb"].get(n, DEFAULT_MB) for p in plans[2:]}}
            for n in base["stages"]]
    return plans, rows, skipped, g, cur


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root", nargs="?", default="history-sweep")
    ap.add_argument("--queries", nargs="*")
    ap.add_argument("--tol", type=float, default=TOL, help="min relative gain to move a stage off 50 MB")
    ap.add_argument("--out", default="notebooks/cost-report/tpch100-v5/opt-sweep")
    a = ap.parse_args()
    P = cm.load_params()
    P.update(serial=True, waves=True, disp_median=True, contention=True)
    data = load(a.root, P)
    os.makedirs(a.out, exist_ok=True)
    all_plans, all_rows = [], []
    for q in sorted(a.queries or data, key=lambda q: int(q[1:])):
        if DEFAULT_MB not in data.get(q, {}):
            print(f"{q}: no {DEFAULT_MB} MB runs, skipped")
            continue
        plans, rows, skipped, g, cur = optimize(q, data[q], P, a.tol)
        all_plans += plans
        all_rows += rows
        d = plans[0]
        print(f"\n{q}: sizes {sorted(cur)}" + (f" (skipped, some runs failed: {skipped})" if skipped else ""))
        for p in plans:
            sz = sorted(set(p["max_size_mb"].values()) | ({DEFAULT_MB} if len(p["max_size_mb"]) < len(rows) else set()))
            print(f"  {p['plan']:11} T={p['T_est']:6.2f}s ({p['T_est'] / d['T_est'] - 1:+5.0%})  "
                  f"C={p['C_est'] * 1e3:8.3f}m$ ({p['C_est'] / d['C_est'] - 1:+5.0%})  "
                  f"resized={len(p['max_size_mb']):2}  sizes used={sz}")
    json.dump(all_plans, open(os.path.join(a.out, "plans.json"), "w"), indent=2)
    keys = sorted({k for r in all_rows for k in r}, key=lambda k: (not k[0].isalpha() or "_" in k, k))
    keys = ["query", "stage", "func", "tunable"] + [k for k in keys if k.startswith("wall_")] + \
        [k for k in keys if k.startswith("pick_")]
    with open(os.path.join(a.out, "stages.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(all_rows)
    print(f"\nsaved -> {a.out}/plans.json, stages.csv")


if __name__ == "__main__":
    main()
