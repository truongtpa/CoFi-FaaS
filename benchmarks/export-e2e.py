"""Export measured plan runs (history-plans-<set>/<plan>/<query>/<run>) as one CSV row per run, in the format of
CoFi-FaaS "4. End-to-End Evaluation on BLOOM-FaaS/data/e2e-runs*.csv": experiment, query, plan, run, T_s, C_usd.
T_s = query time (first function start to last function end); C_usd = resources used by the run priced with the
cost model's prices (as plan_compare.py). Optionally also the optimizer's estimates of the plans.

    python3 benchmarks/export-e2e.py history-plans-grid --experiment grid \
        --plans-json notebooks/cost-report/tpch100-v5/opt-grid/plans.json --out e2e-runs-grid.csv
Copy the CSV(s) into CoFi-FaaS/4. End-to-End Evaluation on BLOOM-FaaS/data/.
"""
import os
import sys
import csv
import glob
import json
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "notebooks"))
import cost_model as cm
from plan_compare import measured


def incomplete(d):
    """Why the run in d did not finish (None when it did): a failed run still logs the stages done before the failure
    (and the failing stage when some of its tasks succeeded), so its T and C would be those of a partial query."""
    stages = [json.loads(l) for l in open(os.path.join(d, "cost-stages.jsonl")) if l.strip()]
    failed = [s["stage"] for s in stages if s.get("failed")]
    if failed:
        return f"failed tasks in {failed[0]}"
    dag = os.path.join(d, "cost-dag.json")
    if os.path.exists(dag):
        done = {s["stage"] for s in stages}
        missing = [s["name"] for s in json.load(open(dag))["steps"] if s["func"] != "summary_clean" and s["name"] not in done]
        if missing:
            return f"no log for {missing[0]}" + (f" (+{len(missing) - 1} more)" if len(missing) > 1 else "")
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root", help="history-plans-<set> with <plan>/<query>/<run> folders")
    ap.add_argument("--experiment", required=True, help="name written in the experiment column, e.g. grid")
    ap.add_argument("--out", default=None, help="default: e2e-runs-<experiment>.csv")
    ap.add_argument("--plans-json", help="optimizer plans.json: also write <out stem>-plans.csv with the estimates")
    a = ap.parse_args()
    P = cm.load_params()
    P.update(serial=True, waves=True, disp_median=True, contention=True)
    out = a.out or f"e2e-runs-{a.experiment}.csv"
    rows, skipped = [], []
    for d in sorted(glob.glob(os.path.join(a.root, "*", "q*", "*"))):
        if not os.path.isdir(d):
            continue
        plan, q = d.split(os.sep)[-3], d.split(os.sep)[-2]
        why = "no cost-stages.jsonl" if not os.path.exists(os.path.join(d, "cost-stages.jsonl")) else incomplete(d)
        if why:
            skipped.append((q, plan, os.path.basename(d), why))
            continue
        T, C = measured(d, P)
        rows.append({"experiment": a.experiment, "query": q, "plan": plan, "run": os.path.basename(d), "T_s": T, "C_usd": C})
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["experiment", "query", "plan", "run", "T_s", "C_usd"])
        w.writeheader()
        w.writerows(rows)
    counts = {}
    for r in rows:
        counts[r["plan"]] = counts.get(r["plan"], 0) + 1
    print(f"{len(rows)} runs -> {out}  {counts}")
    if skipped:
        print(f"{len(skipped)} failed / incomplete runs left out:")
        for q, plan, run, why in skipped:
            print(f"  {q:4} {plan:10} {run}  {why}")
    if a.plans_json:
        est = [{"query": p["query"], "plan": p["plan"], "T_est_s": p["T_est"], "C_est_usd": p["C_est"],
                "max_size_mb": json.dumps(p["max_size_mb"])} for p in json.load(open(a.plans_json))]
        pout = os.path.splitext(out)[0] + "-plans.csv"
        with open(pout, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(est[0]))
            w.writeheader()
            w.writerows(est)
        print(f"{len(est)} plan estimates -> {pout}")


if __name__ == "__main__":
    main()
