"""Run on the server after run-sweep-grid.py --mode query: analyse the sweep and write the plan templates there,
so only small result files need to be copied back.

  1. notebooks/size_sweep.py   -> <report>/grid/         T and C per query and size, per-type fit
  2. notebooks/plan_sweep.py   -> <report>/opt-grid/     per-stage size choice, plans.json, stages.csv
  3. benchmarks/gen-plans.py   -> benchmarks/plans-j2-grid/<plan>/<query>.yml.j2, ready for run-seek15.py
and a Telegram message with the knee / min_T / min_C of every query. Also called by run-sweep-grid.py --then-analyze.

    python3 benchmarks/after-sweep-grid.py
    python3 benchmarks/after-sweep-grid.py --root history-grid/query/mp70 --report notebooks/cost-report/tpch100-v5
Copy back afterwards (small): <report>/grid/ and <report>/opt-grid/.
"""
import os
import re
import sys
import json
import argparse
import subprocess

sys.path.insert(0, os.getcwd())
from operations.Nofityme import sendMessage

PLANS = ["default", "best_global", "knee", "min_T", "min_C"]


def notify(text):
    print(f"[notify] {text}")
    try:
        sendMessage(text)
    except Exception as e:
        print(f"[notify] failed: {e}")


def step(name, cmd):
    print(f"\n=== {name}: {' '.join(cmd)}")
    r = subprocess.run(cmd, capture_output=True, text=True)
    print(r.stdout[-6000:])
    if r.returncode:
        print(r.stderr[-3000:])
        notify(f"[after-sweep-grid] LỖI ở bước {name}:\n{(r.stderr or r.stdout)[-800:]}")
        sys.exit(r.returncode)
    return r.stdout


def summary(plans_json):
    """One line per query: knee / min_T / min_C against default (estimated from the measured stage curves)."""
    plans = json.load(open(plans_json))
    by = {}
    for p in plans:
        by.setdefault(p["query"], {})[p["plan"]] = p
    lines = []
    for q in sorted(by, key=lambda q: int(re.sub(r"\D", "", q) or 0)):
        d = by[q].get("default")
        if not d:
            continue
        cells = []
        for name in ("knee", "min_T", "min_C"):
            p = by[q].get(name)
            if p:
                cells.append(f"{name} T{p['T_est'] / d['T_est'] - 1:+.0%} C{p['C_est'] / d['C_est'] - 1:+.0%}")
        lines.append(f"{q.upper()}: " + ", ".join(cells))
    return lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="history-grid/query/mp70")
    ap.add_argument("--report", default="notebooks/cost-report/tpch100-v5")
    ap.add_argument("--j2-dir", default="./benchmarks/plans-j2-grid/")
    a = ap.parse_args()
    py = sys.executable
    grid, opt = os.path.join(a.report, "grid"), os.path.join(a.report, "opt-grid")
    step("size_sweep", [py, "notebooks/size_sweep.py", a.root, "--out", grid])
    step("plan_sweep", [py, "notebooks/plan_sweep.py", a.root, "--out", opt])
    step("gen-plans", [py, "benchmarks/gen-plans.py", os.path.join(opt, "plans.json"), "--emit-j2",
                       "--j2-dir", a.j2_dir, "--plans", *PLANS])
    lines = summary(os.path.join(opt, "plans.json"))
    notify("[after-sweep-grid] Xong. Ước lượng so với default:\n" + "\n".join(lines) +
           f"\n\nTemplates: {a.j2_dir}<plan>/q*.yml.j2\nKết quả: {grid}/, {opt}/")


if __name__ == "__main__":
    main()
