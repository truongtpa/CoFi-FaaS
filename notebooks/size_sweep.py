"""Analysis of the max_size_mb sweep (benchmarks/run-seek15-max-size-series.py -> history-sweep/size-<MB>/).

What the cost model needs from it, per function type (scan / aggregate / join):
  * the fixed time per function (t at ~0 bytes), the time per MB, and how both change with concurrency k;
  * the aggregate data rate of the cluster (stage bytes / wall, per size);
  * the end-to-end effect: query time T and cost C per query and size, with the 50 MB run repeated first and last
    of the sweep as a drift check.

    .venv/bin/python notebooks/size_sweep.py history-sweep --out notebooks/cost-report/tpch100-v5/sweep
Outputs (in --out): sweep-query.csv (T, C per query and size), sweep-tasks.csv (one row per function),
sweep-fit.csv (per function type: fixed s/task, s/MB, k exponent), sweep-T.{pdf,png}.
"""
import os
import re
import sys
import csv
import glob
import argparse
import statistics as st

import numpy as np
try:   # the chart is optional: the server running the sweep may not have matplotlib
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    plt = None

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cost_model as cm

MB = 2 ** 20
KIND = {"/scan-bloomfaas": "scan", "/aggregate": "aggregate", "/join": "join"}


def load(root, P):
    """{size: {query: [run, ...]}} with the measured overlap k on every task."""
    out = {}
    for d in sorted(glob.glob(os.path.join(root, "size-*", "q*", "*"))):
        if not os.path.exists(os.path.join(d, "cost-stages.jsonl")):
            continue
        size = int(re.search(r"size-(\d+)", d).group(1))
        q = d.split(os.sep)[-2]
        run = cm.load(d, P)   # cm.load computes t["k"] over the whole run
        out.setdefault(size, {}).setdefault(q, []).append(run)
    return out


def task_rows(data):
    rows = []
    for size, qs in data.items():
        for q, runs in qs.items():
            for run in runs:
                for name, m in run["m"].items():
                    kind = KIND.get(m["endpoint"])
                    if not kind:
                        continue
                    for t in m["tasks"]:
                        b_in = sum(cm.io(t, "in", c, "bytes") for c in cm.CLASSES)
                        rows.append({"size": size, "query": q, "stage": name, "kind": kind, "p": m["p"], "k": t["k"],
                                     "MB_in": b_in / MB, "t_read": t["t_read"], "t_comp": t["t_comp"],
                                     "t_write": t["t_write"], "t_total": t["t_total"]})
    return rows


def fit(rows):
    """t_total = a + b * MB_in * k^g, per function type: a = fixed s/task, b = s/MB at k = 1, g = contention exponent."""
    out = []
    for kind in sorted({r["kind"] for r in rows}):
        R = [r for r in rows if r["kind"] == kind and r["t_total"] > 0]
        if len(R) < 20:
            continue
        mb, k, t = (np.array([r[c] for r in R], float) for c in ("MB_in", "k", "t_total"))
        best = None
        for g in np.linspace(0, 1.2, 25):   # grid over the exponent, least squares for a and b
            x = mb * k ** g
            X = np.c_[np.ones_like(mb), x]
            (a, b), *_ = np.linalg.lstsq(X, t, rcond=None)
            if a < 0:   # no fixed part: refit through the origin
                a, b = 0.0, float(x @ t / (x @ x)) if x @ x > 0 else 0.0
            if b < 0:
                continue
            err = float(np.median(np.abs(X @ [a, b] - t) / t))
            if best is None or err < best[-1]:
                best = (g, a, b, err)
        small = t[mb < np.percentile(mb, 10)] if len(mb) >= 10 else t
        if best:
            g, a, b, err = best
            out.append({"kind": kind, "tasks": len(R), "fixed_s": a, "s_per_MB_k1": b, "k_exp": g,
                        "median_rel_err_%": 100 * err, "t_at_smallest_10%_s": float(np.median(small))})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root", nargs="?", default="history-sweep")
    ap.add_argument("--out", default="notebooks/cost-report/tpch100-v5/sweep")
    a = ap.parse_args()
    P = cm.load_params()
    P.update(serial=True, waves=True, disp_median=True, contention=True)
    data = load(a.root, P)
    if not data:
        raise SystemExit(f"no runs under {a.root}/size-*/q*/")
    os.makedirs(a.out, exist_ok=True)

    # end to end: T and C per query and size (C priced from the measured resources, as plan_compare.py does)
    qrows = []
    for size in sorted(data):
        for q, runs in sorted(data[size].items()):
            tots = [cm.evaluate(r, cm.calibrate([r], P), P)[1] for r in runs]
            stg = [m for r in runs for m in r["m"].values() if m["d_wall"] > 0]
            agg = [(m["D_in"] + m["D_out"]) / m["d_wall"] / MB for m in stg if m["D_in"] + m["D_out"] > 256 * MB]
            qrows.append({"query": q, "size_MB": size, "runs": len(runs),
                          "T_s": st.median(x["T_meas"] for x in tots), "C_m$": 1e3 * st.median(x["C_meas"] for x in tots),
                          "functions": st.median(sum(m["p"] for m in r["m"].values()) for r in runs),
                          "stage_MBps_max": max(agg) if agg else None})
    rows = task_rows(data)
    fits = fit(rows)
    for name, R in (("sweep-query.csv", qrows), ("sweep-tasks.csv", rows), ("sweep-fit.csv", fits)):
        if R:
            with open(os.path.join(a.out, name), "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(R[0].keys()))
                w.writeheader()
                w.writerows(R)

    print(f"{'query':5} {'MB':>5} {'runs':>4} {'T s':>7} {'C m$':>8} {'fns':>6} {'max MB/s':>9}")
    for r in qrows:
        print(f"{r['query']:5} {r['size_MB']:5} {r['runs']:4} {r['T_s']:7.2f} {r['C_m$']:8.3f} {r['functions']:6.0f} "
              f"{r['stage_MBps_max'] or 0:9.0f}")
    print("\nper function type: t_total = fixed + s/MB * MB_in * k^exp")
    for f in fits:
        print(f"  {f['kind']:9} fixed={f['fixed_s']:.3f}s  {1 / f['s_per_MB_k1'] if f['s_per_MB_k1'] else float('inf'):7.1f} MB/s at k=1"
              f"  k^{f['k_exp']:.2f}  median err {f['median_rel_err_%']:.0f}%  (smallest 10% of tasks: {f['t_at_smallest_10%_s']:.2f}s)")

    if plt is None:
        print(f"\nsaved -> {a.out}/sweep-query.csv, sweep-tasks.csv, sweep-fit.csv (no chart: matplotlib missing)")
        return
    fig, axs = plt.subplots(1, 2, figsize=(8, 3))
    for q in sorted({r["query"] for r in qrows}, key=lambda q: int(q[1:])):
        R = sorted((r for r in qrows if r["query"] == q), key=lambda r: r["size_MB"])
        axs[0].plot([r["size_MB"] for r in R], [r["T_s"] for r in R], marker="o", label=q.upper())
        axs[1].plot([r["size_MB"] for r in R], [r["C_m$"] for r in R], marker="o", label=q.upper())
    for ax, yl in zip(axs, ("Query time T (s)", "Cost C (m$)")):
        ax.set_xscale("log")
        ax.set_xlabel("max_size_mb")
        ax.set_ylabel(yl)
        ax.grid(True, alpha=0.3)
    axs[0].legend(frameon=False)
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(a.out, f"sweep-T.{ext}"), dpi=200, bbox_inches="tight")
    print(f"\nsaved -> {a.out}/sweep-query.csv, sweep-tasks.csv, sweep-fit.csv, sweep-T.{{pdf,png}}")


if __name__ == "__main__":
    main()
