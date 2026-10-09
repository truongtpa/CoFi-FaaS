"""Effect of the number of functions reading at once on S3: analysis of run-sweep-grid.py --mode s3
(history-grid/s3/mp<P>/size-<MB>/s3probe/...: one lineitem scan, nothing written).

Per (size, max_parallel): number of functions p, functions really reading at once, stage wall time, per-function
read throughput and aggregate throughput (all bytes read / wall). Then a fit of
    per-function MB/s = min(B1, B_agg / k)
B1 = what one function gets alone, B_agg = what the whole cluster delivers; k* = B_agg / B1 is the number of
concurrent functions beyond which adding functions no longer speeds the scan up.

    python3 notebooks/s3_sweep.py history-grid/s3 --out notebooks/cost-report/tpch100-v5/s3grid
Outputs: s3-grid.csv, s3-fit.txt, s3-grid.{pdf,png} (if matplotlib is installed).

NFS (run-sweep-grid.py --mode nfs, stage step2-nfs-read): the same with the NFS bytes; the function reads NFS files
through DuckDB, which the probe does not time apart (t_read = 0), so the read time is t_comp (the filter is always
false and the output empty: almost all of t_comp is the read).
    python3 notebooks/s3_sweep.py history-grid/nfs --storage nfs --out notebooks/cost-report/tpch100-v5/nfsgrid
Outputs: nfs-grid.csv, nfs-fit.txt, nfs-grid.{pdf,png}.
"""
import os
import re
import sys
import csv
import glob
import argparse
import statistics as st
from datetime import datetime

import numpy as np
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    plt = None

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cost_model as cm

MB = 2 ** 20
STAGE = {"s3": "step1-lineitem", "nfs": "step2-nfs-read"}


def t_rd(t):
    """Read time of a task: the timed S3 reads, else (NFS, read inside DuckDB) the untimed part of the task."""
    return t["t_read"] if t["t_read"] > 0 else t["t_comp"]


def ts(x):
    return datetime.fromisoformat(x).timestamp()


def readers(tasks):
    """Median over tasks of the time-averaged number of tasks reading during that task's read
    (read window = start + dispatch .. + t_read; itself included)."""
    iv = [(ts(t["start"]) + t.get("disp", 0.0), ts(t["start"]) + t.get("disp", 0.0) + t_rd(t)) for t in tasks]
    avg = [sum(max(0.0, min(b, b2) - max(a, a2)) for a2, b2 in iv) / (b - a) for a, b in iv if b > a]
    return st.median(avg) if avg else 1.0


def load(root, stage, P, storage="s3"):
    rows = []
    for d in sorted(glob.glob(os.path.join(root, "mp*", "size-*", "*", "*"))):
        if not os.path.exists(os.path.join(d, "cost-stages.jsonl")):
            continue
        mp = int(re.search(r"mp(\d+)", d).group(1))
        size = int(re.search(r"size-(\d+)", d).group(1))
        run = cm.load(d, P)
        m = run["m"].get(stage)
        if not m:
            continue
        tk = m["tasks"]
        b = [cm.io(t, "in", storage, "bytes") for t in tk]
        rows.append({"mp": mp, "size": size, "p": m["p"], "wall": m["d_wall"], "MB_total": sum(b) / MB,
                     "MB_fn": st.median(b) / MB, "t_read": st.median(t_rd(t) for t in tk),
                     "t_read_p95": sorted(t_rd(t) for t in tk)[int(0.95 * (len(tk) - 1))],
                     "fn_MBps": st.median(x / MB / t_rd(t) for x, t in zip(b, tk) if t_rd(t) > 0),
                     "readers": readers(tk), "agg_MBps": sum(b) / MB / m["d_wall"] if m["d_wall"] else 0.0})
    return rows


def group(rows):
    out = []
    for key in sorted({(r["mp"], r["size"]) for r in rows}):
        R = [r for r in rows if (r["mp"], r["size"]) == key]
        out.append({"mp": key[0], "size": key[1], "runs": len(R), "p": R[0]["p"],
                    **{c: st.median(r[c] for r in R) for c in ("MB_fn", "readers", "t_read", "t_read_p95",
                                                                "fn_MBps", "wall", "agg_MBps")}})
    return out


def fit(rows):
    """per-function MB/s = min(B1, B_agg / k): grid over B_agg, B1 from the low-k points, least relative error."""
    k = np.array([r["readers"] for r in rows], float)
    y = np.array([r["fn_MBps"] for r in rows], float)
    best = None
    for b_agg in np.linspace(max(y * k) * 0.3, max(y * k) * 1.2, 200):
        for b1 in np.linspace(y.min(), y.max() * 1.2, 120):
            pred = np.minimum(b1, b_agg / k)
            err = float(np.median(np.abs(pred - y) / y))
            if best is None or err < best[0]:
                best = (err, b1, b_agg)
    return {"B1_MBps": best[1], "B_agg_MBps": best[2], "k_star": best[2] / best[1], "median_rel_err": best[0]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root", nargs="?")
    ap.add_argument("--storage", choices=list(STAGE), default="s3")
    ap.add_argument("--stage", help="measured stage (default: step1-lineitem for s3, step2-nfs-read for nfs)")
    ap.add_argument("--out")
    a = ap.parse_args()
    a.root = a.root or f"history-grid/{a.storage}"
    a.stage = a.stage or STAGE[a.storage]
    a.out = a.out or f"notebooks/cost-report/tpch100-v5/{a.storage}grid"
    S = a.storage
    P = cm.load_params()
    rows = load(a.root, a.stage, P, S)
    if not rows:
        raise SystemExit(f"no {a.stage} runs under {a.root}/mp*/size-*/")
    os.makedirs(a.out, exist_ok=True)
    g = group(rows)
    with open(os.path.join(a.out, f"{S}-grid.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(g[0].keys()))
        w.writeheader()
        w.writerows(g)

    print(f"{'mp':>4} {'MB':>4} {'fns':>4} {'MB/fn':>6} {'readers':>7} {'t_read':>6} {'p95':>6} {'fn MB/s':>8} "
          f"{'wall s':>7} {'agg MB/s':>9}")
    for r in g:
        print(f"{r['mp']:4} {r['size']:4} {r['p']:4} {r['MB_fn']:6.1f} {r['readers']:7.0f} {r['t_read']:6.2f} "
              f"{r['t_read_p95']:6.2f} {r['fn_MBps']:8.1f} {r['wall']:7.2f} {r['agg_MBps']:9.0f}")
    fb = min(g, key=lambda r: r["wall"])
    F = fit(g)
    text = (f"per-function MB/s = min(B1, B_agg / k):  B1 = {F['B1_MBps']:.0f} MB/s (one function alone), "
            f"B_agg = {F['B_agg_MBps']:.0f} MB/s (cluster), k* = {F['k_star']:.0f} concurrent readers "
            f"(median error {100 * F['median_rel_err']:.0f}%)\n"
            f"[{S}] fastest scan: max_parallel {fb['mp']}, {fb['size']} MB ({fb['p']} functions): {fb['wall']:.2f} s, "
            f"{fb['agg_MBps']:.0f} MB/s")
    print("\n" + text)
    open(os.path.join(a.out, f"{S}-fit.txt"), "w").write(text + "\n")

    if plt:
        fig, axs = plt.subplots(1, 3, figsize=(12, 3.2))
        for mp in sorted({r["mp"] for r in g}):
            R = sorted((r for r in g if r["mp"] == mp), key=lambda r: r["size"])
            axs[0].plot([r["size"] for r in R], [r["wall"] for r in R], marker="o", ms=3, label=f"mp {mp}")
            axs[1].plot([r["size"] for r in R], [r["agg_MBps"] for r in R], marker="o", ms=3, label=f"mp {mp}")
        k = np.array(sorted({r["readers"] for r in g}))
        axs[2].scatter([r["readers"] for r in g], [r["fn_MBps"] for r in g], s=10, c="#2a78d6")
        axs[2].plot(k, np.minimum(F["B1_MBps"], F["B_agg_MBps"] / k), color="#eb6834", label="min(B1, B_agg/k)")
        for ax, (xl, yl) in zip(axs, [("max_size_mb", "Scan wall time (s)"), ("max_size_mb", "Aggregate MB/s"),
                                       ("Functions reading at once", "Per-function MB/s")]):
            ax.set_xlabel(xl)
            ax.set_ylabel(yl)
            ax.grid(True, alpha=0.3)
            ax.legend(frameon=False, fontsize=7)
        fig.tight_layout()
        for ext in ("pdf", "png"):
            fig.savefig(os.path.join(a.out, f"{S}-grid.{ext}"), dpi=200, bbox_inches="tight")
    print(f"\nsaved -> {a.out}/{S}-grid.csv, {S}-fit.txt" + (f", {S}-grid.{{pdf,png}}" if plt else ""))


if __name__ == "__main__":
    main()
