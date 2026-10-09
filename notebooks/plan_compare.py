"""Section 3: measured performance of the optimized plans (history-plans/ from benchmarks/gen-plans.py).

For every query and plan: median measured T and C over all runs, the optimizer's estimate (plans.json), the change
against the default plan run in the same session, and optionally against the no-BF baseline (history/, starling-base).

    .venv/bin/python notebooks/plan_compare.py history-plans --plans-json notebooks/cost-report/tpch100-v5/opt/plans.json \
        --baseline history --out notebooks/cost-report/tpch100-v5/opt/measured
"""
import os
import sys
import csv
import glob
import json
import argparse
import statistics
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cost_model as cm

ORDER = ["baseline", "default", "best_global", "knee", "min_T", "min_C"]
COLORS = {"baseline": "#8a8983", "default": "#eb6834", "best_global": "#c98a00", "knee": "#2a78d6", "min_T": "#1baf7a", "min_C": "#4a3aa7"}


def measured(folder, P):
    """Measured T and C of one run (C from the measured per-stage resources, priced like the model)."""
    run = cm.load(folder, P)
    K = cm.calibrate([run], P)
    tot = cm.evaluate(run, K, P)[1]
    return tot["T_meas"], tot["C_meas"]


def collect(root, P, tag="*"):
    out = {}
    for d in glob.glob(os.path.join(root, "q*", f"*--{tag}*")):
        if os.path.exists(os.path.join(d, "cost-stages.jsonl")):
            out.setdefault(os.path.basename(os.path.dirname(d)).lower(), []).append(measured(d, P))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("plans_root", nargs="?", default="history-plans")
    ap.add_argument("--plans-json", default="notebooks/cost-report/tpch100-v5/opt/plans.json")
    ap.add_argument("--baseline", help="history dir with starling-base runs (no-BF baseline)")
    ap.add_argument("--out", default="notebooks/cost-report/tpch100-v5/opt/measured")
    a = ap.parse_args()
    P = cm.load_params()
    P.update(serial=True, waves=True, disp_median=True, contention=True)
    est = {(p["query"], p["plan"]): p for p in json.load(open(a.plans_json))} if a.plans_json and os.path.exists(a.plans_json) else {}

    meas = {}
    for pdir in sorted(glob.glob(os.path.join(a.plans_root, "*"))):
        for q, xs in collect(pdir, P).items():
            meas[(q, os.path.basename(pdir))] = xs
    if a.baseline:
        have = {q for q, _ in meas}
        for q, xs in collect(a.baseline, P, "starling-base").items():
            if q in have:
                meas[(q, "baseline")] = xs

    order = ORDER + sorted({p for _, p in meas} - set(ORDER))   # also scenarios not in ORDER (e.g. scan_mp20)
    rows = []
    for q in sorted({q for q, _ in meas}, key=lambda q: int(q[1:])):
        ref = meas.get((q, "default"))
        T0 = statistics.median(t for t, _ in ref) if ref else None
        C0 = statistics.median(c for _, c in ref) if ref else None
        for plan in order:
            xs = meas.get((q, plan))
            if not xs:
                continue
            T, C = statistics.median(t for t, _ in xs), statistics.median(c for _, c in xs)
            e = est.get((q, plan), {})
            rows.append({
                "query": q, "plan": plan, "runs": len(xs), "T_meas": T, "C_meas": C,
                "T_est": e.get("T_est"), "C_est": e.get("C_est"),
                "err_T_%": 100 * (e["T_est"] / T - 1) if e else None, "err_C_%": 100 * (e["C_est"] / C - 1) if e else None,
                "speedup_vs_default": T0 / T if T0 else None, "saving_vs_default_%": 100 * (1 - C / C0) if C0 else None,
            })
    if not rows:
        sys.exit(f"no runs found under {a.plans_root}")
    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, "plans-measured.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    fmt = lambda v, s: "-" if v is None else s % v
    print(f"{'query':5} {'plan':11} {'runs':>4} {'T_meas':>7} {'T_est':>7} {'errT':>6} {'C_meas m$':>9} {'C_est':>8} {'errC':>6} {'speedup':>7} {'saving':>7}")
    for r in rows:
        print(f"{r['query']:5} {r['plan']:11} {r['runs']:4} {r['T_meas']:7.2f} {fmt(r['T_est'], '%7.2f')} {fmt(r['err_T_%'], '%+5.0f%%')} "
              f"{r['C_meas']*1e3:9.3f} {fmt(r['C_est'] and r['C_est']*1e3, '%8.3f')} {fmt(r['err_C_%'], '%+5.0f%%')} "
              f"{fmt(r['speedup_vs_default'], '%6.2fx')} {fmt(r['saving_vs_default_%'], '%+6.0f%%')}")

    # grouped bars: T and C per query, one bar per plan
    qs = sorted({r["query"] for r in rows}, key=lambda q: int(q[1:]))
    plans = [p for p in order if any(r["plan"] == p for r in rows)]
    extra = ["#c0397f", "#7a9a01", "#00838f", "#8d6e63"]
    fig, axs = plt.subplots(2, 1, figsize=(max(6, 0.45 * len(qs) * len(plans) / 2), 4.2), sharex=True)
    w = 0.8 / len(plans)
    for ax, key, lab in ((axs[0], "T_meas", "Measured time T (s)"), (axs[1], "C_meas", "Measured cost C (m$)")):
        for i, p in enumerate(plans):
            xs = [j + (i - (len(plans) - 1) / 2) * w for j, q in enumerate(qs)]
            ys = [next((r[key] * (1e3 if key == "C_meas" else 1) for r in rows if r["query"] == q and r["plan"] == p), 0) for q in qs]
            ax.bar(xs, ys, width=w * 0.9, color=COLORS.get(p) or extra[i % len(extra)], label=p)
        ax.set_ylabel(lab)
        ax.set_yscale("log")
        ax.grid(True, axis="y", color="#e6e5e1", lw=0.6)
        ax.set_axisbelow(True)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
    axs[1].set_xticks(range(len(qs)), [q.upper() for q in qs])
    axs[0].legend(ncol=len(plans), frameon=False, loc="upper left", fontsize=8)
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(a.out, f"plans-measured.{ext}"), dpi=200, bbox_inches="tight")
    print(f"saved -> {a.out}/plans-measured.csv, plans-measured.{{pdf,png}}")


if __name__ == "__main__":
    main()
