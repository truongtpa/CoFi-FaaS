"""Pareto charts and summary table from plan_opt.py output.

    .venv/bin/python notebooks/plot_opt.py notebooks/cost-report/tpch100-v5/opt --queries q5 q9 q20 q21 --ncol 2 --ncol-all 4
"""
import os
import csv
import json
import argparse
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.ticker import MaxNLocator
from matplotlib.lines import Line2D

INK, INK2, GRID, GRAY = "#0b0b0b", "#52514e", "#e6e5e1", "#c9c8c2"
BLUE, ORANGE, GREEN = "#2a78d6", "#eb6834", "#1baf7a"
plt.rcParams.update({"font.size": 13, "axes.edgecolor": INK2,
                     "axes.labelcolor": INK, "xtick.color": INK2,
                     "ytick.color": INK2, "axes.spines.top": True,
                     "axes.spines.right": True})   # 4-sided frame
fm.fontManager.addfont(os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts", "LinLibertine_R.otf"))
plt.rcParams['font.family'] = 'Linux Libertine O'
plt.rcParams['font.serif'] = ['Linux Libertine O']
PAD = 0.08  # axis margin around the Pareto front, as a fraction of its span
DPI = 300   # PNG resolution (PDF is vector); override with --dpi
COST_UNIT, COST_SCALE = "¢", 100   # cost shown in cents ("m$", 1e3 for milli-dollars)


def load(out):
    pts = list(csv.DictReader(open(os.path.join(out, "points.csv"))))
    for r in pts:
        r["T"], r["C"] = float(r["T"]), float(r["C"]) * COST_SCALE
        for k in ("pareto", "knee", "default"):
            r[k] = r[k] == "True"
    plans = {(p["query"], p["plan"]): p for p in json.load(open(os.path.join(out, "plans.json")))}
    return pts, plans


MEAS = {}   # (query, scenario) -> (T, C in COST_UNIT), measured runs overlaid with --measured (plan_compare.py csv)
MEAS_STYLE = {   # scenario -> (marker, size, colour, legend label)
    "default": ("D", 26, "#52514e", "Meas. default"),
    "scan_mp20": ("v", 34, "#00838f", "Meas. scan mp20"),
    "scan_opt": ("P", 40, "#7b3fa0", "Meas. scan mp20, 70 MB"),
    "all_mp20": ("*", 60, "#c0397f", "Meas. all mp20"),
}


def panel(ax, q, pts, plans, labels=True):
    R = [r for r in pts if r["query"] == q]
    F = sorted((r for r in R if r["pareto"]), key=lambda r: r["T"])
    ax.scatter([r["T"] for r in R], [r["C"] for r in R], s=5, color=GRAY, lw=0, label="Evaluated plans", rasterized=True)
    ax.plot([r["T"] for r in F], [r["C"] for r in F], color=BLUE, lw=1.4, zorder=2)
    ax.scatter([r["T"] for r in F], [r["C"] for r in F], s=14, color=BLUE, edgecolor="white", lw=0.6, zorder=3,
               label="Pareto front")
    d, k = plans[(q, "default")], plans[(q, "knee")]
    ax.scatter([k["T_est"]], [k["C_est"] * COST_SCALE], s=95, facecolor="none", edgecolor=INK, lw=1.2, zorder=4,
               label="Selected plan")
    for key, mk, col, lab in (("min_T", "^", ORANGE, "Min-time plan"), ("min_C", "s", GREEN, "Min-money plan")):
        p = plans[(q, key)]
        ax.scatter([p["T_est"]], [p["C_est"] * COST_SCALE], marker=mk, s=30, color=col, edgecolor="white", lw=0.6,
                   zorder=5, label=lab)
    meas = [(sc, MEAS[(q, sc)]) for sc in MEAS_STYLE if (q, sc) in MEAS]
    for sc, (t, c) in meas:
        m_, s_, col, lab = MEAS_STYLE[sc]
        ax.scatter([t], [c], marker=m_, s=s_, color=col, edgecolor="white", lw=0.5, zorder=6, label=lab)
    if labels:
        dt, dc = k["T_est"] / d["T_est"] - 1, k["C_est"] / d["C_est"] - 1
        pct = lambda v: f"{v:+.0%}".replace("-", "\u2212")
     #   ax.text(0.97, 0.97, f"Selected vs. default:\nT {pct(dt)}, C {pct(dc)}", transform=ax.transAxes, ha="right", va="top", fontsize=6.5, color=INK)
    ax.set_title(q.upper(), color=INK, fontsize=12, loc="center")
    ax.grid(True, color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    # zoom on the front (not the far-off plans), with a small margin relative to its span
    for vals, setlim in (([r["T"] for r in F] + [t for _, (t, _) in meas], ax.set_xlim),
                         ([r["C"] for r in F] + [c for _, (_, c) in meas], ax.set_ylim)):
        lo, hi = min(vals), max(vals)
        pad = (hi - lo) * PAD or lo * 0.05   # single-point front: ±5% of the value
        setlim(lo - pad, hi + pad)
    # tight limits leave few "nice" ticks: ask for at least 3 per axis (fewer on x, where labels are wider)
    ax.xaxis.set_major_locator(MaxNLocator(nbins=4, steps=[1, 2, 4, 5, 10], min_n_ticks=3))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5, steps=[1, 2, 2.5, 5, 10], min_n_ticks=3))


def figure(pts, plans, queries, path, ncol, dpi=DPI):
    nrow = (len(queries) + ncol - 1) // ncol
    height = 1.9 * nrow + 0.5
    fig, axs = plt.subplots(nrow, ncol, figsize=(1.95 * ncol + 0.3, height), squeeze=False)
    for ax, q in zip(axs.flat, queries):
        panel(ax, q, pts, plans)
    for ax in list(axs.flat)[len(queries):]:
        ax.axis("off")
    for ax in axs[:, 0]:
        ax.set_ylabel(f"Estimated money C ({COST_UNIT})")
    for c in range(ncol):   # bottom-most used panel of each column (the last row may be partly empty)
        used = [axs[r, c] for r in range(nrow) if r * ncol + c < len(queries)]
        if used:
            used[-1].set_xlabel("Estimated time T (s)")
    # legend symbols drawn larger than in the panels (the plotted sizes are tiny at legend scale)
    mk = lambda m, ms, fc, ec="white", mew=0.6: Line2D([], [], ls="", marker=m, ms=ms, mfc=fc, mec=ec, mew=mew)
    h = [mk("o", 4, GRAY, GRAY, 0), Line2D([], [], color=BLUE, lw=1.4, marker="o", ms=6, mfc=BLUE, mec="white", mew=0.6),
         mk("o", 11, "none", INK, 1.2), mk("^", 8, ORANGE), mk("s", 7, GREEN)]
    l = ["Evaluated plans", "Pareto front", "Selected plan", "Min-time plan", "Min-cost plan"]
    for sc, (m_, s_, col, lab) in MEAS_STYLE.items():   # measured scenarios present in the figure
        if any((q, sc) in MEAS for q in queries):
            h.append(mk(m_, {"D": 6, "v": 7, "P": 8, "*": 10}[m_], col))
            l.append(lab)
    leg_ncol = min(len(l), 5 if ncol >= 4 else 3)   # as many legend columns as the figure is wide enough for
    leg_rows = -(-len(l) // leg_ncol)
    leg_ncol = -(-len(l) // leg_rows)                                  # balanced rows (3+2, not 4+1)
    fig.legend(h, l, loc="upper center", ncol=leg_ncol, frameon=False, bbox_to_anchor=(0.5, 1.0),
               fontsize=11, handlelength=1.6, handletextpad=0.4, columnspacing=1.0, labelspacing=0.3, borderpad=0.2)
    fig.tight_layout(pad=0.3, w_pad=0.8, h_pad=0.6, rect=(0, 0, 1, 1 - 0.26 * leg_rows / height))   # legend fontsize 11: ~0.26 in per row
    for ext in ("pdf", "png"):
        fig.savefig(f"{path}.{ext}", dpi=dpi, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)


def table(plans, out):
    qs = sorted({q for q, _ in plans}, key=lambda q: int(q[1:]))
    rows = []
    for q in qs:
        d, k = plans[(q, "default")], plans[(q, "knee")]
        rows.append({"query": q, "T_default": d["T_est"], "C_default": d["C_est"], "T_knee": k["T_est"], "C_knee": k["C_est"],
                     "dT_%": 100 * (k["T_est"] / d["T_est"] - 1), "dC_%": 100 * (k["C_est"] / d["C_est"] - 1),
                     "bf_off": len(k["bf_off"]), "resized": len(k["max_size_mb"]), "max_fn_mb": k["max_fn_mb"]})
    with open(os.path.join(out, "knee-vs-default.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out", nargs="?", default="notebooks/cost-report/tpch100-v5/opt")
    ap.add_argument("--queries", nargs="*", default=["q5", "q9", "q20", "q21"])
    ap.add_argument("--dpi", type=int, default=DPI)
    ap.add_argument("--measured", help="plans-measured.csv (plan_compare.py) to overlay measured scenarios")
    ap.add_argument("--ncol", type=int, default=3, help="columns in pareto-opt (the --queries figure)")
    ap.add_argument("--ncol-all", type=int, default=2, help="columns in pareto-opt-all (every query)")
    a = ap.parse_args()
    pts, plans = load(a.out)
    if a.measured:
        for r in csv.DictReader(open(a.measured)):
            MEAS[(r["query"], r["plan"])] = (float(r["T_meas"]), float(r["C_meas"]) * COST_SCALE)
    allq = sorted({q for q, _ in plans}, key=lambda q: int(q[1:]))
    figure(pts, plans, [q for q in a.queries if q in allq], os.path.join(a.out, "pareto-opt"), ncol=a.ncol, dpi=a.dpi)
    figure(pts, plans, allq, os.path.join(a.out, "pareto-opt-all"), ncol=a.ncol_all, dpi=a.dpi)
    rows = table(plans, a.out)
    for r in rows:
        print(f"{r['query']:4} T {r['T_default']:6.2f} -> {r['T_knee']:6.2f} ({r['dT_%']:+5.0f}%)   "
              f"C {r['C_default']*COST_SCALE:7.3f} -> {r['C_knee']*COST_SCALE:7.3f} {COST_UNIT} ({r['dC_%']:+5.0f}%)   "
              f"bf_off={r['bf_off']} resized={r['resized']} fn={r['max_fn_mb']:.0f}MB")
    print(f"saved -> {a.out}/pareto-opt.{{pdf,png}}, pareto-opt-all.{{pdf,png}}, knee-vs-default.csv")


if __name__ == "__main__":
    main()
