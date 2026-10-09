"""Cost Model Accuracy per query, from the reports written by cost_model_batch.py.

Reads <report>/<query>/<variant>-run<i>/report.json and, for each query and system
(no BF, BF + S3 shuffle, BF + NFS shuffle), compares the model's T and C with the
measured ones. err = (model - meas) / meas.

Usage: .venv/bin/python notebooks/cost_accuracy.py [notebooks/cost-report/tpch100-v5] [--compare <old report>] [--last q2 ...|all]
  --compare: also draw cost-accuracy-compare.{pdf,png}, this report against an older one
  --last: keep only the last run of these queries (e.g. drop warm-up runs)
Outputs (in <report>/accuracy/): accuracy-runs.csv, accuracy-query.csv, accuracy-query.tex,
cost-accuracy-err.{pdf,png}, cost-accuracy-rate.{pdf,png}, cost-accuracy-scatter.{pdf,png}
"""
import os
import re
import sys
import json
import glob

import pandas as pd
import matplotlib
if "ipykernel" not in sys.modules:  # keep inline figures when imported from the notebook
    matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

SYS = ["base", "s3", "nfs"]
LABEL = {"base": "No BF", "s3": "BF + S3 shuffle", "nfs": "BF + NFS shuffle"}
COLOR = {"base": "#2a78d6", "s3": "#eb6834", "nfs": "#1baf7a"}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
METRIC = {"T": "Latency T", "C": "Cost C"}

plt.rcParams.update({
    "font.size": 9, "axes.edgecolor": INK2, "axes.labelcolor": INK, "xtick.color": INK2, "ytick.color": INK2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.spines.top": False,
    "axes.spines.right": False, "legend.frameon": False, "savefig.dpi": 200, "savefig.bbox": "tight",
})


def fs(r=1.0):
    """Font size relative to rcParams["font.size"], so a notebook can restyle every chart (default 9 pt)."""
    return plt.rcParams["font.size"] * r


def qnum(q):
    return int(re.sub(r"\D", "", q) or 0)


def load_runs(report, last=()):
    """One row per (query, system, run, metric). The no-BF run is paired with both BF variants
    and gets the same kappa under loqo calibration, so it is taken from the s3 pairing only."""
    rows = []
    for path in sorted(glob.glob(os.path.join(report, "q*", "*-run*", "report.json"))):
        q, tag = path.split(os.sep)[-3:-1]
        variant, run = re.match(r"(\w+)-run(\d+)", tag).groups()
        r = json.load(open(path))
        sides = [("bf", variant)] + ([("nobf", "base")] if variant == "s3" else [])
        for side, system in sides:
            t = r["runs"][side]["totals"]
            for m in METRIC:
                meas, model = t[f"{m}_meas"], t[f"{m}_model"]
                rows.append({"query": q, "system": system, "run": int(run), "metric": m,
                             "meas": meas, "model": model, "err_%": 100 * (model - meas) / meas})
        d = r["decision"]
        rows.append({"query": q, "system": variant, "run": int(run), "metric": "decision",
                     "meas": float(d["bf_better_meas"]), "model": float(d["bf_better_model"]),
                     "err_%": float(not d["decision_match"])})
    df = pd.DataFrame(rows)
    if last:
        sel = df["query"].isin(df["query"].unique() if "all" in last else last)
        lastrun = df.groupby(["query", "system"])["run"].transform("max")
        df = df[~sel | (df["run"] == lastrun)].reset_index(drop=True)
    df["abs_err_%"] = df["err_%"].abs()
    return df


def per_query(df):
    """Median over runs of meas/model/err, mean |err| (MAPE), and min/max err for whiskers."""
    acc = df[df.metric.isin(list(METRIC))]
    g = acc.groupby(["query", "system", "metric"])
    out = g.agg(runs=("run", "nunique"), meas=("meas", "median"), model=("model", "median"),
                err=("err_%", "median"), err_min=("err_%", "min"), err_max=("err_%", "max"),
                mape=("abs_err_%", "mean")).reset_index()
    dec = df[df.metric == "decision"].groupby(["query", "system"]).agg(
        decision_match=("err_%", lambda s: f"{int((s == 0).sum())}/{len(s)}")).reset_index()
    out = out.merge(dec, on=["query", "system"], how="left")
    out["_q"] = out["query"].map(qnum)
    out["_s"] = out["system"].map(SYS.index)
    return out.sort_values(["_q", "_s", "metric"]).drop(columns=["_q", "_s"]).reset_index(drop=True)


def wide(pq):
    """One row per query: median signed error of T and C for every system, plus decision match."""
    w = pq.pivot_table(index="query", columns=["metric", "system"], values="err")
    w = w.reindex(columns=[(m, s) for m in METRIC for s in SYS if (m, s) in w.columns])
    w.columns = [f"err_{m}_{s}_%" for m, s in w.columns]
    dec = pq.drop_duplicates(["query", "system"]).pivot(index="query", columns="system", values="decision_match")
    for s in ("s3", "nfs"):
        if s in dec:
            w[f"decision_{s}"] = dec[s]
    w = w.loc[sorted(w.index, key=qnum)]
    mape = pq.groupby(["metric", "system"])["mape"].mean()
    w.loc["MAPE"] = {f"err_{m}_{s}_%": mape.get((m, s)) for m in METRIC for s in SYS}
    return w


def to_latex(w, path):
    cols = [c for c in w.columns if c.startswith("err_")]
    fmt = lambda v: "--" if pd.isna(v) else f"{v:+.1f}"
    lines = [r"\begin{tabular}{l" + "r" * len(cols) + "cc}", r"\toprule",
             r" & \multicolumn{3}{c}{Error of $T$ (\%)} & \multicolumn{3}{c}{Error of $C$ (\%)} & \multicolumn{2}{c}{Decision} \\",
             r"\cmidrule(lr){2-4}\cmidrule(lr){5-7}\cmidrule(lr){8-9}",
             r"Query & No BF & BF-S3 & BF-NFS & No BF & BF-S3 & BF-NFS & S3 & NFS \\", r"\midrule"]
    for q, r in w.iterrows():
        if q == "MAPE":
            lines.append(r"\midrule")
            cells = [f"{r[c]:.1f}" for c in cols] + ["", ""]
        else:
            cells = [fmt(r[c]) for c in cols] + [r.get("decision_s3", ""), r.get("decision_nfs", "")]
        lines.append(f"{q.upper()} & " + " & ".join(str(c) for c in cells) + r" \\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    open(path, "w").write("\n".join(lines) + "\n")


def plot_err(pq, out, figsize=None, note=True):
    """Signed error per query, one panel per metric; bar = median over runs, whisker = min..max."""
    queries = sorted(pq["query"].unique(), key=qnum)
    fig, axes = plt.subplots(2, 1, figsize=figsize or (10, 5.6), sharex=True)
    bw = 0.8 / len(SYS)
    for ax, m in zip(axes, METRIC):
        d = pq[pq.metric == m].set_index(["query", "system"])
        for k, s in enumerate(SYS):
            xs = [i + (k - 1) * bw for i, q in enumerate(queries) if (q, s) in d.index]
            r = [d.loc[(q, s)] for q in queries if (q, s) in d.index]
            ys = [x["err"] for x in r]
            ax.bar(xs, ys, width=bw * 0.9, color=COLOR[s], label=LABEL[s], zorder=2)
            ax.vlines(xs, [x["err_min"] for x in r], [x["err_max"] for x in r], color=INK2, lw=0.8, zorder=3)
        ax.axhline(0, color=INK2, lw=0.8, zorder=1)
        mape = pq[pq.metric == m].groupby("system")["mape"].mean()
        ax.set_title(f"{METRIC[m]}: model vs measured   (MAPE  " +
                     "   ".join(f"{LABEL[s]} {mape[s]:.0f}%" for s in SYS if s in mape) + ")",
                     loc="left", color=INK, fontsize=fs())
        ax.set_ylabel("Error (%)")
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:+.0f}"))
    axes[-1].set_xticks(range(len(queries)), [q.upper() for q in queries])
    axes[0].legend(loc="upper left", bbox_to_anchor=(0, 1.32), ncol=3, fontsize=fs(0.94))
    if note:
        fig.text(0.0, -0.02, "Error = (model − measured) / measured. Bar = median over runs, line = min–max over runs.",
                 fontsize=fs(0.83), color=INK2)
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}")
    return fig


def plot_ratio(pq, out, figsize=None, note=True):
    """Rate model / measured per query (log scale, 1 = exact), one panel per metric;
    dot = median over runs, line = min..max over runs."""
    queries = sorted(pq["query"].unique(), key=qnum)
    fig, axes = plt.subplots(2, 1, figsize=figsize or (10, 5.6), sharex=True)
    off = 0.8 / len(SYS)
    for ax, m in zip(axes, METRIC):
        d = pq[pq.metric == m].set_index(["query", "system"])
        ax.axhspan(1 / 1.25, 1.25, color=GRID, alpha=0.6, lw=0, zorder=0)
        ax.axhline(1, color=INK2, lw=0.8, zorder=1)
        for k, s in enumerate(SYS):
            r = [(i + (k - 1) * off, d.loc[(q, s)]) for i, q in enumerate(queries) if (q, s) in d.index]
            xs = [x for x, _ in r]
            ax.vlines(xs, [1 + x["err_min"] / 100 for _, x in r], [1 + x["err_max"] / 100 for _, x in r],
                      color=COLOR[s], lw=1.2, zorder=2)
            ax.scatter(xs, [1 + x["err"] / 100 for _, x in r], s=36, color=COLOR[s], edgecolor="white", lw=1,
                       label=LABEL[s], zorder=3)
        ax.set_yscale("log")
        ax.set_yticks([0.2, 0.3, 0.5, 0.7, 1, 1.5, 2, 3], minor=False)
        ax.set_ylim(0.13, 4.5)  # same scale for T and C so the two panels compare directly
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}×"))
        ax.yaxis.set_minor_formatter(FuncFormatter(lambda v, _: ""))
        ax.set_ylabel(f"{m} model / measured")
        ax.set_title(f"{METRIC[m]} rate (model / measured)", loc="left", color=INK, fontsize=fs())
    axes[-1].set_xticks(range(len(queries)), [q.upper() for q in queries])
    axes[0].legend(loc="upper left", bbox_to_anchor=(0, 1.32), ncol=3, fontsize=fs(0.94))
    if note:
        fig.text(0.0, -0.02, "Rate = model / measured: 1× = exact, >1× = model overestimates. "
                 "Dot = median over runs, line = min–max over runs, gray band = ±25%.", fontsize=fs(0.83), color=INK2)
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}")
    return fig


def plot_compare(before, after, out, labels=("old", "new"), figsize=None, note=True):
    """Rate model / measured per query before (hollow) -> after (filled), one panel per metric."""
    queries = sorted(after["query"].unique(), key=qnum)
    fig, axes = plt.subplots(2, 1, figsize=figsize or (10, 5.8), sharex=True)
    off = 0.8 / len(SYS)
    for ax, m in zip(axes, METRIC):
        b = before[before.metric == m].set_index(["query", "system"])["err"]
        a = after[after.metric == m].set_index(["query", "system"])["err"]
        ax.axhspan(1 / 1.25, 1.25, color=GRID, alpha=0.6, lw=0, zorder=0)
        ax.axhline(1, color=INK2, lw=0.8, zorder=1)
        for k, s in enumerate(SYS):
            pts = [(i + (k - 1) * off, 1 + b[(q, s)] / 100, 1 + a[(q, s)] / 100)
                   for i, q in enumerate(queries) if (q, s) in a.index and (q, s) in b.index]
            for x, y0, y1 in pts:
                ax.plot([x, x], [y0, y1], color=COLOR[s], lw=1, alpha=0.5, zorder=2)
            ax.scatter([x for x, _, _ in pts], [y for _, y, _ in pts], s=26, facecolor="white", edgecolor=COLOR[s],
                       lw=1.2, zorder=3)
            ax.scatter([x for x, _, _ in pts], [y for _, _, y in pts], s=34, color=COLOR[s], edgecolor="white", lw=1,
                       label=LABEL[s], zorder=4)
        mb = before[before.metric == m]["mape"].mean()
        ma = after[after.metric == m]["mape"].mean()
        ax.set_yscale("log")
        ax.set_yticks([0.2, 0.3, 0.5, 0.7, 1, 1.5, 2, 3], minor=False)
        ax.set_ylim(0.13, 4.5)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}×"))
        ax.yaxis.set_minor_formatter(FuncFormatter(lambda v, _: ""))
        ax.set_ylabel(f"{m} model / measured")
        ax.set_title(f"{METRIC[m]} rate: {labels[0]} (hollow) → {labels[1]} (filled)    MAPE {mb:.0f}% → {ma:.0f}%",
                     loc="left", color=INK, fontsize=fs())
    axes[-1].set_xticks(range(len(queries)), [q.upper() for q in queries])
    axes[0].legend(loc="upper left", bbox_to_anchor=(0, 1.32), ncol=3, fontsize=fs(0.94))
    if note:
        fig.text(0.0, -0.02, "Rate = model / measured (median over runs): 1× = exact. Gray band = ±25%. "
                 "Line joins the same query and system before and after.", fontsize=fs(0.83), color=INK2)
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}")
    return fig


SHORT = {"base": "No BF", "s3": "BF-S3", "nfs": "BF-NFS"}
DIVERGING = ["#184f95", "#6da7ec", "#f0efec", "#ef8f8e", "#b8302f"]  # blue (model low) - gray - red (model high)


def plot_heatmap(pq, out, figsize=None, clip=60):
    """Signed error per query (rows) x metric/system (columns), value written in each cell; last row = MAPE."""
    import numpy as np
    from matplotlib.colors import LinearSegmentedColormap
    queries = sorted(pq["query"].unique(), key=qnum)
    cols = [(m, s) for m in METRIC for s in SYS]
    e = pq.set_index(["query", "metric", "system"])["err"]
    Z = np.array([[e.get((q, m, s), np.nan) for m, s in cols] for q in queries])
    mape = [pq[(pq.metric == m) & (pq.system == s)]["mape"].mean() for m, s in cols]
    cmap = LinearSegmentedColormap.from_list("div", DIVERGING)
    fig, ax = plt.subplots(figsize=figsize or (6.4, 8.6))
    ax.imshow(np.clip(Z, -clip, clip), cmap=cmap, vmin=-clip, vmax=clip, aspect="auto")
    for i in range(len(queries)):
        for j in range(len(cols)):
            v = Z[i, j]
            if v == v:
                ax.text(j, i, "0" if round(v) == 0 else f"{v:+.0f}", ha="center", va="center", fontsize=fs(0.85),
                        color="white" if abs(v) > 0.6 * clip else INK)
    n = len(queries)
    for j, v in enumerate(mape):  # MAPE row under a gap
        ax.text(j, n + 0.15, f"{v:.0f}", ha="center", va="center", fontsize=fs(0.85), weight="bold", color=INK)
    ax.set_xticks(range(len(cols)), [SHORT[s] for _, s in cols], fontsize=fs(0.85))
    ax.xaxis.tick_top()
    ax.set_yticks(list(range(n)) + [n + 0.15], [q.upper() for q in queries] + ["MAPE"], fontsize=fs(0.85))
    ax.set_ylim(n + 0.7, -0.5)
    ax.axvline(len(SYS) - 0.5, color="white", lw=4)
    ax.axhline(n - 0.5, color=INK2, lw=0.6)
    for k, m in enumerate(METRIC):
        ax.text(k * len(SYS) + (len(SYS) - 1) / 2, -1.55, METRIC[m], ha="center", va="bottom", fontsize=fs(), color=INK)
    ax.tick_params(length=0)
    ax.grid(False)
    for sp in ax.spines.values():
        sp.set_visible(False)
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}")
    return fig


def plot_dumbbell(before, after, out, labels=("old", "new"), figsize=None, xmax=100):
    """|error| per query, before (gray hollow) -> after (filled, system color); one panel per metric x system."""
    queries = sorted(after["query"].unique(), key=qnum)
    fig, axes = plt.subplots(len(METRIC), len(SYS), figsize=figsize or (11, 9), sharey=True, sharex=True)
    ys = range(len(queries))
    for r, m in enumerate(METRIC):
        for c, s in enumerate(SYS):
            ax = axes[r, c]
            bs = before[(before.metric == m) & (before.system == s)].set_index("query")
            as_ = after[(after.metric == m) & (after.system == s)].set_index("query")
            b, a = bs["err"].abs(), as_["err"].abs()
            ax.axvspan(0, 25, color=GRID, alpha=0.6, lw=0, zorder=0)
            for y, q in zip(ys, queries):
                if q in a.index and q in b.index:
                    x0, x1 = min(b[q], xmax), min(a[q], xmax)
                    ax.plot([x0, x1], [y, y], color=COLOR[s], lw=1.6, alpha=0.45, zorder=1, solid_capstyle="round")
                    ax.scatter(x0, y, s=34, facecolor="white", edgecolor="#9a9994", lw=1.2, zorder=2)
                    ax.scatter(x1, y, s=46, color=COLOR[s], edgecolor="white", lw=1, zorder=3)
                    if a[q] > xmax or b[q] > xmax:
                        ax.text(xmax, y, " ›", va="center", fontsize=fs(0.8), color=INK2)
            ax.set_xlim(0, xmax * 1.04)
            ax.grid(axis="y", visible=False)
            ax.set_title(f"{METRIC[m]} · {SHORT[s]}   MAPE {bs['mape'].mean():.0f}% → {as_['mape'].mean():.0f}%", loc="left",
                         fontsize=fs(0.9), color=INK)
            if r == len(METRIC) - 1:
                ax.set_xlabel("|error| (%)")
    axes[0, 0].set_yticks(list(ys), [q.upper() for q in queries])
    for ax in axes.flat:
        ax.tick_params(labelsize=fs(0.8))
    axes[0, 0].set_ylim(len(queries) - 0.4, -0.6)
    from matplotlib.lines import Line2D
    fig.legend(handles=[Line2D([], [], ls="", marker="o", mfc="white", mec="#9a9994", ms=7, label=labels[0]),
                        Line2D([], [], ls="", marker="o", color=INK2, ms=7, label=labels[1] + " (system color)"),
                        __import__("matplotlib.patches", fromlist=["Patch"]).Patch(color=GRID, label="|error| ≤ 25%")],
               loc="upper left", bbox_to_anchor=(0.0, 1.04), ncol=3, fontsize=fs(0.9))
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}")
    return fig


def plot_scatter(pq, out, figsize=None, note=True):
    """Model vs measured (log–log) per query median; the diagonal is a perfect model."""
    fig, axes = plt.subplots(1, 2, figsize=figsize or (9, 4))
    for ax, m in zip(axes, METRIC):
        d = pq[pq.metric == m]
        lo, hi = d[["meas", "model"]].min().min() / 1.5, d[["meas", "model"]].max().max() * 1.5
        ax.plot([lo, hi], [lo, hi], color=INK2, lw=0.8, zorder=1)
        ax.fill_between([lo, hi], [lo / 1.25, hi / 1.25], [lo * 1.25, hi * 1.25], color=GRID, alpha=0.6, lw=0, zorder=0)
        for s in SYS:
            x = d[d.system == s]
            ax.scatter(x["meas"], x["model"], s=28, color=COLOR[s], edgecolor="white", lw=1, label=LABEL[s], zorder=3)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlim(lo, hi)
        ax.set_ylim(lo, hi)
        unit = "s" if m == "T" else "$"
        ax.set_xlabel(f"Measured {m} ({unit})")
        ax.set_ylabel(f"Model {m} ({unit})")
        ax.set_title(METRIC[m], loc="left", color=INK)
    axes[1].legend(loc="upper left", bbox_to_anchor=(1.02, 1), fontsize=fs(0.89))
    if note:
        fig.text(0.0, -0.03, "One point per query (median over runs). Diagonal = exact; gray band = ±25%.",
                 fontsize=fs(0.83), color=INK2)
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}")
    return fig


def main(report="notebooks/cost-report/tpch100-v5", *args):
    opt = lambda name: list(__import__("itertools").takewhile(lambda x: not x.startswith("--"),
                                                             args[args.index(name) + 1:])) if name in args else []
    last = [q.lower() for q in opt("--last")]
    base = (opt("--compare") or [None])[0]
    out = os.path.join(report, "accuracy" + ("-last-" + "-".join(last) if last else ""))
    os.makedirs(out, exist_ok=True)
    df = load_runs(report, last)
    pq = per_query(df)
    w = wide(pq)
    df.to_csv(os.path.join(out, "accuracy-runs.csv"), index=False)
    pq.to_csv(os.path.join(out, "accuracy-query.csv"), index=False)
    w.to_csv(os.path.join(out, "accuracy-query-wide.csv"))
    to_latex(w, os.path.join(out, "accuracy-query.tex"))
    plot_err(pq, os.path.join(out, "cost-accuracy-err"))
    plot_ratio(pq, os.path.join(out, "cost-accuracy-rate"))
    plot_scatter(pq, os.path.join(out, "cost-accuracy-scatter"))
    plot_heatmap(pq, os.path.join(out, "cost-accuracy-heatmap"))
    if base:
        plot_compare(per_query(load_runs(base, last)), pq, os.path.join(out, "cost-accuracy-compare"),
                     (os.path.basename(base.rstrip("/")), os.path.basename(report.rstrip("/"))))
        plot_dumbbell(per_query(load_runs(base, last)), pq, os.path.join(out, "cost-accuracy-dumbbell"),
                      (os.path.basename(base.rstrip("/")), os.path.basename(report.rstrip("/"))))
    shown = w.copy()
    for c in shown.columns:
        if c.startswith("err_"):
            shown[c] = [f"{v:.1f}" if q == "MAPE" else f"{v:+.1f}" for q, v in shown[c].items()]
    with pd.option_context("display.width", 200, "display.max_columns", 20):
        print(shown.fillna(""))
    print("saved ->", out)


if __name__ == "__main__":
    main(*sys.argv[1:])
