"""MinIO (S3) read model of the Grid'5000 testbed, fitted on the size x parallelism microbenchmark
(run-sweep-grid.py --mode s3 -> s3_sweep.py -> s3grid/s3-grid.csv), and the paper chart of it.

With k functions reading at once, the cluster delivers
    A(k) = min(k * B1, Bmax / (1 + gamma * k))        aggregate MB/s
and each function gets A(k) / k: B1 alone, then the shared bandwidth, which itself degrades under contention
(gamma). cost_model.py uses it for the S3 reads of a stage (P["minio"]), with k = min(p_v, max_parallel_v).

    .venv/bin/python notebooks/minio_model.py notebooks/cost-report/tpch100-v5/s3grid
Writes minio.json (B1, Bmax, gamma in MB/s) and minio-g5k.{pdf,png}.

The same model for NFS (run-sweep-grid.py --mode nfs -> s3_sweep.py --storage nfs -> nfsgrid/nfs-grid.csv):
    .venv/bin/python notebooks/minio_model.py notebooks/cost-report/tpch100-v5/nfsgrid --storage nfs
Writes nfs.json and nfs-g5k.{pdf,png}.
"""
import os
import csv
import json
import argparse

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

HERE = os.path.dirname(os.path.abspath(__file__))
fm.fontManager.addfont(os.path.join(HERE, "fonts", "LinLibertine_R.otf"))
plt.rcParams.update({"font.family": "Linux Libertine O", "font.size": 11, "axes.linewidth": 0.8,
                     "mathtext.fontset": "custom", "mathtext.rm": "Linux Libertine O",
                     "mathtext.it": "Linux Libertine O:italic", "axes.unicode_minus": True})
INK2, GRID, ORANGE = "#52514e", "#e4e3df", "#eb6834"
DPI = 300


def agg(k, b1, bmax, g):
    return np.minimum(k * b1, bmax / (1 + g * k))


def fit(rows):
    """Least relative squares on both the aggregate and the per-function throughput of every configuration."""
    k = np.array([r["readers"] for r in rows])
    A = np.array([r["agg_MBps"] for r in rows])
    f = np.array([r["fn_MBps"] for r in rows])
    best = None
    for b1 in np.linspace(f.max() * 0.6, f.max() * 1.3, 60):
        for bmax in np.linspace(A.max() * 0.8, A.max() * 2.0, 80):
            for g in np.linspace(0, 0.05, 51):
                a = agg(k, b1, bmax, g)
                err = np.mean(((a - A) / A) ** 2) + np.mean(((a / k - f) / f) ** 2)
                if best is None or err < best[0]:
                    best = (err, b1, bmax, g)
    _, b1, bmax, g = best
    a = agg(k, b1, bmax, g)
    return {"B1_MBps": float(b1), "Bmax_MBps": float(bmax), "gamma": float(g),
            "k_star": float(bmax / (b1 * (1 + g * bmax / b1))) if b1 else None,   # where k*B1 meets Bmax/(1+gk)
            "peak_MBps": float(max(agg(np.arange(1, 101), b1, bmax, g))),
            "mape_agg_%": float(100 * np.median(np.abs(a - A) / A)),
            "mape_fn_%": float(100 * np.median(np.abs(a / k - f) / f))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir", nargs="?")
    ap.add_argument("--storage", choices=["s3", "nfs"], default="s3")
    a = ap.parse_args()
    a.dir = a.dir or f"notebooks/cost-report/tpch100-v5/{a.storage}grid"
    name, label = ("minio", "S3") if a.storage == "s3" else ("nfs", "NFS")
    rows = [{k: float(v) for k, v in r.items()}
            for r in csv.DictReader(open(os.path.join(a.dir, f"{a.storage}-grid.csv")))]
    m = fit(rows)
    # solve k* exactly: k B1 = Bmax / (1 + g k)
    b1, bmax, g = m["B1_MBps"], m["Bmax_MBps"], m["gamma"]
    m["k_star"] = float((-1 + np.sqrt(1 + 4 * g * bmax / b1)) / (2 * g)) if g > 0 else bmax / b1
    json.dump(m, open(os.path.join(a.dir, f"{name}.json"), "w"), indent=2)
    print(json.dumps(m, indent=2))

    mps = sorted({r["mp"] for r in rows})
    cmap = plt.get_cmap("viridis", len(mps))
    xmax = max(70, 5 * np.ceil(max(r["readers"] for r in rows) / 5 + 1))
    kk = np.linspace(1, xmax, 300)
    fig, axs = plt.subplots(1, 2, figsize=(9.6, 3.4))
    for i, mp in enumerate(mps):
        R = [r for r in rows if r["mp"] == mp]
        kw = dict(s=26, color=cmap(i), edgecolor="white", lw=0.4, zorder=3, label=f"{int(mp)}")
        axs[0].scatter([r["readers"] for r in R], [r["agg_MBps"] / 1024 for r in R], **kw)
        axs[1].scatter([r["readers"] for r in R], [r["fn_MBps"] for r in R], **kw)
    axs[0].plot(kk, agg(kk, b1, bmax, g) / 1024, color=ORANGE, lw=1.8, zorder=2, label="model")
    axs[1].plot(kk, agg(kk, b1, bmax, g) / kk, color=ORANGE, lw=1.8, zorder=2, label="model")
    for ax in axs:
        ax.axvline(m["k_star"], color=ORANGE, lw=0.8, ls="--", zorder=1)
    axs[0].annotate(f"$k^*$ ≈ {m['k_star']:.0f}", (m["k_star"], 0.2), xytext=(4, 0), textcoords="offset points",
                    color=ORANGE, fontsize=10)
    axs[0].annotate(f"peak {m['peak_MBps'] / 1024:.2f} GB/s", (m["k_star"], m["peak_MBps"] / 1024),
                    xytext=(40, -4), textcoords="offset points", fontsize=10, color=INK2, va="top")
    axs[1].set_yscale("log")
    f = [r["fn_MBps"] for r in rows] + [b1]
    yt = [t for t in (10, 20, 50, 100, 200, 500, 1000, 2000) if min(f) / 2 <= t <= max(f) * 2]
    axs[1].set_yticks(yt, [str(t) for t in yt])
    axs[1].yaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    axs[1].annotate(f"$B_1$ ≈ {b1:.0f} MB/s", (m["k_star"], b1), xytext=(8, 0), textcoords="offset points",
                    fontsize=10, color=INK2, va="center")
    for ax, t, yl in ((axs[0], f"(a) Aggregate {label} read throughput", "Aggregate throughput (GB/s)"),
                      (axs[1], "(b) Throughput per function", "Per-function throughput (MB/s)")):
        ax.set_title(t, loc="left", fontsize=11)
        ax.set_xlabel("Functions reading concurrently, $k$")
        ax.set_ylabel(yl)
        ax.grid(True, color=GRID, lw=0.6)
        ax.set_axisbelow(True)
        ax.set_xlim(0, xmax)
    axs[0].set_ylim(0, None)
    h, l = axs[0].get_legend_handles_labels()
    fig.legend(h, l, title="max_parallel:", title_fontsize=9, alignment="left", loc="upper center",
               ncol=len(l), frameon=False, bbox_to_anchor=(0.5, 1.09), fontsize=9, handletextpad=0.3,
               columnspacing=1.0)
    fig.tight_layout(w_pad=2)
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(a.dir, f"{name}-g5k.{ext}"), dpi=DPI, bbox_inches="tight", pad_inches=0.02)
    print(f"saved -> {a.dir}/{name}.json, {name}-g5k.{{pdf,png}}")


if __name__ == "__main__":
    main()
