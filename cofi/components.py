"""Model accuracy per component (paper section "Cost-Time Model Accuracy"): the query time T is built from the time
of one function (task level), composed into the stage time with the waves of max_parallel functions (stage level),
then summed over the stages run one after another (query level). Comparing the model at the three levels shows where
the error comes from.

    python -m cofi.components <history> [--sweep <history-sweep>] --out "1. Cost-Time Model Accuracy/data/components-v5.csv"

<history>: <query>/<run>--<system>/ (the runs of the accuracy section; kappa is calibrated leave-one-query-out on it).
<history-sweep>: size-<mb>/<query>/<run>/ (BLOOM-FaaS S3 at other partition sizes), predicted with the kappa
calibrated on the other queries of <history>, so none of its runs is used for calibration.

One row per stage of every run: functions p, measured mean / median function time (execution + dispatch), model
function time (before the waves), measured stage wall time, model stage time, and the query totals.
"""
import os
import glob
import argparse
from dataclasses import replace

import pandas as pd

from . import model as cm
from .accuracy import load_history, SYSTEMS, runs, qnum


def stage_rows(run, K, cfg, dataset, query, system, run_id, size=None):
    task_cfg = replace(cfg, waves=False)                    # time of one function: no composition into waves
    pt, ps = cm.predict(run, K, task_cfg), cm.predict(run, K, cfg)
    tot = cm.evaluate(run, K, cfg)[1]
    out = []
    for s in run["steps"]:
        n, m = s["name"], run["m"][s["name"]]
        d = sorted(t["t_total"] + t["disp"] for t in m["tasks"])
        out.append({"dataset": dataset, "query": query, "system": system, "run": run_id, "size_mb": size,
                    "stage": n, "func": s["func"], "endpoint": m["endpoint"], "p": m["p"],
                    "max_parallel": cm.stage_mp(run, s),
                    "task_meas_mean": sum(d) / len(d), "task_meas_median": d[len(d) // 2], "task_model": pt[n]["d"],
                    "stage_meas": m["d_wall"], "stage_model": ps[n]["d"],
                    "T_meas": tot["T_meas"], "T_model": tot["T_model"]})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("history")
    ap.add_argument("--sweep")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    cfg = cm.Config.v5()
    data = load_history(a.history, cfg)
    rows = []
    for q, R in data.items():
        K = cm.calibrate([r for q2, R2 in data.items() if q2 != q for rs in R2.values() for r in rs], cfg)
        for system, rs in R.items():
            for i, run in enumerate(rs, 1):
                rows += stage_rows(run, K, cfg, "accuracy", q, system, i)
        if a.sweep:
            for sd in sorted(glob.glob(os.path.join(a.sweep, "size-*"))):
                qd = os.path.join(sd, q)
                for i, d in enumerate(runs(qd, SYSTEMS["s3"]), 1):
                    rows += stage_rows(cm.load(d, cfg), K, cfg, "sweep", q, "s3", i, int(sd.rsplit("-", 1)[1]))
        print(f"{q}: {len(rows)} stage rows", flush=True)
    df = pd.DataFrame(rows)
    df["_q"] = df["query"].map(qnum)
    df.sort_values(["dataset", "_q", "system", "size_mb", "run"]).drop(columns="_q").to_csv(a.out, index=False)
    print(f"{len(df)} rows -> {a.out}")


if __name__ == "__main__":
    main()
