"""Estimate T and C of every Bloom Filter subset of a query from two measured plans (all BFs on, all BFs off).

A BF j (one merge_bf step) has builders (stages writing the BF), appliers (scans with filter_by_bf) and a semijoin
target J_j (first join whose other input contains a builder). BF pushdown keeps the exact join result, so turning
j on or off only changes the stages between an applier and J_j (its region) plus the BF stages themselves.
For a subset `on`, each stage takes its data demand from the BF run if every BF whose region covers it is on, from
the no-BF run if none is, and scales the no-BF demand by the measured filter factors of the BFs that are on
otherwise. The BF demand of a stage (build / apply work, BF file I/O) is kept only for BFs that are on.

    .venv/bin/python notebooks/plan_space.py history --queries q2 q20 q21 --out notebooks/cost-report/tpch100-v5/plans
"""
import os
import re
import sys
import csv
import glob
import json
import copy
import math
import argparse
import itertools
import statistics

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cost_model as cm
from cost_model_batch import runs

BF_RUN, BASE = "bloomfaas-s3", "starling-base"
DATA = ["D_data", "D_rx", "Q_rx", "Q_impl", "D_wx", "Q_wx", "rows_in", "rows_out", "D_in", "D_out"] + \
    [f"{x}_{s}_{c}" for x in ("D", "Q") for s in ("in", "out") for c in cm.CLASSES]
BFF = ["bf_rx_b", "bf_rx_n", "bf_wx_b", "bf_wx_n", "bf_impl_b", "bf_impl_n", "W_bf", "W_merge"]


def descendants(run, name):
    kids = {}
    for s in run["dag"]["steps"]:
        for d in s["deps"]:
            kids.setdefault(d, []).append(s["name"])
    seen, stack = set(), [name]
    while stack:
        n = stack.pop()
        if n not in seen:
            seen.add(n)
            stack.extend(kids.get(n, []))
    return seen


def bloom_filters(bf, nobf):
    """One entry per merge_bf step: builders, appliers, region (stages whose data changes), filter factor."""
    out = []
    for s in bf["steps"]:
        if s["func"] != "merge_bf":
            continue
        builders = [d for d in s["deps"] if d in bf["m"]]
        appliers = [a["name"] for a in bf["steps"] if s["name"] in a["deps"] and a["params"].get("filter_by_bf")]
        region, phi = set(), []
        for a in appliers:
            J, _ = cm.semijoin(nobf, builders, a)
            down = descendants(nobf, a)
            region |= {a} | (down & cm.ancestors(nobf, J["name"]) if J else down)
            n0, n1 = nobf["m"][a]["rows_out"], bf["m"][a]["rows_out"]
            phi.append(n1 / n0 if n0 else 1.0)
        out.append({"name": s["name"], "builders": builders, "appliers": appliers,
                    "region": region, "phi": math.prod(phi) if phi else 1.0})
    return out


def synth(bf, nobf, bfs, on):
    """A run dict for the plan where exactly the BFs in `on` (names of merge_bf steps) are pushed down."""
    off = {j["name"] for j in bfs} - set(on)
    steps, M = [], {}
    for s in bf["steps"]:
        n = s["name"]
        if n in off:
            continue
        cover = [j for j in bfs if n in j["region"]]
        on_c = [j for j in cover if j["name"] in on]
        if n not in nobf["m"]:                     # merge_bf of a BF that is on
            m = copy.copy(bf["m"][n])
        elif len(on_c) == len(cover) and cover:    # every BF that changes this stage is on
            m = copy.copy(bf["m"][n])
        else:
            m = copy.copy(nobf["m"][n])
            if on_c:                               # some on, some off: scale the no-BF demand
                f = math.prod(j["phi"] for j in on_c)
                for k in DATA:
                    m[k] = m[k] * f
                m["p"] = max(math.ceil(m["p"] * f), bf["m"][n]["p"])
        # BF demand of this stage: from the BF run for BFs that are on, none otherwise
        own = [j for j in bfs if n in j["builders"] or n in j["appliers"]]
        b = bf["m"][n]
        keep = n not in nobf["m"] or any(j["name"] in on for j in own)
        for side, rk in (("in", "bf_read"), ("out", "bf_write")):   # BF files are counted in the NFS totals
            m[f"D_{side}_nfs"] += (b[f"{rk}_b"] if keep else 0) - m[f"{rk}_b"]
            m[f"Q_{side}_nfs"] += (b[f"{rk}_n"] if keep else 0) - m[f"{rk}_n"]
        for k in BFF:
            m[k] = b[k] if keep else 0
        m["bf"] = b["bf"] if keep else None
        M[n] = m
        steps.append({**s, "deps": [d for d in s["deps"] if d not in off]})
    return {"folder": bf["folder"], "dag": bf["dag"], "steps": steps, "by_name": {s["name"]: s for s in steps}, "m": M}


def estimate(run, K, P):
    pred = cm.predict(run, K, P)
    T, eft = cm.critical_path(run, {n: v["d"] for n, v in pred.items()}, P.get("serial"))
    C = sum(sum(cm.cost(run["m"][n], pred[n]["billed"], T - eft[n], P)) for n in pred)
    return T, C


def pareto(rows):
    return [r for r in rows if not any(o["T"] <= r["T"] and o["C"] <= r["C"] and (o["T"], o["C"]) != (r["T"], r["C"]) for o in rows)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("history", nargs="?", default="history")
    ap.add_argument("--queries", nargs="*", default=["q2", "q20", "q21"])
    ap.add_argument("--out", default="notebooks/cost-report/tpch100-v5/plans")
    ap.add_argument("--parallel", nargs="*", type=int, default=[], help="also sweep max_parallel (runner slots)")
    a = ap.parse_args()
    P = cm.load_params()
    P.update(serial=True, waves=True, disp_median=True, contention=True)   # best flags (cost-report v5)

    qdirs = {os.path.basename(d).lower(): d for d in glob.glob(os.path.join(a.history, "*")) if os.path.isdir(d)}
    data = {}
    for q, d in qdirs.items():
        data[q] = {t: [cm.load(x, P) for x in runs(d, t)] for t in (BF_RUN, BASE, "bloomfaas-nfs")}
        print(f"loaded {q}", file=sys.stderr)

    os.makedirs(a.out, exist_ok=True)
    allrows = []
    for q in a.queries:
        R = data[q]
        K = cm.calibrate([r for q2, R2 in data.items() if q2 != q for rs in R2.values() for r in rs], P)  # LOQO
        pairs = list(zip(R[BF_RUN], R[BASE]))
        bfs = bloom_filters(*pairs[0])
        names = [j["name"] for j in bfs]
        rows = []
        mp0 = pairs[0][0]["dag"].get("max_parallel")
        for mp, mask in itertools.product(a.parallel or [mp0], itertools.product((0, 1), repeat=len(names))):
            on = [n for n, b in zip(names, mask) if b]
            est = []
            for bf, nobf in pairs:
                run = synth(bf, nobf, bloom_filters(bf, nobf), on)
                run["dag"] = {**run["dag"], "max_parallel": mp}
                est.append(estimate(run, K, P))
            rows.append({"query": q, "mp": mp, "plan": "".join(map(str, mask)) + (f"@{mp}" if a.parallel else ""),
                         "n_on": sum(mask), "on": " ".join(n.replace("--mergebf", "") for n in on),
                         "T": statistics.median(t for t, _ in est), "C": statistics.median(c for _, c in est)})
        # measured extremes for the check: all on = BF run, all off = no-BF run
        meas = {} if a.parallel else {"1" * len(names): [cm.evaluate(bf, K, P)[1] for bf, _ in pairs],
                "0" * len(names): [cm.evaluate(nb, K, P)[1] for _, nb in pairs]} if not a.parallel else {}
        tmin, cmin = min(r["T"] for r in rows), min(r["C"] for r in rows)
        front = pareto(rows)
        for r in rows:
            r["pareto"] = r in front
            r["dist"] = math.hypot(r["T"] / tmin - 1, r["C"] / cmin - 1)
            if r["plan"] in meas:
                r["T_meas"] = statistics.median(x["T_meas"] for x in meas[r["plan"]])
                r["C_meas"] = statistics.median(x["C_meas"] for x in meas[r["plan"]])
        knee = min(rows, key=lambda r: r["dist"])
        for r in rows:
            r["knee"] = r is knee
        print(f"\n=== {q}: {len(names)} BFs, {len(rows)} plans, {len(front)} on the Pareto front")
        for j in bfs:
            print(f"  {j['name']:40} appliers={j['appliers']} phi={j['phi']:.3f} region={len(j['region'])} stages")
        for r in sorted(front if a.parallel else rows, key=lambda r: r["T"]):
            mark = ("P" if r["pareto"] else " ") + ("K" if r["knee"] else " ")
            ms = f"  meas T={r['T_meas']:.2f} C={r['C_meas']:.5f}" if "T_meas" in r else ""
            print(f"  {mark} {r['plan']}  T={r['T']:6.2f}s  C=${r['C']:.5f}  [{r['on']}]{ms}")
        allrows += rows

    keys = ["query", "mp", "plan", "n_on", "on", "T", "C", "pareto", "knee", "dist", "T_meas", "C_meas"]
    with open(os.path.join(a.out, "plans.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(allrows)
    print(f"\nsaved -> {a.out}/plans.csv")


if __name__ == "__main__":
    main()
