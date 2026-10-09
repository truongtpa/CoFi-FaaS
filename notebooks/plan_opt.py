"""Cost-time plan optimization: per-stage parallelism (max_size_mb -> p_v) and per-BF on/off.

For every query the knobs are
  * one bit per Bloom Filter (merge_bf step): pushed down or not,
  * max_size_mb of every scan / aggregate / broadcast_join stage, which sets p_v the same way the Runner does
    (partition_tasks over row groups for scans, over the upstream output files for combines and joins).
A plan is estimated from the measured BF and no-BF runs: plan_space.synth picks the demand of each stage for the BF
subset, resize() re-derives p_v and rescales the per-function demand (bytes are kept, requests / invocations /
BF file reads follow p_v), and the v5 cost model predicts T and C. Coordinate descent on
lambda*T/T0 + (1-lambda)*C/C0 for a grid of lambda, every evaluated plan kept, gives the Pareto front; the knee is
the front point closest to (T_min, C_min) after normalisation.

    .venv/bin/python notebooks/plan_opt.py history --queries q5 q20 q21 --out notebooks/cost-report/tpch100-v5/opt
"""
import os
import sys
import csv
import json
import glob
import math
import argparse
import statistics
import importlib.util

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import cost_model as cm
from cost_model_batch import runs
from plan_space import BF_RUN, BASE, bloom_filters, synth, estimate, pareto
from operations.runner.Helpers import partition_tasks
from operations.libs.ParquetS3Utils import parse_select_columns
from operations.runner.FlowParser import FlowParser

MPS = []          # max_parallel tried per stage (--mps 10 20 30 50 70); empty = keep the run's (70)
DEFAULT_MP = 70
SIZES = [10, 25, 50, 100]   # max_size_mb tried per stage: the model is calibrated near 50 MB; 200+ was measured
                            # slower than estimated (q9, history-plans), so larger sizes need --sizes explicitly
DEFAULT_MB = 50
LAMBDAS = [i / 10 for i in range(11)]
MAX_FN_MB = 200   # largest per-function input kept: ~163 MB is the most a 2 GiB function has read in the history
MAX_P = 0         # most functions per stage (0 = no limit); a stage may keep the p it has at the default size
MB = 2 ** 20


def bench():
    spec = importlib.util.spec_from_file_location("rs", os.path.join(ROOT, "benchmarks", "run-seek15.py"))
    rs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(rs)
    return rs


def pipeline(q, tmp):
    """Steps of the rendered BF-on S3 pipeline (seed1), keyed by name."""
    rs = bench()
    rs.TMP_YAML = tmp
    cwd = os.getcwd()
    os.chdir(ROOT)
    try:
        import contextlib, io
        with contextlib.redirect_stdout(io.StringIO()):
            rs.render_tmp_yaml(f"benchmarks/tpch-parms/{q}.yml.j2", rs.CONFIGS["bloom_faas_v2"],
                               rs.get_tpch_parms(rs.load_params(), q, "seed1"))
        return {s["name"]: s for s in FlowParser(tmp).get_steps()}
    finally:
        os.chdir(cwd)


class Sizer:
    """p_v for a given max_size_mb, mirroring Runner._execute_step."""

    def __init__(self, steps, catalog):
        self.steps = steps
        self.files = {d["path"].rstrip("/"): d["files"] for d in catalog["datasets"].values()}

    def scan(self, name, x):
        s = self.steps[name]
        prm = s["params"]
        files = self.files.get(str(s["inputs"]).rstrip("/"), [])
        cols = parse_select_columns(prm.get("select"), prm.get("sql_clauses"))   # same call as the Runner
        return sum(len(partition_tasks([f], max_size_mb=x, split_row_groups=True, columns=cols,
                                       single_file=bool(prm.get("single_row_group_per_task")))) for f in files)

    @staticmethod
    def groups(n_files, total_mb, x):
        if n_files <= 0:
            return 1
        each = total_mb / n_files
        return len(partition_tasks([{"full_path": f"f{i}", "size_mb": each} for i in range(n_files)], max_size_mb=x))

    def default(self, name):
        return self.steps[name].get("max_size_mb") or DEFAULT_MB

    def tunable(self, run):
        return [s["name"] for s in run["steps"] if s["func"] in ("scan", "aggregate", "broadcast_join")
                and s["name"] in self.steps and not (s["func"] == "aggregate" and self.steps[s["name"]]["func"] != "aggregate")]

    def p_of(self, run, name, x, P_new):
        """New p_v of stage `name` at max_size_mb=x, given the new p of its upstream stages (P_new)."""
        s, m = run["by_name"][name], run["m"][name]
        if s["func"] == "scan":
            return self.scan(name, x)
        data_deps = [d for d in s["deps"] if run["by_name"].get(d, {}).get("func") != "merge_bf"]
        if s["func"] == "aggregate" and data_deps:
            d = data_deps[0]
            return self.groups(P_new.get(d, run["m"][d]["p"]), run["m"][d]["D_out"] / MB, x)
        if s["func"] == "broadcast_join" and len(data_deps) >= 2:
            b, pr = data_deps[0], data_deps[1]
            return (self.groups(P_new.get(b, run["m"][b]["p"]), run["m"][b]["D_out"] / MB, x) *
                    self.groups(P_new.get(pr, run["m"][pr]["p"]), run["m"][pr]["D_out"] / MB, x))
        return m["p"]


def resize(run, sizer, X, bfs):
    """Copy of `run` with max_size_mb X[stage] applied to the tunable stages; returns (run, {stage: p})."""
    out = {**run, "m": {}}
    P_new, ratio_in = {}, {}
    tun = set(sizer.tunable(run))
    for s in run["steps"]:
        n, m0 = s["name"], run["m"][s["name"]]
        m = dict(m0)
        p0 = m0["p"]
        if n in tun:
            x0, x = sizer.default(n), X.get(n, sizer.default(n))
            sim0 = sizer.p_of(run, n, x0, {})
            sim = sizer.p_of(run, n, x, P_new)
            p = max(1, round(p0 * sim / sim0)) if sim0 else p0   # keep the measured p at the default size
        else:
            p = p0
        P_new[n] = p
        r = p / p0 if p0 else 1.0
        data_deps = [d for d in s["deps"] if run["by_name"].get(d, {}).get("func") != "merge_bf"]
        fin = [P_new[d] / run["m"][d]["p"] for d in data_deps if d in P_new and run["m"][d]["p"]]
        rin = statistics.mean(fin) if fin else 1.0        # change in the number of input files
        if s["func"] == "scan":                           # footer + one range GET per column chunk
            dq = 2 * (p - p0)
            m["Q_rx"] = max(m["Q_rx"] + dq, p)
            m["Q_in_s3"] = max(m["Q_in_s3"] + dq, p)
        else:
            m["Q_impl"] *= rin
            m["Q_in_s3"] *= rin
        m["Q_wx"] *= r                                     # one output file per function
        m["Q_out_s3"] *= r
        m["bf_impl_b"] *= r                                # each function reads / writes its own BF file
        m["bf_impl_n"] *= r
        if s["func"] == "merge_bf":
            m["bf_rx_b"] *= rin
            m["bf_rx_n"] *= rin
            m["Q_in_nfs"] *= rin
            m["D_in_nfs"] *= rin
        m["p"] = p
        out["m"][n] = m
    load = max(m["D_data"] / m["p"] / MB for m in out["m"].values())
    # per-stage max_parallel (keys "mp:<stage>"): what cost_model.conc / makespan read from the stage
    mp = {k[3:]: v for k, v in X.items() if k.startswith("mp:")}
    if mp:
        out["steps"] = [dict(s, max_parallel=mp[s["name"]]) if s["name"] in mp else s for s in run["steps"]]
        out["by_name"] = {s["name"]: s for s in out["steps"]}
    return out, P_new, load


class Query:
    def __init__(self, q, data, P, sizer, seeds=5):
        R = data[q]
        self.q, self.P, self.sizer = q, P, sizer
        self.K = cm.calibrate([r for q2, R2 in data.items() if q2 != q for rs in R2.values() for r in rs], P)  # LOQO
        pairs = list(zip(R[BF_RUN], R[BASE]))
        step = max(1, len(pairs) // seeds)
        self.pairs = pairs[::step]                         # one run per seed
        self.bfs = bloom_filters(*self.pairs[0])
        self.names = [j["name"] for j in self.bfs]
        self.cache, self.base_cache = {}, {}
        self.tun = sizer.tunable(synth(*self.pairs[0], self.bfs, self.names))

    def dflt(self, k):
        return DEFAULT_MP if k.startswith("mp:") else self.sizer.default(k)

    def plan_key(self, on, X):
        return (tuple(sorted(on)), tuple(sorted((k, v) for k, v in X.items() if v != self.dflt(k))))

    def eval(self, on, X):
        key = self.plan_key(on, X)
        if key not in self.cache:
            est, ps, load, over = [], None, 0.0, False
            for i, (bf, nobf) in enumerate(self.pairs):
                bk = (i, tuple(sorted(on)))
                if bk not in self.base_cache:
                    self.base_cache[bk] = synth(bf, nobf, bloom_filters(bf, nobf), list(on))
                run, ps, ld = resize(self.base_cache[bk], self.sizer, X, self.bfs)
                load = max(load, ld)
                base = self.base_cache[bk]["m"]
                over = over or (MAX_P > 0 and any(p > max(MAX_P, base[n]["p"]) for n, p in ps.items() if n in base))
                est.append(estimate(run, self.K, self.P))
            self.cache[key] = {"on": sorted(on), "X": {k: v for k, v in X.items() if v != self.dflt(k)},
                               "p": ps, "T": statistics.median(t for t, _ in est),
                               "C": statistics.median(c for _, c in est), "max_fn_mb": load,
                               "ok": (load <= MAX_FN_MB and not over) or not self.plan_key(on, X)[1]}
        return self.cache[key]

    def descend(self, lam, T0, C0, on, X, passes=4):
        f = lambda r: lam * r["T"] / T0 + (1 - lam) * r["C"] / C0 if r["ok"] else math.inf
        on, X = set(on), dict(X)
        best = self.eval(on, X)
        for _ in range(passes):
            improved = False
            for j in self.names:
                cand = on ^ {j}
                r = self.eval(cand, X)
                if f(r) < f(best) - 1e-12:
                    on, best, improved = cand, r, True
            for n in self.tun:
                for x in SIZES:
                    cand = {**X, n: x}
                    r = self.eval(on, cand)
                    if f(r) < f(best) - 1e-12:
                        X, best, improved = cand, r, True
                for mp in MPS:
                    cand = {**X, f"mp:{n}": mp}
                    r = self.eval(on, cand)
                    if f(r) < f(best) - 1e-12:
                        X, best, improved = cand, r, True
            if not improved:
                break
        return best

    def optimize(self):
        default = self.eval(self.names, {})
        T0, C0 = default["T"], default["C"]
        for lam in LAMBDAS:
            self.descend(lam, T0, C0, self.names, {})
            self.descend(lam, T0, C0, [], {})
        allp = [r for r in self.cache.values() if r["ok"]]
        front = sorted({(r["T"], r["C"]): r for r in pareto(allp)}.values(), key=lambda r: r["T"])
        tmin, cmin = min(r["T"] for r in front), min(r["C"] for r in front)
        knee = min(front, key=lambda r: math.hypot(r["T"] / tmin - 1, r["C"] / cmin - 1))
        return {"default": default, "knee": knee, "min_T": front[0], "min_C": front[-1], "front": front, "all": allp}


def plan_json(q, name, r, sizer, names):
    return {"query": q, "plan": name, "T_est": r["T"], "C_est": r["C"], "max_fn_mb": r["max_fn_mb"],
            "bf_off": [n for n in names if n not in r["on"]],
            "max_size_mb": {k: v for k, v in r["X"].items() if not k.startswith("mp:")},
            "max_parallel": {k[3:]: v for k, v in r["X"].items() if k.startswith("mp:")}, "p": r["p"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("history", nargs="?", default="history")
    ap.add_argument("--queries", nargs="*")
    ap.add_argument("--out", default="notebooks/cost-report/tpch100-v5/opt")
    ap.add_argument("--sizes", type=int, nargs="*", default=SIZES, help="max_size_mb values tried per stage")
    ap.add_argument("--mps", type=int, nargs="*", default=MPS, help="max_parallel values tried per stage")
    ap.add_argument("--minio", help="minio.json from notebooks/minio_model.py: S3 reads from the MinIO model")
    ap.add_argument("--front-k", type=int, default=0,
                    help="also export K plans spread along the Pareto front (front_01 = fastest ... front_K = cheapest) "
                         "to run them and check the front")
    ap.add_argument("--max-fn-mb", type=float, default=MAX_FN_MB, help="drop plans with a function reading more")
    ap.add_argument("--model-flags", nargs="*", default=[],
                    help="extra cost-model flags on top of the v5 ones, e.g. agg_bw c0 (see cost_model.PARAMS)")
    ap.add_argument("--max-p", type=int, default=MAX_P,
                    help="drop plans splitting a stage into more functions than this (or than at the default size)")
    ap.add_argument("--metadata", default=os.path.join(ROOT, "benchmarks/metadata/tpch-100.json"))
    a = ap.parse_args()
    globals()["MAX_FN_MB"] = a.max_fn_mb
    globals()["MAX_P"] = a.max_p
    globals()["SIZES"] = a.sizes
    globals()["MPS"] = a.mps
    P = cm.load_params()
    P.update(serial=True, waves=True, disp_median=True, contention=True)   # best flags (cost-report v5)
    P.update({k: True for k in a.model_flags})
    if a.minio:
        P["minio"] = {**json.load(open(a.minio)), "k_cap": 66}   # ~66 concurrent instances measured on the cluster
    catalog = json.load(open(a.metadata))
    qdirs = {os.path.basename(d).lower(): d for d in glob.glob(os.path.join(a.history, "*")) if os.path.isdir(d)}
    data = {q: {t: [cm.load(x, P) for x in runs(d, t)] for t in (BF_RUN, BASE, "bloomfaas-nfs")} for q, d in qdirs.items()}
    data = {q: R for q, R in data.items() if R[BF_RUN] and R[BASE]}
    queries = a.queries or sorted(data, key=lambda q: int(q[1:]))
    os.makedirs(a.out, exist_ok=True)
    tmp = os.path.join(a.out, "_tmp.yml.j2")

    plans, fronts, check = [], [], []
    for q in queries:
        sizer = Sizer(pipeline(q, tmp), catalog)
        Q = Query(q, data, P, sizer)
        # p_v check: simulated p at the default max_size_mb vs measured p in the BF run
        bf = Q.pairs[0][0]
        for n in sizer.tunable(bf):
            check.append({"query": q, "stage": n, "p_meas": bf["m"][n]["p"], "p_sim": sizer.p_of(bf, n, sizer.default(n), {})})
        res = Q.optimize()
        for name in ("default", "knee", "min_T", "min_C"):
            plans.append(plan_json(q, name, res[name], sizer, Q.names))
        if a.front_k:   # K front points evenly spread by position along the front (sorted by T)
            F = res["front"]
            idx = sorted({round(i * (len(F) - 1) / max(a.front_k - 1, 1)) for i in range(a.front_k)})
            for j, i in enumerate(idx, 1):
                plans.append(plan_json(q, f"front_{j:02d}", F[i], sizer, Q.names))
        for r in res["all"]:
            fronts.append({"query": q, "T": r["T"], "C": r["C"], "pareto": r in res["front"], "n_on": len(r["on"]),
                           "n_resized": len(r["X"]), "knee": r is res["knee"], "default": r is res["default"]})
        d, k = res["default"], res["knee"]
        print(f"{q:4} plans={len(res['all']):4} front={len(res['front']):3}  default T={d['T']:6.2f} C={d['C']*1e3:7.3f}m$"
              f"  knee T={k['T']:6.2f} ({k['T']/d['T']-1:+.0%}) C={k['C']*1e3:7.3f}m$ ({k['C']/d['C']-1:+.0%})"
              f"  bf_off={len(Q.names) - len(k['on'])}/{len(Q.names)} resized={len(k['X'])}")
    os.remove(tmp)
    json.dump(plans, open(os.path.join(a.out, "plans.json"), "w"), indent=2)
    for fn, rows in (("points.csv", fronts), ("p-check.csv", check)):
        with open(os.path.join(a.out, fn), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
    ok = sum(c["p_meas"] == c["p_sim"] for c in check)
    print(f"p_v check: simulated == measured for {ok}/{len(check)} tunable stages")
    print(f"saved -> {a.out}/plans.json, points.csv, p-check.csv")


if __name__ == "__main__":
    main()
