import os
import sys
import csv
import json
import math
import heapq
import bisect
import argparse
from datetime import datetime

CLASSES = ("s3", "nfs")
JOINS = ("broadcast_join", "broadcast_join_pushdown", "hash_join", "range_join", "adaptive_join")
GB = 2 ** 30
MB = 2 ** 20

PARAMS = {
    "bw": {"s3": 100e6, "nfs": 200e6},
    "lat": {"s3": 0.01, "nfs": 0.001},
    "r_hash": 5e7,
    "mem_gb": 2.0,
    "cpu": 2.0,
    "calibrate_storage": ["s3", "nfs"],
    # per_instance: d_v = max_i d_{v,i} and C^C uses sum_i d_{v,i} from each function's own D_{v,i}, Q_{v,i}
    #               (paper eq. stage-processing-time / compute cost); False = balanced D_v/p_v, C^C = p_v d_v
    # merge_work:   W^{bf-merge} = p^b_j m_j bit-ORs, rate kappa^or calibrated on merge_bf t_comp
    "per_instance": False,
    "merge_work": False,
    # waves: the runner runs each stage through a pool of max_parallel workers, so d_v is the list-scheduling
    #        makespan of the p_v function durations on max_parallel slots (= ceil(p_v/max_parallel) d_v if balanced)
    # orch:  kappa^orch, runner gap between the last upstream stage finishing and this stage starting
    "waves": False,
    "orch": False,
    # serial: Runner.execute runs the stages one after another in pipeline order (RunnerDAG overlaps branches),
    #         so each stage also waits for the previous stage; T = sum_v d_v instead of the longest DAG path
    "serial": False,
    # rel_fit: fit kappa^cpu / qlat on relative error (rows weighted by 1/t) so small stages are not dominated by large
    #          ones; disp_median: kappa^disp = median dispatch (mean is pulled up by cold starts)
    "rel_fit": False,
    "disp_median": False,
    # contention: functions running at the same time share storage bandwidth. Time per byte = 1/bw + beta*k,
    #             k = concurrent functions (measured overlap when calibrating, min(p_v, max_parallel) when predicting).
    #             Applied to explicit S3 I/O (beta_s) and to the reads DuckDB does inside t_comp (beta per endpoint).
    "contention": False,
    "price": {
        "inv": 0.2e-6,
        "gbs": 16.6667e-6,
        "cpus": 0.0,
        "get": {"s3": 0.4e-6, "nfs": 0.0},
        "put": {"s3": 5e-6, "nfs": 0.0},
        "sto": {"s3": 0.023 / (30 * 24 * 3600), "nfs": 0.30 / (30 * 24 * 3600)},
        "net": 0.0,
    },
}


def load(folder, P):
    dag = json.load(open(os.path.join(folder, "cost-dag.json")))
    stages = {}
    for line in open(os.path.join(folder, "cost-stages.jsonl")):
        r = json.loads(line)
        stages[r["stage"]] = r
    steps = [s for s in dag["steps"] if s["func"] != "summary_clean" and s["name"] in stages]
    overlap([t for s in steps for t in stages[s["name"]]["tasks"]])
    return {"folder": folder, "dag": dag, "steps": steps, "by_name": {s["name"]: s for s in steps},
            "m": {s["name"]: measure(stages[s["name"]], P) for s in steps}}


def overlap(tasks):
    """t["k"] = number of tasks of the run (itself included) whose [start, end] overlaps this task's."""
    iv = [(ts(t["start"]), ts(t["end"])) for t in tasks]
    starts, ends = sorted(a for a, _ in iv), sorted(b for _, b in iv)
    for t, (a, b) in zip(tasks, iv):
        t["k"] = max(bisect.bisect_left(starts, b) - bisect.bisect_right(ends, a), 1)


def io(t, side, cls, key):
    return t.get(side, {}).get(cls, {}).get(key, 0)


def ts(x):
    return datetime.fromisoformat(x).timestamp()


def measure(r, P):
    tk = r["tasks"]
    m = {
        "p": len(tk), "cpu": tk[0].get("cpu") or P["cpu"], "mem_gb": (tk[0].get("mem") or P["mem_gb"] * GB) / GB,
        "endpoint": tk[0].get("endpoint"), "tasks": tk,
        "d_fn": max(t["t_total"] + t["disp"] for t in tk), "d_wall": r["wall_s"],
        "billed": sum(t["t_total"] for t in tk), "disp": sum(t["disp"] for t in tk) / len(tk),
        "t_read": sum(t["t_read"] for t in tk) / len(tk), "t_comp": sum(t["t_comp"] for t in tk) / len(tk),
        "t_write": sum(t["t_write"] for t in tk) / len(tk),
        "rows_in": sum(t.get("rows_in", 0) for t in tk), "rows_out": sum(t.get("rows_out", 0) for t in tk),
        "rows_build": sum(t.get("rows_build", 0) for t in tk), "rows_probe": sum(t.get("rows_probe", 0) for t in tk),
        "cold": sum(1 for t in tk if t.get("cold")),
        "start": min(ts(t["start"]) for t in tk), "end": max(ts(t["end"]) for t in tk),
        "bf": next((t["bf"] for t in tk if t.get("bf")), None),
        "bf_read_n": sum(t["bf_read"]["n"] for t in tk), "bf_read_b": sum(t["bf_read"]["bytes"] for t in tk),
        "bf_write_n": sum(t["bf_write"]["n"] for t in tk), "bf_write_b": sum(t["bf_write"]["bytes"] for t in tk),
    }
    for c in CLASSES:
        for side in ("in", "out"):
            m[f"D_{side}_{c}"] = sum(io(t, side, c, "bytes") for t in tk)
            m[f"Q_{side}_{c}"] = sum(io(t, side, c, "req") for t in tk)
    m["D_in"] = sum(m[f"D_in_{c}"] for c in CLASSES)
    m["D_out"] = sum(m[f"D_out_{c}"] for c in CLASSES)
    f = [feat(t, m["endpoint"]) for t in tk]
    for k in f[0]:
        if k != "mode":
            m[k] = sum(x[k] for x in f)
    m["mode"] = f[0]["mode"]
    return m


def data_io(t, side, c):
    # BF files are counted in the same in/out totals; they live on NFS (folder_bf / bf_path under /mnt)
    b, q = io(t, side, c, "bytes"), io(t, side, c, "req")
    if c == "nfs":
        bf = t.get("bf_read" if side == "in" else "bf_write") or {"bytes": 0, "n": 0}
        b, q = b - bf["bytes"], q - bf["n"]
    return max(b, 0), max(q, 0)


def feat(t, endpoint):
    """Split a task's I/O into what is timed explicitly (t_read / t_write) and what DuckDB does inside t_comp."""
    scan, merge = endpoint.startswith("/scan"), endpoint == "/merge-bf"
    s3i, nfsi = data_io(t, "in", "s3"), data_io(t, "in", "nfs")
    s3o, nfso = data_io(t, "out", "s3"), data_io(t, "out", "nfs")
    up = endpoint != "/hash-partition"  # hash_partition uploads buckets in its own thread pool, untimed
    bfr, bfw = t.get("bf_read") or {"bytes": 0, "n": 0}, t.get("bf_write") or {"bytes": 0, "n": 0}
    return {
        "mode": "nfs" if nfsi[0] + nfso[0] > 0 else "s3",
        "D_data": s3i[0] + nfsi[0],
        "D_rx": s3i[0] if scan else 0, "Q_rx": s3i[1] if scan else 0,
        "Q_impl": (nfsi[1] if scan else s3i[1] + nfsi[1]),
        "D_wx": s3o[0] if up else 0, "Q_wx": s3o[1] if up else 0,
        "bf_rx_b": bfr["bytes"] if merge else 0, "bf_rx_n": bfr["n"] if merge else 0,
        "bf_wx_b": bfw["bytes"] if merge else 0, "bf_wx_n": bfw["n"] if merge else 0,
        "bf_impl_b": 0 if merge else bfr["bytes"] + bfw["bytes"], "bf_impl_n": 0 if merge else bfr["n"] + bfw["n"],
        "W_bf": task_bf_work(t),
        "W_merge": 8 * bfr["bytes"] if merge else 0,
    }


def task_bf_work(t):
    bf = t.get("bf") or {}
    k = k_of(bf.get("error_rate"))
    return k * (t.get("rows_out") or 0) if bf.get("role") == "build" else k * (t.get("rows_in") or 0) if bf.get("role") == "apply" else 0


def lstsq2(points, rel=False):
    if rel:
        points = [(d / t, q / t, 1.0) for d, q, t in points if t > 0]
    a11 = sum(d * d for d, q, t in points)
    a12 = sum(d * q for d, q, t in points)
    a22 = sum(q * q for d, q, t in points)
    b1 = sum(d * t for d, q, t in points)
    b2 = sum(q * t for d, q, t in points)
    det = a11 * a22 - a12 * a12
    if not points or abs(det) < 1e-12:
        return None
    return (b1 * a22 - b2 * a12) / det, (a11 * b2 - a12 * b1) / det


def io_time(bw, lat, d, q, cls, k=1, beta=None):
    return d / bw[cls] + q * lat[cls] + (d * beta.get(cls, 0.0) * k if beta else 0.0)


def nnls(rows, y, rel=False):
    """Non-negative least squares; rel=True weights each row by 1/y (relative error)."""
    import itertools
    import numpy as np
    A, b = np.array(rows, float), np.array(y, float)
    if rel:
        A, b = A / b[:, None], np.ones_like(b)
    n, best, best_err = A.shape[1], np.zeros(A.shape[1]), float(b @ b)
    for r in range(1, n + 1):  # exact NNLS for a handful of features: best non-negative fit over all supports
        for cols in itertools.combinations(range(n), r):
            x = np.linalg.lstsq(A[:, cols], b, rcond=None)[0]
            if (x >= 0).all():
                full = np.zeros(n)
                full[list(cols)] = x
                err = float(((A @ full - b) ** 2).sum())
                if err < best_err:
                    best, best_err = full, err
    return [float(v) for v in best]


def fit_io(pts, bw, lat):
    fit = lstsq2(pts)
    if fit and fit[0] > 0 and fit[1] >= 0:
        return 1 / fit[0], fit[1]
    d, t = sum(p[0] for p in pts), sum(p[2] for p in pts)
    return (d / t, 0.0) if d > 0 and t > 0 else (bw, lat)


def calibrate(runs, P):
    """kappa from one or more runs. S3: scan reads + timed uploads. NFS: BF load/save in merge_bf.
    Per (endpoint, mode): t_comp = D_data/(c*rate) + Q_impl*lat_impl, which absorbs the DuckDB reads and NFS writes."""
    runs = runs if isinstance(runs, list) else [runs]
    bw, lat = dict(P["bw"]), dict(P["lat"])
    tasks = [(m, t, feat(t, m["endpoint"])) for r in runs for m in r["m"].values() for t in m["tasks"]]
    beta, rk = {}, {}
    for c in P["calibrate_storage"]:
        if P.get("contention") and c == "s3":
            rows, y = [], []
            for m, t, f in tasks:
                for d, q, tt in ((f["D_rx"], f["Q_rx"], t["t_read"]), (f["D_wx"], f["Q_wx"], t["t_write"])):
                    if tt > 0 and d > 0:
                        rows.append((d, d * t["k"], q))
                        y.append(tt)
            if rows:
                a, beta[c], lat[c] = nnls(rows, y, P.get("rel_fit"))
                bw[c] = 1 / a if a > 0 else 1e15
            continue
        pts = []
        for m, t, f in tasks:
            if c == "s3":
                if t["t_read"] > 0 and f["D_rx"] > 0:
                    pts.append((f["D_rx"], f["Q_rx"], t["t_read"]))
                if t["t_write"] > 0 and f["D_wx"] > 0:
                    pts.append((f["D_wx"], f["Q_wx"], t["t_write"]))
            else:
                if t["t_read"] > 0 and f["bf_rx_b"] > 0:
                    pts.append((f["bf_rx_b"], f["bf_rx_n"], t["t_read"]))
                if t["t_write"] > 0 and f["bf_wx_b"] > 0:
                    pts.append((f["bf_wx_b"], f["bf_wx_n"], t["t_write"]))
        if pts:
            bw[c], lat[c] = fit_io(pts, bw[c], lat[c])
    groups, disp = {}, []
    for m, t, f in tasks:
        y = t["t_comp"] - f["W_bf"] / (m["cpu"] * P["r_hash"]) - io_time(bw, lat, f["bf_impl_b"], f["bf_impl_n"], "nfs")
        if f["D_data"] > 0:
            groups.setdefault(f"{m['endpoint']}@{f['mode']}", []).append(
                (f["D_data"] / m["cpu"], f["Q_impl"], max(y, 1e-3), f["D_data"] * t["k"]))
        disp.append(t["disp"])
    mt = [(m["cpu"], f["W_merge"], t["t_comp"]) for m, t, f in tasks if f["W_merge"] > 0]
    r_or = sum(w / c for c, w, _ in mt) / sum(y for _, _, y in mt) if mt and sum(y for _, _, y in mt) > 0 else 0.0
    gaps = [stage_gap(r, s, P.get("serial")) for r in runs for s in r["steps"]]
    rate, qlat, c0 = {}, {}, {}
    for key, pts in groups.items():
        if P.get("contention"):
            if P.get("c0"):   # + a fixed time per function (engine start, metadata), whatever its data size
                a, rk[key], qlat[key], c0[key] = nnls([(d, dk, q, 1.0) for d, q, _, dk in pts],
                                                      [t for _, _, t, _ in pts], P.get("rel_fit"))
            else:
                a, rk[key], qlat[key] = nnls([(d, dk, q) for d, q, _, dk in pts], [t for _, _, t, _ in pts], P.get("rel_fit"))
            rate[key] = 1 / a if a > 0 else 1e15
            continue
        pts = [x[:3] for x in pts]
        fit = lstsq2(pts, P.get("rel_fit"))
        if fit and fit[0] > 0 and fit[1] >= 0:
            rate[key], qlat[key] = 1 / fit[0], fit[1]
        else:
            rate[key], qlat[key] = sum(p[0] for p in pts) / sum(p[2] for p in pts), 0.0
    # agg_bw: the cluster's aggregate data rate (S3/MinIO + network) caps a stage, however it is split: high quantile
    # of measured stage throughput (D_in + D_out) / wall over stages that move enough data to reach it
    stg = [r["m"][s["name"]] for r in runs for s in r["steps"]]
    thr = sorted((m["D_in"] + m["D_out"]) / m["d_wall"] for m in stg if m["d_wall"] > 0 and m["D_in"] + m["D_out"] > 256 * MB)
    agg_bw = thr[int(P.get("agg_q", 0.95) * (len(thr) - 1))] if thr else 0.0
    # tail: the stage waits for its slowest function; tau = median over stages of (max / median task time - 1)
    tails = []
    for m in stg:
        tt = sorted(t["t_total"] for t in m["tasks"])
        if len(tt) >= 10 and tt[len(tt) // 2] > 0:
            tails.append(tt[-1] / tt[len(tt) // 2] - 1)
    tau = sorted(tails)[len(tails) // 2] if tails else 0.0
    # rho: share of the concurrent functions that read at the same time, fitted on the scans' t_read (MinIO model)
    rho = 1.0
    if P.get("minio"):
        pts = [(f["D_rx"], f["Q_rx"], t["t_read"], min(m["p"], (r["dag"].get("max_parallel") or m["p"])))
               for r in runs for m in r["m"].values() if m["endpoint"].startswith("/scan")
               for t in m["tasks"] for f in [feat(t, m["endpoint"])] if f["D_rx"] > MB and t["t_read"] > 0]
        best = None
        for x in [i / 40 for i in range(1, 41)]:
            e = sorted(abs((d / minio_bw(1 + (k - 1) * x, P["minio"]) + q * lat["s3"]) / tr - 1) for d, q, tr, k in pts)
            e = e[len(e) // 2] if e else 0
            if best is None or e < best[0]:
                best = (e, x)
        rho = best[1] if best else 1.0
    return {"bw": bw, "lat": lat, "rate": rate, "qlat": qlat, "beta": beta, "rk": rk, "agg_bw": agg_bw, "tau": tau, "c0": c0,
            "rho": rho,
            "disp": (sorted(disp)[len(disp) // 2] if P.get("disp_median") else sum(disp) / len(disp)) if disp else 0.0, "r_hash": P["r_hash"], "r_or": r_or,
            "orch": sum(gaps) / len(gaps) if gaps else 0.0}


def preds(run, s, serial):
    """Stages that must finish before s starts: its DAG deps, plus the previous stage when the runner is serial."""
    i = run["steps"].index(s)
    return list(s["deps"]) + ([run["steps"][i - 1]["name"]] if serial and i else [])


def stage_gap(run, s, serial=False):
    """Measured time between the stage becoming ready (preds done, or the query start) and its first task starting."""
    M = run["m"]
    ready = max([M[d]["end"] for d in preds(run, s, serial) if d in M] or [min(m["start"] for m in M.values())])
    return max(M[s["name"]]["start"] - ready, 0.0)


def makespan(durations, slots):
    """List scheduling in submission order on `slots` workers."""
    if not slots or len(durations) <= slots:
        return max(durations, default=0.0)
    free = [0.0] * slots
    for d in durations:
        heapq.heappush(free, heapq.heappop(free) + d)
    return max(free)


def k_of(err):
    return max(1, round(-math.log2(float(err or 0.001))))


def bf_work(m, step):
    bf = m["bf"] or {}
    k = k_of(bf.get("error_rate") or step["params"].get("error_rate"))
    if bf.get("role") == "build":
        return k * m["rows_out"]
    if bf.get("role") == "apply":
        return k * m["rows_in"]
    return 0.0


def rate_key(K, m):
    for mode in (m["mode"], "s3" if m["mode"] == "nfs" else "nfs"):
        key = f"{m['endpoint']}@{mode}"
        if key in K["rate"]:
            return key
    return None


def stage_mp(run, s):
    """max_parallel of a stage: its own override (cost-dag step "max_parallel") or the run's."""
    return (s or {}).get("max_parallel") or run["dag"].get("max_parallel")


def conc(run, m, P, s=None):
    """Predicted number of functions running at once in this stage: min(p_v, max_parallel_v)."""
    mp = stage_mp(run, s) or m["p"]
    return min(m["p"], mp) if P.get("contention") else 1


def minio_bw(k, M):
    """Bytes/s one function gets from MinIO when k functions read at once (notebooks/minio_model.py):
    A(k) / k with A(k) = min(k * B1, Bmax / (1 + gamma * k)), k capped at the cluster's concurrent instances."""
    k = max(1.0, min(k, M.get("k_cap", 1e9)))
    return min(k * M["B1_MBps"], M["Bmax_MBps"] / (1 + M["gamma"] * k)) / k * MB


def s3_read(K, d, q, k, P, beta):
    """S3 read time of one function: the MinIO model if given, else the calibrated bw/lat (+ contention).
    With MinIO, only a share rho of the functions running at once are reading (the others compute or write)."""
    if P.get("minio"):
        k_read = 1 + (k - 1) * K.get("rho", 1.0)
        return d / minio_bw(k_read, P["minio"]) + q * K["lat"]["s3"]
    return io_time(K["bw"], K["lat"], d, q, "s3", k, beta)


def predict_task(m, f, K, P, k=1):
    """d_{v,i} of one function instance from its own D_{v,i}, Q_{v,i}, W_{v,i}."""
    c, beta = m["cpu"], K.get("beta") if P.get("contention") else None
    t_in = s3_read(K, f["D_rx"], f["Q_rx"], k, P, beta) + io_time(K["bw"], K["lat"], f["bf_rx_b"], f["bf_rx_n"], "nfs")
    t_out = io_time(K["bw"], K["lat"], f["D_wx"], f["Q_wx"], "s3", k, beta) + io_time(K["bw"], K["lat"], f["bf_wx_b"], f["bf_wx_n"], "nfs")
    key = rate_key(K, m)
    t_c = f["D_data"] / (c * K["rate"][key]) + f["Q_impl"] * K["qlat"][key] + K.get("c0", {}).get(key, 0.0) if key else 0.0
    if key and beta is not None:
        t_c += f["D_data"] * K["rk"].get(key, 0.0) * k
    t_bf = f["W_bf"] / (c * K["r_hash"]) + io_time(K["bw"], K["lat"], f["bf_impl_b"], f["bf_impl_n"], "nfs")
    if P.get("merge_work") and K.get("r_or"):
        t_bf += f["W_merge"] / (c * K["r_or"])
    return {"t_in": t_in, "t_c": t_c + t_bf, "t_bf": t_bf, "t_out": t_out, "d": t_in + t_c + t_bf + t_out + K["disp"]}


def predict(run, K, P=PARAMS):
    pred = {}
    for s in run["steps"]:
        m = run["m"][s["name"]]
        p, c = m["p"], m["cpu"]
        if P.get("per_instance"):
            ti = [predict_task(m, feat(t, m["endpoint"]), K, P, conc(run, m, P, s)) for t in m["tasks"]]
            slow = max(ti, key=lambda x: x["d"])
            pred[s["name"]] = {**slow, "billed": sum(x["d"] for x in ti)}
            durations = [x["d"] for x in ti]
        else:
            k, beta = conc(run, m, P, s), K.get("beta") if P.get("contention") else None
            t_in = s3_read(K, m["D_rx"] / p, m["Q_rx"] / p, k, P, beta) + \
                io_time(K["bw"], K["lat"], m["bf_rx_b"] / p, m["bf_rx_n"] / p, "nfs")
            t_out = io_time(K["bw"], K["lat"], m["D_wx"] / p, m["Q_wx"] / p, "s3", k, beta) + \
                io_time(K["bw"], K["lat"], m["bf_wx_b"] / p, m["bf_wx_n"] / p, "nfs")
            key = rate_key(K, m)
            t_c = (m["D_data"] / p) / (c * K["rate"][key]) + (m["Q_impl"] / p) * K["qlat"][key] + \
                K.get("c0", {}).get(key, 0.0) if key else 0.0
            if key and beta is not None:
                t_c += (m["D_data"] / p) * K["rk"].get(key, 0.0) * k
            t_bf = m["W_bf"] / p / (c * K["r_hash"]) + io_time(K["bw"], K["lat"], m["bf_impl_b"] / p, m["bf_impl_n"] / p, "nfs")
            if P.get("merge_work") and K.get("r_or"):
                t_bf += m["W_merge"] / p / (c * K["r_or"])
            d = t_in + t_c + t_bf + t_out + K["disp"]
            pred[s["name"]] = {"t_in": t_in, "t_c": t_c + t_bf, "t_bf": t_bf, "t_out": t_out, "d": d, "billed": p * d}
            durations = [d] * p
        # billing stays sum_i d_{v,i}: queueing in the runner and its scheduling gap are not billed
        if P.get("waves"):
            pred[s["name"]]["d"] = makespan(durations, stage_mp(run, s))
        if P.get("tail") and p > 1:   # the last function to finish runs (1 + tau) times the typical one
            pred[s["name"]]["d"] += K.get("tau", 0.0) * sorted(durations)[len(durations) // 2]
        if P.get("agg_bw") and K.get("agg_bw"):   # cannot move the stage's data faster than the cluster allows
            pred[s["name"]]["d"] = max(pred[s["name"]]["d"], (m["D_in"] + m["D_out"]) / K["agg_bw"] + K["disp"])
        if P.get("orch"):
            pred[s["name"]]["d"] += K.get("orch", 0.0)
    return pred


def critical_path(run, dur, serial=False):
    eft = {}
    for s in run["steps"]:
        eft[s["name"]] = max([eft[d] for d in preds(run, s, serial) if d in eft] or [0.0]) + dur[s["name"]]
    return max(eft.values()), eft


def cost(m, billed, t_ret, P):
    pr = P["price"]
    ci = sum(m[f"Q_in_{c}"] * pr["get"][c] for c in CLASSES)
    cc = m["p"] * pr["inv"] + billed * (m["mem_gb"] * pr["gbs"] + m["cpu"] * pr["cpus"])
    co = sum(m[f"Q_out_{c}"] * pr["put"][c] + m[f"D_out_{c}"] / GB * t_ret * pr["sto"][c] for c in CLASSES)
    return ci, cc, co


def pct(model, meas):
    return (model - meas) / meas * 100 if meas else float("nan")


def evaluate(run, K, P):
    pred = predict(run, K, P)
    ser = P.get("serial")
    T_model, eft = critical_path(run, {n: v["d"] for n, v in pred.items()}, ser)
    T_cp_fn, _ = critical_path(run, {n: m["d_fn"] for n, m in run["m"].items()}, ser)
    T_cp_wall, _ = critical_path(run, {n: m["d_wall"] for n, m in run["m"].items()}, ser)
    q_start = min(m["start"] for m in run["m"].values())
    q_end = max(m["end"] for m in run["m"].values())
    rows = []
    for s in run["steps"]:
        n, m, pv = s["name"], run["m"][s["name"]], pred[s["name"]]
        cm = cost(m, pv["billed"], T_model - eft[n], P)
        cx = cost(m, m["billed"], q_end - m["end"], P)
        rows.append({
            "stage": n, "func": s["func"], "p": m["p"], "cpu": m["cpu"], "mem_gb": round(m["mem_gb"], 3),
            "cold": m["cold"], "rows_in": m["rows_in"], "rows_out": m["rows_out"],
            "D_in_s3": m["D_in_s3"], "Q_in_s3": m["Q_in_s3"], "D_in_nfs": m["D_in_nfs"], "Q_in_nfs": m["Q_in_nfs"],
            "D_out_s3": m["D_out_s3"], "Q_out_s3": m["Q_out_s3"], "D_out_nfs": m["D_out_nfs"], "Q_out_nfs": m["Q_out_nfs"],
            "meas_t_read": m["t_read"], "model_t_in": pv["t_in"],
            "meas_t_comp": m["t_comp"], "model_t_c": pv["t_c"], "model_t_bf": pv["t_bf"],
            "meas_t_write": m["t_write"], "model_t_out": pv["t_out"],
            "meas_disp": m["disp"], "model_disp": K["disp"],
            "meas_d_fn": m["d_fn"], "meas_d_wall": m["d_wall"], "model_d": pv["d"], "err_d_%": pct(pv["d"], m["d_fn"]),
            "meas_CI": cx[0], "model_CI": cm[0], "meas_CC": cx[1], "model_CC": cm[1],
            "meas_CO": cx[2], "model_CO": cm[2],
            "meas_C": sum(cx), "model_C": sum(cm), "err_C_%": pct(sum(cm), sum(cx)),
        })
    net = sum(m["D_in_s3"] + m["D_out_s3"] for m in run["m"].values()) / GB * P["price"]["net"]
    tot = {
        "T_meas": q_end - q_start, "T_cp_wall": T_cp_wall, "T_cp_fn": T_cp_fn, "T_model": T_model,
        "err_T_%": pct(T_model, q_end - q_start),
        "C_meas": sum(r["meas_C"] for r in rows) + net, "C_model": sum(r["model_C"] for r in rows) + net,
        "C_net": net,
    }
    tot["err_C_%"] = pct(tot["C_model"], tot["C_meas"])
    return rows, tot


def ancestors(run, name):
    deps = {x["name"]: x["deps"] for x in run["dag"]["steps"]}
    seen, stack = set(), [name]
    while stack:
        n = stack.pop()
        if n not in seen:
            seen.add(n)
            stack.extend(deps.get(n, []))
    return seen


def semijoin(run, builders, applier):
    """First join with the BF build side on one input and the BF-filtered scan on the other.
    Returns (join step, index of the applier-side dep); sigma = rows_out(join) / rows_out(applier-side dep)."""
    for s in run["steps"]:
        if s["func"] not in JOINS or len(s["deps"]) < 2:
            continue
        anc = [ancestors(run, d) for d in s["deps"]]
        for i, a in enumerate(anc):
            if applier in a and any(b in x for x in anc[:i] + anc[i + 1:] for b in builders):
                return s, i
    return None, None


def bf_report(bf, nobf, K):
    out = []
    for s in bf["steps"]:
        if s["func"] != "merge_bf":
            continue
        M = bf["m"][s["name"]]
        builds = [bf["m"][d] for d in s["deps"] if d in bf["m"]]
        appliers = [a for a in bf["steps"] if s["name"] in a["deps"] and a["params"].get("filter_by_bf")]
        A = [bf["m"][a["name"]] for a in appliers]
        p_b = sum(b["p"] for b in builds)
        p_p = sum(a["p"] for a in A)
        m_bits = M["bf_write_b"] * 8
        bbf = next((b["bf"] for b in builds if b["bf"]), {}) or {}
        err = bbf.get("error_rate") or 0.001
        k = k_of(err)
        n_est = float(bbf.get("est_elements") or 0)
        n_act = sum(b["rows_out"] for b in builds)
        f_model = (1 - math.exp(-k * n_act / m_bits)) ** k if m_bits else float("nan")
        R_model = 2 * p_b + 1 + p_p
        R_meas = sum(b["bf_write_n"] for b in builds) + M["bf_read_n"] + M["bf_write_n"] + sum(a["bf_read_n"] for a in A)
        X_meas = sum(b["bf_write_b"] for b in builds) + M["bf_read_b"] + M["bf_write_b"] + sum(a["bf_read_b"] for a in A)
        W_build = k * n_act
        W_apply = k * sum(a["rows_in"] for a in A)
        row = {
            "join": s["name"], "p_b": p_b, "p_p": p_p, "R_model": R_model, "R_meas": R_meas,
            "m_bits": m_bits, "m_bits_design": -n_est * math.log(err) / math.log(2) ** 2 if n_est else None,
            "k": k, "n_est": n_est, "n_act": n_act, "f_design": err, "f_model": f_model,
            "X_model": m_bits / 8 * R_model, "X_meas": X_meas,
            "W_build": W_build, "W_apply": W_apply,
            "t_bf_model_cpu_s": (W_build + W_apply) / K["r_hash"],
            "appliers": [],
        }
        for a in appliers:
            ab, an = bf["m"][a["name"]], (nobf["m"].get(a["name"]) if nobf else None)
            item = {"stage": a["name"], "rows_in": ab["rows_in"], "rows_out_bf": ab["rows_out"],
                    "D_out_bf": ab["D_out"], "bf_role_ok": (ab["bf"] or {}).get("role") == "apply"}
            if an:
                phi = ab["rows_out"] / an["rows_out"] if an["rows_out"] else float("nan")
                J, side = semijoin(nobf, s["deps"], a["name"])
                Jm = nobf["m"].get(J["name"]) if J else None
                Pm = nobf["m"].get(J["deps"][side]) if J else None
                sigma = min(Jm["rows_out"] / Pm["rows_out"], 1.0) if Jm and Pm and Pm["rows_out"] else float("nan")
                phi_model = sigma + (1 - sigma) * f_model
                d_cpu = lambda x: sum(t["t_comp"] for t in x["tasks"]) * x["cpu"]
                item.update({
                    "rows_out_nobf": an["rows_out"], "D_out_nobf": an["D_out"], "join_for_sigma": J["name"] if J else None,
                    "sigma_semi": sigma, "phi_meas": phi, "phi_model": phi_model, "err_phi_%": pct(phi_model, phi),
                    "f_implied": (phi - sigma) / (1 - sigma) if sigma == sigma and sigma < 1 else float("nan"),
                    "D_out_model": phi_model * an["D_out"], "err_D_out_%": pct(phi_model * an["D_out"], ab["D_out"]),
                    "delta_comp_cpu_s": d_cpu(ab) - d_cpu(an),
                })
            row["appliers"].append(item)
        out.append(row)
    return out


def fmt(v):
    if isinstance(v, float):
        return f"{v:.4g}"
    return str(v)


def table(rows, cols):
    w = {c: max(len(c), *(len(fmt(r.get(c))) for r in rows)) for c in cols}
    print("  ".join(c.ljust(w[c]) for c in cols))
    for r in rows:
        print("  ".join(fmt(r.get(c)).ljust(w[c]) for c in cols))


def save_csv(path, rows):
    if rows:
        with open(path, "w", newline="") as f:
            wr = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            wr.writeheader()
            wr.writerows(rows)


def load_params(path=None):
    P = json.loads(json.dumps(PARAMS))
    if path:
        for k, v in json.load(open(path)).items():
            P[k] = {**P[k], **v} if isinstance(v, dict) and isinstance(P.get(k), dict) else v
    return P


def analyze(bf, nobf, K, P, out, calib="given", quiet=False):
    say = (lambda *a: None) if quiet else print
    runs = {"bf": bf, "nobf": nobf} if nobf else {"bf": bf}
    os.makedirs(out, exist_ok=True)
    report = {"params": P, "calib": calib, "kappa": K, "runs": {}}
    for name, run in runs.items():
        rows, tot = evaluate(run, K, P)
        say(f"\n=== {name}: {run['folder']}  (calib={calib})")
        say("kappa: " + json.dumps({"bw_MBps": {c: K["bw"][c] / 1e6 for c in CLASSES}, "lat_s": K["lat"],
                                    "disp_s": K["disp"], "rate_MBps_per_cpu": {e: r / 1e6 for e, r in K["rate"].items()},
                                    "qlat_s": K["qlat"]}, default=fmt))
        if not quiet:
            table(rows, ["stage", "p", "cold", "meas_t_read", "model_t_in", "meas_t_comp", "model_t_c",
                         "meas_t_write", "model_t_out", "meas_d_fn", "meas_d_wall", "model_d", "err_d_%"])
            print()
            table(rows, ["stage", "Q_in_s3", "Q_out_s3", "D_in_s3", "D_out_s3", "D_in_nfs", "D_out_nfs",
                         "meas_C", "model_C", "err_C_%"])
        say("totals: " + json.dumps(tot, default=fmt))
        save_csv(os.path.join(out, f"stages-{name}.csv"), rows)
        report["runs"][name] = {"folder": run["folder"], "totals": tot, "stages": rows}

    bfr = bf_report(bf, nobf, K)
    report["bf"] = bfr
    for r in bfr:
        say(f"\n=== BF {r['join']}")
        say(json.dumps({k: v for k, v in r.items() if k != "appliers"}, default=fmt))
        if r["appliers"] and not quiet:
            table(r["appliers"], [c for c in r["appliers"][0].keys()])

    if nobf:
        t1, t0 = report["runs"]["bf"]["totals"], report["runs"]["nobf"]["totals"]
        dec = {"T1_meas": t1["T_meas"], "T0_meas": t0["T_meas"], "T1_model": t1["T_model"], "T0_model": t0["T_model"],
               "C1_meas": t1["C_meas"], "C0_meas": t0["C_meas"], "C1_model": t1["C_model"], "C0_model": t0["C_model"]}
        dec["bf_better_meas"] = dec["T1_meas"] < dec["T0_meas"] and dec["C1_meas"] < dec["C0_meas"]
        dec["bf_better_model"] = dec["T1_model"] < dec["T0_model"] and dec["C1_model"] < dec["C0_model"]
        dec["decision_match"] = dec["bf_better_meas"] == dec["bf_better_model"]
        report["decision"] = dec
        say("\n=== decision: " + json.dumps(dec, default=fmt))

    with open(os.path.join(out, "report.json"), "w") as f:
        json.dump(report, f, indent=2, default=str)
    say(f"\nsaved -> {out}/")
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("bf_run")
    ap.add_argument("nobf_run", nargs="?")
    ap.add_argument("--params")
    ap.add_argument("--calib", choices=["nobf", "bf", "both"], default="nobf")
    ap.add_argument("--calib-dirs", nargs="*", help="calibrate on these runs instead (e.g. other queries)")
    ap.add_argument("--out", default="cost-report")
    a = ap.parse_args()

    P = load_params(a.params)
    bf = load(a.bf_run, P)
    nobf = load(a.nobf_run, P) if a.nobf_run else None
    if a.calib_dirs:
        src, calib = [load(d, P) for d in a.calib_dirs], f"{len(a.calib_dirs)} runs"
    else:
        src = {"nobf": [nobf or bf], "bf": [bf], "both": [bf] + ([nobf] if nobf else [])}[a.calib]
        calib = a.calib
    K = calibrate(src, P)
    # endpoints never seen in the calibration runs (e.g. /merge-bf when calibrating on nobf) come from all runs
    fb = calibrate([bf] + ([nobf] if nobf else []), P)
    K["rate"], K["qlat"] = {**fb["rate"], **K["rate"]}, {**fb["qlat"], **K["qlat"]}
    analyze(bf, nobf, K, P, a.out, calib)


if __name__ == "__main__":
    sys.exit(main())
