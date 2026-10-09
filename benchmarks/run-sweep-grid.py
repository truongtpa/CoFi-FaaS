"""Fine sweep of max_size_mb x max_parallel, for the cost model and the per-stage size choice.

Three modes:
  s3     one scan of lineitem (benchmarks/sweep/s3probe.yml.j2), nothing written: S3 read time per function vs bytes
         per function and number of functions reading at once. Sizes giving the same split as a smaller size are
         skipped (scans split by row group, so most sizes repeat: 5..40 MB all give 580 functions).
  nfs    the same for NFS (benchmarks/sweep/nfsprobe.yml.j2): a fixed stage writes half of lineitem to /mnt as 580
         files of ~10 MB (like an NFS shuffle), the measured stage (step2-nfs-read, an aggregate) reads them back with
         an always-false filter, split by the swept size. The NFS split is only known after the run, so no size is
         skipped: the default sizes are coarser.
  query  every TPC-H template (BF on, S3 shuffle) at each size, max_parallel 70: the per-stage U curves for all queries.

History: history-grid/<mode>/mp<P>/size-<MB>/<query>/<time>--bloomfaas-s3-bloomfaas--ms<MB>-mp<P>
(size_sweep.py / plan_sweep.py read history-grid/query/mp70 like history-sweep/).
Jobs run in a shuffled order (fixed seed) so cluster drift is not mistaken for a size or parallelism effect, and the
anchor (50 MB, mp70) is rerun every --anchor-every jobs as a drift check. Re-running the script resumes: a
configuration with --runs runs already in its folder is skipped.

    python3 benchmarks/run-sweep-grid.py --mode s3 --dry-run          # list the jobs and the estimated time
    python3 benchmarks/run-sweep-grid.py --mode s3
    python3 benchmarks/run-sweep-grid.py --mode nfs --then-analyze
    python3 benchmarks/run-sweep-grid.py --mode query --queries q3 q5 --sizes 10 20 30
"""
import os
import re
import sys
import glob
import json
import time
import random
import argparse
import importlib.util
from datetime import datetime

sys.path.insert(0, os.getcwd())
from operations.runner.Runner import Runner
from operations.Nofityme import sendMessage
from operations.runner.Helpers import partition_tasks
from operations.libs.ParquetS3Utils import parse_select_columns

spec = importlib.util.spec_from_file_location("rs", os.path.join(os.path.dirname(__file__), "run-seek15.py"))
rs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rs)

ROOT = "./history-grid/"
CFG = rs.CONFIGS["bloom_faas_v2"]
CFGS = {"s3": CFG, "query": CFG, "nfs": {**CFG, "enable_nfs_shuffle": 1}}   # nfs: base_path = /mnt/
PROBES = {"s3": "./benchmarks/sweep/s3probe.yml.j2", "nfs": "./benchmarks/sweep/nfsprobe.yml.j2"}
PROBE = PROBES["s3"]
ANCHOR = (50, 70)
DEFAULTS = {
    "s3": {"sizes": list(range(5, 205, 5)), "parallel": [10, 20, 30, 50, 70, 100], "seeds": ["seed1"], "runs": 3},
    "nfs": {"sizes": [10, 20, 30, 40, 50, 60, 80, 100, 130, 160, 200], "parallel": [10, 20, 30, 50, 70, 100],
            "seeds": ["seed1"], "runs": 3},
    "query": {"sizes": list(range(5, 105, 5)) + [125, 150, 200], "parallel": [70], "seeds": ["seed1"], "runs": 3},
}
SECONDS = {"s3": 12, "nfs": 25, "query": 30}   # rough time per run incl. delays, for the estimate only


def probe_split(size):
    """Number of scan functions of the probe at this size (from the bucket metadata, no cluster needed)."""
    meta = json.load(open(os.path.join(rs.METADATA_DIR, f"{rs.BUCKET}.json")))["datasets"]["lineitem.parquet"]["files"]
    text = open(PROBE).read()
    sel = re.search(r"select: (.*)", text).group(1)
    where = re.search(r"sql_clauses: (WHERE[^#\n]*)", text).group(1).strip()
    cols = parse_select_columns(sel, where)
    return sum(len(partition_tasks([f], max_size_mb=size, split_row_groups=True, columns=cols)) for f in meta)


def notify(text):
    """Telegram message; a network error must not stop the sweep."""
    print(f"[notify] {text}")
    try:
        sendMessage(text)
    except Exception as e:
        print(f"[notify] failed: {e}")


def history_dir(mode, size, mp):
    return os.path.join(ROOT, mode, f"mp{mp}", f"size-{size}")


def done(mode, size, mp, query):
    d = os.path.join(history_dir(mode, size, mp), query)
    return len([x for x in glob.glob(os.path.join(d, "*")) if os.path.exists(os.path.join(x, "cost-stages.jsonl"))])


def jobs(a):
    if a.mode == "nfs":
        templates, sizes = [PROBES["nfs"]], a.sizes
    elif a.mode == "s3":
        templates = [PROBE]
        seen = {}
        for s in a.sizes:   # keep one size per distinct split: the anchor size if it has that split, else the smallest
            p = probe_split(s)
            if p not in seen or s == ANCHOR[0]:
                seen[p] = s
        sizes = sorted(seen.values())
        print("probe splits: " + ", ".join(f"{s}MB->{p}fn" for p, s in seen.items()))
    else:
        templates = sorted((f for f in glob.glob("./benchmarks/tpch-parms/q*.yml.j2")
                            if not a.queries or os.path.basename(f).split(".")[0] in a.queries),
                           key=lambda f: int(re.search(r"q(\d+)", f).group(1)))
        sizes = a.sizes
    grid = [(t, seed, s, mp) for t in templates for seed in a.seeds for s in sizes for mp in a.parallel]
    random.Random(a.order_seed).shuffle(grid)
    out = []
    for i, job in enumerate(grid):
        if a.anchor_every and i and i % a.anchor_every == 0 and ANCHOR[0] in sizes and ANCHOR[1] in a.parallel:
            out.append((job[0], job[1], *ANCHOR, True))   # drift check, same query as the next job
        out.append((*job, False))
    return out


def run_one(template, seed, size, mp, all_params, anchor):
    q = os.path.basename(template).split(".")[0]
    hist = history_dir(a_mode, size, mp)
    os.environ["MAX_SIZE_MB_DEFAULT"] = str(size)
    parms = rs.get_tpch_parms(all_params, q, seed) if q.startswith("q") else {}
    import contextlib, io
    with contextlib.redirect_stdout(io.StringIO()):
        rs.render_tmp_yaml(template, CFGS[a_mode], parms)
    start = datetime.now()
    result = Runner(rs.TMP_YAML).execute(history_logs=hist, knative_func=rs.KNATIVE_FUNC, k8s=rs.K8S_ENDPOINT,
                                         max_parallel=mp, func_ver=CFGS[a_mode]["function_version"], tag=f"ms{size}-mp{mp}")
    ok = bool(result) and not isinstance(result, dict)
    entry = {"mode": a_mode, "query": q, "seed": seed, "size_mb": size, "max_parallel": mp, "anchor": anchor,
             "start": start.isoformat(), "duration_s": (datetime.now() - start).total_seconds(),
             "status": "success" if ok else "failed", **({"error": result} if isinstance(result, dict) else {})}
    with open(LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")
    return ok, entry["duration_s"]


def main():
    global a_mode, LOG
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=list(DEFAULTS), required=True)
    ap.add_argument("--sizes", type=int, nargs="*")
    ap.add_argument("--parallel", type=int, nargs="*")
    ap.add_argument("--queries", nargs="*", help="query mode: subset of templates (default: all)")
    ap.add_argument("--seeds", nargs="*")
    ap.add_argument("--runs", type=int)
    ap.add_argument("--anchor-every", type=int, default=25, help="rerun (50 MB, mp70) every N jobs; 0 = never")
    ap.add_argument("--order-seed", type=int, default=7)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--then-analyze", action="store_true",
                    help="when the sweep ends, analyse it: query -> after-sweep-grid.py (plans), s3/nfs -> s3_sweep.py")
    a = ap.parse_args()
    for k, v in DEFAULTS[a.mode].items():
        if getattr(a, k) is None:
            setattr(a, k, v)
    a_mode = a.mode
    LOG = f"./benchmarks/logs/sweep-grid-{a.mode}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.jsonl"

    todo = jobs(a)
    runs_left = sum(0 if (not anc and done(a.mode, s, mp, os.path.basename(t).split(".")[0]) >= a.runs) else a.runs
                    for t, _, s, mp, anc in todo)
    print(f"{a.mode}: {len(todo)} jobs ({sum(x[4] for x in todo)} anchors), {runs_left} runs to do "
          f"≈ {runs_left * SECONDS[a.mode] / 3600:.1f} h  -> {ROOT}{a.mode}/")
    if a.dry_run:
        for t, seed, s, mp, anc in todo[:15]:
            print(f"  {os.path.basename(t):18} {seed} {s:4}MB mp{mp}{'  (anchor)' if anc else ''}")
        print("  ...")
        return

    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    all_params = rs.load_params()
    t0 = time.time()
    n_ok, failed, run_i, next_pct = 0, [], 0, 25
    notify(f"[sweep-grid {a.mode}] Bắt đầu: {len(todo)} jobs, {runs_left} runs "
           f"(≈ {runs_left * SECONDS[a.mode] / 3600:.1f} h)\n-> {ROOT}{a.mode}/")
    for i, (t, seed, s, mp, anc) in enumerate(todo, 1):
        q = os.path.basename(t).split(".")[0]
        have = done(a.mode, s, mp, q)
        n = a.runs if anc else max(a.runs - have, 0)
        if not n:
            continue
        for r in range(n):
            try:
                ok, dur = run_one(t, seed, s, mp, all_params, anc)
            except Exception as e:   # a crashed run is reported and skipped, the sweep goes on
                ok, dur = False, 0.0
                print(f"  [ERROR] {e}")
            run_i += 1
            el = (time.time() - t0) / 3600
            print(f"[{i}/{len(todo)}] {q} {seed} {s}MB mp{mp}{' anchor' if anc else ''} run {r + 1}/{n}: "
                  f"{'ok' if ok else 'FAILED'} {dur:.1f}s  (elapsed {el:.2f} h)")
            if ok:
                n_ok += 1
            else:
                failed.append(f"{q} {s}MB mp{mp}")
                notify(f"[sweep-grid {a.mode}] FAILED: {q} {seed} {s}MB mp{mp} (lỗi thứ {len(failed)})")
                break
            pct = 100 * run_i / max(runs_left, 1)
            if pct >= next_pct and next_pct < 100:
                left = el / run_i * (runs_left - run_i)
                notify(f"[sweep-grid {a.mode}] {next_pct}%: {run_i}/{runs_left} runs, {el:.1f} h, "
                       f"còn ≈ {left:.1f} h, {len(failed)} lỗi")
                next_pct += 25
            time.sleep(rs.RUN_DELAY_S)
    el = (time.time() - t0) / 3600
    notify(f"[sweep-grid {a.mode}] Xong sau {el:.1f} h: {n_ok} runs ok, {len(failed)} lỗi"
           + (f"\nLỗi: {', '.join(failed[:20])}" + (" ..." if len(failed) > 20 else "") if failed else "")
           + f"\nHistory: {ROOT}{a.mode}/")
    print(f"done. log: {LOG}")
    if a.then_analyze:
        import subprocess
        if a.mode == "query":
            subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "after-sweep-grid.py"),
                            "--root", os.path.join(ROOT, "query", "mp70")])
        else:   # s3 / nfs: effect of the number of concurrent functions on S3 / NFS reads
            out = f"notebooks/cost-report/tpch100-v5/{a.mode}grid"
            r = subprocess.run([sys.executable, "notebooks/s3_sweep.py", os.path.join(ROOT, a.mode), "--out", out,
                                "--storage", a.mode], capture_output=True, text=True)
            print(r.stdout[-4000:], r.stderr[-2000:])
            fit = open(os.path.join(out, f"{a.mode}-fit.txt")).read() if r.returncode == 0 else r.stderr[-800:]
            notify(f"[sweep-grid {a.mode}] Phân tích {'xong' if r.returncode == 0 else 'LỖI'}:\n{fit}\n-> {out}/")


if __name__ == "__main__":
    main()
