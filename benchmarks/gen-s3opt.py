"""Scenarios built from the S3 size x parallelism sweep (notebooks/s3_sweep.py -> s3grid/s3-grid.csv), as templates for
every TPC-H query, to run with run-seek15.py and compare with plan_compare.py.

From the sweep: mp* = the max_parallel with the fastest lineitem scan (ties within --tol go to the cheaper one, i.e.
less billed read time), size* = the fastest max_size_mb at mp* (same tie rule). Scenarios:
  default        the template as is (every stage max_parallel 70, 50 MB)
  scan_mp<mp*>   scan stages run with at most mp* functions at once (S3 is saturated beyond it), the rest unchanged
  scan_opt       scan stages at mp* and size*           (only written if size* != 50)
  all_mp<mp*>    every stage at mp*                     (does the rest of the query also gain, or need more functions?)

    python3 benchmarks/gen-s3opt.py notebooks/cost-report/tpch100-v5/s3grid/s3-grid.csv
Templates: benchmarks/plans-j2-s3opt/<scenario>/<query>.yml.j2 ($parm*, $bucket, $base_path kept). Run them with
run-seek15.py (YAML_FILES glob ./benchmarks/plans-j2-s3opt/*/q*.yml.j2) -> history-plans-s3opt/<scenario>/, then
    python3 notebooks/plan_compare.py history-plans-s3opt --baseline history-v6 --out notebooks/cost-report/tpch100-v5/s3opt
"""
import os
import re
import sys
import csv
import glob
import json
import argparse
import importlib.util

import yaml

sys.path.insert(0, os.getcwd())
from operations.runner.FlowParser import FlowParser, load_and_render_yaml
from operations.runner.J2FileEditor import J2FileEditor

spec = importlib.util.spec_from_file_location("rs", os.path.join(os.path.dirname(__file__), "run-seek15.py"))
rs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rs)

OUT = "./benchmarks/plans-j2-s3opt/"
DEFAULT_MB, DEFAULT_MP = 50, 70


def pick(rows, tol):
    """(mp*, size*) from the s3 grid: fastest wall, ties within tol -> less billed read time (p * t_read)."""
    def best(cands):
        fast = min(r["wall"] for r in cands)
        near = [r for r in cands if r["wall"] <= fast * (1 + tol)]
        return min(near, key=lambda r: r["p"] * r["t_read"])
    by_mp = {}
    for r in rows:   # per mp: the median scan wall over sizes, so mp* does not hinge on one lucky size
        by_mp.setdefault(r["mp"], []).append(r)
    agg = [{"mp": mp, "wall": sorted(x["wall"] for x in R)[len(R) // 2],
            "p": 1, "t_read": sum(x["p"] * x["t_read"] for x in R) / len(R)} for mp, R in by_mp.items()]
    mp = int(best(agg)["mp"])
    size = int(best([r for r in rows if r["mp"] == mp])["size"])
    return mp, size


def scenarios(mp, size):
    s = {"default": lambda st: None,
         f"scan_mp{mp}": lambda st: st.update(max_parallel=mp) if st["func"] == "scan" else None,
         f"all_mp{mp}": lambda st: st.update(max_parallel=mp)}
    if size != DEFAULT_MB:
        s["scan_opt"] = lambda st: st.update(max_parallel=mp, max_size_mb=size) if st["func"] == "scan" else None
    return s


def emit(template, name, change, all_params):
    q = os.path.basename(template).split(".")[0]
    os.makedirs(OUT, exist_ok=True)
    tmp = os.path.join(OUT, f"_{q}.yml.j2")
    J2FileEditor(template).replace("$enable_bf", 1).save(tmp)   # BF on, S3 shuffle, like the other plans
    raw = load_and_render_yaml(tmp)
    for st in raw["steps"]:
        change(st)
    path = os.path.join(OUT, name, f"{q}.yml.j2")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump(raw, f, sort_keys=False, allow_unicode=True)
    # check: render like run-seek15.py and read it back with the runner's parser
    rs.TMP_YAML = tmp
    import contextlib, io
    with contextlib.redirect_stdout(io.StringIO()):
        rs.render_tmp_yaml(path, rs.CONFIGS["bloom_faas_v2"], rs.get_tpch_parms(all_params, q, "seed1"))
    steps = FlowParser(tmp).get_steps()
    left = [l for l in open(tmp) if "$parm" in l or "$bucket" in l or "$base_path" in l]
    os.remove(tmp)
    assert not left, f"{path}: placeholders left: {left[0].strip()}"
    by = {s["name"]: s for s in steps}
    for st in raw["steps"]:
        for k in ("max_parallel", "max_size_mb"):
            assert by[st["name"]].get(k) == st.get(k), f"{path}: {st['name']} {k} not applied"
    return path, sum(1 for s in steps if s.get("max_parallel"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("grid_csv", nargs="?", default="notebooks/cost-report/tpch100-v5/s3grid/s3-grid.csv")
    ap.add_argument("--tol", type=float, default=0.05, help="walls within this fraction count as a tie")
    ap.add_argument("--mp", type=int, help="override mp*")
    ap.add_argument("--size", type=int, help="override size*")
    ap.add_argument("--queries", nargs="*")
    a = ap.parse_args()
    rows = [{k: float(v) for k, v in r.items()} for r in csv.DictReader(open(a.grid_csv))]
    mp, size = pick(rows, a.tol)
    mp, size = a.mp or mp, a.size or size
    ref = next(r for r in rows if r["mp"] == DEFAULT_MP and r["size"] == DEFAULT_MB)
    at = next(r for r in rows if r["mp"] == mp and r["size"] == size)
    print(f"from {a.grid_csv}: mp* = {mp}, size* = {size} MB  (lineitem scan {at['wall']:.2f}s, "
          f"billed read {at['p'] * at['t_read']:.0f}s  vs  mp{DEFAULT_MP}/{DEFAULT_MB}MB {ref['wall']:.2f}s, "
          f"{ref['p'] * ref['t_read']:.0f}s)")
    all_params = rs.load_params()
    templates = sorted((f for f in glob.glob("./benchmarks/tpch-parms/q*.yml.j2")
                        if not a.queries or os.path.basename(f).split(".")[0] in a.queries),
                       key=lambda f: int(re.search(r"q(\d+)", f).group(1)))
    sc = scenarios(mp, size)
    for t in templates:
        out = [f"{name}: {emit(t, name, change, all_params)[1]} stages limited" for name, change in sc.items()]
        print(f"{os.path.basename(t).split('.')[0]:4} " + " | ".join(out))
    json.dump({"mp_star": mp, "size_star": size, "scenarios": list(sc), "grid": a.grid_csv},
              open(os.path.join(OUT, "scenarios.json"), "w"), indent=2)
    print(f"\n{len(templates)} queries x {len(sc)} scenarios -> {OUT}<scenario>/q*.yml.j2")


if __name__ == "__main__":
    main()
