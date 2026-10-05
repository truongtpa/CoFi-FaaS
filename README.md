# CoFi-FaaS: experiment data and notebooks

Exported results and notebooks of the evaluation of CoFi-FaaS, a cost-time model and plan optimizer for serverless
query processing with Bloom-filter pushdown (system under test: BLOOM-FaaS on Grid'5000, TPC-H SF100).
Each section folder has `data/` (exported CSV / JSON), where relevant `pipeline/` (the BLOOM-FaaS pipelines that
were run), and one notebook that shows the data and draws the figures of the paper into `figures/`.

| Folder | Notebook | Data | Pipeline |
|---|---|---|---|
| `1. Cost-Time Model Accuracy/` | `model_accuracy.ipynb` | model vs. measured T and C per run (v5 model and paper equations), Bloom-filter demand, calibrated κ | TPC-H query pipelines `q*.yml.j2` + parameters |
| `2. Tuning Parameters/` | `tuning_parameters.ipynb` | MinIO size × parallelism microbenchmark and its fit, time breakdown and CPU use per operator | `s3probe.yml.j2` |
| `3. Cost-Time Plan Optimization/` | `plan_optimization.ipynb` | estimated plans (default / knee / min-time / min-cost) and every evaluated plan | — |
| `4. End-to-End Evaluation on BLOOM-FaaS/` | `end_to_end.ipynb` | measured runs of optimized plans (per-stage sizes, limited parallelism) and the no-BF baseline | — |

## Quick start

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
jupyter notebook
```

or, to regenerate every figure:

```bash
for d in 1* 2* 3* 4*; do (cd "$d" && jupyter nbconvert --to notebook --execute --inplace *.ipynb); done
```

The notebooks only read the CSVs (a few seconds each); nothing needs the cluster.

## Data

**1. Cost–Time Model Accuracy** — 19 TPC-H queries × 3 systems (`base`: no Bloom filter; `s3` / `nfs`: Bloom-filter
pushdown, intermediate data on S3 / NFS) × 15 runs, max_parallel 70, functions with 2 vCPU / 2 GiB.
`runs-<config>.csv` has one row per query / system / run / metric (`T` seconds, `C` dollars, `decision`):
measured, model and error %. κ is calibrated leave-one-query-out. `config`: `v5` = configuration of the reported
results, `paper` = the equations of the paper as written.

**2. Tuning Parameters** — `s3-grid.csv`: lineitem scan for 11 splits (580 → 70 functions) × max_parallel
{10, 20, 30, 50, 70, 100}, 3 runs each; `minio.json`: fit A(k) = min(k·B₁, B_max/(1+γk)) of the aggregate
throughput; `task-profile.csv`: per operator, time of all functions of the 285 BLOOM-FaaS S3 runs split into
dispatch / input / compute / output demand, CPU time used and vCPU-seconds allocated.

**3. Cost–Time Plan Optimization** — `plans.csv`, `points.csv` (every plan evaluated by the search), `p-check.csv`.
Estimates of the model, not measurements.

**4. End-to-End Evaluation** — `e2e-runs.csv`: one row per measured run (`T_s`, `C_usd`) of
* `sweep`: Q3, Q9, Q18 with per-stage sizes chosen from a measured size sweep (`default`, `best_global`, `knee`,
  `min_T`, `min_C`; estimates in `sweep-plans.csv`);
* `s3opt`: 19 queries with concurrent functions limited after the MinIO characterisation (`default`,
  `scan_mp20`, `scan_opt`, `all_mp20`; see `s3opt-scenarios.json`);
* `baseline`: no Bloom filter (`no_bf`).

Measured cost = resources used by the run (function time × memory, invocations, storage requests and bytes)
priced with AWS list prices; Grid'5000 is not billed.

## Code

`cofi/` is the implementation that produced the exported data (paper notation): `model.py` (resource demand,
calibration of κ, projection to T and C; `Config.paper()` and `Config.v5()`), `bloom.py` (Bloom-filter demand),
`accuracy.py` (leave-one-query-out evaluation), `plans.py` and `partition.py` (plan optimizer), `style.py` (figure
style). It runs on the per-function cost logs of BLOOM-FaaS (`cost-dag.json`, `cost-stages.jsonl` per run), which
are not included here to keep the repository small.

## Main numbers

| | |
|---|---|
| Model MAPE (v5), all systems | T 17.7%, C 15.4% (paper equations as written: T 58.5%, C 40.6%) |
| Bloom-filter decision, model = measured | 241 / 285 (S3), 234 / 285 (NFS) paired runs |
| Extra BF requests R = 2p^b + 1 + p^p | exact in 510 / 510 |
| MinIO | 234 MB/s per function alone; saturates at k* ≈ 9 functions, 2.1 GB/s |
| CPU used by functions | 18% (scan) – 37% (merge BF) of the 2 allocated vCPU |
| Limiting concurrent functions (19 queries) | total cost −18% to −34%, geomean speedup 1.04–1.05× |
