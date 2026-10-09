---
name: bloom-faas-pipeline
description: >
  Generate, modify, review, or debug BLOOM-FaaS pipeline files (.yml.j2) for
  TPC-H queries. Use this skill whenever the user asks to: create a new pipeline
  for any TPC-H query, add or remove Bloom Filter (BF) pushdown, change join
  order or scan steps, understand the purpose of a pipeline step, or convert a
  SQL query into BLOOM-FaaS DAG steps. Also trigger for any question about func
  types (scan, aggregate, broadcast_join, hash_join, merge_bf, hash_partition,
  summary_clean), BF sizing (est_elements, error_rate), multi-hop BF cascades,
  reduce loops, HAVING-via-join patterns, hash shuffle joins, single_file
  aggregation, or NFS/S3 output path conventions.
---
 
# BLOOM-FaaS Pipeline Skill
 
BLOOM-FaaS executes TPC-H queries as FaaS/Knative DAG pipelines. Each pipeline
is a **Jinja2-templated YAML** file (`.yml.j2`). The key optimization is
**Bloom Filter pushdown**: build a BF from a small/filtered table and apply it
to large table scans before joins, reducing S3 reads and intermediate data.
 
---
 
## File Skeleton
 
```yaml
name: q<N>
description: TPC-H Query <N> - <Name>
 
variables:
  enable_bf: $enable_bf             # bool – enables BF pushdown path
  enable_nfs_shuffle: $enable_nfs_shuffle
  bucket: $bucket                   # MinIO bucket, e.g. tpch-100
  base_path: $base_path             # S3 prefix for intermediate files
  s3_metadata: '$s3_metadata/{{ bucket }}.json'
  nfs_folder: '/mnt/filters/'       # shared NFS path for .bin BF files
 
  # TPC-H query parameters
  parm1: '<value>'
  parm2: '<value>'
 
steps:
  # ... ordered list of steps ...
 
  - name: stepN-summary-clean      # always last
    func: summary_clean
    params:
      output_s3: '{{ base_path }}'
      nfs_folder: '{{ nfs_folder }}'
      is_delete: True
```
 
**Special tokens** injected by runner (not Jinja2):
- `$task_name` — slugified step name used as S3 folder
- `$random` — UUID fragment for output file uniqueness
 
---
 
## func Reference
 
### 1. `scan`
 
Fan-out parallel read of a Parquet table from S3. Each invocation processes
one row group. Optionally filters rows using an incoming BF.
 
```yaml
- name: step1-region
  func: scan
  input: 's3://{{ bucket }}/region.parquet'
  from_previous_task: None          # None = starts immediately (no BF dependency)
  params:
    select: SELECT r_regionkey, r_name
    sql_clauses: >
      WHERE r_name = '{{ parm1 }}' AND r_regionkey IS NOT NULL
    output_file: '{{ base_path }}/$task_name/$random.parquet'
 
    # BF consume — only when filtering this table using an upstream BF
    {% if enable_bf %}
    bf_column: r_regionkey          # column to check against incoming BF
    filter_by_bf: True
    {% endif %}
```
 
**Rules:**
- `from_previous_task: None` if no BF dependency. When BF is active, set it
  to the upstream `merge_bf` step name (inside `{% if enable_bf %}`).
- `bf_column` = the join key column in **this** table that matches the BF.
- `filter_by_bf: True` activates row-group-level BF filtering before reading.
- Always include `IS NOT NULL` on join key columns in `sql_clauses`.
 
---
 
### 2. `aggregate`
 
Fan-in reducer. Collects outputs from the previous fan-out step into one file.
Also used for partial GROUP BY / ORDER BY, and optionally **builds a BF**.
 
```yaml
- name: step1-region--combine
  func: aggregate
  from_previous_task: step1-region
  params:
    output_file: '{{ base_path }}/$task_name/$random.parquet'
 
    # Optional SQL transform (plain fan-in if omitted)
    select: SELECT ps_partkey, SUM(value) AS value
    sql_clauses: GROUP BY ps_partkey ORDER BY value DESC
 
    # BF build — only when this step produces a BF for a downstream scan
    {% if enable_bf %}
    bf_column: r_regionkey          # column to encode into BF
    bf_path: '{{ nfs_folder }}/$task_name/$random.bin'
    est_elements: 5                 # estimated distinct values (see §est_elements)
    {% endif %}
```
 
**Rules:**
- `select` + `sql_clauses` are optional; omit for a plain fan-in collect.
- BF build params and SQL params can coexist in the same step.
- After a BF-building `aggregate`, always add a `merge_bf` step next.
- `est_elements` must reflect the number of **distinct values** of `bf_column`
  after any WHERE filter is applied.
 
**Reduce loop** — for large distributed aggregations that need multiple
progressive merges (used in Q11, Q16, Q20):
 
```yaml
{% for i in range(1, 4) %}
- name: step6-agg--reduce-{{ i }}
  func: aggregate
  from_previous_task: "{{ 'step6-agg' if i == 1 else 'step6-agg--reduce-' ~ (i-1) }}"
  max_size_mb: {{ 50 * i }}
  params:
    select: SELECT p_brand, p_type, p_size, SUM(supplier_cnt) AS supplier_cnt
    sql_clauses: GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC
    output_file: '{{ base_path }}/$task_name/$random.parquet'
{% endfor %}
```
 
Use 3–4 reduce passes when aggregation output is large. `max_size_mb` grows
each pass to allow progressively larger merges.
 
---
 
### 3. `merge_bf`
 
Merges all partial BF `.bin` files (one per `aggregate` worker) into a single
BF file on shared NFS. Downstream `scan` steps wait for this step.
 
```yaml
{% if enable_bf %}
- name: step1-region--mergebf
  func: merge_bf
  from_previous_task: step1-region--combine
  params:
    file_output: '{{ nfs_folder }}/$task_name/$random.bin'
    error_rate: 0.001
{% endif %}
```
 
**Rules:**
- Always wrap the **entire step block** in `{% if enable_bf %}`.
- `error_rate: 0.001` is standard (0.1% false positive rate).
- The downstream `scan` step's `from_previous_task` references this step
  (also inside `{% if enable_bf %}`).
 
---
 
### 4. `broadcast_join`
 
Hash join where the **build side** (smaller) is broadcast to all workers
holding the **probe side** (larger). Fan-out; always follow with `aggregate`.
 
```yaml
- name: step3-join-region-nation
  func: broadcast_join
  max_size_mb: 50                   # optional memory guard for build side
  from_previous_task:
    - step1-region--combine         # BUILD side — first in list, smaller table
    - step2-nation--combine         # PROBE side — second in list, larger table
  params:
    probe_select: SELECT n_nationkey, n_name   # columns to keep from probe
    join_condition: r_regionkey = n_regionkey
    output_file: '{{ base_path }}/$task_name/$random.parquet'
```
 
**Rules:**
- `from_previous_task` is always a **2-element list** (build first, probe second).
- `probe_select` lists only columns needed downstream; drop build columns
  already used for joining.
- Always follow with an `aggregate --combine` step to fan-in results.
- For **multi-column or inequality conditions**, use `probe_table.` /
  `build_table.` prefixes to disambiguate:
 
```yaml
    join_condition: >
      probe_table.l_partkey = build_table.l_partkey
      AND probe_table.l_quantity < build_table.avg_qty
```
 
- **HAVING-via-join** (Q11, Q17, Q20): compute threshold in a prior aggregate,
  use it as build side, join with inequality to filter probe rows.
- **Anti-join / exclusion** (Q13, Q16): apply `NOT LIKE` / `!=` during `scan`
  `sql_clauses` before the join — no special join func needed.
 
---
 
### 5. `hash_partition`
 
Repartitions data by a hash key into N buckets on NFS/S3. Used to co-locate
matching rows before a `hash_join` (shuffle join). Always follows an
`aggregate` fan-in step; outputs to a **folder** (not a single file).
 
```yaml
- name: step5-lineitem-l1--hash
  func: hash_partition
  from_previous_task: step5-lineitem-l1--combine   # must come from an aggregate, not directly from scan or hash_join
  params:
    hash_column: l_orderkey
    num_buckets: '{{ hash_buckets }}'              # declare hash_buckets in variables
    output_folder: '{{ base_path }}/$task_name'    # folder, not output_file
```
 
**Rules:**
- `output_folder` (not `output_file`) — each bucket is written as a separate
  file inside the folder.
- `num_buckets` must match across **all** `hash_partition` steps that feed the
  same `hash_join` (build and probe sides must have identical bucket counts).
- Always precede with an `aggregate --combine` to fan-in; never partition
  directly from a raw `scan` or from a `hash_join` output.
- Declare `hash_buckets` as a variable (e.g. `hash_buckets: 15`) so the value
  can be tuned in one place.
 
**⚠️ Critical — `aggregate` after hash_partition must use `single_file: True`:**
 
When data has been hash-partitioned, each bucket is already a self-contained
partition. The subsequent `aggregate` per bucket **must not merge across
buckets**, so set `single_file: True` to process each bucket file independently:
 
```yaml
- name: step10-agg
  func: aggregate
  from_previous_task: step9-join-supplier--combine
  params:
    select: SELECT s_name, COUNT(*) AS numwait
    sql_clauses: GROUP BY s_name
    output_file: '{{ base_path }}/$task_name/$random.parquet'
    single_file: True             # ← REQUIRED when upstream data is hash-partitioned
                                  #   each bucket processed independently, no cross-bucket merge
```
 
Then follow with a normal `aggregate --combine` to merge bucket results:
 
```yaml
- name: step10-agg--combine
  func: aggregate
  from_previous_task: step10-agg
  params:
    select: SELECT s_name, SUM(numwait) AS numwait
    sql_clauses: GROUP BY s_name ORDER BY numwait DESC, s_name
    output_file: '{{ base_path }}/$task_name/$random.parquet'
    # no single_file here — this is the global merge
```
 
---
 
### 6. `hash_join`
 
Partition-aligned join (shuffle join). Both sides must have been
hash-partitioned on the **same key** with the **same `num_buckets`**.
Supports `join_type`: `inner`, `left_semi`, `left_anti`.
 
```yaml
- name: step7-join-orders-l1
  func: hash_join
  from_previous_task:
    - step4-orders--hash        # BUILD side — first, hash-partitioned
    - step5-lineitem-l1--hash   # PROBE side — second, hash-partitioned
  params:
    join_type: left_semi        # inner | left_semi | left_anti
    probe_select: SELECT l_orderkey, l_suppkey
    join_condition: l_orderkey = o_orderkey
    output_file: '{{ base_path }}/$task_name/$random.parquet'
```
 
**`join_type` semantics:**
 
| `join_type` | Returns | Typical use |
|---|---|---|
| `inner` | Rows from probe that match build | Standard equi-join |
| `left_semi` | Probe rows where a match exists (no duplicates) | EXISTS / IN subquery |
| `left_anti` | Probe rows where **no** match exists | NOT EXISTS / NOT IN |
 
**Rules:**
- `from_previous_task` is always a **2-element list** (build first, probe second).
- Both sides **must** be `hash_partition` outputs with identical `num_buckets`
  and the same `hash_column` as the join key.
- After `hash_join`, always fan-in with an `aggregate --combine` step before
  any further `hash_partition` (never partition directly from `hash_join`).
- For **multi-condition joins** (equality + inequality), use `probe_table.` /
  `build_table.` prefixes:
 
```yaml
    join_condition: >
      probe_table.l_orderkey = build_table.l_orderkey
      AND probe_table.l_suppkey != build_table.l_suppkey
```
 
**Chaining hash_joins** — when the output of one `hash_join` must feed another,
always go through `aggregate --combine` then `hash_partition` again:
 
```
[hash_join] → [aggregate --combine] → [hash_partition] → [hash_join]
```
 
Never feed a `hash_join` output directly into another `hash_partition`.
 
---
 
### 7. `summary_clean`
 
Final step of every pipeline. Deletes all intermediate S3 files and NFS BF
files. No `from_previous_task` needed.
 
```yaml
- name: stepN-summary-clean
  func: summary_clean
  params:
    output_s3: '{{ base_path }}'
    nfs_folder: '{{ nfs_folder }}'
    is_delete: True
```
 
Always the very last step. Step number N = last data step number + 1.
 
---
 
## Pipeline Structure Patterns
 
### A. Simple BF pushdown (1 chain)
 
*Used by: Q3, Q9, Q12, Q14, Q17, Q19*
 
```
[Scan small table (filter)] → [aggregate + build BF] → [merge_bf]
                                                              │ (if enable_bf)
                                           [Scan large table, filter_by_bf]
                                                              │
                                                        [aggregate]
                                                              │
                                                    [broadcast_join]
                                                              │
                                               [aggregate --combine]
                                                              │
                                                  [aggregate final agg]
                                                              │
                                                    [summary_clean]
```
 
### B. Cascade BF chain (multi-hop pushdown)
 
*Used by: Q2, Q5, Q7, Q8, Q11, Q20*
 
Each hop: filtered result builds a new BF that filters the next table.
 
```
region → combine+BF → mergebf
                          │
         nation scan (filter n_regionkey) → combine+BF → mergebf
                                                               │
                        supplier scan (filter s_nationkey) → combine+BF → mergebf
                                                                                │
                                            lineitem scan (filter l_suppkey/key)
```
 
### C. Parallel BF fan-out (same BF filters multiple tables)
 
*Used by: Q5 (supplier + customer both filtered by nation BF), Q9 (partsupp +
lineitem both filtered by part BF)*
 
```
                       [mergebf]
                      /         \
         [Scan table A]         [Scan table B]
         (filter_by_bf)         (filter_by_bf)
               │                       │
          [combine]               [combine]
               └──────────┬────────────┘
                    [broadcast_join]
```
 
Both scan steps set `from_previous_task` to the same `mergebf` step name.
 
### D. Two-pass aggregation (HAVING / subquery filter)
 
*Used by: Q2 (min supplycost), Q11 (HAVING value > total × fraction), Q17
(avg quantity filter)*
 
```
[join result] → [aggregate per group] → [reduce-1..3]  ← global threshold
[join result] ──────────────────────────────────────── ← probe side
                    [broadcast_join: probe.metric > build.threshold]
                              │
                    [aggregate --combine]
```
 
### E. Anti-join / exclusion filter
 
*Used by: Q13 (orders NOT LIKE special requests), Q16 (suppliers NOT LIKE
Customer Complaints)*
 
Apply the exclusion predicate during the `scan` step's `sql_clauses` using
`NOT LIKE` or `!=`. Then proceed with normal broadcast joins. No special func
needed for the exclusion.
 
---
 
## Naming Conventions
 
| Pattern | Meaning |
|---|---|
| `step<N>-<table>` | Scan of `<table>` |
| `step<N>-<table>--combine` | Aggregate fan-in after scan |
| `step<N>-<table>--mergebf` | BF merge after combine |
| `step<N>-join-<A>-<B>` | Broadcast join OR hash join: A = build side, B = probe side |
| `step<N>-join-<A>-<B>--combine` | Fan-in after join |
| `step<N>-<table>--hash` | Hash partition of `<table>` output |
| `step<N>-agg` | Final aggregation / ORDER BY |
| `step<N>-agg--reduce-<i>` | i-th progressive reduce pass |
| `step<N>-summary-clean` | Cleanup — always last |
 
Step numbers are sequential integers. When `enable_bf` is False, all
`--mergebf` steps vanish and scan `from_previous_task` reverts to `None` or
the non-BF predecessor aggregate step.
 
---
 
## BF Placement Decision
 
| Situation | Add BF? | Reason |
|---|---|---|
| Large table (lineitem, orders, partsupp) with selective upstream filter | ✅ Yes | Maximum invocation reduction |
| Small table (region=5, nation=25 rows) | ❌ No — build BF from them | Too small to benefit from filtering |
| Near-zero selectivity (filtered keys ≈ full table) | ❌ No | Q14: lineitem BF on part yields no gain |
| Join key is computed / derived | ❌ No | Cannot match raw column in BF |
| Cross-column predicate (`l_commitdate < l_receiptdate`) | ❌ No | Cannot push via row-group min/max |
| Query has no joins | ❌ No | Q1: pure scan + agg |
 
**Direction rule**: always build BF from the **smaller/filtered** side;
push it to filter the **larger** side before the join.
 
---
 
## est_elements Reference (SF100)
 
| Table | Approx. distinct keys | Typical `est_elements` |
|---|---|---|
| region | 5 | 5 – 10 |
| nation | 25 | 25 – 50 |
| supplier | 1 000 000 | 100_000 – 1_000_000 |
| customer | 15 000 000 | 1_000_000 |
| part | 20 000 000 | 200_000 – 2_000_000 |
| partsupp | 80 000 000 | 800_000 |
| orders | 150 000 000 | 1_000_000 |
| lineitem | 600 000 000 | 6_500_000 |
 
After a WHERE filter reduces cardinality, scale down proportionally
(e.g. region filtered to 1 row → `est_elements: 1`).
 
---
 
## Jinja2 Conditional Patterns Quick Reference
 
**BF params inside a step's `params` block:**
```yaml
    {% if enable_bf %}
    bf_column: n_nationkey
    bf_path: '{{ nfs_folder }}/$task_name/$random.bin'
    est_elements: 25
    {% endif %}
```
 
**BF-conditional `from_previous_task` on a scan:**
```yaml
- name: step2-nation
  func: scan
  input: 's3://{{ bucket }}/nation.parquet'
  {% if enable_bf %}
  from_previous_task: step1-region--mergebf
  {% endif %}
  params:
    select: SELECT n_nationkey, n_name, n_regionkey
    sql_clauses: WHERE n_nationkey IS NOT NULL AND n_regionkey IS NOT NULL
    output_file: '{{ base_path }}/$task_name/$random.parquet'
    {% if enable_bf %}
    bf_column: n_regionkey
    filter_by_bf: True
    {% endif %}
```
 
**Entire merge_bf block:**
```yaml
{% if enable_bf %}
- name: step1-region--mergebf
  func: merge_bf
  from_previous_task: step1-region--combine
  params:
    file_output: '{{ nfs_folder }}/$task_name/$random.bin'
    error_rate: 0.001
{% endif %}
```
 
When `enable_bf` is False the key is absent; the runner defaults to no BF
dependency and the scan starts independently.