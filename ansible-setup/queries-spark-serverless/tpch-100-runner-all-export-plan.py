import csv, os, time
from datetime import datetime, timezone
from itertools import groupby
from pyspark.sql import SparkSession

MINIO     = "s3a://tpch-100"
FOLDER_QUERY    = 3
RUNS      = 3
SLEEP_SEC = 5
SEEDS     = [f"seed{i}.sql" for i in range(1, 6)]

spark = SparkSession.builder.appName("tpch-benchmark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

RUN_TS   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
OUT_CSV  = f"/tmp/tpch-results-{RUN_TS}.csv"
S3_CSV   = f"{MINIO}/results/spark/{RUN_TS}/tpch-results-{RUN_TS}.csv"
S3_PLANS = f"{MINIO}/results/spark/{RUN_TS}/plans"   # prefix cho plan files

TABLES = ["lineitem","orders","customer","part","partsupp","supplier","nation","region"]
for t in TABLES:
    spark.read.parquet(f"{MINIO}/{t}.parquet").createOrReplaceTempView(t)

QUERIES = {}
for seed in SEEDS:
    QUERIES[seed] = {}
    for q in range(1, 23):
        try:
            QUERIES[seed][f"Q{q}"] = "\n".join(
                spark.sparkContext.textFile(f"{MINIO}/{FOLDER_QUERY}/q{q}/{seed}").collect()
            )
        except Exception as e:
            print(f"WARN: skip {seed}/Q{q}: {e}")
    print(f"Loaded {len(QUERIES[seed])} queries for {seed}")

CSV_FIELDS = ["seed","query","run","time_start","time_end","elapsed_s","status","error"]

def ts_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def write_csv(records):
    with open(OUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(records)

def upload_via_hadoop(local_path, s3_path):
    jvm  = spark.sparkContext._jvm
    jsc  = spark.sparkContext._jsc
    conf = jsc.hadoopConfiguration()
    fs   = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(s3_path), conf)
    src  = jvm.org.apache.hadoop.fs.Path(f"file://{os.path.abspath(local_path)}")
    dst  = jvm.org.apache.hadoop.fs.Path(s3_path)
    fs.copyFromLocalFile(False, True, src, dst)

def upload_text_via_hadoop(text: str, s3_path: str):
    """Write a string directly to S3 qua Hadoop FSDataOutputStream."""
    jvm  = spark.sparkContext._jvm
    jsc  = spark.sparkContext._jsc
    conf = jsc.hadoopConfiguration()
    fs   = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(s3_path), conf)
    dst  = jvm.org.apache.hadoop.fs.Path(s3_path)
    out  = fs.create(dst, True)          # overwrite=True
    out.write(text.encode("utf-8"))
    out.close()

def export_plans(seed: str, qname: str, sql: str):
    """
    Export physical plan trước AQE (queryExecution.sparkPlan)
    và sau AQE (queryExecution.executedPlan) lên S3.

    File layout:
      {S3_PLANS}/{seed}/{qname}_plan_before_aqe.txt
      {S3_PLANS}/{seed}/{qname}_plan_after_aqe.txt
    """
    try:
        df  = spark.sql(sql)
        qe  = df._jdf.queryExecution()

        # ── before AQE: sparkPlan (sau optimize, trước adaptive execution) ──
        plan_before = qe.sparkPlan().toString()
        s3_before   = f"{S3_PLANS}/{seed}/{qname}_plan_before_aqe.txt"
        upload_text_via_hadoop(plan_before, s3_before)

        # ── after AQE: executedPlan (sau khi AQE đã re-optimize) ──
        # executedPlan chỉ được finalize sau khi action chạy xong;
        # gọi count() ở đây chỉ để materialize plan, timing thật vẫn ở vòng lặp runs.
        df.count()
        plan_after = qe.executedPlan().toString()
        s3_after   = f"{S3_PLANS}/{seed}/{qname}_plan_after_aqe.txt"
        upload_text_via_hadoop(plan_after, s3_after)

        print(f"    plans exported → {s3_before.split('/')[-1]}, {s3_after.split('/')[-1]}")
    except Exception as e:
        print(f"    WARN export_plans failed: {e}")

# ── export plans một lần trước khi benchmark (run 1 dùng lại kết quả) ──
print("\n=== Exporting physical plans ===")
for seed, seed_queries in QUERIES.items():
    for qname, sql in seed_queries.items():
        print(f"  {seed}/{qname} ...", end=" ", flush=True)
        export_plans(seed, qname, sql)

# ── benchmark runs ──
records = []

for seed, seed_queries in QUERIES.items():
    total = len(seed_queries)
    print(f"\n{'='*60}\nSeed: {seed}  ({total} queries)\n{'='*60}")

    for q_idx, (qname, sql) in enumerate(seed_queries.items(), 1):
        print(f"\n  [{q_idx}/{total}] {qname}")

        for run in range(1, RUNS + 1):
            if run > 1:
                print(f"    sleeping {SLEEP_SEC}s ...", flush=True)
                time.sleep(SLEEP_SEC)

            start_iso = ts_iso()
            t0        = time.perf_counter()
            status    = "OK"
            error     = None
            try:
                spark.sql(sql).count()
            except Exception as e:
                status = "FAIL"
                error  = str(e)
                print(f"    ERROR: {e}", flush=True)

            elapsed_s = round(time.perf_counter() - t0, 3)
            end_iso   = ts_iso()

            records.append({
                "seed":       seed,
                "query":      qname,
                "run":        run,
                "time_start": start_iso,
                "time_end":   end_iso,
                "elapsed_s":  elapsed_s,
                "status":     status,
                "error":      error,
            })

            flag = "✓" if status == "OK" else "✗"
            print(f"    run {run}/{RUNS} {flag}  {elapsed_s}s", flush=True)
            write_csv(records)

upload_via_hadoop(OUT_CSV, S3_CSV)
print(f"\nCSV local → {OUT_CSV}")
print(f"CSV s3    → {S3_CSV}")

print("\n=== SUMMARY — avg elapsed_s per query (all seeds) ===")
print(f"{'Query':<6}  {'min':>7}  {'avg':>7}  {'max':>7}  {'ok':>4}")

sorted_records = sorted(records, key=lambda r: int(r["query"][1:]))
for qname, grp in groupby(sorted_records, key=lambda r: r["query"]):
    ok_runs = [r for r in grp if r["status"] == "OK"]
    times   = [r["elapsed_s"] for r in ok_runs]
    mn = min(times)                      if times else -1
    mx = max(times)                      if times else -1
    av = round(sum(times)/len(times), 3) if times else -1
    print(f"{qname:<6}  {mn:>7}  {av:>7}  {mx:>7}  {len(ok_runs):>4}")

spark.stop()
print("\nDone.")