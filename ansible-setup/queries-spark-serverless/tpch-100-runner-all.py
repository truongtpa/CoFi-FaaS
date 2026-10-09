import csv, os, time
from datetime import datetime, timezone
from itertools import groupby
from pyspark.sql import SparkSession

MINIO     = "s3a://tpch-100"
RUNS      = 4
QUERY_F    = 'tpch100'
SLEEP_SEC = 5
SEEDS     = [f"seed{i}.sql" for i in range(1, 11)]

spark = SparkSession.builder.appName("tpch-benchmark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

_aqe = spark.conf.get("spark.sql.adaptive.enabled", "false").lower() == "true"
_bf  = spark.conf.get("spark.sql.optimizer.runtime.bloomFilter.enabled", "false").lower() == "true"
_tag = f"aqe{'1' if _aqe else '0'}_bf{'1' if _bf else '0'}"

RUN_TS  = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
OUT_CSV = f"/tmp/tpch-results-{RUN_TS}-{_tag}.csv"
S3_CSV  = f"{MINIO}/results/spark/{RUN_TS}/tpch-results-{RUN_TS}-{_tag}.csv"

TABLES = ["lineitem","orders","customer","part","partsupp","supplier","nation","region"]
for t in TABLES:
    spark.read.parquet(f"{MINIO}/{t}.parquet").createOrReplaceTempView(t)

QUERIES = {}
for seed in SEEDS:
    QUERIES[seed] = {}
    for q in range(1, 23):
        s3_path = f"{MINIO}/{QUERY_F}/q{q}/{seed}"
        try:
            jvm  = spark.sparkContext._jvm
            jsc  = spark.sparkContext._jsc
            conf = jsc.hadoopConfiguration()
            fs   = jvm.org.apache.hadoop.fs.FileSystem.get(
                       jvm.java.net.URI(s3_path), conf)
            path = jvm.org.apache.hadoop.fs.Path(s3_path)
            if not fs.exists(path):
                print(f"WARN: skip {seed}/Q{q}: file not found {s3_path}")
                continue
        except Exception as e:
            print(f"WARN: skip {seed}/Q{q}: cannot check existence: {e}")
            continue

        try:
            QUERIES[seed][f"Q{q}"] = "\n".join(
                spark.sparkContext.textFile(s3_path).collect()
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