import time, json, textwrap
from pyspark.sql import SparkSession

MINIO = "s3a://tpch-100"

spark = SparkSession.builder.appName("tpch-benchmark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── Print relevant config values so diff is visible in logs ──────────────────
WATCH_CONFIGS = [
    "spark.sql.optimizer.runtime.bloomFilter.enabled",
    "spark.sql.adaptive.enabled",
    "spark.dynamicAllocation.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.hadoop.fs.s3a.connection.ssl.enabled",
]
print("\n=== ACTIVE CONFIG SNAPSHOT ===")
for k in WATCH_CONFIGS:
    try:
        v = spark.conf.get(k)
    except Exception:
        v = "<not set>"
    print(f"  {k} = {v}")
print("=" * 60)

TABLES = ["lineitem","orders","customer","part","partsupp","supplier","nation","region"]
for t in TABLES:
    spark.read.parquet(f"{MINIO}/{t}.parquet").createOrReplaceTempView(t)

QUERIES = {
"Q21": """
SELECT s_name, count(*) as numwait
FROM supplier,lineitem l1,orders,nation
WHERE s_suppkey=l1.l_suppkey AND o_orderkey=l1.l_orderkey
  AND o_orderstatus='F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND exists (
    SELECT * FROM lineitem l2
    WHERE l2.l_orderkey=l1.l_orderkey AND l2.l_suppkey<>l1.l_suppkey
  )
  AND not exists (
    SELECT * FROM lineitem l3
    WHERE l3.l_orderkey=l1.l_orderkey AND l3.l_suppkey<>l1.l_suppkey
      AND l3.l_receiptdate > l3.l_commitdate
  )
  AND s_nationkey=n_nationkey AND n_name='SAUDI ARABIA'
GROUP BY s_name ORDER BY numwait DESC, s_name
LIMIT 100
"""
}

# Keywords whose presence/absence shows BF and AQE effects in the plan
PLAN_KEYWORDS = [
    # Bloom filter
    "BloomFilter",
    "MightContain",
    "bloomFilter",
    # AQE
    "AdaptiveSparkPlan",
    "CustomShuffleReader",
    "AQEShuffleRead",
    "OptimizeSkewedJoin",
    # Join strategy (AQE may switch SortMergeJoin -> BroadcastHashJoin)
    "BroadcastHashJoin",
    "SortMergeJoin",
    "BroadcastExchange",
    "ShuffleQueryStage",
]

def extract_plan_summary(df):
    """Return the plan string and a hit-list of detected keywords."""
    import io
    from contextlib import redirect_stdout
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(mode="formatted")
    plan_str = buf.getvalue()

    hits   = [kw for kw in PLAN_KEYWORDS if kw in plan_str]
    missing = [kw for kw in PLAN_KEYWORDS if kw not in plan_str]
    return plan_str, hits, missing


results = []
for qname, sql in QUERIES.items():
    print(f"\nRunning {qname}...", flush=True)
    try:
        df = spark.sql(sql)

        plan_str, hits, missing = extract_plan_summary(df)

        print(f"\n=== Physical Plan: {qname} ===")
        print(plan_str)

        print(f"--- Plan keyword scan ---")
        print(f"  PRESENT : {hits   if hits    else '(none)'}")
        print(f"  ABSENT  : {missing if missing else '(none)'}")

        # Highlight key findings
        bf_active  = any(h in ("BloomFilter", "MightContain", "bloomFilter") for h in hits)
        aqe_active = "AdaptiveSparkPlan" in hits
        bcast_used = "BroadcastHashJoin" in hits
        smj_used   = "SortMergeJoin" in hits
        print(f"  -> Bloom filter in plan : {'YES' if bf_active  else 'NO'}")
        print(f"  -> AQE wrapper present  : {'YES' if aqe_active else 'NO'}")
        print(f"  -> Join strategy        : {'BroadcastHashJoin (AQE promoted)' if bcast_used else 'SortMergeJoin' if smj_used else 'other'}")
        print("=" * 60)

        t0 = time.time()
        df.write.mode("overwrite").parquet(f"{MINIO}/results/spark/{qname}")
        elapsed = round((time.time() - t0) * 1000)

        results.append({
            "query": qname,
            "ms": elapsed,
            "status": "OK",
            "plan_keywords_present": hits,
            "plan_keywords_absent": missing,
            "bloom_filter_in_plan": bf_active,
            "aqe_in_plan": aqe_active,
            "join_strategy": "BroadcastHashJoin" if bcast_used else "SortMergeJoin" if smj_used else "other",
        })
        print(f"OK {elapsed}ms")

    except Exception as e:
        results.append({"query": qname, "ms": -1, "status": f"FAIL: {str(e)}"})
        print(f"FAIL: {e}")

out = json.dumps(results, indent=2)
print("\n=== RESULTS ===")
print(out)

spark.createDataFrame(results) \
    .write.mode("overwrite").json(f"{MINIO}/results/spark/summary")

spark.stop()