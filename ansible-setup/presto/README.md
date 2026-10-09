### Create namespace
```
kubectl create namespace presto
```

### Step 1 - PostgreSQL HMS
```
#postgres.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hms-postgres
  namespace: presto
spec:
  replicas: 1
  selector:
    matchLabels:
      app: hms-postgres
  template:
    metadata:
      labels:
        app: hms-postgres
    spec:
      containers:
      - name: postgres
        image: postgres:15
        env:
        - name: POSTGRES_DB
          value: metastore
        - name: POSTGRES_USER
          value: hive
        - name: POSTGRES_PASSWORD
          value: hive123
        ports:
        - containerPort: 5432
        volumeMounts:
        - name: data
          mountPath: /var/lib/postgresql/data
      volumes:
      - name: data
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: hms-postgres
  namespace: presto
spec:
  selector:
    app: hms-postgres
  ports:
  - port: 5432
```

### Step 2 - Hive Metastore
```
# hive-metastore.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hive-metastore
  namespace: presto
spec:
  replicas: 1
  selector:
    matchLabels:
      app: hive-metastore
  template:
    metadata:
      labels:
        app: hive-metastore
    spec:
      containers:
      - name: metastore
        image: naushadh/hive-metastore
        ports:
        - containerPort: 9083
        env:
        - name: DATABASE_HOST
          value: hms-postgres
        - name: DATABASE_PORT
          value: "5432"
        - name: DATABASE_DB
          value: metastore
        - name: DATABASE_USER
          value: hive
        - name: DATABASE_PASSWORD
          value: hive123
        - name: S3_ENDPOINT_URL
          value: http://10.158.1.20:8000
        - name: S3_ACCESS_KEY
          value: admin
        - name: S3_SECRET_KEY
          value: lannion-enssat
        - name: S3_BUCKET
          value: tpch-100
        - name: S3_PREFIX
          value: s3a
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
---
apiVersion: v1
kind: Service
metadata:
  name: hive-metastore
  namespace: presto
spec:
  selector:
    app: hive-metastore
  ports:
  - port: 9083
```

```
kubectl -n presto delete deployment hive-metastore
kubectl apply -f hive-metastore.yaml
kubectl -n presto logs -l app=hive-metastore -f
kubectl -n presto set env deployment/hive-metastore AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=lannion-enssat
kubectl -n presto rollout status deployment/hive-metastore
```

### Step 3 - Presto qua Helm
```
helm repo add trino https://trinodb.github.io/charts
helm repo update

cat > trino-values.yaml << 'EOF'
coordinator:
  resources:
    requests:
      memory: "4Gi"
      cpu: "1"
    limits:
      memory: "4Gi"
worker:
  replicas: 2
  resources:
    requests:
      memory: "4Gi"
      cpu: "2"
    limits:
      memory: "4Gi"
server:
  workers: 2
  config:
    query:
      maxMemory: "4GB"
      maxMemoryPerNode: "4GB"
coordinatorExtraConfig: |
  query.max-memory-per-node=4GB
  spill-enabled=true
  spiller-spill-path=/tmp/trino-spill
  spiller-max-used-space-threshold=0.9
workerExtraConfig: |
  query.max-memory-per-node=4GB
  spill-enabled=true
  spiller-spill-path=/tmp/trino-spill
  spiller-max-used-space-threshold=0.9
additionalCatalogs:
  hive: |
    connector.name=hive
    hive.metastore.uri=thrift://hive-metastore.presto.svc.cluster.local:9083
    fs.native-s3.enabled=true
    s3.endpoint=http://10.158.1.20:8000
    s3.region=us-east-1
    s3.aws-access-key=admin
    s3.aws-secret-key=lannion-enssat
    s3.path-style-access=true
service:
  type: NodePort
  nodePort: 30080
EOF

kubectl -n presto get pods -l app=trino,component=worker -o name | while read pod; do
  kubectl -n presto exec $pod -- mkdir -p /tmp/trino-spill
done

kubectl -n presto rollout restart deployment/trino-coordinator
kubectl -n presto rollout restart deployment/trino-worker
```

```yaml
helm uninstall trino -n presto
helm install trino trino/trino -n presto -f trino-values.yaml
kubectl -n presto get pods -w
```

### Get GUI
```
export NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[0].address}')
export NODE_PORT=$(kubectl get svc -n presto trino -o jsonpath='{.spec.ports[0].nodePort}')
echo "Trino UI: http://$NODE_IP:$NODE_PORT"
```

```
kubectl -n presto exec -it deploy/trino-coordinator -- trino --user admin --execute "
CREATE SCHEMA IF NOT EXISTS hive.tpch100 WITH (location = 's3a://tpch-100/');
"

kubectl -n presto exec -it deploy/trino-coordinator -- trino --user admin << 'EOF'
CREATE TABLE hive.tpch100.nation (n_nationkey BIGINT, n_name VARCHAR, n_regionkey BIGINT, n_comment VARCHAR) WITH (format='PARQUET', external_location='s3a://tpch-100/nation.parquet');
CREATE TABLE hive.tpch100.region (r_regionkey BIGINT, r_name VARCHAR, r_comment VARCHAR) WITH (format='PARQUET', external_location='s3a://tpch-100/region.parquet');
CREATE TABLE hive.tpch100.supplier (s_suppkey BIGINT, s_name VARCHAR, s_address VARCHAR, s_nationkey BIGINT, s_phone VARCHAR, s_acctbal DECIMAL(15,2), s_comment VARCHAR) WITH (format='PARQUET', external_location='s3a://tpch-100/supplier.parquet');
CREATE TABLE hive.tpch100.customer (c_custkey BIGINT, c_name VARCHAR, c_address VARCHAR, c_nationkey BIGINT, c_phone VARCHAR, c_acctbal DECIMAL(15,2), c_mktsegment VARCHAR, c_comment VARCHAR) WITH (format='PARQUET', external_location='s3a://tpch-100/customer.parquet');
CREATE TABLE hive.tpch100.part (p_partkey BIGINT, p_name VARCHAR, p_mfgr VARCHAR, p_brand VARCHAR, p_type VARCHAR, p_size INTEGER, p_container VARCHAR, p_retailprice DECIMAL(15,2), p_comment VARCHAR) WITH (format='PARQUET', external_location='s3a://tpch-100/part.parquet');
CREATE TABLE hive.tpch100.partsupp (ps_partkey BIGINT, ps_suppkey BIGINT, ps_availqty INTEGER, ps_supplycost DECIMAL(15,2), ps_comment VARCHAR) WITH (format='PARQUET', external_location='s3a://tpch-100/partsupp.parquet');
CREATE TABLE hive.tpch100.orders (o_orderkey BIGINT, o_custkey BIGINT, o_orderstatus VARCHAR, o_totalprice DECIMAL(15,2), o_orderdate DATE, o_orderpriority VARCHAR, o_clerk VARCHAR, o_shippriority INTEGER, o_comment VARCHAR) WITH (format='PARQUET', external_location='s3a://tpch-100/orders.parquet');
CREATE TABLE hive.tpch100.lineitem (l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INTEGER, l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2), l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR) WITH (format='PARQUET', external_location='s3a://tpch-100/lineitem.parquet');
SHOW TABLES FROM hive.tpch100;
EOF
```



### Bash run TPC-H query
```bash
cat > run-tpch.sh << 'SCRIPT'
#!/bin/bash
TRINO="kubectl -n presto exec -i deploy/trino-coordinator -- trino --user admin"
RESULTS="trino_tpch_$(date +%Y%m%d_%H%M%S).csv"
echo "query,elapsed_sec,status" > $RESULTS

run_query() {
  local Q=$1
  local SQL=$2
  echo -n "Running $Q... "
  START=$(date +%s%3N)
  echo "$SQL" | $TRINO > /tmp/${Q}_out.txt 2>/tmp/${Q}_err.txt
  STATUS=$?
  END=$(date +%s%3N)
  ELAPSED=$(echo "scale=3; ($END-$START)/1000" | bc)
  if [ $STATUS -eq 0 ]; then
    echo "OK (${ELAPSED}s)"
    echo "$Q,${ELAPSED},OK" >> $RESULTS
  else
    echo "FAIL - $(cat /tmp/${Q}_err.txt | grep 'failed:' | head -1)"
    echo "$Q,${ELAPSED},FAIL" >> $RESULTS
  fi
}

# Q1 - Pricing Summary
run_query "Q1" "SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice), SUM(l_extendedprice*(1-l_discount)), COUNT(*) FROM hive.tpch100.lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;"

# Q2 - Minimum Cost Supplier
run_query "Q2" "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM hive.tpch100.part, hive.tpch100.supplier, hive.tpch100.partsupp, hive.tpch100.nation, hive.tpch100.region WHERE p_partkey=ps_partkey AND s_suppkey=ps_suppkey AND p_size=15 AND p_type LIKE '%BRASS' AND s_nationkey=n_nationkey AND n_regionkey=r_regionkey AND r_name='EUROPE' AND ps_supplycost=(SELECT MIN(ps_supplycost) FROM hive.tpch100.partsupp, hive.tpch100.supplier, hive.tpch100.nation, hive.tpch100.region WHERE p_partkey=ps_partkey AND s_suppkey=ps_suppkey AND s_nationkey=n_nationkey AND n_regionkey=r_regionkey AND r_name='EUROPE') ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100;"

# Q3 - Shipping Priority
run_query "Q3" "SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) AS revenue, o_orderdate, o_shippriority FROM hive.tpch100.customer, hive.tpch100.orders, hive.tpch100.lineitem WHERE c_mktsegment='BUILDING' AND c_custkey=o_custkey AND l_orderkey=o_orderkey AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;"

# Q4 - Order Priority Checking
run_query "Q4" "SELECT o_orderpriority, COUNT(*) AS order_count FROM hive.tpch100.orders WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01' AND EXISTS (SELECT * FROM hive.tpch100.lineitem WHERE l_orderkey=o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority;"

# Q5 - Local Supplier Volume
run_query "Q5" "SELECT n_name, SUM(l_extendedprice*(1-l_discount)) AS revenue FROM hive.tpch100.customer, hive.tpch100.orders, hive.tpch100.lineitem, hive.tpch100.supplier, hive.tpch100.nation, hive.tpch100.region WHERE c_custkey=o_custkey AND l_orderkey=o_orderkey AND l_suppkey=s_suppkey AND c_nationkey=s_nationkey AND s_nationkey=n_nationkey AND n_regionkey=r_regionkey AND r_name='ASIA' AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01' GROUP BY n_name ORDER BY revenue DESC;"

# Q6 - Forecasting Revenue Change
run_query "Q6" "SELECT SUM(l_extendedprice*l_discount) AS revenue FROM hive.tpch100.lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN DECIMAL '0.05' AND DECIMAL '0.07' AND l_quantity < 24;"

# Q7 - Volume Shipping
run_query "Q7" "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, EXTRACT(YEAR FROM l_shipdate) AS l_year, l_extendedprice*(1-l_discount) AS volume FROM hive.tpch100.supplier, hive.tpch100.lineitem, hive.tpch100.orders, hive.tpch100.customer, hive.tpch100.nation n1, hive.tpch100.nation n2 WHERE s_suppkey=l_suppkey AND o_orderkey=l_orderkey AND c_custkey=o_custkey AND s_nationkey=n1.n_nationkey AND c_nationkey=n2.n_nationkey AND ((n1.n_name='FRANCE' AND n2.n_name='GERMANY') OR (n1.n_name='GERMANY' AND n2.n_name='FRANCE')) AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year;"

# Q8 - National Market Share
run_query "Q8" "SELECT o_year, SUM(CASE WHEN nation='BRAZIL' THEN volume ELSE 0 END)/SUM(volume) AS mkt_share FROM (SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice*(1-l_discount) AS volume, n2.n_name AS nation FROM hive.tpch100.part, hive.tpch100.supplier, hive.tpch100.lineitem, hive.tpch100.orders, hive.tpch100.customer, hive.tpch100.nation n1, hive.tpch100.nation n2, hive.tpch100.region WHERE p_partkey=l_partkey AND s_suppkey=l_suppkey AND l_orderkey=o_orderkey AND o_custkey=c_custkey AND c_nationkey=n1.n_nationkey AND n1.n_regionkey=r_regionkey AND r_name='AMERICA' AND s_nationkey=n2.n_nationkey AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' AND p_type='ECONOMY ANODIZED STEEL') AS all_nations GROUP BY o_year ORDER BY o_year;"

# Q9 - Product Type Profit Measure
run_query "Q9" "SELECT nation, o_year, SUM(amount) AS sum_profit FROM (SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice*(1-l_discount)-ps_supplycost*l_quantity AS amount FROM hive.tpch100.part, hive.tpch100.supplier, hive.tpch100.lineitem, hive.tpch100.partsupp, hive.tpch100.orders, hive.tpch100.nation WHERE s_suppkey=l_suppkey AND ps_suppkey=l_suppkey AND ps_partkey=l_partkey AND p_partkey=l_partkey AND o_orderkey=l_orderkey AND s_nationkey=n_nationkey AND p_name LIKE '%green%') AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC;"

# Q10 - Returned Item Reporting
run_query "Q10" "SELECT c_custkey, c_name, SUM(l_extendedprice*(1-l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM hive.tpch100.customer, hive.tpch100.orders, hive.tpch100.lineitem, hive.tpch100.nation WHERE c_custkey=o_custkey AND l_orderkey=o_orderkey AND o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01' AND l_returnflag='R' AND c_nationkey=n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20;"

# Q11 - Important Stock Identification
run_query "Q11" "SELECT ps_partkey, SUM(ps_supplycost*ps_availqty) AS value FROM hive.tpch100.partsupp, hive.tpch100.supplier, hive.tpch100.nation WHERE ps_suppkey=s_suppkey AND s_nationkey=n_nationkey AND n_name='GERMANY' GROUP BY ps_partkey HAVING SUM(ps_supplycost*ps_availqty) > (SELECT SUM(ps_supplycost*ps_availqty)*0.0001 FROM hive.tpch100.partsupp, hive.tpch100.supplier, hive.tpch100.nation WHERE ps_suppkey=s_suppkey AND s_nationkey=n_nationkey AND n_name='GERMANY') ORDER BY value DESC;"

# Q12 - Shipping Modes
run_query "Q12" "SELECT l_shipmode, SUM(CASE WHEN o_orderpriority='1-URGENT' OR o_orderpriority='2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority<>'1-URGENT' AND o_orderpriority<>'2-HIGH' THEN 1 ELSE 0 END) AS low_line_count FROM hive.tpch100.orders, hive.tpch100.lineitem WHERE o_orderkey=l_orderkey AND l_shipmode IN ('MAIL','SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1995-01-01' GROUP BY l_shipmode ORDER BY l_shipmode;"

# Q13 - Customer Distribution
run_query "Q13" "SELECT c_count, COUNT(*) AS custdist FROM (SELECT c_custkey, COUNT(o_orderkey) AS c_count FROM hive.tpch100.customer LEFT OUTER JOIN hive.tpch100.orders ON c_custkey=o_custkey AND o_comment NOT LIKE '%special%requests%' GROUP BY c_custkey) AS c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC;"

# Q14 - Promotion Effect
run_query "Q14" "SELECT 100.00*SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice*(1-l_discount) ELSE 0 END)/SUM(l_extendedprice*(1-l_discount)) AS promo_revenue FROM hive.tpch100.lineitem, hive.tpch100.part WHERE l_partkey=p_partkey AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01';"

# Q15 - Top Supplier
run_query "Q15" "WITH revenue AS (SELECT l_suppkey AS supplier_no, SUM(l_extendedprice*(1-l_discount)) AS total_revenue FROM hive.tpch100.lineitem WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01' GROUP BY l_suppkey) SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM hive.tpch100.supplier, revenue WHERE s_suppkey=supplier_no AND total_revenue=(SELECT MAX(total_revenue) FROM revenue) ORDER BY s_suppkey;"

# Q16 - Parts/Supplier Relationship
run_query "Q16" "SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt FROM hive.tpch100.partsupp, hive.tpch100.part WHERE p_partkey=ps_partkey AND p_brand<>'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size IN (49,14,23,45,19,3,36,9) AND ps_suppkey NOT IN (SELECT s_suppkey FROM hive.tpch100.supplier WHERE s_comment LIKE '%Customer%Complaints%') GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;"

# Q17 - Small-Quantity Order Revenue
run_query "Q17" "SELECT SUM(l_extendedprice)/7.0 AS avg_yearly FROM hive.tpch100.lineitem, hive.tpch100.part WHERE p_partkey=l_partkey AND p_brand='Brand#23' AND p_container='MED BOX' AND l_quantity < (SELECT 0.2*AVG(l_quantity) FROM hive.tpch100.lineitem WHERE l_partkey=p_partkey);"

# Q18 - Large Volume Customer
run_query "Q18" "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) FROM hive.tpch100.customer, hive.tpch100.orders, hive.tpch100.lineitem WHERE o_orderkey IN (SELECT l_orderkey FROM hive.tpch100.lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300) AND c_custkey=o_custkey AND o_orderkey=l_orderkey GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate LIMIT 100;"

# Q19 - Discounted Revenue
run_query "Q19" "SELECT SUM(l_extendedprice*(1-l_discount)) AS revenue FROM hive.tpch100.lineitem, hive.tpch100.part WHERE (p_partkey=l_partkey AND p_brand='Brand#12' AND p_container IN ('SM CASE','SM BOX','SM PACK','SM PKG') AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5 AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct='DELIVER IN PERSON') OR (p_partkey=l_partkey AND p_brand='Brand#23' AND p_container IN ('MED BAG','MED BOX','MED PKG','MED PACK') AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10 AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct='DELIVER IN PERSON') OR (p_partkey=l_partkey AND p_brand='Brand#34' AND p_container IN ('LG CASE','LG BOX','LG PACK','LG PKG') AND l_quantity >= 20 AND l_quantity <= 30 AND p_size BETWEEN 1 AND 15 AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct='DELIVER IN PERSON');"

# Q20 - Potential Part Promotion
run_query "Q20" "SELECT s_name, s_address FROM hive.tpch100.supplier, hive.tpch100.nation WHERE s_suppkey IN (SELECT ps_suppkey FROM hive.tpch100.partsupp WHERE ps_partkey IN (SELECT p_partkey FROM hive.tpch100.part WHERE p_name LIKE 'forest%') AND ps_availqty > (SELECT 0.5*SUM(l_quantity) FROM hive.tpch100.lineitem WHERE l_partkey=ps_partkey AND l_suppkey=ps_suppkey AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01')) AND s_nationkey=n_nationkey AND n_name='CANADA' ORDER BY s_name;"

# Q21 - Suppliers Who Kept Orders Waiting
run_query "Q21" "SELECT s_name, COUNT(*) AS numwait FROM hive.tpch100.supplier, hive.tpch100.lineitem l1, hive.tpch100.orders, hive.tpch100.nation WHERE s_suppkey=l1.l_suppkey AND o_orderkey=l1.l_orderkey AND o_orderstatus='F' AND l1.l_receiptdate > l1.l_commitdate AND EXISTS (SELECT * FROM hive.tpch100.lineitem l2 WHERE l2.l_orderkey=l1.l_orderkey AND l2.l_suppkey<>l1.l_suppkey) AND NOT EXISTS (SELECT * FROM hive.tpch100.lineitem l3 WHERE l3.l_orderkey=l1.l_orderkey AND l3.l_suppkey<>l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey=n_nationkey AND n_name='SAUDI ARABIA' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100;"

# Q22 - Global Sales Opportunity
run_query "Q22" "SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal FROM (SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal FROM hive.tpch100.customer WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17') AND c_acctbal > (SELECT AVG(c_acctbal) FROM hive.tpch100.customer WHERE c_acctbal > 0.00 AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17')) AND NOT EXISTS (SELECT * FROM hive.tpch100.orders WHERE o_custkey=c_custkey)) AS custsale GROUP BY cntrycode ORDER BY cntrycode;"

echo ""
echo "=== RESULTS ==="
cat $RESULTS
SCRIPT
```