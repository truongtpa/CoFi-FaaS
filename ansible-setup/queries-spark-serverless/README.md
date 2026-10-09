### Install helm

```
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-4
chmod 700 get_helm.sh
./get_helm.sh
```

### Kubeflow Spark Operator

```
kubectl create namespace benchmark
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
helm uninstall spark-operator -n spark-operator

helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --wait \
  --set "spark.jobNamespaces={benchmark}"
```

### RBAC cho spark driver
```
kubectl create serviceaccount spark -n benchmark
kubectl create clusterrolebinding spark-benchmark \
  --clusterrole=edit \
  --serviceaccount=benchmark:spark
```

### Move docker from Docker hub to local
```
podman pull docker.io/truongtpa/spark-s3a:3.5.1
podman tag docker.io/truongtpa/spark-s3a:3.5.1  10.158.1.10:32000/spark-s3a:3.5.1
podman push --tls-verify=false 10.158.1.10:32000/spark-s3a:3.5.1
```

### Create a file ```spark-tpch.yaml```
```
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: tpch-benchmark
  namespace: benchmark
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: 10.158.1.10:32000/spark-s3a:3.5.1
  imagePullPolicy: IfNotPresent
  mainApplicationFile: "s3a://tpch-100/tpch-100-runner.py"

  sparkConf:
    "spark.hadoop.fs.s3a.endpoint":          "http://10.158.1.20:8000"
    "spark.hadoop.fs.s3a.access.key":        "admin"
    "spark.hadoop.fs.s3a.secret.key":        "lannion-enssat"
    "spark.hadoop.fs.s3a.path.style.access": "true"
    "spark.hadoop.fs.s3a.impl":              "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.sql.shuffle.partitions":          "200"
    "spark.sql.adaptive.enabled":            "false"
    "spark.sql.optimizer.runtime.bloomFilter.enabled": "false"
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
    "spark.dynamicAllocation.enabled":                    "true"
    "spark.dynamicAllocation.minExecutors":               "1"
    "spark.dynamicAllocation.maxExecutors":               "8"
    "spark.dynamicAllocation.initialExecutors":           "2"
    "spark.dynamicAllocation.executorIdleTimeout":        "30s"
    "spark.dynamicAllocation.schedulerBacklogTimeout":    "5s"
    "spark.dynamicAllocation.shuffleTracking.enabled":    "true"
    "spark.dynamicAllocation.shuffleTracking.timeout":    "5min"

  restartPolicy:
    type: Never
  timeToLiveSeconds: 7200

  driver:
    cores: 2
    memory: "2g"
    serviceAccount: spark
    labels:
      benchmark: tpch

  executor:
    cores: 2
    memory: "2g"
    memoryOverhead: "2g"
    instances: 2
    labels:
      benchmark: tpch
```

### Update spark
```
kubectl delete sparkapplication tpch-benchmark -n benchmark
kubectl apply -f spark-tpch.yaml
kubectl apply -f spark-tpch-aqe.yaml.yaml
kubectl get pods -n benchmark -w
```

### Spark history server
```
mc mb localhost/spark-logs/history

```