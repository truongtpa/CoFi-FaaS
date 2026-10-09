# Knative and Podman
```
{
wget https://github.com/knative/func/releases/download/knative-v1.20.1/func_linux_amd64
mv func_linux_amd64 func
chmod +x func
mv func /usr/local/bin
func version
}

func create -l python hello

apt install -y podman
sudo nano /etc/containers/registries.conf

func build --registry docker.io/xx -v
func deploy --registry docker.io/xx -v

podman system reset --force
sudo systemctl restart podman.socket
export DOCKER_HOST=unix:///run/podman/podman.sock
export BUILDAH_FORMAT=docker
rm -rf .func/
podman login docker.io -u xx 
```

```
apt install -y podman
export DOCKER_HOST=unix:///run/podman/podman.sock

echo dckr_pat_xx | podman login docker.io -u xx 
```

## CÁCH 2: Manual (nhanh nhất - 1 phút)
```
# Trên master node
kubectl patch cm -n kube-flannel kube-flannel-cfg --type merge -p '
{
  "data": {
    "net-conf.json": "{\"Network\": \"10.244.0.0/16\", \"Backend\": {\"Type\": \"host-gw\"}}"
  }
}'

# Restart Flannel
kubectl delete pods -n kube-flannel --all
```

## CÁCH 3: Hybrid mode (DirectRouting - khuyến nghị)
```
kubectl patch cm -n kube-flannel kube-flannel-cfg --type merge -p '
{
  "data": {
    "net-conf.json": "{\"Network\": \"10.244.0.0/16\", \"Backend\": {\"Type\": \"vxlan\", \"DirectRouting\": true}}"
  }
}'

kubectl delete pods -n kube-flannel --all
```


```bash
python3 << 'EOF'
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("""
    SET s3_endpoint='10.158.1.20:9000';
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='lannion-enssat';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")
result = conn.execute("SELECT COUNT(DISTINCT l_orderkey) FROM read_parquet('tpch-30/benchmarks/q12/step1-lineitem//*')").fetchone()[0]
print(f"Distinct l_orderkey: {result:,}")
EOF
```

```python 
def load_bf(path):
    return BloomFilter.from_file_with_hashes(path)

def bloom_contains(key):
    return bf.contains(key.to_bytes(8, byteorder='little', signed=True))

duckdb.create_function('bloom_contains_py', bloom_contains, return_type='BOOLEAN')
result = duckdb.execute("SELECT COUNT(*) FROM lineitem WHERE bloom_contains_py(l_orderkey)")
```


```python
duckdb.execute(f"LOAD 'bfextension.duckdb_extension'")
duckdb.execute(f"SELECT bloom_load('path_bloom_filter.bin')")
result = duckdb.execute("SELECT COUNT(*) FROM lineitem WHERE bloom_contains(l_orderkey) ")
```

```aiignore
kubectl delete pods --all -n bloomfaas --grace-period=0 --force
```

###
Plan

- BLOOM-FaaS (25MB - COMPRESSION ZSTD) 
- 