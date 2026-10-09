### Install node-exporter

```
ansible-playbook -i inventory/hosts.ini install-node-exporter.yml
ansible all -i inventory/hosts.ini -m shell -a "systemctl status node_exporter" -b
```


### RDAF
```
wget https://dl.influxdata.com/telegraf/releases/telegraf_1.33.0-1_amd64.deb
sudo dpkg -i telegraf_1.33.0-1_amd64.deb


cat /sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/energy_uj
sudo setcap cap_sys_rawio,cap_dac_read_search+ep /usr/bin/telegraf
sudo cat /sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/energy_uj
```

#### Test
```
sudo telegraf --config /etc/telegraf/telegraf.conf --input-filter intel_powerstat --test
```


#### Push leen InfluxDB
```
sudo tee /etc/telegraf/telegraf.conf << 'EOF'
[agent]
  interval = "1s"
  flush_interval = "1s"

[[inputs.intel_powerstat]]
  package_metrics = [
    "current_power_consumption",
    "current_dram_power_consumption",
    "thermal_design_power"
  ]

[[outputs.influxdb_v2]]
  urls = ["http://10.158.1.30:8086"]
  token = "VT_xheyZI0cHeftJCEfUMD8drD5RNA5ga7-NURn4fnPZUttyDRdDhmKGhDI36cK1PX9-tmUxc7qhqAgcVl9hNg=="
  organization = "myorg"
  bucket = "metrics"
EOF
```

```
sudo systemctl enable telegraf
sudo systemctl start telegraf
sudo systemctl status telegraf

# Xem log nếu có lỗi
sudo journalctl -u telegraf -f
```


### k8s cadvitor

```
# 1. Tạo ServiceAccount
kubectl create serviceaccount telegraf-cadvisor -n kube-system

# 2. Tạo RBAC
kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: telegraf-cadvisor
rules:
  - apiGroups: [""]
    resources:
      - nodes
      - nodes/metrics
      - nodes/proxy
      - nodes/stats
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: telegraf-cadvisor
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: telegraf-cadvisor
subjects:
  - kind: ServiceAccount
    name: telegraf-cadvisor
    namespace: kube-system
EOF

# 3. Tạo Secret token
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: telegraf-cadvisor-token
  namespace: kube-system
  annotations:
    kubernetes.io/service-account.name: telegraf-cadvisor
type: kubernetes.io/service-account-token
EOF

# 4. Đợi K8s populate token
sleep 3

# 5. Lấy token
kubectl get secret telegraf-cadvisor-token -n kube-system -o jsonpath='{.data.token}' | base64 -d
```


### Export and Import InfluxDB

```
export TOKEN=$(cat /etc/influxdb/admin-token.txt)
export TOKEN=Bt5JRwAzqQIGnPakazzwWS4oaUyQanFlNGjbFWlhP_iyoVkdD23BIbLnzV7xGJpEjJ8zyPLVqLDR8DZBvI1mNA==
influx backup ./backup/ \
  --host http://localhost:8086 \
  --token $TOKEN \
  --org myorg
  
tar -czvf influx-backup-$(date +%F).tar.gz ./backup
tar -xzvf influx-backup-2026-03-02.tar.gz
  
influx bucket delete \
  --name metrics \
  --org myorg \
  --token $TOKEN
  
 influx restore /backup/t1/ --bucket telegraf --new-bucket telegraf_t1
 
influx restore \
  --host http://localhost:8086 \
  --token $TOKEN \
  --org myorg \
  ./backup
  
  
influx restore --host http://localhost:8086
  --token $TOKEN \
  --org myorg \
  --bucket metrics \ 
  --new-bucket metrics \
  ./backup
```