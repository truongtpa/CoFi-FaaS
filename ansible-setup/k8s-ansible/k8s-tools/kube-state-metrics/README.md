# Add repo
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Install kube-state-metrics với NodePort
helm install kube-state-metrics prometheus-community/kube-state-metrics \
  --namespace monitoring \
  --create-namespace \
  --set service.type=NodePort \
  --set service.nodePort=30880

# Install node-exporter
helm install node-exporter prometheus-community/prometheus-node-exporter \
  --namespace monitoring \
  --set service.type=ClusterIP \
  --set hostNetwork=true \
  --set hostPID=true

# Tạo ServiceAccount
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: prometheus-external
  namespace: monitoring
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: prometheus-external
rules:
- apiGroups: [""]
  resources:
  - nodes
  - nodes/proxy
  - nodes/metrics
  - services
  - endpoints
  - pods
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources:
  - configmaps
  verbs: ["get"]
- nonResourceURLs: ["/metrics", "/metrics/cadvisor"]
  verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: prometheus-external
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: prometheus-external
subjects:
- kind: ServiceAccount
  name: prometheus-external
  namespace: monitoring
EOF

# Lấy token (K8s >= 1.24)
kubectl create token prometheus-external -n monitoring --duration=876000h > /tmp/k8s-token.txt


# /etc/prometheus/prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  # Kube-state-metrics
  - job_name: 'kube-state-metrics'
    static_configs:
    - targets: ['<K8S_NODE_IP>:30880']
      labels:
        cluster: 'bloom-faas'

  # Node metrics từ tất cả nodes
  - job_name: 'kubernetes-nodes'
    static_configs:
    - targets:
      - '<K8S_NODE1_IP>:9100'
      - '<K8S_NODE2_IP>:9100'
      - '<K8S_NODE3_IP>:9100'
      labels:
        cluster: 'bloom-faas'

  # Container metrics (cAdvisor)
  - job_name: 'kubernetes-cadvisor'
    scheme: https
    tls_config:
      insecure_skip_verify: true
    bearer_token_file: /etc/prometheus/k8s-token.txt
    metrics_path: /metrics/cadvisor
    static_configs:
    - targets:
      - '<K8S_NODE1_IP>:10250'
      - '<K8S_NODE2_IP>:10250'
      - '<K8S_NODE3_IP>:10250'
      labels:
        cluster: 'bloom-faas'

  # Kubelet metrics
  - job_name: 'kubernetes-kubelet'
    scheme: https
    tls_config:
      insecure_skip_verify: true
    bearer_token_file: /etc/prometheus/k8s-token.txt
    static_configs:
    - targets:
      - '<K8S_NODE1_IP>:10250'
      - '<K8S_NODE2_IP>:10250'
      - '<K8S_NODE3_IP>:10250'
      labels:
        cluster: 'bloom-faas'
