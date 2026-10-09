

### Prometheus

```
helm install prometheus --repo https://prometheus-community.github.io/helm-charts prometheus \
  --namespace prometheus-system --create-namespace \
  --set prometheus-pushgateway.enabled=false \
  --set alertmanager.enabled=false \
  -f https://raw.githubusercontent.com/opencost/opencost/develop/kubernetes/prometheus/extraScrapeConfigs.yaml
```

### values.yaml

```
clusterName: "vlute-k8s"

service:
  type: NodePort
  nodePort: {}

opencost:
  exporter:
    defaultClusterId: "vlute-k8s"
    resources:
      requests:
        cpu: "50m"
        memory: "256Mi"
      limits:
        memory: "1Gi"

  prometheus:
    internal:
      enabled: true
      serviceName: prometheus-server
      namespaceName: prometheus-system
      port: 80
      scheme: http

  metrics:
    serviceMonitor:
      enabled: false
      additionalLabels:
        release: kube-prometheus-stack
      scrapeInterval: 30s
    kubeStateMetrics:
      emitKsmV1MetricsOnly: true

  customPricing:
    enabled: true
    provider: custom
    costModel:
      description: "Bang gia noi bo VLUTE"
      CPU: 0.0028
      spotCPU: 0.0028
      RAM: 0.0007
      spotRAM: 0.0007
      GPU: 0.15
      storage: 0.00007
      zoneNetworkEgress: 0.0
      regionNetworkEgress: 0.0
      internetNetworkEgress: 0.0

  cloudCost:
    enabled: false

  ui:
    enabled: true
    uiPort: 9090
    useIPv6: false
    resources:
      requests:
        cpu: "10m"
        memory: "64Mi"
      limits:
        memory: "256Mi"

  mcp:
    enabled: false

plugins:
  enabled: false

extraObjects: []
```