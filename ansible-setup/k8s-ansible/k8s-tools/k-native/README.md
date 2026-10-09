# Bước 1: Cài đặt Knative Serving

### Setup Knative Serving CRDs
```
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.12.0/serving-crds.yaml
```

### Setup core components
```
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.12.0/serving-core.yaml
```

### Kiểm tra pods đã ready
```
kubectl get pods -n knative-serving
```

# Bước 2: Cài đặt Networking Layer (Kourier)
### Cài đặt Kourier (lightweight alternative cho Istio)
```
kubectl apply -f https://github.com/knative/net-kourier/releases/download/knative-v1.12.0/kourier.yaml
```

### Configure Knative Serving Kourier
```
-- external-ip
kubectl patch configmap/config-network \
--namespace knative-serving \
--type merge \
--patch '{"data":{"ingress-class":"kourier.ingress.networking.knative.dev"}}'

-- nodeport
kubectl patch service kourier -n kourier-system --type='json' \
  -p='[{"op": "replace", "path": "/spec/type", "value": "NodePort"}]'

```

### Lấy External IP của Kourier
```
kubectl get svc kourier -n kourier-system
kubectl get nodes -o wide
```

### Cấu hình domain để dùng nip.io
```
kubectl patch configmap/config-domain --namespace knative-serving  --type merge  --patch '{"data":{"10.158.3.227.nip.io":""}}'
```

### Lấy NodePort của Kourier

```
kubectl get svc kourier -n kourier-system
```

# Example
```
kubectl apply -f - <<EOF
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: hello2
  namespace: default
spec:
  template:
    metadata:
      annotations:
        autoscaling.knative.dev/min-scale: "1"
        autoscaling.knative.dev/max-scale: "10"
    spec:
      containers:
        - image: gcr.io/knative-samples/helloworld-go
          ports:
            - containerPort: 8080
          env:
            - name: TARGET
              value: "Knative on K8s Cluster 2"
EOF
```

```
kubectl get ksvc hello
kubectl get pods -l serving.knative.dev/service=hello
curl -H "Host: scan-project-filter-operation.default.10.158.3.227.nip.io" http://10.158.3.227:30304
curl http://hello.default.10.158.3.228.nip.io:31611

curl -H "Host: http://step-0.default.svc.cluster.local" http://10.158.3.228:31611
```

# Full setup
```
{
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.12.0/serving-crds.yaml
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.12.0/serving-core.yaml
kubectl get pods -n knative-serving
kubectl apply -f https://github.com/knative/net-kourier/releases/download/knative-v1.12.0/kourier.yaml
kubectl patch configmap/config-network --namespace knative-serving --type merge --patch '{"data":{"ingress-class":"kourier.ingress.networking.knative.dev"}}'
kubectl patch service kourier -n kourier-system --type='json' -p='[{"op": "replace", "path": "/spec/type", "value": "NodePort"}]'
kubectl get svc kourier -n kourier-system
kubectl get nodes -o wide
kubectl patch configmap/config-domain --namespace knative-serving  --type merge  --patch '{"data":{"10.158.1.10.nip.io":""}}'
}

kubectl patch service kourier -n kourier-system --type='json' -p='[
  {"op": "replace", "path": "/spec/type", "value": "NodePort"},
  {"op": "add", "path": "/spec/ports/0/nodePort", "value": 30080},
  {"op": "add", "path": "/spec/ports/1/nodePort", "value": 30443}
]'
```

# Enable Knative storage
```
kubectl patch configmap config-features -n knative-serving \
  -p '{"data":{"kubernetes.podspec-persistent-volume-claim":"enabled"}}'
  
kubectl patch configmap config-features -n knative-serving \
  -p '{"data":{"kubernetes.podspec-persistent-volume-write":"enabled"}}'
```

# Restart webhook (quan trọng nhất)
```
kubectl rollout restart deployment webhook -n knative-serving
```

# Restart controller
```
kubectl rollout restart deployment controller -n knative-serving
```

# Restart autoscaler
```
kubectl rollout restart deployment autoscaler -n knative-serving
```
