
```
kubectl create namespace monitoring
```

```
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: telegraf-token
  namespace: monitoring
  annotations:
    kubernetes.io/service-account.name: telegraf
type: kubernetes.io/service-account-token
EOF
```

```
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install kube-state-metrics prometheus-community/kube-state-metrics \
  --namespace monitoring \
  --create-namespace
```

```aiignore
kubectl rollout restart daemonset/telegraf -n monitoring
```