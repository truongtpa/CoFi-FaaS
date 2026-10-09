# Kubernetes-dashboard

### Setup
```
{
git clone https://github.com/beta21s/kubernetes-dashboard.git
cd kubernetes-dashboard

kubectl apply -f recommended.yaml
kubectl apply -f dashboard-admin.yaml
kubectl apply -f components.yaml
kubectl apply -f dashboard-admin-bind-cluster-role.yaml
}

{
kubectl delete -f recommended.yaml
kubectl delete -f dashboard-admin.yaml
kubectl delete -f components.yaml
kubectl delete -f dashboard-admin-bind-cluster-role.yaml
}
```

### Get token
```
kubectl -n kubernetes-dashboard create token admin-user --duration=488h
```