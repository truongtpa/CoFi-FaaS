# Bước 1: Cài đặt Knative Serving

``` 
kubectl create namespace argo
kubectl apply -n argo -f https://github.com/argoproj/argo-workflows/releases/download/v3.7.7/install.yaml

kubectl patch svc argo-server -n argo -p '{"spec":{"type":"NodePort"}}'

kubectl patch deployment argo-server -n argo --type='json' -p='[
  {"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": [
    "server",
    "--auth-mode=server"
  ]}
]'

kubectl apply -n argo -f - <<'EOF'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: workflow-sa
  namespace: argo
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: workflow-role
  namespace: argo
rules:
  - apiGroups:
      - ""
    resources:
      - pods
      - pods/log
    verbs:
      - get
      - watch
      - patch
  - apiGroups:
      - ""
    resources:
      - pods/exec
    verbs:
      - create
  - apiGroups:
      - argoproj.io
    resources:
      - workflowtaskresults
      - workflowtasksets
      - workflowtasksets/status
    verbs:
      - create
      - get
      - list
      - watch
      - update
      - patch
      - delete
  - apiGroups:
      - argoproj.io
    resources:
      - workflows
      - workflows/finalizers
    verbs:
      - get
      - list
      - watch
      - update
      - patch
  - apiGroups:
      - argoproj.io
    resources:
      - workflowtemplates
    verbs:
      - get
      - list
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: workflow-role-binding
  namespace: argo
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: workflow-role
subjects:
  - kind: ServiceAccount
    name: workflow-sa
    namespace: argo
EOF

kubectl get svc argo-server -n argo
```
