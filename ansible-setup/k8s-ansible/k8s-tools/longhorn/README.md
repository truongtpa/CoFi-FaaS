
```
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-4
chmod 700 get_helm.sh
./get_helm.sh

helm repo add longhorn https://charts.longhorn.io
helm repo update
helm install longhorn longhorn/longhorn \
  --namespace longhorn-system \
  --create-namespace \
  --version 1.7.2
  
kubectl patch svc longhorn-frontend -n longhorn-system \
-p '{"spec": {"type": "NodePort"}}'

kubectl get svc longhorn-frontend -n longhorn-system


cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: longhorn-rwx
provisioner: driver.longhorn.io
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
parameters:
  numberOfReplicas: "3"
  staleReplicaTimeout: "30"
  fromBackup: ""
  fsType: "ext4"
  nfsOptions: "vers=4.1,noresvport,timeo=600,retrans=5,hard"
  migratable: "true"
EOF
 -----
 
 
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: bloomfaas-storage-rwx
  namespace: default
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: longhorn-rwx
  resources:
    requests:
      storage: 10Gi
EOF
 ------
 
 
 Enable Knative storage
 
 kubectl patch configmap config-features -n knative-serving \
  -p '{"data":{"kubernetes.podspec-persistent-volume-claim":"enabled"}}'

kubectl patch configmap config-features -n knative-serving \
  -p '{"data":{"kubernetes.podspec-persistent-volume-write":"enabled"}}'
  
  
 # Restart webhook (quan trọng nhất)
kubectl rollout restart deployment webhook -n knative-serving

# Restart controller
kubectl rollout restart deployment controller -n knative-serving

# Restart autoscaler
kubectl rollout restart deployment autoscaler -n knative-serving
 
```