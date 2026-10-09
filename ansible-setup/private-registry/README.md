
### Registry
```
kubectl delete deployment registry -n registry
kubectl apply -f registry.yml
```

```yaml
---
apiVersion: v1
kind: Namespace
metadata:
  name: registry

---
apiVersion: v1
kind: ConfigMap
metadata:
  name: registry-config
  namespace: registry
data:
  config.yml: |
    version: 0.1
    log:
      level: info
    storage:
      filesystem:
        rootdirectory: /var/lib/registry
      delete:
        enabled: true
      cache:
        blobdescriptor: inmemory
    http:
      addr: :5000
      headers:
        X-Content-Type-Options: [nosniff]
    health:
      storagedriver:
        enabled: true
        interval: 10s
        threshold: 3

---
# 3. Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: registry
  namespace: registry
  labels:
    app: registry
spec:
  replicas: 1
  selector:
    matchLabels:
      app: registry
  template:
    metadata:
      labels:
        app: registry
    spec:
      containers:
        - name: registry
          image: registry:2.8.3
          ports:
            - containerPort: 5000
          env:
            - name: REGISTRY_STORAGE_DELETE_ENABLED
              value: "true"
          volumeMounts:
            - name: config
              mountPath: /etc/docker/registry
              readOnly: true
            - name: data
              mountPath: /var/lib/registry
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 512Mi
          livenessProbe:
            httpGet:
              path: /
              port: 5000
            initialDelaySeconds: 10
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /
              port: 5000
            initialDelaySeconds: 5
            periodSeconds: 10
      volumes:
        - name: config
          configMap:
            name: registry-config
        - name: data
          hostPath:
            path: /data/registry
            type: DirectoryOrCreate

---
apiVersion: v1
kind: Service
metadata:
  name: registry
  namespace: registry
spec:
  selector:
    app: registry
  type: NodePort
  ports:
    - name: http
      port: 5000
      targetPort: 5000
      nodePort: 32000
```