### Install minio

#### Double check the inventory file and ansible config

```
ansible-playbook install-docker.yml 
```
#### Setup minio

```
ansible-setup/k8s-ansible/inventory
```

```bash
mc mb localhost/tpch-100
mc cp --recursive ./ localhost/tpch-100/
```

```aiignore
mc ls localhost/tpch-300/endpoints/ | sort | head -n -1 | awk '{print $NF}' | xargs -I{} mc rm --recursive --force localhost/tpch-300/endpoints/{}
* * * * * /usr/local/bin/mc ls localhost/tpch-100/endpoints/ | sort | head -n -1 | awk '{print $NF}' | xargs -I{} /usr/local/bin/mc rm --recursive --force localhost/tpch-100/endpoints/{}
```