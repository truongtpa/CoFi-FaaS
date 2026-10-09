## Setup ```env``` for ```BLOOM-FaaS```

BLOOM-FaaS need:
- 01 Kubernetes cluster (Cluster size depends on the resources you have on grid5000 system);
- 01 MinIO system;
- 01 Network file system

All ```env``` on this project can be used with Ansible to setup automatically 

#### 1. Kubernetes cluster (K8s)

This document guides the setup of 3 VMs for a K8S cluster. On ```physical VM```: run command ```g5k-subnets -im``` that shows the list of MAC addresses available that can be used on g5k-section (Read more detail at https://www.grid5000.fr/w/Virtualization_in_Grid%275000#Usage)

```bash
# ansible-setup/grid5000/vms.sh

bash vms.sh name=k8s-master cpu=4 ram=8GB disk=100GB mac=<<fill the mac address from g5k-subnets -im>>
bash vms.sh name=k8s-w1 cpu=6 ram=12GB disk=100GB mac=<<fill the mac address from g5k-subnets -im>>
bash vms.sh name=k8s-w2 cpu=6 ram=12GB disk=100GB mac=<<fill the mac address from g5k-subnets -im>>
```

After all VMs for K8S are online (You can ping each IP address of VMs), the next step is to use Ansible to set up the K8S cluster, make sure all information is correct with your case:
- A file ```ansible-setup/k8s-ansible/ansible.cfg```, a line ```private_key_file```
- A file ```ansible-setup/k8s-ansible/inventory.ini```, refill list IPs of VMs ```[control_plane]``` and ```[workers]```
- A file ```ansible-setup/k8s-ansible/group_vars/all.yml```, a line 21 ```control_plane_endpoint``` is the same IP with master node on ```inventory.ini```

All right, now run the command line on your computer (Make sure your VPN is connected and your computer can communicate with each VM)

```bash
cd ansible-setup/k8s-ansible
ansible-playbook install.yml
```

If the result after a command ```ansible-playbook ``` is fine, you can check K8s cluster on K8s master node, from your computer:
- Run ```ssh root@<K8s master node IP>```
- Run the command line ```kubectl get nodes```, the result shows a list of the K8S cluster, which is finished, and you can move to the next step.

The next step is setup a lot of tools for K8s, from the K8s master node you are running:
- Kubernetes dashboard: Read the section setup on the ```ansible-setup/k8s-ansible/k8s-tools/kubernetes-dashboard/README.md```, open the browser and enter ```https://<k8s-master-node>:30001```
- Private registry: Set up as follows on the file ```ansible-setup/private-registry/README.md```
- Knative (FaaS services): Setip as follows on the file (From section ```Full setup``` to the end) ```ansible-setup/k8s-ansible/k8s-tools/k-native```

Because K8S requires the Docker registry with HTTPS, but in this `env`, you just have HTTP, you need to register the HTTP IP to all K8S clusters. 

```bash
cd ansible-setup/k8s-ansible

# remember that it needs to be updated private_registry_host (IP of K8s master node) in this file
ansible-playbook update-registry.yml
```

#### 2. Setup storage (MinIO)

Currently, MinIO has 2 versions: Community and Enterprise. Request a License Key. Follow this link to get a trial key https://www.min.io/download

From g5k ```physical VM``` run the command line bellow

```bash
# Because MinIO uses a RAM disk (which means using RAM as storage instead of traditional storage), you need to create VM with higher GB than usual
bash vms.sh name=minio cpu=4 ram=40GB disk=50GB mac=<<fill the mac address from g5k-subnets -im>>
```

Make sure you check the MinIO IP on ```ansible-setup/minio/inventory/hosts.ini``` and ssh key on ```ansible-setup/minio/ansible.cfg```

```bash
cd ansible-setup/minio
ansible-playbook install.yml

# mc is the CLI to manage MinIO
ansible-playbook setup-mc.yml
```

You can accept the web console to manage, like create a bucket, update/download a file via browser in this link ```http://<minio-ip>:9000```


#### 3. Setup NFS

In the simple version, you can use 1 VM to set up both the MinIO and NFS server.

Make sure you check the NFS IP on ```ansible-setup/nfs-storage/inventory/hosts.ini``` and ssh key on ```ansible-setup/nfs-storage/ansible.cfg```

```bash
cd ansible-setup/nfs-storage
ansible-playbook site.yml
```

NFS is the layer that storage will be attached to Pods (The Bloom-FaaS function) to share intermediate data or a Bloom filter. You need to set up an NFS endpoint for K8S.

Make sure the line number 12 on the the ```nfs-pv-pvc.yaml``` will be updated correct NFS IP

From K8s master node run the command ```kubectl BLOOM-FaaS/nfs-pv-pvc.yaml```, after that you can check the result form K8s dashboard from left menu ```Persistent Volume Claims->Storage Classes```


## Setup ```BLOOM-FaaS```

First, run the command from K8s master node

```bash
cd bash BLOOM-FaaS
bash setup.sh

cd BLOOM-FaaS/operations/docker-based
bash build.sh

cd BLOOM-FaaS
bash build.sh
```

From now on, you have already finished 95% the environment of BLOOM-FaaS, the next step is to run the experiment/benchmark:
- Make a metadata ```operations/make-s3metadata.py```
- And then, move to the ```benchmarks``` folder to run the main benchmark.

In this project, there are some places you need to update, including:
- The MinIO from line 15 - 17 and 34 on the file ```/Users/truongtpa/Sites/BLOOM-FaaS/operations/libs/Tools.py```
- The NFS server in line 107 of the file ```operations/runner/EstimateBF.py```
- The file ```operations/Nofityme.py``` is the feature to notify, like send a message, when the benchmark is finished (https://www.youtube.com/watch?v=vF7MaDR6zX4).
