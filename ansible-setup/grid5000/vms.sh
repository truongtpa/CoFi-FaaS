#!/bin/bash

# Default configuration
VM_NAME=""
VCPU=1
MEMORY=1048576
DISK_SIZE="20G"
MAC=""
IMAGE=jammy-server-cloudimg-amd64.qcow2
VM_FOLDER=/mnt/ssd
CLOUD_INIT=/mnt/ssd/cloud-base
BRIDGE=br0

# Parse arguments
for arg in "$@"; do
    case $arg in
        name=*)
            VM_NAME="${arg#*=}"
            ;;
        cpu=*)
            VCPU="${arg#*=}"
            ;;
        ram=*)
            RAM="${arg#*=}"
            RAM_NUM=$(echo "$RAM" | sed 's/[^0-9.]//g')
            MEMORY=$(echo "$RAM_NUM * 1048576" | bc | cut -d'.' -f1)
            ;;
        disk=*)
            DISK_INPUT="${arg#*=}"
            # Normalize disk size: remove 'B' if present (20GB -> 20G)
            DISK_SIZE=$(echo "$DISK_INPUT" | sed 's/GB$/G/i; s/MB$/M/i; s/TB$/T/i; s/KB$/K/i')
            ;;
        mac=*)
            MAC="${arg#*=}"
            ;;
        image=*)
            IMAGE="${arg#*=}"
            ;;
        folder=*)
            VM_FOLDER="${arg#*=}"
            ;;
        bridge=*)
            BRIDGE="${arg#*=}"
            ;;
    esac
done

echo "You can display all the available IP by g5k-subnets -im"

# Validate VM name
if [ -z "$VM_NAME" ]; then
    echo "Error: VM name is required"
    echo "Usage: $0 name=vm1 [cpu=2] [ram=4GB] [disk=100GB] [mac=AA:BB:CC:DD:EE:3F]"
    exit 1
fi

# Auto-generate MAC if not provided
if [ -z "$MAC" ]; then
    MAC="52:54:00:$(openssl rand -hex 3 | sed 's/\(..\)/\1:/g; s/:$//')"
fi

mkdir -p "$VM_FOLDER"

echo "=========================================="
echo "Creating VM: $VM_NAME"
echo "vCPU: $VCPU"
echo "Memory: $((MEMORY/1048576))GB"
echo "Disk: $DISK_SIZE"
echo "MAC: $MAC"
echo "Image: $IMAGE"
echo "Bridge: $BRIDGE"
echo "=========================================="


# Verify configuration
echo ""
read -p "Do you want to create this VM? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "VM creation cancelled."
    exit 0
fi

# Copy base image
if [ ! -f "$VM_FOLDER/$IMAGE" ]; then
    echo "Dowload base image..."
    wget https://cloud-images.ubuntu.com/jammy/current/jammy-server-cloudimg-amd64.img -O "$VM_FOLDER/jammy-server-cloudimg-amd64.img"
    mv $VM_FOLDER/jammy-server-cloudimg-amd64.img $VM_FOLDER/$IMAGE
fi

# Create disk with backing file
echo "Creating disk..."
qemu-img create -f qcow2 -F qcow2 -b "$VM_FOLDER/$IMAGE" "$VM_FOLDER/$VM_NAME.qcow2" "$DISK_SIZE"

if [ $? -ne 0 ]; then
    echo "Error: Failed to create disk"
    echo "Disk size format: $DISK_SIZE"
    exit 1
fi

# Cloud-based init
cloud_init_key='ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCR4lNZk+KfnFUTp4RRTyDLEDnABwBvnJveQxUUq2aTd6bCNFc3TlA+QdZr3/9ddNMjoHgoRLbQCtRtHSjTv94ge5rvdos2EHPALRl+jYDImWipfAdW5EkkahR27w8XabkwLO9SXKUdnp6uazm7BVx62ju7x6iFd3dBl7a8vkzDOCkWOcahpshh8uA6Mwn+3ONS4RYd1irELRYcX+KrFqVyR0IZVDjH6jPgSRS7uQbP7DS2QsIra3h8XQg0r8y6R5JkyximMU/K42O0mdLKAT3Hn752SlkrPo3LEt+1YMMxrOxzANaaZjI2yJJuTpeflQqdTwOowRlwHE2AGCMqpP9Z'

if [ ! -d "$CLOUD_INIT/$VM_NAME" ]; then
  mkdir -p "$CLOUD_INIT/$VM_NAME"
fi

# Create meta-data
echo "instance-id: iid-$VM_NAME" > "$CLOUD_INIT/$VM_NAME/meta-data"

# Create user-data with proper YAML format
cat > "$CLOUD_INIT/$VM_NAME/user-data" <<EOF
#cloud-config
hostname: $VM_NAME

disable_root: false
ssh_pwauth: true

chpasswd:
  expire: false
  list: |
    root:grid5000

ssh_authorized_keys:
  - $cloud_init_key

runcmd:
  - echo "truongtpa cloud-init ok" > /root/cloud-init.txt
  - systemctl restart ssh
EOF

# Generate ISO
genisoimage -output "$CLOUD_INIT/$VM_NAME.iso" \
  -volid cidata \
  -joliet \
  -rock \
  "$CLOUD_INIT/$VM_NAME/user-data" \
  "$CLOUD_INIT/$VM_NAME/meta-data"

# Generate XML
echo "Generating XML configuration..."
cat > "$VM_FOLDER/$VM_NAME.xml" <<EOF
<domain type='kvm'>
  <name>$VM_NAME</name>
  <memory>$MEMORY</memory>
  <vcpu>$VCPU</vcpu>
  <cpu mode='host-model'/>
  <os>
    <type arch="x86_64">hvm</type>
  </os>
        <sysinfo type='smbios'>
          <system>
            <entry name='serial'>ds=nocloud</entry>
          </system>
        </sysinfo>
  <clock offset="localtime"/>
  <on_poweroff>destroy</on_poweroff>
  <on_reboot>restart</on_reboot>
  <on_crash>destroy</on_crash>
  <devices>
    <emulator>/usr/bin/kvm</emulator>
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='$VM_FOLDER/$VM_NAME.qcow2'/>
      <target dev='vda' bus='virtio'/>
    </disk>
    <disk type='file' device='cdrom'>
      <source file='$CLOUD_INIT/$VM_NAME.iso'/>
      <target dev='hdc' bus='ide'/>
      <readonly/>
    </disk>
    <interface type='bridge'>
      <source bridge='$BRIDGE'/>
      <mac address='$MAC'/>
      <model type='virtio'/>
    </interface>
    <serial type='pty'>
      <source path='/dev/ttyS0'/>
      <target port='0'/>
    </serial>
    <console type='pty'>
      <source path='/dev/ttyS0'/>
      <target port='0'/>
    </console>
  </devices>
</domain>
EOF

# Define VM
echo "Defining VM..."
virsh define "$VM_FOLDER/$VM_NAME.xml"

if [ $? -ne 0 ]; then
    echo "Error: Failed to define VM"
    exit 1
fi

# Start VM
echo "Starting VM..."
virsh start "$VM_NAME"

if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "VM $VM_NAME created and started"
    echo "=========================================="
    virsh dominfo "$VM_NAME" | grep -E "Name:|State:|CPU|Memory"
    echo ""
    virsh domiflist "$VM_NAME"
else
    echo "Error: Failed to start VM"
    exit 1
fi