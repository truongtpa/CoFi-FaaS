#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to display menu
show_menu() {
    clear
    echo -e "${BLUE}=========================================="
    echo "         VM Management System"
    echo -e "==========================================${NC}"
    echo ""
    echo "1. List all VMs"
    echo "2. Start VM(s)"
    echo "3. Restart VM(s)"
    echo "4. Stop VM(s)"
    echo "5. Delete VM(s)"
    echo "6. Show VM details"
    echo "7. Connect to VM console"
    echo "0. Exit"
    echo ""
    echo -e "${BLUE}==========================================${NC}"
}

# Function to list all VMs
list_vms() {
    echo -e "\n${BLUE}=== All Virtual Machines ===${NC}\n"

    echo -e "${YELLOW}Running VMs:${NC}"
          virsh list --state-running | grep -v "^$" | tail -n +3

    echo -e "\n${YELLOW}Stopped VMs:${NC}"
    virsh list --state-shutoff | grep -v "^$" | tail -n +3

    echo -e "\n${YELLOW}All VMs:${NC}"
    virsh list --all

    echo ""
    read -p "Press Enter to continue..."
}

# Function to get VM list for selection
get_vm_list() {
    local state=$1
    case $state in
        running)
            virsh list --state-running --name | grep -v "^$"
            ;;
        shutoff)
            virsh list --state-shutoff --name | grep -v "^$"
            ;;
        all)
            virsh list --all --name | grep -v "^$"
            ;;
    esac
}

# Function to select VMs
select_vms() {
    local state=$1
    local vms=($(get_vm_list $state))

    if [ ${#vms[@]} -eq 0 ]; then
        echo -e "${RED}No VMs found in '$state' state${NC}"
        return 1
    fi

    echo -e "\n${YELLOW}Available VMs:${NC}"
    for i in "${!vms[@]}"; do
        echo "$((i+1)). ${vms[$i]}"
    done
    echo "a. All VMs"
    echo "0. Cancel"

    read -p "Select VM(s) (e.g., 1,3 or 'a' for all): " selection

    if [ "$selection" = "0" ]; then
        return 1
    elif [ "$selection" = "a" ] || [ "$selection" = "A" ]; then
        SELECTED_VMS=("${vms[@]}")
    else
        SELECTED_VMS=()
        IFS=',' read -ra INDICES <<< "$selection"
        for idx in "${INDICES[@]}"; do
            idx=$((idx-1))
            if [ $idx -ge 0 ] && [ $idx -lt ${#vms[@]} ]; then
                SELECTED_VMS+=("${vms[$idx]}")
            fi
        done
    fi

    if [ ${#SELECTED_VMS[@]} -eq 0 ]; then
        echo -e "${RED}No valid VMs selected${NC}"
        return 1
    fi

    return 0
}

# Function to start VMs
start_vms() {
    echo -e "\n${BLUE}=== Start VMs ===${NC}"

    if ! select_vms "shutoff"; then
        read -p "Press Enter to continue..."
        return
    fi

    echo ""
    for vm in "${SELECTED_VMS[@]}"; do
        echo -n "Starting $vm... "
        if virsh start "$vm" > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Success${NC}"
        else
            echo -e "${RED}✗ Failed${NC}"
        fi
    done

    echo ""
    read -p "Press Enter to continue..."
}

# Function to restart VMs
restart_vms() {
    echo -e "\n${BLUE}=== Restart VMs ===${NC}"

    if ! select_vms "running"; then
        read -p "Press Enter to continue..."
        return
    fi

    echo ""
    for vm in "${SELECTED_VMS[@]}"; do
        echo -n "Restarting $vm... "
        if virsh reboot "$vm" > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Success${NC}"
        else
            echo -e "${RED}✗ Failed${NC}"
        fi
    done

    echo ""
    read -p "Press Enter to continue..."
}

# Function to stop VMs
stop_vms() {
    echo -e "\n${BLUE}=== Stop VMs ===${NC}"

    if ! select_vms "running"; then
        read -p "Press Enter to continue..."
        return
    fi

    echo -e "\n${YELLOW}Stop method:${NC}"
    echo "1. Graceful shutdown (recommended)"
    echo "2. Force shutdown"
    read -p "Select method [1]: " method
    method=${method:-1}

    echo ""
    for vm in "${SELECTED_VMS[@]}"; do
        echo -n "Stopping $vm... "
        if [ "$method" = "1" ]; then
            if virsh shutdown "$vm" > /dev/null 2>&1; then
                echo -e "${GREEN}✓ Shutdown initiated${NC}"
            else
                echo -e "${RED}✗ Failed${NC}"
            fi
        else
            if virsh destroy "$vm" > /dev/null 2>&1; then
                echo -e "${GREEN}✓ Force stopped${NC}"
            else
                echo -e "${RED}✗ Failed${NC}"
            fi
        fi
    done

    echo ""
    read -p "Press Enter to continue..."
}

# Function to delete VMs
delete_vms() {
    echo -e "\n${BLUE}=== Delete VMs ===${NC}"

    if ! select_vms "all"; then
        read -p "Press Enter to continue..."
        return
    fi

    echo -e "\n${RED}WARNING: This will permanently delete the selected VMs and their disks!${NC}"
    read -p "Are you sure? (yes/no): " confirm

    if [ "$confirm" != "yes" ]; then
        echo "Deletion cancelled"
        read -p "Press Enter to continue..."
        return
    fi

    echo ""
    for vm in "${SELECTED_VMS[@]}"; do
        echo "Deleting $vm..."

        # Stop VM if running
        virsh destroy "$vm" > /dev/null 2>&1

        # Get disk path
        DISK_PATH=$(virsh domblklist "$vm" | grep vda | awk '{print $2}')

        # Undefine VM
        if virsh undefine "$vm" > /dev/null 2>&1; then
            echo -e "  ✓ VM undefined"
        else
            echo -e "  ${RED}✗ Failed to undefine VM${NC}"
        fi

        # Delete disk
        if [ -n "$DISK_PATH" ] && [ -f "$DISK_PATH" ]; then
            if rm -f "$DISK_PATH" 2>/dev/null; then
                echo -e "  ✓ Disk deleted: $DISK_PATH"
            else
                echo -e "  ${RED}✗ Failed to delete disk${NC}"
            fi
        fi

        # Delete XML
        XML_PATH="${DISK_PATH%.qcow2}.xml"
        if [ -f "$XML_PATH" ]; then
            rm -f "$XML_PATH" 2>/dev/null
            echo -e "  ✓ XML deleted"
        fi

        echo -e "${GREEN}✓ $vm deleted successfully${NC}\n"
    done

    read -p "Press Enter to continue..."
}

# Function to show VM details
show_vm_details() {
    echo -e "\n${BLUE}=== VM Details ===${NC}"

    if ! select_vms "all"; then
        read -p "Press Enter to continue..."
        return
    fi

    for vm in "${SELECTED_VMS[@]}"; do
        echo -e "\n${YELLOW}=== $vm ===${NC}"
        virsh dominfo "$vm"
        echo -e "\n${YELLOW}Network:${NC}"
        virsh domiflist "$vm"
        echo -e "\n${YELLOW}Disks:${NC}"
        virsh domblklist "$vm"
        echo ""
    done

    read -p "Press Enter to continue..."
}

# Function to connect to VM console
connect_console() {
    echo -e "\n${BLUE}=== Connect to VM Console ===${NC}"

    if ! select_vms "running"; then
        read -p "Press Enter to continue..."
        return
    fi

    if [ ${#SELECTED_VMS[@]} -ne 1 ]; then
        echo -e "${RED}Please select only one VM for console connection${NC}"
        read -p "Press Enter to continue..."
        return
    fi

    vm="${SELECTED_VMS[0]}"
    echo -e "\n${YELLOW}Connecting to $vm console...${NC}"
    echo -e "${YELLOW}Press Ctrl+] to exit console${NC}\n"
    sleep 2

    virsh console "$vm"
}

# Main loop
while true; do
    show_menu
    read -p "Enter your choice [0-7]: " choice

    case $choice in
        1) list_vms ;;
        2) start_vms ;;
        3) restart_vms ;;
        4) stop_vms ;;
        5) delete_vms ;;
        6) show_vm_details ;;
        7) connect_console ;;
        0)
            echo -e "\n${GREEN}Goodbye!${NC}\n"
            exit 0
            ;;
        *)
            echo -e "\n${RED}Invalid option. Please try again.${NC}"
            sleep 1
            ;;
    esac
done