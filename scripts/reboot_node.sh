#!/usr/bin/bash
serial_port=/dev/ttyACM0
node_data_dir="/home/james/meshcore-node-list/node_data"
log_file="${node_data_dir}/reboot_node.log"


function log() {
  echo "$(date +'%Y-%m-%d %H:%M:%S') $@" | tee -a "${log_file}"
}

> "${log_file}"
# Reboot the device
log "Rebooting device at ${serial_port}" 
/usr/bin/echo -ne "reboot\x0D" > ${serial_port}
# wait 30 seconds
sleep 30
log "Syncing the clock" 
# sync the clock
/home/james/.local/bin/uv run meshcli -s ${serial_port} clock sync  2>&1 >> "${log_file}"
