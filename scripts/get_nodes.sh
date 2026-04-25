#!/usr/bin/bash
serial_port=/dev/ttyACM0
node_data_dir="/home/james/meshcore-node-list/node_data"
log_file="${node_data_dir}/get_nodes.log"
node_list="${node_data_dir}/node_list.txt"
node_data_list="${node_data_dir}/nodes.json"
node_data_list_tmp="${node_data_dir}/_nodes.json.tmp"
# node_messages=${node_data_dir}/node_messages.txt
# node_messages_tmp=${node_data_dir}/_node_messages.txt_tmp

function log() {
  echo "$(date +'%Y-%m-%d %H:%M:%S') $@" | tee -a "${log_file}"
}

>"${log_file}"

# Get the node list first
log "Getting the node list"
# absolute carnage now the node list contains more than just the name
/home/james/.local/bin/uv run meshcli -s ${serial_port} list \
| cut -d $'\e' -f1 | sed 's/[[:space:]]*$//' \
| grep -v 'contacts in device' > ${node_list} 2>>"${log_file}"
# no point in doing it if there aren't any nodes
if [ $(ls | wc -l) -gt 0 ] ; then
  log "Looping through the nodes..."
  echo -n "[" > ${node_data_list_tmp}
  # get the local node data
  self_data=$(/home/james/.local/bin/uv run meshcli -s ${serial_port} infos 2>>"${log_file}")
  echo -n $self_data >> ${node_data_list_tmp}
  # looping through the nodes in the list
  while IFS= read -r line || [ -n "$line" ]; do
    # First check - as it is
    node_data=$(/home/james/.local/bin/uv run meshcli -s ${serial_port} contact_info "${line}" 2>>"${log_file}")
    # Second check, if "Unknown contact" then try the name with a trailing space
    if echo "$node_data" | grep -q "Unknown contact"; then
      log "First attempt failed for node ${line}, trying with a trailing space"
      node_data=$(/home/james/.local/bin/uv run meshcli -s ${serial_port} contact_info "${line} " 2>>"${log_file}")
    fi
    if echo "$node_data" | grep -q "Unknown contact"; then
      log "Failed to get data for node ${line}"
      continue
    fi
      log "Successfully got data for node ${line}"
      echo -n ",${node_data}" >> ${node_data_list_tmp}
  done < ${node_list}
  echo "]" >> ${node_data_list_tmp}
fi
mv ${node_data_list_tmp} ${node_data_list} 

# Finally, sync the clock
log "Syncing the clock" 
/home/james/.local/bin/uv run meshcli -s ${serial_port} clock sync 2>&1 >> "${log_file}"
# # Next get any messages
# /home/james/.local/bin/uv run meshcli -s ${serial_port} sync_msgs > ${node_messages_tmp} 2>> ${node_data_dir}/get_nodes.log
# mv ${node_messages_tmp} ${node_messages}
