#!/usr/bin/bash
serial_port=/dev/ttyACM0
node_data_dir="/home/james/meshcore-node-list/node_data"
node_list="${node_data_dir}/node_list.txt"
node_data_list="${node_data_dir}/nodes.json"
node_data_list_tmp="${node_data_dir}/_nodes.json.tmp"
# node_messages=${node_data_dir}/node_messages.txt
# node_messages_tmp=${node_data_dir}/_node_messages.txt_tmp

# Get the node list fist
/home/james/.local/bin/uv run meshcli -s /dev/ttyACM0 list > ${node_list} 2> ${node_data_dir}/get_nodes.log
# no point in doing it if there aren't any nodes
if [ $(ls | wc -l) -gt 0 ] ; then
  echo -n "[" > ${node_data_list_tmp}
  # get the local node data
  self_data=$(/home/james/.local/bin/uv run meshcli -s /dev/ttyACM0 infos 2>> ${node_data_dir}/get_nodes.log)
  echo -n $self_data >> ${node_data_list_tmp}
  while IFS= read -r line || [ -n "$line" ]; do
    node_data=$(/home/james/.local/bin/uv run meshcli -s /dev/ttyACM0 contact_info "${line}" 2>> ${node_data_dir}/get_nodes.log) 
      echo -n ",${node_data}" >> ${node_data_list_tmp}
  done < ${node_list}
  echo "]" >> ${node_data_list_tmp}
fi
mv ${node_data_list_tmp} ${node_data_list} 

# # Next get any messages
# /home/james/.local/bin/uv run meshcli -s /dev/ttyACM0 sync_msgs > ${node_messages_tmp} 2>> ${node_data_dir}/get_nodes.log
# mv ${node_messages_tmp} ${node_messages}
