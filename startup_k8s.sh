#!/bin/bash

. /usr/bin/helper.sh

log_info "Beluga Container Start, BELUGA_MODE ${BELUGA_MODE}, BELUGA_POD_NAME ${BELUGA_POD_NAME}"

if [[ $BELUGA_MODE != single ]];then
    if [[ $BELUGA_POD_NAME == "haishen-beluga-0" ]];then
        export node_name=$BELUGA_POD_NAME
        sed -i 's/haishen-beluga-0//g' /usr/share/chat/tinode.conf
    fi

    if [[ $BELUGA_POD_NAME == "haishen-beluga-1" ]];then
        export node_name=$BELUGA_POD_NAME
        sed -i 's/haishen-beluga-1//g' /usr/share/chat/tinode.conf
    fi

    if [[ $BELUGA_POD_NAME == "haishen-beluga-2" ]];then
        export node_name=$BELUGA_POD_NAME
        sed -i 's/haishen-beluga-2//g' /usr/share/chat/tinode.conf
    fi
fi

export nat_port=$BELUGA_PORT
export host_ip=$BELUGA_HOST_IP
export cert_path=$BELUGA_CERT_PATH
export redis_svc_name=rfs-redisfailover
export redis_port=26379

natips_json=""
while true
do
    natips_json=$(curl http://$HAISHEN_HIM_PORT_9091_TCP_ADDR:9091/haishen/v1/get-mru-nat)
    if [[ $natips_json =~ "HostIP" ]]; then
        break
    fi
    log_info "Get http://$HAISHEN_HIM_PORT_9091_TCP_ADDR:9091/haishen/v1/get-mru-nat fail!"
    sleep 5
done

host_ips=($(echo $natips_json | tr "\n" " " | awk 'BEGIN{RS = ","} /HostIP/ ' | awk -F\: '{print $2}'| awk -F\" '{printf "-%s\n", $2}'))
nat_ips=($(echo $natips_json | tr "\n" " " | awk 'BEGIN{RS = ","} /NATIP/ ' | awk -F\: '{print $2}'| awk -F\" '{printf "-%s\n", $2}'))

i=0
for ip in ${host_ips[@]}
do
    if [[ "-$host_ip" == $ip ]]; then
        export nat_ip=${nat_ips[i]:1}
        log_info "export nat ip is '$nat_ip'"
    fi
    let "i++"
done

#containerid=$(cat /proc/self/cgroup | awk -F "/" '{print $3 }' |grep -v '^$'| head -n 1 | cut -c 1-12)
#export containerid=$containerid
mkdir -p /log
rm -rf /logs
ln -s /log /logs
rm -rf /var/log
ln -s /log /var/log

ulimit -c unlimited
mkdir -p /var/log/cores

#tinode-db -reset -data /usr/share/chat/hexmeet_data.json
#sleep5

#nohup server -static_data /usr/share/chat/static &

python3 /usr/share/chat/startup.py

sleep5

if [[ "$nat_ip" == "" ]]; then
    nat_ip=$host_ip
fi

if [[ "$node_name" != "" ]] && [[ "$nat_ip" != "" ]]; then
    cmd_line="nohup /usr/share/chat/server -config=/usr/share/chat/tinode.conf -cluster_self=$node_name -cluster_port=9615 -cluster_nat_addr=$nat_ip -cluster_host_addr=$host_ip -redis_mode=1 -redis_addr=$redis_svc_name -redis_port=$redis_port -static_data=/usr/share/chat/static -rpc_cert_path=/conf &"
else
    cmd_line="nohup /usr/share/chat/server -config=/usr/share/chat/tinode.conf -static_data=/usr/share/chat/static &"
fi

log_info "$cmd_line."

while true
do
    count=$(ps -ef | grep "server" | grep -v auto|grep -v "grep"|wc -l)
    if [[ $count == 0 ]]; then
        log_info "The server process is not alive, restart it"
        #/usr/share/chat/server &
        $cmd_line
        sleep 5
    else
        sleep 1
    fi
done

log_info "Beluga Container End"
