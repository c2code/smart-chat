#!/bin/bash

# Start/stop test cluster on localhost. This is NOT a production script. Use it for reference only.

# Names of cluster nodes
node_names=( localhost localhost localhost )
# Port where the first node will listen for client connections over http
base_http_port=6080
# Port where the first node will listen for gRPC connections.
base_grpc_port=6090
base_worker_id=1020
base_cluster_port=12000

case "$1" in
  start)
    echo 'Running cluster on localhost, ports 6080-6082'

    http_port=$base_http_port
    grpc_port=$base_grpc_port
    cluster_local_port=$base_cluster_port
    worker_id=$base_worker_id
    for node_name in "${node_names[@]}"
    do
      ./server -config=./tinode.conf -cluster_self=$node_name -listen=:${http_port} -grpc_listen=:${grpc_port} -cluster_port=${cluster_local_port} -worker_id=${worker_id}&
      # /var/tmp/ does not requre root access
      echo $!> "/var/tmp/tinode-${cluster_local_port}.pid"
      echo $!> "/var/tmp/tinode-${grpc_port}.pid"
      http_port=$((http_port+1))
      grpc_port=$((grpc_port+1))
      cluster_local_port=$((cluster_local_port+1))
      worker_id=$((worker_id+1))
    done
    ;;
  stop)
  echo 'Stopping cluster'
  cluster_local_port=$base_cluster_port
  grpc_port=$base_grpc_port
    for node_name in "${node_names[@]}"
    do
      kill `cat /var/tmp/tinode-${cluster_local_port}.pid`
      rm "/var/tmp/tinode-${cluster_local_port}.pid"
      cluster_local_port=$((cluster_local_port+1))
#      kill `cat /var/tmp/tinode-${grpc_port}.pid`
#      rm "/var/tmp/tinode-${grpc_port}.pid"
#      grpc_port=$((grpc_port+1))
    done
    ;;
  *)
    echo $"Usage: $0 {start|stop}"
esac
