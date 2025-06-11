#!/bin/sh
set -e

ARGUS_HOST=${ARGUS_HOST:-127.0.0.1}
ARGUS_PORT=${ARGUS_PORT:-561}
REDIS_HOST=${REDIS_HOST:-127.0.0.1}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_KEY=${REDIS_QUEUE_ARGUS:-argus_data_stream}

export RA_FIELDS="stime,proto,saddr,sport,daddr,dport,state,ltime,spkts,dpkts,\
sbytes,dbytes,sttl,dttl,sload,dload,sloss,dloss,sintpkt,dintpkt,\
sjit,djit,stcpb,dtcpb,tcprtt,synack,ackdat,smeansz,dmeansz,dur"

sleep 6

echo "Conectando ra a argus://${ARGUS_HOST}:${ARGUS_PORT}"

taskset -c 2 stdbuf -o0 -e0 \
  ra -S ${ARGUS_HOST}:${ARGUS_PORT} -L0 -n -u -c, -s "${RA_FIELDS}" \
  -- 'not ( man or ether proto llc )' \
| python3 /app/ra_to_redis.py \
    --redis_key "${REDIS_KEY}" \
    --redis_host "${REDIS_HOST}" \
    --redis_port "${REDIS_PORT}"
