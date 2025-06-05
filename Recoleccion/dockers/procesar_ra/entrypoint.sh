#!/bin/sh
set -e

ARGUS_HOST=${ARGUS_HOST:-procesar-argus}
ARGUS_PORT=${ARGUS_PORT:-561}
REDIS_HOST=${REDIS_HOST:-redis}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_QUEUE=${REDIS_QUEUE_ARGUS:-argus_data_stream}

RA_FIELDS="stime,proto,saddr,sport,daddr,dport,state,ltime,spkts,dpkts,sbytes,dbytes,sttl,dttl,sload,dload,sloss,dloss,sintpkt,dintpkt,sjit,djit,stcpb,dtcpb,tcprtt,synack,ackdat,smeansz,dmeansz,dur"

echo "Conectando ra a argus://${ARGUS_HOST}:${ARGUS_PORT}"

while true; do
  stdbuf -o0 ra -S ${ARGUS_HOST}:${ARGUS_PORT} \
        -L0 -n -N -u -c, -s "${RA_FIELDS}" 'not (proto man or proto llc)' |
    python3 /app/ra_to_redis.py --redis_key "${REDIS_QUEUE}" \
                                --redis_host "${REDIS_HOST}" \
                                --redis_port "${REDIS_PORT}"
  echo "[ra] ⚠️  Argus caído, reintento en 2 s…"; sleep 2
done