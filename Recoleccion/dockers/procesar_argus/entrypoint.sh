#!/bin/sh
set -e

TCPDUMP_HOST=${TCPDUMP_HOST:-127.0.0.1}
TCPDUMP_PORT=${TCPDUMP_BROADCAST_PORT:-5555}
ARGUS_PORT=${ARGUS_PORT:-561}

/app/wait-for-it.sh "${TCPDUMP_HOST}:${TCPDUMP_PORT}" -t 30

echo "[Argus] conectado a ${TCP_HOST}:${TCP_PORT}"

socat -u TCP:${TCPDUMP_HOST}:${TCPDUMP_PORT} - | argus -r - -P ${ARGUS_PORT} -m -w -
