#!/bin/sh
set -e

CAPTURE_IF=${CAPTURE_INTERFACE:-ens3}
REDIS_HOST=${REDIS_HOST:-127.0.0.1}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_KEY=${REDIS_QUEUE_ZEEK:-zeek_data_stream}

export ZEEK_AF_PACKET_BUFFER_SIZE=1073741824
ZEEK_HOME=/usr/local/zeek

echo "[Zeek] cd ${ZEEK_HOME} && zeekctl deploy"
cd "${ZEEK_HOME}"

if ! zeekctl deploy ; then
    echo "[Zeek] ERROR: deploy falló; dump diag:"
    DZ diag || true
    exit 1
fi

echo "[Zeek] Zeek corriendo con PID ${ZEEK_PID}. Esperando a logs JSON..."

echo "[Zeek] Iniciando zeek_to_redis.py..."
python3 /usr/local/bin/zeek_to_redis.py \
    --redis_host "${REDIS_HOST}" \
    --redis_port "${REDIS_PORT}" \
    --redis_key "${REDIS_KEY}"