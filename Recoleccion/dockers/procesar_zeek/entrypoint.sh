#!/bin/sh
set -e

# ── Parámetros de conexión ─────────────────────────────────────────────
TCP_HOST=${TCPDUMP_HOST:-127.0.0.1}
TCP_PORT=${TCPDUMP_BROADCAST_PORT:-5555}
ZEEK_SCRIPT="/usr/local/zeek/share/zeek/site/local.zeek"

# ── FIFO para mantener un flujo continuo ───────────────────────────────
mkfifo /tmp/pcap.pipe

# 1) Receptor TCP ▸ FIFO  (socat se reconecta si el socket cae)
socat -u TCP:${TCP_HOST}:${TCP_PORT},interval=2,retry=600 - \
      > /tmp/pcap.pipe &
SOCAT_PID=$!

# 2) Zeek procesa la tubería sin terminar aunque el socket se reinicie
zeek -C -r /tmp/pcap.pipe "$ZEEK_SCRIPT" policy/tuning/json-logs.zeek &
ZEEK_PID=$!

# 3) Publicador de logs Zeek → Redis
python3 /app/zeek_to_redis.py &
FEED_PID=$!

# 4) Limpieza ordenada
trap 'kill $SOCAT_PID $ZEEK_PID $FEED_PID 2>/dev/null' TERM INT
wait $ZEEK_PID
