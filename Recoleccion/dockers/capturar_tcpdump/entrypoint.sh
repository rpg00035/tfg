#!/bin/sh
set -e

PORT=${TCPDUMP_BROADCAST_PORT:-5555}
IFACE=${CAPTURE_INTERFACE:-ens4}

echo "🟢 tcpdump sirviendo PCAP por ${PORT} desde ${IFACE}"
exec socat -u TCP-LISTEN:"${PORT}",reuseaddr,fork EXEC:"tcpdump -nn -i ${IFACE} -s0 -U -w - 2>/dev/null",nofork > /dev/null 2>&1
