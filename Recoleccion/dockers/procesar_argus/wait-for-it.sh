#!/usr/bin/env bash
HOSTPORT="$1"; TIMEOUT="${2:--t 30}"
HOST="${HOSTPORT%%:*}"; PORT="${HOSTPORT##*:}"
END=$((SECONDS+30))

while : ; do
  (echo > /dev/tcp/"$HOST"/"$PORT") >/dev/null 2>&1 && exit 0
  [[ $SECONDS -ge $END ]] && echo "timeout waiting for $HOSTPORT" >&2 && exit 1
  sleep 1
done
