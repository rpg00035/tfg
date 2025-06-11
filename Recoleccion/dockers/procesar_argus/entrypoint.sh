#!/bin/sh
set -e

CAPTURE_IF=${CAPTURE_INTERFACE:-ens3} 
ARGUS_PORT=${ARGUS_PORT:-561}

sleep 5

echo "[Argus] Arrancando argus-server en primer plano."
echo "[Argus] Capturando de la interfaz: ${CAPTURE_IF}"
echo "[Argus] Escuchando conexiones de clientes (ra) en el puerto: ${ARGUS_PORT}"

taskset -c 1 argus -i "${CAPTURE_IF}" -P "${ARGUS_PORT}"
