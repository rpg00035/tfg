#!/bin/bash
set -euo pipefail

COMPOSE="docker compose"
TAIL="100"
USE_CACHE=${1:-1}  # Por defecto usa caché si no se pasa argumento

echo "🔄 Deteniendo contenedores (sin borrar redes)..."
$COMPOSE stop

echo "🧹 Eliminando contenedores antiguos (pero no redes ni volúmenes)..."
$COMPOSE rm -f

echo "🔨 Compilando servicios con BuildKit en paralelo..."

if [[ "$USE_CACHE" == "0" ]]; then
  echo "⚠️  Compilando SIN caché..."
  DOCKER_BUILDKIT=1 $COMPOSE build --no-cache --parallel
else
  echo "✅ Compilando CON caché..."
  DOCKER_BUILDKIT=1 $COMPOSE build --parallel
fi

echo "🚀 Levantando contenedores..."
$COMPOSE up -d --remove-orphans

echo "✅ Contenedores activos. Logs (tail=$TAIL): Ctrl-C para salir."
echo "──────────────────────────────────────────────────────────────"
$COMPOSE logs --follow --tail="$TAIL" &

LOG_PID=$!
trap 'echo; echo "🛑  Log tail detenido. Los contenedores siguen activos."; kill $LOG_PID 2>/dev/null || true; exit 0' INT
wait $LOG_PID
