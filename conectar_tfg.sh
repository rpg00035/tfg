#!/bin/bash

# Ruta al perfil
UNISON_PROFILE="tfg"
SYNC_SCRIPT="/home/ruben/unison_sync_to_cloud.sh"

# 1. Sincronización inicial bidireccional
echo "📥📤 Sincronización inicial (ambos sentidos)..."
unison "$UNISON_PROFILE"

# 2. Lanza escucha de cambios en local
if ! pgrep -f "$SYNC_SCRIPT" > /dev/null; then
    echo "👂 Iniciando sincronizador automático local..."
    nohup "$SYNC_SCRIPT" > /home/ruben/unison_auto.log 2>&1 &
else
    echo "🟢 Sincronizador local ya activo."
fi

# 3. Conexión a la nube
echo "🔐 Conectando con SSH..."
gcloud compute ssh tfg --zone europe-southwest1-b

