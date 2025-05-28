#!/bin/bash

# Ruta a la carpeta local a vigilar
WATCH_DIR="/home/ruben/TFG"
UNISON_PROFILE="tfg"

echo "👀 Vigilando $WATCH_DIR en busca de cambios..."
inotifywait -mrq -e modify,create,delete,move "$WATCH_DIR" | while read path action file; do
    echo "📦 Cambio detectado en $file, sincronizando..."
    unison "$UNISON_PROFILE"
done

