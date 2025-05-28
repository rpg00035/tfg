#!/bin/bash
set -e

INTERFACE=${CAPTURE_INTERFACE:-enp6s0} # O la variable de entorno
ARGUS_PORT=${ARGUS_SERVER_PORT:-561}

echo "Iniciando tcpdump en la interfaz $INTERFACE y enviando a Argus en el puerto $ARGUS_PORT..."

# Ejecutar tcpdump y pipear su salida a argus en modo servidor
tcpdump -i "$INTERFACE" -U -w - | argus -r - -P "$ARGUS_PORT" -e "$(hostname)"

# Si el comando anterior terminara por alguna razón, el script y el contenedor se detendrían.
# El error 'stat /entrypoint.sh: no such file or directory' ocurre cuando Docker intenta REINICIAR
# el contenedor y no puede encontrar/ejecutar el script (típicamente por finales de línea).

# echo "El pipe de tcpdump | argus ha terminado. El script finalizará." # Para depuración
# exec sleep infinity # Descomentar esto SOLO si estás 100% seguro de que los finales de línea son correctos
                    # y el problema es que el script termina prematuramente.
                    # Esto mantendría el contenedor vivo incluso si tcpdump/argus fallan,
                    # permitiéndote entrar con 'docker exec'.
