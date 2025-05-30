#!/bin/bash
set -e

ARGUS_PORT=${ARGUS_SERVER_PORT:-561}
HOSTNAME_VAR=$(hostname)
ARGUS_ERROR_LOG_FILE="/tmp/argus_server_stderr.log" # Para errores de este argus
ARGUS_DATA_OUTPUT_FILE="/tmp/argus.out" # Argus escribirá aquí los datos que procesa

echo "INFO: [captura-argus] Iniciando entrypoint..."
echo "INFO: [captura-argus] Intentando iniciar Argus SOLO en modo servidor en puerto $ARGUS_PORT."
echo "INFO: [captura-argus] Argus escribirá los datos procesados a $ARGUS_DATA_OUTPUT_FILE."
echo "INFO: [captura-argus] Errores de este Argus irán a $ARGUS_ERROR_LOG_FILE."

# Ejecuta argus para que escuche en la red (-P) y escriba los datos que recibe de los clientes 'ra'
# a un archivo local (-w). Es crucial que -P esté ANTES de -w si queremos que -w
# sea el output de lo que recibe de la red.
#
# Sin embargo, el servidor 'argus' con -P es para que 'ra' se conecte y LEA datos.
# El servidor 'argus' no recibe datos de 'ra' para escribir con -w.
# Lo que necesitamos es:
# 1. tcpdump -> argus1 (procesa y escribe a un archivo temporal o socket)
# 2. argus2 (servidor -P) -> lee del archivo temporal o socket y sirve a 'ra'
#
# O, como lo tenías:
# tcpdump -> argus (lee de stdin con -r -, sirve con -P)
#
# Dado que el pipe directo falla, intentemos una configuración de servidor argus más "tradicional"
# donde Argus captura directamente de la interfaz.

INTERFACE=${CAPTURE_INTERFACE:-enp6s0}
echo "INFO: [captura-argus] Configuración: Argus capturará de '$INTERFACE' y servirá en puerto $ARGUS_PORT."

# -i <interface>: Capturar paquetes directamente desde la interfaz.
# -P <port>: Actuar como servidor para que 'ra' se conecte.
# -e <id>: Identificador de fuente.
# -w <file>: Escribir los registros de Argus a un archivo (opcional, pero útil para ver si procesa algo).
#            Usaremos '-' para stdout para ver sus datos en los logs de Docker.
# -DD : Debugging máximo.
argus -i "$INTERFACE" -P "$ARGUS_PORT" -e "$HOSTNAME_VAR" -w - -DD > /dev/null 2> "$ARGUS_ERROR_LOG_FILE"

# Si el comando anterior se ejecuta en primer plano (que debería con -w -),
# el script se mantendrá vivo mientras argus corra.
echo "ALERTA: [captura-argus] El proceso 'argus -i -P -w -' ha terminado."
echo "ALERTA: [captura-argus] Contenido de $ARGUS_ERROR_LOG_FILE (stderr de argus):"
cat "$ARGUS_ERROR_LOG_FILE" || echo "INFO: [captura-argus] $ARGUS_ERROR_LOG_FILE no existe o está vacío."
