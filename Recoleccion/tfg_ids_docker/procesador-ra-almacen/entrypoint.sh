#!/bin/bash
set -eo pipefail # Salir en error y si un comando en un pipe falla

ARGUS_SERVER_HOST=${ARGUS_SERVER_HOST:-captura-argus}
ARGUS_SERVER_PORT=${ARGUS_SERVER_PORT:-561}

# Campos para ra. Asegúrate de que coincidan con lo que tu modelo espera.
# Importante: srcip,sport,dstip,dsport,proto,state,dur,sbytes,dbytes,sttl,dttl,sloss,dloss,sload,dload,spkts,dpkts,stcpb,dtcpb,smeansz,dmeansz,sjit,djit,stime,ltime,sintpkt,dintpkt,tcprtt,synack,ackdat
# Estos son los 30 campos que mencionaste. Los usaré sin 'attack_cat' ni 'label' ya que esos los generarás después.
RA_FIELDS="stime,proto,saddr,sport,daddr,dport,state,ltime,spkts,dpkts,sbytes,dbytes,sttl,dttl,sload,dload,sloss,dloss,sintpkt,dintpkt,sjit,djit,stcpb,dtcpb,tcprtt,synack,ackdat,smeansz,dmeansz,dur"

echo "Iniciando ra para conectar a Argus en $ARGUS_SERVER_HOST:$ARGUS_SERVER_PORT"
echo "Campos solicitados a ra: $RA_FIELDS"

# -L 0: Reportar datos en cuanto se reciben (modo vivo)
# -n: No resolver nombres (más rápido)
# -u: Tiempo en formato human-readable (Unix epoch es mejor para procesar, -t da epoch)
# -c , : Delimitador coma
# -p 6 : Precisión de 6 decimales para algunos campos flotantes
ra -S "$ARGUS_SERVER_HOST:$ARGUS_SERVER_PORT" -L 0 -n -u -c, -p 6 -s "$RA_FIELDS" \
    | awk -F, '$2 != "man" && $2 != "llc"' \
    | python /app/ra_to_redis.py
