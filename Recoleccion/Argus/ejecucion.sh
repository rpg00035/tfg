#!/bin/bash

# Variables
INTERFACE="ens4"
ARGUS_FILE="/home/ruben/TFG/Recoleccion/Argus/1.argus"
CSV_ARGUS_FILE="/home/ruben/TFG/Recoleccion/Argus/flujo.csv"
TEMP_CSV_ARGUS="/home/ruben/TFG/Recoleccion/Argus/temp.csv"
VENV_DIR="/home/ruben/TFG/venv"
PYTHON_SCRIPT="/home/ruben/TFG/Recoleccion/Argus/transformar_flujo.py"
FINAL_CSV_OUTPUT="/home/ruben/TFG/Recoleccion/Argus/flujo_reconstruido.csv"

# Crear carpeta de logs para esta ejecución
mkdir -p "$ZEEK_RUN_DIR"

# Limpiar logs anteriores si es necesario (opcional)
rm -f "$ZEEK_RUN_DIR"/*.log

# Paso 1: Captura tráfico y lo entrega a Argus y Zeek al mismo tiempo
#sudo tcpdump -i "$INTERFACE" -U -w - | \
#tee >(sudo argus -J -A -r - -w "$ARGUS_FILE") \
#    >(zeek -b -r - "$ZEEK_CONF" "Log::default_logdir=$ZEEK_RUN_DIR") > /dev/null

echo "Ejecutando Zeek sobre $PCAP_FILE..."
zeek -C -b -r "$PCAP_FILE" "$ZEEK_CONF" "Log::default_logdir=$ZEEK_RUN_DIR"

# Paso 2: Generación del archivo CSV con ra
echo "Procesando datos de Argus con ra..."
ra -r "$ARGUS_FILE" \
   -n -u -L0 -c, -p 6\
   -s stime,proto,saddr,sport,daddr,dport,state,ltime,spkts,dpkts, \
      sbytes,dbytes,sttl,dttl,sload,dload, \
      sloss,dloss,sintpkt,dintpkt,sjit,djit, \
      stcpb,dtcpb,tcprtt,synack,ackdat,smeansz,dmeansz,dur \
   > "$TEMP_CSV_ARGUS"

# Paso 3: Filtrar líneas donde el protocolo sea "man" o "llc"
awk -F, '$2 != "man" && $2 != "llc"' "$TEMP_CSV_ARGUS" > "$CSV_ARGUS_FILE"

# Paso 4: Ajustar permisos del archivo CSV
sudo chmod 777 "$CSV_ARGUS_FILE"

# Paso 5: Crear y activar el entorno virtual si no existe
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
    source "$VENV_DIR/bin/activate"
    pip install --upgrade pip
    pip install pandas numpy
else
    source "$VENV_DIR/bin/activate"
fi

# Paso 6: Ejecutar el script Python dentro del entorno virtual
echo "Ejecutando script..."
python "$PYTHON_SCRIPT" "$CSV_ARGUS_FILE" "$FINAL_CSV_OUTPUT" 

# Paso 7: Verificar si el script Python tuvo éxito
if [ $? -ne 0 ]; then
    echo "Error durante la ejecución del script Python."
    deactivate
    exit 1
fi

# Paso 8: Desactivar el entorno virtual
deactivate

# Mostrar resultados
echo "-------------------------------------"
echo "Proceso completado."
echo "CSV final (Reestructurado) generado en: $FINAL_CSV_OUTPUT"
echo "-------------------------------------"
