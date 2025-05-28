#!/bin/bash

# 📌 Definir la cabecera correcta en una variable
CORRECT_HEADER="srcip,sport,dstip,dsport,proto,state,dur,sbytes,dbytes,sttl,dttl,sloss,dloss,service,sload,dload,spkts,dpkts,swin,dwin,stcpb,dtcpb,smeansz,dmeansz,trans_depth,response_body_len,sjit,djit,stime,ltime,sintpkt,dintpkt,tcprtt,synack,ackdat,is_sm_ips_ports,ct_state_ttl,ct_flw_http_mthd,is_ftp_login,ct_ftp_cmd,ct_srv_src,ct_srv_dst,ct_dst_ltm,ct_src_ltm,ct_src_dport_ltm,ct_dst_sport_ltm,ct_dst_src_ltm,attack_cat,label"

# 📌 Directorio donde están los CSV originales (modifica esto si es necesario)
INPUT_DIR="./Datos_entrenamiento"
OUTPUT_DIR="./Datos_entrenamiento/Datos_corregidos"

# 📌 Crear directorio de salida si no existe
mkdir -p "$OUTPUT_DIR"

# 📌 Procesar cada archivo CSV en el directorio
for file in "$INPUT_DIR"/UNSW-*; do
    # 📌 Obtener el nombre del archivo sin la ruta
    filename=$(basename -- "$file")
    
    # 📌 Verificar si el archivo ya tiene la cabecera correcta
    first_line=$(head -n 1 "$file")

    if [[ "$first_line" == "$CORRECT_HEADER" ]]; then
        echo "✅ '$filename' ya tiene la cabecera correcta."
        cp "$file" "$OUTPUT_DIR/$filename"  # Copiar sin modificar
    else
        echo "🔄 Corrigiendo cabecera en '$filename'..."
        
        # 📌 Crear un nuevo archivo con la cabecera correcta y añadir el contenido original (sin la primera línea)
        echo "$CORRECT_HEADER" > "$OUTPUT_DIR/$filename"
        tail -n +2 "$file" >> "$OUTPUT_DIR/$filename"
        
        echo "✅ Archivo corregido guardado en '$OUTPUT_DIR/$filename'"
    fi
done

echo "🎉 Proceso completado. Todos los archivos corregidos están en '$OUTPUT_DIR'"
