#!/bin/bash

# Definir la carpeta de entrada (modifica esto con la ruta de la carpeta)
input_folder="/home/ruben/TFG/Entrenamiento/Datos_entrenamiento/Datos_corregidos"

# Definir el archivo de salida
output_file="merged_dataset.csv"

# Obtener la lista de archivos CSV en la carpeta
datasets=($(ls "$input_folder"/*.csv))

# Verificar si hay archivos CSV en la carpeta
if [ ${#datasets[@]} -eq 0 ]; then
    echo "No se encontraron archivos CSV en la carpeta '$input_folder'."
    exit 1
fi

# Extraer la cabecera del primer archivo y escribirla en el archivo de salida
head -n 1 "${datasets[0]}" > "$output_file"

# Concatenar el contenido de todos los archivos ignorando la cabecera
for file in "${datasets[@]}"; do
    tail -n +2 "$file" >> "$output_file"
done

echo "Fusión completada. Archivo generado: $output_file"
