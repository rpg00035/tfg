import pandas as pd

# 📌 Ruta del archivo CSV
ruta_csv = "/home/ruben/TFG/Entrenamiento/Datos_entrenamiento/Datos_corregidos/Datos_fusionados/Dataset_definitivo.csv"  # Cambia esto por la ruta real de tu dataset

# 📌 Cargar el dataset
df = pd.read_csv(ruta_csv)

# 📌 Verificar que la columna 'proto' existe
if "proto" not in df.columns:
    print("❌ Error: No se encontró la columna 'proto' en el dataset.")
else:
    # 📌 Obtener los protocolos únicos
    protocolos_unicos = df["proto"].unique()
    
    # 📌 Contar la cantidad de veces que aparece cada protocolo
    conteo_protocolos = df["proto"].value_counts()

    # 📌 Mostrar resultados
    print("✅ Protocolos encontrados en el dataset:")
    print(conteo_protocolos)
    
    # 📌 Guardar los resultados en un archivo CSV (opcional)
    conteo_protocolos.to_csv("protocolos_detectados.csv", header=True)
    print("📁 Se ha guardado el archivo 'protocolos_detectados.csv' con los datos de los protocolos.")
