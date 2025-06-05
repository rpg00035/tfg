import pandas as pd

# Definir el archivo de entrada
input_file = "/home/ruben/TFG/Entrenamiento/Datos_entrenamiento/Datos_corregidos/Datos_fusionados/Dataset_definitivo.csv"

# Cargar el archivo CSV
try:
    df = pd.read_csv(input_file)
except FileNotFoundError:
    print(f"Error: El archivo '{input_file}' no existe.")
    exit(1)
except pd.errors.EmptyDataError:
    print(f"Error: El archivo '{input_file}' está vacío.")
    exit(1)
except pd.errors.ParserError:
    print(f"Error: No se pudo analizar el archivo CSV correctamente.")
    exit(1)

# Verificar si la columna 'label' existe
if 'label' not in df.columns:
    print("Error: No se encontró la columna 'label' en el dataset.")
    exit(1)

# Contar el total de registros
total_records = len(df)

# Contar ataques y no ataques
attack_count = df[df['label'] == 1].shape[0]
no_attack_count = df[df['label'] == 0].shape[0]

# Calcular los porcentajes
attack_percentage = (attack_count / total_records) * 100 if total_records > 0 else 0
no_attack_percentage = (no_attack_count / total_records) * 100 if total_records > 0 else 0

# Mostrar los resultados
print(f"Total de registros: {total_records}")
print(f"Registros de ataque (1): {attack_count} ({attack_percentage:.2f}%)")
print(f"Registros de no ataque (0): {no_attack_count} ({no_attack_percentage:.2f}%)")
