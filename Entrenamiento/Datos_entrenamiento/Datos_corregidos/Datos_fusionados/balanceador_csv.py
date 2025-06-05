import pandas as pd
import os

# --- Configuración del Usuario ---
INPUT_CSV_PATH = "/home/ruben/TFG/Entrenamiento/Datos_entrenamiento/Datos_corregidos/Datos_fusionados/Dataset_definitivo_filtrado.csv"
OUTPUT_CSV_PATH = "/home/ruben/TFG/Entrenamiento/Datos_entrenamiento/Datos_corregidos/Datos_fusionados/Dataset_balanceado.csv"
LABEL_COLUMN_NAME = "label"

COLUMNS_TO_KEEP = [
    "srcip", "sport", "dstip", "dport", "proto", "state", "dur", "sbytes",
    "dbytes", "sttl", "dttl", "sloss", "dloss", "sload", "dload", "spkts",
    "dpkts", "stcpb", "dtcpb", "smeansz", "dmeansz", "sjit", "djit",
    "stime", "ltime", "sintpkt", "dintpkt", "tcprtt", "synack", "ackdat",
    "attack_cat", "label"
]

ATTACK_SAMPLES = 321283
NORMAL_SAMPLES = ATTACK_SAMPLES * 3
SEED = 32
# --- Fin de la Configuración ---

def balance_dataset_attack3xnormal(input_path, output_path, label_col, columns_to_keep, attack_samples, normal_samples, seed):
    print(f"🔄 Cargando el dataset desde: {input_path}")
    try:
        df = pd.read_csv(input_path, low_memory=False)
        print(f"✅ Dataset cargado con forma: {df.shape}")
    except Exception as e:
        print(f"❌ Error al cargar el archivo CSV: {e}")
        return

    if label_col not in df.columns:
        print(f"❌ La columna '{label_col}' no está presente.")
        return
    missing = [col for col in columns_to_keep if col not in df.columns]
    if missing:
        print(f"❌ Faltan columnas requeridas: {missing}")
        return

    df = df[columns_to_keep].copy()
    df[label_col] = pd.to_numeric(df[label_col], errors='coerce').fillna(-1).astype(int)
    df = df[df[label_col].isin([0, 1])]

    available_attack = df[df[label_col] == 1].shape[0]
    available_normal = df[df[label_col] == 0].shape[0]
    print(f"📊 Disponibles: Ataques = {available_attack}, No ataques = {available_normal}")
    print(f"🎯 Muestreo objetivo: Ataques = {attack_samples}, No ataques = {normal_samples}")

    if available_attack < attack_samples or available_normal < normal_samples:
        print("❌ No hay suficientes muestras para realizar el muestreo solicitado.")
        return

    df_attack = df[df[label_col] == 1].sample(n=attack_samples, random_state=seed)
    df_normal = df[df[label_col] == 0].sample(n=normal_samples, random_state=seed)
    df_balanced = pd.concat([df_attack, df_normal]).sample(frac=1, random_state=seed).reset_index(drop=True)

    print(f"✅ Dataset balanceado generado: {df_balanced.shape[0]} filas.")
    print(f"   ➤ Ataques (1): {(df_balanced[label_col] == 1).sum()} | No ataques (0): {(df_balanced[label_col] == 0).sum()}")

    try:
        df_balanced.to_csv(output_path, index=False)
        print(f"💾 Guardado en: {output_path}")
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"📦 Tamaño final del archivo: {size_mb:.2f} MB")
    except Exception as e:
        print(f"❌ Error al guardar el archivo CSV: {e}")

if __name__ == "__main__":
    balance_dataset_attack3xnormal(INPUT_CSV_PATH, OUTPUT_CSV_PATH, LABEL_COLUMN_NAME, COLUMNS_TO_KEEP, ATTACK_SAMPLES, NORMAL_SAMPLES, SEED)
