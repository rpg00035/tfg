import redis
import os
import signal
import pandas as pd
import numpy as np
import io
import json
import sys
import joblib
import traceback 
# Descomenta si vas a cargar y usar un modelo cuML y la imagen base lo permite
from cuml.ensemble import RandomForestClassifier as cuRF

# --- Global Configuration ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_QUEUE_NAME = os.getenv("REDIS_QUEUE", "argus_flow_data")
COLUMN_NAMES = os.getenv("COLUMN_NAMES").split(',') # Nombres de Argus

running = True
model = None
string_indexer_maps = {}
model_feature_order = [] # Se cargará desde JSON

# Características categóricas que SÍ usas y para las que tienes mapas JSON
CATEGORICAL_FEATURES_FOR_ML = ["proto", "state"]
# Características numéricas que SÍ usas (deben coincidir con model_feature_order.json y entrenamiento)
NUMERIC_FEATURES_FOR_ML = [
    "sport", "dsport", "dur", "sbytes", "dbytes", "sttl", "dttl", "sloss", "dloss",
    "sload", "dload", "spkts", "dpkts", "stcpb", "dtcpb", "smeansz",
    "dmeansz", "sjit", "djit", "stime", "ltime",
    "sintpkt", "dintpkt", "tcprtt", "synack", "ackdat"
]
MAPS_DIR = "string_indexer_maps" # Subcarpeta para los mapas JSON

def signal_handler(signum, frame):
    global running
    print(f"Señal {signum} recibida, deteniendo ml_processor.py...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

try:
    print("Cargando modelo y artefactos de preprocesamiento...")
    MODEL_PATH = "random_forest_gpu_model.pkl"
    model = joblib.load(MODEL_PATH)
    print(f"✅ Modelo {MODEL_PATH} cargado exitosamente.")

    FEATURE_ORDER_PATH = "model_feature_order.json"
    with open(FEATURE_ORDER_PATH, 'r') as f:
        model_feature_order = json.load(f)
    print(f"✅ Orden de características del modelo cargado desde {FEATURE_ORDER_PATH}.")
    if not model_feature_order:
        raise ValueError("model_feature_order.json está vacío o no se cargó correctamente.")

    for feature_name in CATEGORICAL_FEATURES_FOR_ML:
        map_path = os.path.join(MAPS_DIR, f"string_indexer_{feature_name}_map.json")
        try:
            with open(map_path, 'r') as f:
                string_indexer_maps[feature_name] = json.load(f)
            print(f"✅ Mapeo para '{feature_name}' cargado desde {map_path}")
        except FileNotFoundError:
            print(f"Error CRÍTICO: Mapa de StringIndexer '{map_path}' no encontrado.", file=sys.stderr)
            sys.exit(1) # Salir si un mapa esperado falta
        except json.JSONDecodeError:
            print(f"Error CRÍTICO: El archivo de mapa '{map_path}' no es un JSON válido.", file=sys.stderr)
            sys.exit(1)
    print("✅ Todos los artefactos necesarios cargados.")

except FileNotFoundError as fnf_e:
    print(f"Error CRÍTICO de archivo no encontrado al cargar artefactos: {fnf_e}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"Error CRÍTICO al cargar modelo o artefactos. Tipo: {type(e)}, Error: {e}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr) # Imprime el traceback completo
    sys.exit(1) # Es bueno salir si el modelo no se puede cargar


def tu_funcion_transformar_flujo(df_line):
    if model is None or not model_feature_order or not string_indexer_maps:
        print("Error Interno: Modelo o artefactos no disponibles para transformar.", file=sys.stderr)
        return None

    processed_features = {}

    # 1. Procesar características numéricas
    for col_name in NUMERIC_FEATURES_FOR_ML:
        if col_name in df_line.columns:
            try:
                value = pd.to_numeric(df_line[col_name].iloc[0], errors='coerce')
                processed_features[col_name] = value if not pd.isna(value) else -1.0
            except Exception:
                processed_features[col_name] = -1.0 # Valor por defecto en caso de error
        else:
            # print(f"Alerta: Columna numérica '{col_name}' no encontrada en el flujo. Usando -1.0.", file=sys.stderr)
            processed_features[col_name] = -1.0
    
    # Llenar NaNs (aunque el try-except anterior ya debería manejarlo con -1.0)
    for col_name in NUMERIC_FEATURES_FOR_ML:
        if pd.isna(processed_features.get(col_name)):
            processed_features[col_name] = -1.0

    # 2. Procesar características categóricas (AHORA SIN srcip/dstip explícitamente)
    for col_name_categorical in CATEGORICAL_FEATURES_FOR_ML: # Esta lista ahora solo tiene "proto", "state"
        col_name_index = f"{col_name_categorical}_index"
        
        if col_name_categorical in df_line.columns: # ej. 'proto' o 'state' de Argus
            value_str = str(df_line[col_name_categorical].iloc[0]).strip()
            current_map = string_indexer_maps.get(col_name_categorical)

            if current_map:
                index_value = current_map.get(value_str)
                if index_value is None: # Valor no visto, simular 'keep'
                    index_value = float(len(current_map))
                processed_features[col_name_index] = float(index_value)
            else:
                # Esto no debería ocurrir si la carga de mapas fue exitosa para todas las features en CATEGORICAL_FEATURES_FOR_ML
                print(f"Error Interno: No se encontró mapa para {col_name_categorical} (esto no debería pasar).", file=sys.stderr)
                processed_features[col_name_index] = -1.0
        else:
            print(f"Error CRÍTICO: Columna categórica '{col_name_categorical}' no encontrada en el flujo.", file=sys.stderr)
            processed_features[col_name_index] = -1.0

    # 3. Reordenar características y convertir a array NumPy
    final_ordered_features_values = []
    for feature_name_in_model_order in model_feature_order: # Cargado de model_feature_order.json
        value_to_append = processed_features.get(feature_name_in_model_order)
        if value_to_append is None:
            print(f"Alerta: Característica final '{feature_name_in_model_order}' no encontrada en processed_features después del preprocesamiento. Usando -1.0.", file=sys.stderr)
            value_to_append = -1.0
        final_ordered_features_values.append(value_to_append)
        
    if len(final_ordered_features_values) != len(model_feature_order):
        print(f"Error CRÍTICO: El número de características procesadas ({len(final_ordered_features_values)}) no coincide con el esperado por el modelo ({len(model_feature_order)}).", file=sys.stderr)
        return None

    return np.array(final_ordered_features_values, dtype=np.float32).reshape(1, -1)


def procesar_datos_flujo(csv_line):
    try:
        df_line = pd.read_csv(io.StringIO(csv_line), header=None, names=COLUMN_NAMES, dtype=str, na_filter=False)
        if df_line.empty or df_line.shape[0] != 1:
            print(f"Error: Se esperaba una sola fila en el CSV, se obtuvo forma {df_line.shape}", file=sys.stderr)
            return

        datos_para_predecir = tu_funcion_transformar_flujo(df_line.iloc[[0]]) # Asegurar que es una fila

        if model is not None and datos_para_predecir is not None:
            try:
                prediction = model.predict(datos_para_predecir)
                # Aquí tu lógica para interpretar y usar 'prediction'
                # print(f"Predicción (índice): {prediction[0]}, Flujo procesado.")
            except Exception as pred_e:
                print(f"Error durante la predicción del modelo: {pred_e}", file=sys.stderr)
                print(f"Datos que causaron el error (forma: {datos_para_predecir.shape}):\n{datos_para_predecir}", file=sys.stderr)
        # ... (otros manejos de error) ...

    except pd.errors.ParserError as pe:
        print(f"Error al parsear CSV: {pe}. Línea: {csv_line[:150]}...", file=sys.stderr)
    except Exception as e:
        print(f"Error procesando datos de flujo ('{csv_line[:100]}...'): {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # ... (código de conexión a Redis y bucle principal sin cambios) ...
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        r.ping()
        print(f"Consumiendo de Redis en {REDIS_HOST}:{REDIS_PORT}, cola: {REDIS_QUEUE_NAME}")
    except redis.exceptions.ConnectionError as e:
        print(f"Error CRÍTICO al conectar con Redis: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        while running:
            message_tuple = r.brpop(REDIS_QUEUE_NAME, timeout=1) 
            if message_tuple:
                _queue_name, csv_line = message_tuple
                if csv_line:
                    procesar_datos_flujo(csv_line)
    except KeyboardInterrupt:
        print("Interrupción de teclado, deteniendo ml_processor.py.")
    except redis.exceptions.RedisError as re_main:
        print(f"Error de Redis durante el bucle principal: {re_main}", file=sys.stderr)
    except Exception as e_loop_main:
        print(f"Error inesperado en el bucle principal: {e_loop_main}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        print("ml_processor.py ha finalizado.")
        running = False