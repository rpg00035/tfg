import redis
import os
import signal
import pandas as pd
import numpy as np
import io
import json
import sys
import joblib
from cuml.ensemble import RandomForestClassifier as cuRF

# --- Global Configuration (load once at script start) ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_QUEUE_NAME = os.getenv("REDIS_QUEUE", "argus_flow_data")

# Column names from Argus/ra stream (must match RA_FIELDS in procesador-ra-almacen)
# Example: "srcip,sport,dstip,dsport,proto,state,dur,sbytes,dbytes,sttl,dttl,sloss,dloss,sload,dload,spkts,dpkts,stcpb,dtcpb,smeansz,dmeansz,sjit,djit,stime,ltime,sintpkt,dintpkt,tcprtt,synack,ackdat"
COLUMN_NAMES = os.getenv("COLUMN_NAMES").split(',')

running = True

def signal_handler(signum, frame):
    global running
    print(f"Señal {signum} recibida, deteniendo ml_processor.py...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# --- Load Model and Preprocessing Artifacts (once at script start) ---
model = None
string_indexer_maps = {}

try:
    # 1. Load your trained cuML model
    MODEL_PATH = "random_forest_gpu_model.pkl" # Copied into Docker image
    model = joblib.load(MODEL_PATH)
    print(f"Modelo {MODEL_PATH} cargado exitosamente.")
    print("ADVERTENCIA: Modelo de ML (random_forest_gpu_model.pkl) no cargado. Se simulará.")
except FileNotFoundError:
    print(f"Error CRÍTICO: Archivo de modelo '{MODEL_PATH}' no encontrado.", file=sys.stderr) # Usa sys.stderr si sys está importado
    model = None
    # sys.exit(1) # Exit if model is critical
except Exception as e:
    print(f"Error CRÍTICO al cargar el modelo '{MODEL_PATH}': {e}", file=sys.stderr)
    model = None
    # Considera sys.exit(1)
    
# 2. Load StringIndexer mappings
CATEGORICAL_FEATURES_FOR_PREDICTION = ["proto", "state", "srcip", "dstip"]
MAPS_DIR = "string_indexer_maps"
for feature_name in CATEGORICAL_FEATURES_FOR_ML:
    map_path = os.path.join(MAPS_DIR, f"string_indexer_{feature_name}_map.json") # Usa os.path.join
    try:
        with open(map_path, 'r') as f:
            string_indexer_maps[feature_name] = json.load(f)
        print(f"✅ Mapeo para '{feature_name}' cargado desde {map_path}")
    except FileNotFoundError:
        print(f"ADVERTENCIA: Mapa de StringIndexer para '{feature_name}' ({map_path}) no encontrado...", file=sys.stderr)
        # Considera un error crítico si los mapas son indispensables: sys.exit(1)


# 3. Define Feature Lists (as per your Spark training script)
NUMERIC_FEATURES_FOR_PREDICTION = [
    "sport", "dsport", "dur", "sbytes", "dbytes", "sttl", "dttl", "sloss", "dloss",
    "sload", "dload", "spkts", "dpkts", "stcpb", "dtcpb", "smeansz",
    "dmeansz", "sjit", "djit", "stime", "ltime",
    "sintpkt", "dintpkt", "tcprtt", "synack", "ackdat"
]

# This is the crucial list defining the order and names of features for the model
# It MUST match the 'features' list used by VectorAssembler in Spark.
FEATURE_ORDER_FOR_MODEL = NUMERIC_FEATURES_FOR_PREDICTION + \
                          [f"{c}_index" for c in CATEGORICAL_FEATURES_FOR_PREDICTION]
print(f"Orden de features esperado por el modelo: {FEATURE_ORDER_FOR_MODEL}")


# --- END Global Configuration ---


def tu_funcion_transformar_flujo(df_line):
    if model is None or not model_feature_order or not string_indexer_maps:
        print("Error: Modelo o artefactos no cargados, no se puede transformar.", file=sys.stderr)
        return None

    processed_features = {}

    # 1. Procesar características numéricas (sin cambios, asumiendo que los nombres coinciden)
    for col_name in NUMERIC_FEATURES_FOR_ML:
        if col_name in df_line.columns:
            try:
                value = pd.to_numeric(df_line[col_name].iloc[0], errors='coerce')
                processed_features[col_name] = value
            except Exception:
                processed_features[col_name] = np.nan
        else:
            processed_features[col_name] = np.nan
    
    for col_name in NUMERIC_FEATURES_FOR_ML: # Llenar NaNs
        if pd.isna(processed_features.get(col_name)):
            processed_features[col_name] = -1.0

    # 2. Procesar características categóricas (CON AJUSTE PARA IP)
    for col_name_in_model_training in CATEGORICAL_FEATURES_FOR_ML: # ej: "srcip"
        col_name_index = f"{col_name_in_model_training}_index"    # ej: "srcip_index"
        
        # Mapear nombre de columna del entrenamiento al nombre de columna de Argus
        input_col_name_from_argus = col_name_in_model_training
        if col_name_in_model_training == "srcip":
            input_col_name_from_argus = "saddr"
        elif col_name_in_model_training == "dstip":
            input_col_name_from_argus = "daddr"
        # 'proto' y 'state' deberían tener el mismo nombre

        if input_col_name_from_argus in df_line.columns:
            value_str = str(df_line[input_col_name_from_argus].iloc[0]).strip()
            
            # Usar el mapa correspondiente al nombre de entrenamiento (ej. string_indexer_maps['srcip'])
            current_map = string_indexer_maps.get(col_name_in_model_training)
            if current_map:
                index_value = current_map.get(value_str)
                if index_value is None: # Valor no visto, simular 'keep'
                    index_value = float(len(current_map)) 
                processed_features[col_name_index] = float(index_value)
            else:
                print(f"Error CRÍTICO: No se encontró mapa para {col_name_in_model_training}", file=sys.stderr)
                # Asignar un valor de error o el índice más alto posible si se conoce
                processed_features[col_name_index] = -1.0 # O un valor más apropiado
        else:
            print(f"Error CRÍTICO: Columna '{input_col_name_from_argus}' (para '{col_name_in_model_training}') no encontrada en el flujo.", file=sys.stderr)
            processed_features[col_name_index] = -1.0 # O un valor de error/default

    # 3. Reordenar características y convertir a array NumPy
    final_ordered_features_values = []
    for feature_name_in_model_order in model_feature_order: # model_feature_order es la lista cargada del JSON
        value_to_append = processed_features.get(feature_name_in_model_order)
        if value_to_append is None:
            print(f"Alerta: Característica final '{feature_name_in_model_order}' no encontrada en processed_features. Usando -1.0.", file=sys.stderr)
            value_to_append = -1.0 # Valor por defecto si falta alguna característica procesada
        final_ordered_features_values.append(value_to_append)
        
    if len(final_ordered_features_values) != len(model_feature_order):
        print(f"Error CRÍTICO: El número de características procesadas ({len(final_ordered_features_values)}) no coincide con el esperado por el modelo ({len(model_feature_order)}).", file=sys.stderr)
        return None

    return np.array(final_ordered_features_values, dtype=np.float32).reshape(1, -1)



def procesar_datos_flujo(csv_line):
    # print(f"Procesando línea CSV: {csv_line[:100]}...")
    try:
        df_line = pd.read_csv(io.StringIO(csv_line), header=None, names=COLUMN_NAMES, dtype=str)
        if df_line.shape[0] != 1:
            print(f"Error: Se esperaba una sola fila en el CSV, se obtuvieron {df_line.shape[0]}", file=sys.stderr)
            return

        datos_para_predecir = tu_funcion_transformar_flujo(df_line) # df_line is already a copy from read_csv

        if model is not None and datos_para_predecir is not None:
            # try:
            #     # --- Predicción ---
            #     # Asegúrate de que 'datos_para_predecir' tiene la forma (1, num_features)
            #     prediction = model.predict(datos_para_predecir) # Returns a cupy array or numpy array
            #     # prediction_proba = model.predict_proba(datos_para_predecir)
                
            #     # Convertir predicción a un tipo estándar si es necesario (ej. cupy a numpy)
            #     # final_prediction = prediction[0] # Obtener el valor para la única muestra
            #     # if hasattr(final_prediction, 'get'): # Check if it's a CuPy array element
            #     #     final_prediction = final_prediction.get()

            #     # Aquí tu lógica para manejar la predicción...
            #     # Por ejemplo, si 'attack_labels' (cargado de indexer_attack_model.labels) está disponible:
            #     # predicted_label_str = attack_labels[int(final_prediction)]
            #     # if predicted_label_str != "Normal":
            #     #     print(f"ALERTA: Ataque '{predicted_label_str}' detectado. Flujo: {csv_line[:100]}...")
            # except Exception as pred_e:
            #     print(f"Error durante la predicción del modelo: {pred_e}", file=sys.stderr)
            #     print(f"Datos que causaron el error (forma: {datos_para_predecir.shape}):\n{datos_para_predecir}", file=sys.stderr)
            pass # Simulación de predicción
        elif model is None:
            # print(f"INFO: Modelo no cargado. Datos preprocesados (forma: {datos_para_predecir.shape if datos_para_predecir is not None else 'None'}):\n{datos_para_predecir}")
            pass
        else: # datos_para_predecir is None
            print("Error: datos_para_predecir es None, no se puede hacer la predicción.", file=sys.stderr)


    except pd.errors.ParserError as pe:
        print(f"Error al parsear CSV: {pe}. Línea: {csv_line[:150]}...", file=sys.stderr)
    except Exception as e:
        print(f"Error procesando datos de flujo ('{csv_line[:100]}...'): {e}", file=sys.stderr)


# --- Main Loop (consumidor de Redis) ---
if __name__ == "__main__":
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        r.ping()
        print(f"Consumiendo de Redis en {REDIS_HOST}:{REDIS_PORT}, cola: {REDIS_QUEUE_NAME}")
    except redis.exceptions.ConnectionError as e:
        print(f"Error CRÍTICO al conectar con Redis: {e}", file=sys.stderr)
        sys.exit(1) # Salir si no se puede conectar a Redis

    try:
        while running:
            message_tuple = r.brpop(REDIS_QUEUE_NAME, timeout=1) 
            if message_tuple:
                _queue_name, csv_line = message_tuple
                if csv_line: # Asegurarse de que no es una cadena vacía
                    procesar_datos_flujo(csv_line)
    except KeyboardInterrupt:
        print("Interrupción de teclado, deteniendo ml_processor.py.")
    except redis.exceptions.RedisError as re:
        print(f"Error de Redis durante el bucle principal: {re}", file=sys.stderr)
    except Exception as e_loop:
        print(f"Error inesperado en el bucle principal: {e_loop}", file=sys.stderr)
    finally:
        print("ml_processor.py ha finalizado.")
        running = False
