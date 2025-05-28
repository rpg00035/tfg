from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql.functions import col, when
from cuml.ensemble import RandomForestClassifier as cuRF
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import StratifiedKFold
from joblib import Parallel, delayed
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import joblib
import json
import sys
import os

num_folds = 15

# ─── Configurar Spark ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("IDS_Training_GPU_AntiOverfit_Fixed") \
    .config("spark.driver.memory", "16g") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

spark.conf.set("spark.sql.debug.maxToStringFields", 500)

# ─── Cargar dataset ya fusionado ─
dataset_path = "/home/ruben/TFG/Entrenamiento/Datos_entrenamiento/Datos_corregidos/Datos_fusionados/Dataset_definitivo.csv"
df = spark.read.csv(dataset_path, header=True, inferSchema=True)
print(f"✅ Dataset cargado desde {dataset_path}")

# ─── Crear la nueva etiqueta multiclase "attack_type" ───────────────────
df = df.withColumn("attack_type", when(col("label") == 1, "Attack").otherwise("Normal"))

numeric_features = [
    "sport", "dsport", "dur", "sbytes", "dbytes", "sttl", "dttl", "sloss", "dloss",
    "sload", "dload", "spkts", "dpkts", "stcpb", "dtcpb", "smeansz",
    "dmeansz", "sjit", "djit", "stime", "ltime",
    "sintpkt", "dintpkt", "tcprtt", "synack", "ackdat",
    "label" # Se incluye para el cast, pero se excluirá de las features de entrenamiento
]

for col_name in numeric_features:
    if col_name in df.columns:
        df = df.withColumn(col_name, col(col_name).cast("double"))
print("✅ Columnas numéricas convertidas correctamente.")

df = df.fillna(-1, subset=numeric_features)

categorical_features = ["proto", "state"]
string_indexer_models = {}
string_indexer_maps_for_export = {}

mapping_output_dir = "string_indexer_maps"
os.makedirs(mapping_output_dir, exist_ok=True)
print(f"ℹ️  Los mapeos de StringIndexer se guardarán en: {mapping_output_dir}/")

for col_name in categorical_features:
    if col_name in df.columns:
        print(f"Procesando StringIndexer para: {col_name}")
        indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_index", handleInvalid="keep")
        indexer_model = indexer.fit(df)
        df = indexer_model.transform(df)
        string_indexer_models[col_name] = indexer_model
        labels = indexer_model.labels
        label_map = {label: float(i) for i, label in enumerate(labels)}
        string_indexer_maps_for_export[col_name] = label_map
        map_filename = os.path.join(mapping_output_dir, f"string_indexer_{col_name}_map.json")
        with open(map_filename, 'w') as f:
            json.dump(label_map, f, indent=4)
        print(f"✅ Mapeo para '{col_name}' guardado en {map_filename}")

indexer_attack = StringIndexer(inputCol="attack_type", outputCol="target", handleInvalid="keep")
indexer_attack_model = indexer_attack.fit(df)
df = indexer_attack_model.transform(df)

attack_labels_list = indexer_attack_model.labels
attack_map_filename = os.path.join(mapping_output_dir, "attack_type_map.json")
attack_label_map_for_export = {label: float(i) for i, label in enumerate(attack_labels_list)}
with open(attack_map_filename, 'w') as f:
    json.dump(attack_label_map_for_export, f, indent=4)
print(f"✅ Mapeo para 'attack_type' guardado en {attack_map_filename}")

# ─── Seleccionar características y vectorizarlas ───────────────────
final_feature_columns = [f for f in numeric_features if f != "label"] + \
                        [f"{c}_index" for c in categorical_features if f"{c}_index" in df.columns]

print(f"ℹ️  Características finales para el VectorAssembler: {final_feature_columns}")

assembler = VectorAssembler(inputCols=final_feature_columns, outputCol="features_vector")
df = assembler.transform(df)

df = df.withColumn("target", col("target").cast("double"))

(train_data, test_data) = df.randomSplit([0.8, 0.2], seed=42)

train_pd = train_data.select("features_vector", "target").toPandas()
test_pd = test_data.select("features_vector", "target").toPandas()

X_train = np.array(train_pd["features_vector"].apply(lambda x: x.toArray()).tolist()).astype(np.float32)
y_train = train_pd["target"].values.astype(np.int32)
X_test = np.array(test_pd["features_vector"].apply(lambda x: x.toArray()).tolist()).astype(np.float32)
y_test = test_pd["target"].values.astype(np.int32)

# ─── Configuración de Hiperparámetros (alineado con el primer script) ─────
param_grid = [
    {"n_estimators": 750, "max_depth": 35, "max_features": "sqrt", "min_samples_leaf": 1, "min_samples_split": 4}, 
]

cv = StratifiedKFold(n_splits=num_folds, shuffle=True, random_state=42)

def evaluate_params(params, X, y, cv):
    accuracies = []
    fold_num = 1
    for train_idx, val_idx in cv.split(X, y):
        print(f"Evaluando fold {fold_num}/{cv.get_n_splits()}...")
        model = cuRF(
            n_estimators=params["n_estimators"],
            max_depth=params["max_depth"],
            max_features=params["max_features"],
            min_samples_leaf=params.get("min_samples_leaf", None),
            min_samples_split=params.get("min_samples_split", None)
        )
        model.fit(X[train_idx], y[train_idx])
        y_pred = model.predict(X[val_idx])
        accuracies.append(np.mean(y_pred == y[val_idx]))
        fold_num += 1
    return np.mean(accuracies)

results = Parallel(n_jobs=-1, backend="loky")(
    delayed(lambda p: (p, evaluate_params(p, X_train, y_train, cv)))(params)
    for params in param_grid
)

best_params, best_cv_accuracy = max(results, key=lambda x: x[1])
print(f"Mejores hiperparámetros: {best_params} con precisión CV: {best_cv_accuracy*100:.2f}%")

best_model = cuRF(
    n_estimators=best_params["n_estimators"],
    max_depth=best_params["max_depth"],
    max_features=best_params["max_features"],
    min_samples_leaf=best_params.get("min_samples_leaf", None),
    min_samples_split=best_params.get("min_samples_split", None)
)
best_model.fit(X_train, y_train)

y_test_pred = best_model.predict(X_test)
test_accuracy = np.mean(y_test_pred == y_test)
print(f"🎯 Precisión en conjunto de prueba: {test_accuracy*100:.2f}%")

if "Normal" in attack_labels_list:
    normal_idx = attack_labels_list.index("Normal")
    attack_mask = y_test != normal_idx
    if np.sum(attack_mask) > 0:
        attack_type_accuracy = np.mean(y_test_pred[attack_mask] == y_test[attack_mask])
    else:
        attack_type_accuracy = 0.0
else:
    attack_type_accuracy = None

output_dir = "Matriz_confusion"
os.makedirs(output_dir, exist_ok=True)

param_str = f"n{best_params['n_estimators']}_d{best_params['max_depth']}_f{best_params['max_features']}_l{best_params.get('min_samples_leaf', 'NA')}_s{best_params.get('min_samples_split', 'NA')}_cv{num_folds}"
filename_base = os.path.join(output_dir, f"matriz_{param_str}_sin_zeek_fixed") # Modificado nombre base

conf_matrix = confusion_matrix(y_test, y_test_pred)

plt.figure(figsize=(6, 5))
sns.heatmap(conf_matrix, annot=True, fmt="d", cmap="Blues",
            xticklabels=attack_labels_list, yticklabels=attack_labels_list)
plt.xlabel("Predicción")
plt.ylabel("Real")
plt.title(f"Matriz de Confusión ({param_str})")
plt.tight_layout()
if attack_type_accuracy is not None:
    plt.text(0.5, -0.15, f"Precisión en tipo de ataque: {attack_type_accuracy*100:.2f}%",
             transform=plt.gca().transAxes, fontsize=10, ha='center')
plt.savefig(f"{filename_base}.png")
plt.close()

report = classification_report(y_test, y_test_pred, target_names=attack_labels_list)
with open(f"{filename_base}.txt", "w", encoding="utf-8") as f:
    f.write(f"Reporte de Clasificación - {param_str}\n\n")
    f.write(f"Precisión General en Conjunto de Prueba: {test_accuracy*100:.2f}%\n")
    if attack_type_accuracy is not None:
        f.write(f"Precisión Específica en Tipos de Ataque (excluyendo 'Normal'): {attack_type_accuracy*100:.2f}%\n\n")
    f.write(report)

print(f"✅ Matriz de confusión guardada en {filename_base}.png")
print(f"✅ Reporte de clasificación guardado en {filename_base}.txt")

joblib.dump(best_model, "random_forest_gpu_model.pkl") # Se guarda con el mismo nombre
print("✅ Modelo entrenado con Validación Cruzada (corregido) y guardado como 'random_forest_gpu_model.pkl'.")

feature_order_filename = "model_feature_order.json"
with open(feature_order_filename, 'w') as f:
    json.dump(final_feature_columns, f, indent=4) # final_feature_columns ya está corregido
print(f"✅ Orden de características del modelo guardado en {feature_order_filename}")

spark.stop()