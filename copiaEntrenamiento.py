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
import sys
import os

num_folds = 15

# ─── Configurar Spark ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("IDS_Training_GPU_AntiOverfit") \
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
    "label" # Include for casting, will be removed from features list later
]

for col_name in numeric_features:
    if col_name in df.columns:
        df = df.withColumn(col_name, col(col_name).cast("double"))
print("✅ Columnas numéricas convertidas correctamente.")

df = df.fillna(-1, subset=numeric_features)

categorical_features = ["proto", "state", "srcip", "dstip"]
for col_name in categorical_features:
    if col_name in df.columns:
        indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_index", handleInvalid="keep")
        df = indexer.fit(df).transform(df)

# ─── Indexar la nueva columna "attack_type" para crear la variable objetivo "target" ─
indexer_attack = StringIndexer(inputCol="attack_type", outputCol="target", handleInvalid="keep")
indexer_attack_model = indexer_attack.fit(df)
df = indexer_attack_model.transform(df)

# Obtener la lista de etiquetas (por ejemplo, ["Normal", "Exploits", ...])
attack_labels = indexer_attack_model.labels

# ─── Seleccionar características y vectorizarlas ───────────────────
features = [f for f in numeric_features if f != "label"] + [f"{c}_index" for c in categorical_features if c in df.columns]

assembler = VectorAssembler(inputCols=features, outputCol="features")
df = assembler.transform(df)

# Asegurarse de que la variable objetivo "target" tenga el tipo adecuado
df = df.withColumn("target", col("target").cast("double"))

# ─── Dividir datos en entrenamiento (80%) y prueba (20%) ────────────
(train_data, test_data) = df.randomSplit([0.8, 0.2], seed=42)

# ─── Conversión completa a Pandas (usar todo el dataset) ─────────────
train_pd = train_data.toPandas()  # Se usa todo el dataset de entrenamiento
test_pd = test_data.toPandas()    # Se usa todo el dataset de prueba

X_train = train_pd[features].values.astype(np.float32)
y_train = train_pd["target"].values.astype(np.int32)

X_test = test_pd[features].values.astype(np.float32)
y_test = test_pd["target"].values.astype(np.int32)

# ─── Ampliar la cuadrícula de hiperparámetros ─────────────────────────
param_grid = [
    {"n_estimators": 750, "max_depth": 32, "max_features": "sqrt", "min_samples_leaf": 1, "min_samples_split": 4},
]

# ─── Usar más folds en la validación cruzada ────────
cv = StratifiedKFold(n_splits=num_folds, shuffle=True, random_state=42)

def evaluate_params(params, X, y, cv):
    """Evalúa una combinación de hiperparámetros usando validación cruzada y retorna la precisión promedio."""
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

# ─── Búsqueda en cuadrícula paralelizada ────────────────────────────
results = Parallel(n_jobs=-1, backend="loky")(
    delayed(lambda p: (p, evaluate_params(p, X_train, y_train, cv)))(params)
    for params in param_grid
)

best_params, best_cv_accuracy = max(results, key=lambda x: x[1])
print(f"Mejores hiperparámetros: {best_params} con precisión CV: {best_cv_accuracy*100:.2f}%")

# ─── Entrenar el modelo final con los mejores hiperparámetros ─────
best_model = cuRF(
    n_estimators=best_params["n_estimators"],
    max_depth=best_params["max_depth"],
    max_features=best_params["max_features"],
    min_samples_leaf=best_params.get("min_samples_leaf", None),
    min_samples_split=best_params.get("min_samples_split", None)
)
best_model.fit(X_train, y_train)

# ─── Evaluación en el conjunto de prueba ────────────────
y_test_pred = best_model.predict(X_test)
test_accuracy = np.mean(y_test_pred == y_test)
print(f"🎯 Precisión en conjunto de prueba: {test_accuracy*100:.2f}%")

# ─── Calcular la precisión de tipo de ataque ─────────────────────────
# Se identifica la etiqueta "Normal" en la lista de labels
if "Normal" in attack_labels:
    normal_idx = attack_labels.index("Normal")
    # Seleccionar solo las muestras que corresponden a un ataque (no "Normal")
    attack_mask = y_test != normal_idx
    if np.sum(attack_mask) > 0:
        attack_type_accuracy = np.mean(y_test_pred[attack_mask] == y_test[attack_mask])
    else:
        attack_type_accuracy = 0.0
else:
    attack_type_accuracy = None

# ─── Crear carpeta de almacenamiento si no existe ───────────────────────────────────
output_dir = "Matriz_confusion"
os.makedirs(output_dir, exist_ok=True)

# ─── Crear un nombre de archivo único con los hiperparámetros ───────────────────────
param_str = f"n{best_params['n_estimators']}_d{best_params['max_depth']}_f{best_params['max_features']}_l{best_params.get('min_samples_leaf', 'NA')}_s{best_params.get('min_samples_split', 'NA')}_cv{num_folds}"
filename_base = os.path.join(output_dir, f"matriz_{param_str}_sin_zeek")

# ─── Generar Matriz de Confusión ─────────────────────────────────────────────────────
conf_matrix = confusion_matrix(y_test, y_test_pred)

plt.figure(figsize=(6, 5))
sns.heatmap(conf_matrix, annot=True, fmt="d", cmap="Blues",
            xticklabels=attack_labels, yticklabels=attack_labels)
plt.xlabel("Predicción")
plt.ylabel("Real")
plt.title(f"Matriz de Confusión ({param_str})")
# Anotar la precisión de tipo de ataque en la imagen (si aplica)
if attack_type_accuracy is not None:
    plt.text(0.5, -0.1, f"Precisión en tipo de ataque: {attack_type_accuracy*100:.2f}%",
             transform=plt.gca().transAxes, fontsize=12, ha='center')
plt.savefig(f"{filename_base}.png")  # Guardar imagen
plt.close()

# ─── Generar y Guardar Reporte de Clasificación en un archivo TXT ────────────────────
report = classification_report(y_test, y_test_pred, target_names=attack_labels)

with open(f"{filename_base}.txt", "w", encoding="utf-8") as f:
    f.write(f"Reporte de Clasificación - {param_str}\n")
    f.write(report)

print(f"✅ Matriz de confusión guardada en {filename_base}.png")
print(f"✅ Reporte de clasificación guardado en {filename_base}.txt")

# ─── Guardado del modelo ────────────────────────────────
joblib.dump(best_model, "random_forest_gpu_model.pkl")
print("✅ Modelo entrenado con Validación Cruzada y guardado como 'random_forest_gpu_model.pkl'.")
