import pandas as pd
import numpy as np 
import pandas.api.types

# --- Configuración ---
ruta_csv = "/home/ruben/TFG/Entrenamiento/Datos_entrenamiento/Datos_corregidos/Datos_fusionados/Dataset_definitivo_filtrado.csv"  # Ruta de tu dataset
output_stats_general_csv = "estadisticas_generales_dataset.csv"
output_stats_short_flows_csv = "estadisticas_flujos_cortos_especificos.csv"
output_protocolos_csv = "protocolos_detectados_entrenamiento.csv"
output_states_csv = "states_detectados_entrenamiento.csv"
output_label_distribution_csv = "distribucion_label.csv"
output_attack_category_csv = "categorias_ataque_detectadas_entrenamiento.csv"


numeric_cols_to_analyze = [
    "sport", "dport", "dur", "sbytes", "dbytes", "sttl", "dttl",
    "sloss", "dloss", "sload", "dload", "spkts", "dpkts",
    "stcpb", "dtcpb", "smeansz", "dmeansz", "sjit", "djit",
    "stime", "ltime", "sintpkt", "dintpkt", "tcprtt", "synack", "ackdat"
]

# 
short_flow_features_to_analyze = ["sjit", "djit", "sintpkt", "dintpkt"]
max_packets_for_short_flow_analysis = 3

# --- Cargar el Dataset ---
print(f"🔄 Cargando el dataset desde: {ruta_csv}")
try:
    df = pd.read_csv(ruta_csv, low_memory=False)
    print(f"✅ Dataset cargado. Forma: {df.shape}")
    print(f"💡 Nombres de columnas en el CSV: {df.columns.tolist()}")
except FileNotFoundError:
    print(f"❌ Error: No se encontró el archivo en la ruta: {ruta_csv}")
    exit()
except Exception as e:
    print(f"❌ Error al cargar el CSV: {e}")
    exit()

# --- Análisis de Protocolos y Estados (mejorado) ---
print("\n--- Análisis de Protocolos y Estados ---")
if "proto" in df.columns:
    conteo_protocolos = df["proto"].value_counts(dropna=False) # dropna=False para contar NaNs si existen
    print("\n✅ Conteo de Protocolos (incluyendo NaN si hay):")
    print(conteo_protocolos)
    conteo_protocolos.to_csv(output_protocolos_csv, header=['count'], index_label='protocol')
    print(f"📁 Guardado: {output_protocolos_csv}")
else:
    print("⚠️ Advertencia: No se encontró la columna 'proto'.")

if "state" in df.columns:
    conteo_state = df["state"].value_counts(dropna=False) # dropna=False para contar NaNs si existen
    print("\n✅ Conteo de Estados (incluyendo NaN si hay):")
    print(conteo_state)
    conteo_state.to_csv(output_states_csv, header=['count'], index_label='state')
    print(f"📁 Guardado: {output_states_csv}")
else:
    print("⚠️ Advertencia: No se encontró la columna 'state'.")

# --- Análisis de la columna 'label' (Binaria: Normal/Ataque) ---
print("\n--- Análisis de la columna 'label' (Binaria: Normal/Ataque) ---")
if "label" in df.columns:
    # Asegúrate de que 'label' sea numérica si es posible (0 o 1)
    df['label'] = pd.to_numeric(df['label'], errors='coerce')
    conteo_label = df["label"].value_counts(dropna=False)
    print("\n✅ Conteo de Valores en 'label' (0=Normal, 1=Ataque, incluyendo NaN):")
    print(conteo_label)
    conteo_label.to_csv(output_label_distribution_csv, header=['count'], index_label='label_value')
    print(f"📁 Guardado: {output_label_distribution_csv}")

    # Si tienes ambos 0 y 1, puedes calcular el porcentaje de ataques
    if 0 in conteo_label and 1 in conteo_label:
        total_samples = conteo_label.sum()
        normal_count = conteo_label.get(0, 0)
        attack_count = conteo_label.get(1, 0)
        print(f"   Porcentaje de Tráfico Normal: {((normal_count / total_samples) * 100):.2f}%")
        print(f"   Porcentaje de Tráfico de Ataque: {((attack_count / total_samples) * 100):.2f}%")
    elif 0 in conteo_label:
        print("   Solo tráfico Normal detectado en el dataset (label=0).")
    elif 1 in conteo_label:
        print("   Solo tráfico de Ataque detectado en el dataset (label=1).")
    else:
        print("   No se encontraron valores 0 o 1 en la columna 'label'.")
else:
    print("⚠️ Advertencia: No se encontró la columna 'label'. Esta columna es crucial para tu TFG.")

# --- Análisis de la columna 'attack_cat' (Categorías de Ataque) ---
print("\n--- Análisis de la columna 'attack_cat' (Categorías de Ataque) ---")
if "attack_cat" in df.columns:
    conteo_attack_cat = df["attack_cat"].value_counts(dropna=False)
    print("\n✅ Conteo de Categorías de Ataque (incluyendo NaN si hay):")
    print(conteo_attack_cat)
    conteo_attack_cat.to_csv(output_attack_category_csv, header=['count'], index_label='attack_category')
    print(f"📁 Guardado: {output_attack_category_csv}")

    # Opcional: Mostrar ataques por categoría solo para flujos etiquetados como ataque (label=1)
    if "label" in df.columns and 1 in df['label'].unique():
        attack_df = df[df['label'] == 1]
        conteo_attack_cat_only_attacks = attack_df["attack_cat"].value_counts(dropna=False)
        print("\n✅ Conteo de Categorías de Ataque (SOLO para flujos con label=1):")
        print(conteo_attack_cat_only_attacks)
    else:
        print("⚠️ No se puede analizar 'attack_cat' solo para ataques porque la columna 'label' no existe o no contiene ataques (label=1).")
else:
    print("⚠️ Advertencia: No se encontró la columna 'attack_cat'. Asegúrate de que esta columna esté presente en tu dataset si esperas categorías de ataque.")


# --- Estadísticas Descriptivas Generales ---
print(f"\n--- Estadísticas Descriptivas Generales para Columnas Numéricas ---")

# Validar y convertir columnas a numérico
valid_numeric_cols = []
for col in numeric_cols_to_analyze:
    if col in df.columns:
        # Intentar convertir a numérico. Los errores se convertirán en NaN.
        df[col] = pd.to_numeric(df[col], errors='coerce')
        valid_numeric_cols.append(col)
    else:
        print(f"⚠️ Advertencia: La columna numérica '{col}' no se encontró en el dataset.")

if not valid_numeric_cols:
    print("❌ Error: Ninguna de las columnas numéricas especificadas para análisis general se encontró o es válida.")
else:
    # Calcular estadísticas descriptivas básicas
    # Añadimos percentiles que pueden ser útiles para entender la distribución
    desc_stats = df[valid_numeric_cols].describe(percentiles=[.01, .05, .25, .5, .75, .95, .99]).transpose()

    # Contar valores 0.0, -1.0 y NaN para cada columna
    stats_extended = []
    for col in valid_numeric_cols:
        zeros = (df[col] == 0).sum()
        minus_ones = (df[col] == -1).sum()
        nans = df[col].isnull().sum()
        stats_extended.append({
            'zeros_count': zeros,
            'zeros_percentage': (zeros / len(df[col])) * 100 if len(df[col]) > 0 else 0,
            'minus_ones_count': minus_ones,
            'minus_ones_percentage': (minus_ones / len(df[col])) * 100 if len(df[col]) > 0 else 0,
            'NaN_count': nans,
            'NaN_percentage': (nans / len(df[col])) * 100 if len(df[col]) > 0 else 0,
        })
    if 'spkts' in df.columns and 'dpkts' in df.columns:
        print("\n💡 Tipos de datos para 'spkts' y 'dpkts' después de la conversión a numérico:")
        print(df[['spkts', 'dpkts']].info())
    else:
        print("⚠️ 'spkts' o 'dpkts' no encontradas para mostrar sus dtypes.")
    
    extended_df = pd.DataFrame(stats_extended, index=valid_numeric_cols)
    desc_stats = pd.concat([desc_stats, extended_df], axis=1)

    print("\nEstadísticas Descriptivas Generales Completas:")
    print(desc_stats)
    desc_stats.to_csv(output_stats_general_csv)
    print(f"📁 Guardado: {output_stats_general_csv}")

# --- Análisis Específico para Flujos Cortos (`sjit`, `djit`, `sintpkt`, `dintpkt`) ---
print(f"\n--- Análisis Específico para Flujos Cortos (spkts/dpkts <= {max_packets_for_short_flow_analysis}) ---")

# Validar que las columnas necesarias existen y son numéricas
spkts_col_exists_and_is_numeric = 'spkts' in df.columns and pd.api.types.is_numeric_dtype(df['spkts'].dtype)
dpkts_col_exists_and_is_numeric = 'dpkts' in df.columns and pd.api.types.is_numeric_dtype(df['dpkts'].dtype)

valid_short_flow_analysis_features = [col for col in short_flow_features_to_analyze if col in valid_numeric_cols]
short_flow_stats_list = []


if not (spkts_col_exists_and_is_numeric and dpkts_col_exists_and_is_numeric):
    print("⚠️ Advertencia: Columnas 'spkts' o 'dpkts' no encontradas o no son numéricas. Omitiendo análisis de flujos cortos.")
    if 'spkts' in df.columns and not pd.api.types.is_numeric_dtype(df['spkts'].dtype):
        print(f"   Tipo de 'spkts': {df['spkts'].dtype}")
    if 'dpkts' in df.columns and not pd.api.types.is_numeric_dtype(df['dpkts'].dtype):
        print(f"   Tipo de 'dpkts': {df['dpkts'].dtype}")
elif not valid_short_flow_analysis_features:
    print(f"⚠️ Advertencia: Ninguna de las características para análisis de flujos cortos ({short_flow_features_to_analyze}) es válida o fue encontrada.")
else:
    for feature_to_analyze in valid_short_flow_analysis_features:
        packet_count_column = ''
        if feature_to_analyze.startswith('s'): # ej. sjit, sintpkt
            packet_count_column = 'spkts'
        elif feature_to_analyze.startswith('d'): # ej. djit, dintpkt
            packet_count_column = 'dpkts'
        
        if not packet_count_column:
            print(f"❓ No se pudo determinar la columna de conteo de paquetes para '{feature_to_analyze}'. Se omitirá.")
            continue

        print(f"\nAnalizando '{feature_to_analyze}' basado en '{packet_count_column}':")
        for N in range(1, max_packets_for_short_flow_analysis + 1):
            # Subconjunto de datos donde el conteo de paquetes es exactamente N
            subset_df = df[df[packet_count_column] == N]
            
            current_stats = {
                'feature': feature_to_analyze,
                'packet_count_type': packet_count_column,
                'packet_count_value': N,
                'subset_flow_count': len(subset_df)
            }

            if not subset_df.empty:
                feature_data = subset_df[feature_to_analyze]
                desc_subset = feature_data.describe() # Basic stats for this specific feature in the subset
                
                current_stats.update({
                    'mean': desc_subset.get('mean', np.nan),
                    'std': desc_subset.get('std', np.nan),
                    'min': desc_subset.get('min', np.nan),
                    'max': desc_subset.get('max', np.nan),
                    '25%': desc_subset.get('25%', np.nan),
                    '50% (median)': desc_subset.get('50%', np.nan),
                    '75%': desc_subset.get('75%', np.nan),
                    'zeros_count': (feature_data == 0).sum(),
                    'minus_ones_count': (feature_data == -1).sum(),
                    'NaN_count': feature_data.isnull().sum()
                })
            else: # Si no hay flujos con N paquetes
                current_stats.update({
                    'mean': np.nan, 'std': np.nan, 'min': np.nan, 'max': np.nan,
                    '25%': np.nan, '50% (median)': np.nan, '75%': np.nan,
                    'zeros_count': 0, 'minus_ones_count': 0, 'NaN_count': 0
                })
            short_flow_stats_list.append(current_stats)

    if short_flow_stats_list:
        short_flow_summary_df = pd.DataFrame(short_flow_stats_list)
        print("\nEstadísticas Detalladas para Flujos Cortos:")
        # Imprimir de forma más legible
        for _, row in short_flow_summary_df.iterrows():
            if row['subset_flow_count'] > 0:
                print(f"  Para {row['feature']} con {row['packet_count_type']} == {row['packet_count_value']} ({row['subset_flow_count']} flujos):")
                print(f"    Media: {row['mean']:.4f}, Min: {row['min']:.4f}, Max: {row['max']:.4f}")
                print(f"    Ceros: {row['zeros_count']}, MenosUnos: {row['minus_ones_count']}, NaNs: {row['NaN_count']}")
            else:
                print(f"  No hay datos para {row['feature']} con {row['packet_count_type']} == {row['packet_count_value']}")
        
        short_flow_summary_df.to_csv(output_stats_short_flows_csv, index=False)
        print(f"\n📁 Guardado: {output_stats_short_flows_csv}")
    else:
        print("ℹ️ No se generaron estadísticas para flujos cortos (posiblemente por falta de datos o columnas adecuadas).")

print("\n✅ Análisis estadístico completado.")