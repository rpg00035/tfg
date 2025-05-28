import pandas as pd

columnas = [
    "srcip", "sport", "dstip", "dsport", "proto", "state", "dur", "sbytes", "dbytes", 
    "sttl", "dttl", "sloss", "dloss", "service", "sload", "dload", "spkts", "dpkts", 
    "swin", "dwin", "stcpb", "dtcpb", "smeansz", "dmeansz", "trans_depth", "response_body_len", 
    "sjit", "djit", "stime", "ltime", "sintpkt", "dintpkt", "tcprtt", "synack", "ackdat", 
    "is_sm_ips_ports", "ct_state_ttl", "ct_flw_http_mthd", "is_ftp_login", "ct_ftp_cmd", 
    "ct_srv_src", "ct_srv_dst", "ct_dst_ltm", "ct_src_ltm", "ct_src_dport_ltm", "ct_dst_sport_ltm", 
    "ct_dst_src_ltm", "attack_cat", "label"
]

numeric_features = [
    "sport", "dsport", "dur", "sbytes", "dbytes", "sttl", "dttl", "sloss", "dloss",
    "sload", "dload", "spkts", "dpkts", "stcpb", "dtcpb", "smeansz",
    "dmeansz", "sjit", "djit", "stime", "ltime",
    "sintpkt", "dintpkt", "tcprtt", "synack", "ackdat",
    "label"
]

categorical_features = ["proto", "state", "srcip", "dstip", "attack_cat"]

# Concatenamos las features que queremos conservar
cols_to_keep = numeric_features + categorical_features

# Leemos el CSV (ajusta la ruta)
df = pd.read_csv(
    "/home/ruben/TFG/Entrenamiento/Datos_entrenamiento/Datos_corregidos/Datos_fusionados/Dataset_definitivo.csv",
    names=columnas,
    header=0,
    low_memory=False,
    on_bad_lines='skip'
)

# Ordenamos las columnas según el orden en `columnas`, pero solo las que están en cols_to_keep y en el DataFrame
ordered_cols = [col for col in columnas if col in cols_to_keep and col in df.columns]

# Seleccionamos solo esas columnas
df_filtered = df[ordered_cols]

# Guardamos el resultado
df_filtered.to_csv(
    "/home/ruben/TFG/Entrenamiento/Datos_entrenamiento/Datos_corregidos/Datos_fusionados/Dataset_definitivo_filtrado.csv",
    index=False
)

print("Archivo filtrado y guardado con columnas en el orden deseado.")
