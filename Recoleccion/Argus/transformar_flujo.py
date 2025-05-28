import pandas as pd
import numpy as np
import sys

def main(input_csv, output_csv):
    # Map de columnas original -> columnas deseadas
    column_mapping = {
        "StartTime": "stime",
        "Proto": "proto",
        "SrcAddr": "srcip",
        "Sport": "sport",
        "DstAddr": "dstip",
        "Dport": "dsport",
        "State": "state",
        "LastTime": "ltime",
        "SrcPkts": "spkts",
        "DstPkts": "dpkts",
        "SrcBytes": "sbytes",
        "DstBytes": "dbytes",
        "sTtl": "sttl",
        "dTtl": "dttl",
        "SrcLoad": "sload",
        "DstLoad": "dload",
        "SrcLoss": "sloss",
        "DstLoss": "dloss",
        "SIntPkt": "sintpkt",
        "DIntPkt": "dintpkt",
        "SrcJitter": "sjit",
        "DstJitter": "djit",
        "SrcTCPBase": "stcpb",
        "DstTCPBase": "dtcpb",
        "TcpRtt": "tcprtt",
        "SynAck": "synack",
        "AckDat": "ackdat",
        "sMeanPktSz": "smeansz",
        "dMeanPktSz": "dmeansz",
        "Dur": "dur"
    }

    # Orden deseado para salida (sin attack_cat y label)
    output_columns_order = [
        "srcip","sport","dstip","dsport","proto","state","dur","sbytes","dbytes",
        "sttl","dttl","sloss","dloss","sload","dload","spkts","dpkts",
        "stcpb","dtcpb","smeansz","dmeansz","sjit","djit","stime","ltime",
        "sintpkt","dintpkt","tcprtt","synack","ackdat"
    ]

    # Leer CSV original con nombres de columnas originales
    df = pd.read_csv(input_csv)

    # Renombrar columnas
    df = df.rename(columns=column_mapping)

    # Seleccionar columnas existentes en el orden deseado
    existing_cols = [col for col in output_columns_order if col in df.columns]
    df = df[existing_cols]

    # --- Definir el tipo de formateo para cada columna ---

    # Columnas que deben ser enteros puros
    cols_to_int = [
        "sport", "dsport", "sbytes", "dbytes", "sttl", "dttl",
        "sloss", "dloss", "spkts", "dpkts", "stcpb", "dtcpb",
        "smeansz", "dmeansz", "stime", "ltime"
    ]

    # Columnas que deben ser flotantes con una precisión específica
    # Hemos observado que 6 decimales parece ser una buena precisión para varias columnas
    # y se ajusta a lo que Argus/ra puede generar.
    cols_to_float_6_decimal = [
        "dur", "sload", "dload", "sjit", "djit",
        "sintpkt", "dintpkt", "tcprtt", "synack", "ackdat"
    ]
    
    # Aplicar formateo para columnas enteras
    for col in cols_to_int:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            # Rellenar NaN con 0 (o el valor que tenga sentido para un entero)
            # Y luego convertir a entero (puede haber pérdida de precisión si no es ya un entero)
            df[col] = df[col].fillna(0).astype(int)

    # Aplicar formateo para columnas flotantes
    for col in cols_to_float_6_decimal:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            # Rellenar NaN con 0.0 (o el valor que tenga sentido para un flotante)
            # Asegurarse de que sea flotante antes de redondear y formatear
            df[col] = df[col].fillna(0.0).astype(float).apply(lambda x: f"{x:.6f}")
            # Consideración: Argus podría generar más de 6 decimales para algunas columnas
            # Si se necesita la precisión exacta de Argus, no usar .round() ni f-string
            # y dejar que to_csv con float_format o el default de pandas lo maneje.
            # Pero para forzar el formato como en tu objetivo, esta es la forma.

    # Guardar CSV con formato y sin índice
    # Como ya hemos formateado las columnas como cadenas, no necesitamos float_format aquí.
    # df.to_csv(output_csv, index=False, float_format='%.6f') # Ya no es necesario si se formatean las cadenas antes
    df.to_csv(output_csv, index=False)

    print(f"Archivo transformado guardado en: {output_csv}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python transformar_flujo.py <archivo_entrada.csv> <archivo_salida.csv>")
        sys.exit(1)
    input_csv_path = sys.argv[1]
    output_csv_path = sys.argv[2]
    main(input_csv_path, output_csv_path)