import json
import csv
import redis
import os
import argparse
from collections import defaultdict
import logging
import time # Para la tolerancia en timestamps

# Configuración del Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuración ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
MERGED_DATA_REDIS_KEY = os.getenv("FULL_QUEUE", "pipeline_merged_flow_data")
MODEL_FEATURE_ORDER_FILE = "model_feature_order.json" # Asumiendo que está en el mismo dir que el script

# Tolerancia para el matching de timestamps (en segundos)
TIMESTAMP_MATCH_TOLERANCE = 2.0

# Campos esperados del CSV de Argus (ajusta según tu comando 'ra -s ...')
# Si 'ra' NO usa la opción '-n', la primera línea del CSV será la cabecera y DictReader la usará.
# Si 'ra -n' SÍ se usa, debes definir los nombres de campo aquí en el orden correcto.
ARGUS_FIELD_NAMES = [
    "stime", "dur", "proto", "saddr", "sport", "dir", "daddr", "dport",
    "pkts", "bytes", "state", "flgs", "sttl", "dttl", "sloss", "dloss",
    "sload", "dload", "spkts", "dpkts", "stcpb", "dtcpb", "smeansz",
    "dmeansz", "sjit", "djit", "ltime", "sintpkt", "dintpkt",
    "tcprtt", "synack", "ackdat"
] # Asegúrate de que esto refleje la salida de tu comando `ra`.

# --- Funciones Auxiliares ---

def load_model_feature_order(file_path):
    """Carga el orden de las características desde un archivo JSON."""
    try:
        with open(file_path, 'r') as f:
            order = json.load(f)
        if not isinstance(order, list):
            logging.error(f"El archivo {file_path} debe contener una lista JSON de nombres de características.")
            return None
        logging.info(f"Orden de características cargado desde {file_path} ({len(order)} características).")
        return order
    except FileNotFoundError:
        logging.error(f"Archivo de orden de características no encontrado: {file_path}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error decodificando JSON en {file_path}")
        return None

def calculate_ct_state_ttl(argus_state_str, argus_sttl_str, argus_dttl_str):
    """Calcula la característica ct_state_ttl."""
    try:
        sttl = int(argus_sttl_str) if argus_sttl_str and argus_sttl_str.strip() != '' else 0
        dttl = int(argus_dttl_str) if argus_dttl_str and argus_dttl_str.strip() != '' else 0
    except ValueError:
        sttl, dttl = 0, 0

    state_code = 0 # Debes implementar tu mapeo de argus_state_str a state_code
    # Ejemplo: if "FIN" in str(argus_state_str).upper(): state_code = 1 ...
    # Este mapeo es crucial y depende de los valores de 'state' de Argus.

    orig_ttl_range = 0
    if sttl > 0:
        if sttl <= 64: orig_ttl_range = 1
        elif sttl <= 128: orig_ttl_range = 2
        else: orig_ttl_range = 3
    resp_ttl_range = 0
    if dttl > 0:
        if dttl <= 64: resp_ttl_range = 1
        elif dttl <= 128: resp_ttl_range = 2
        else: resp_ttl_range = 3
    return state_code * 1000 + orig_ttl_range * 100 + resp_ttl_range # Fórmula de ejemplo

def create_flow_key_from_zeek(z_conn):
    """Crea una clave de flujo (5-tupla + timestamp entero) desde un registro de conn.log de Zeek."""
    try:
        # Normalizar protocolo a minúsculas
        proto = z_conn.get('zeek_proto', z_conn.get('id',{}).get('proto','-')).lower()
        key = (
            z_conn.get('zeek_orig_h', z_conn.get('id',{}).get('orig_h')),
            str(z_conn.get('zeek_orig_p', z_conn.get('id',{}).get('orig_p'))),
            z_conn.get('zeek_resp_h', z_conn.get('id',{}).get('resp_h')),
            str(z_conn.get('zeek_resp_p', z_conn.get('id',{}).get('resp_p'))),
            proto,
            int(float(z_conn.get('zeek_ts', 0.0))) # Timestamp de inicio como entero
        )
        return key
    except (AttributeError, KeyError, ValueError) as e:
        # logging.warning(f"No se pudo crear la clave de flujo para el registro de Zeek: {z_conn}. Error: {e}")
        return None

def create_flow_key_from_argus(argus_row):
    """Crea una clave de flujo (5-tupla + timestamp entero) desde una fila de Argus."""
    try:
        # Normalizar protocolo a minúsculas
        proto = argus_row.get('proto','-').lower()
        key = (
            argus_row.get('saddr'),
            str(argus_row.get('sport')),
            argus_row.get('daddr'),
            str(argus_row.get('dport')),
            proto,
            int(float(argus_row.get('stime', 0.0))) # Timestamp de inicio como entero
        )
        return key
    except (AttributeError, KeyError, ValueError) as e:
        # logging.warning(f"No se pudo crear la clave de flujo para la fila de Argus: {argus_row}. Error: {e}")
        return None

# --- Lógica Principal de Fusión ---
def process_and_merge_data(pcap_identifier, argus_csv_template, zeek_logs_dir_template, feature_order):
    logging.info(f"--- Iniciando proceso de fusión para PCAP ID: {pcap_identifier} ---")
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
        r.ping() # Verificar conexión
        logging.info(f"Conectado a Redis: {REDIS_HOST}:{REDIS_PORT}")
    except redis.exceptions.ConnectionError as e:
        logging.error(f"No se pudo conectar a Redis: {e}")
        return

    argus_csv_file = argus_csv_template.replace("<pcap_identifier>", pcap_identifier)
    zeek_log_dir = zeek_logs_dir_template.replace("<pcap_identifier>", pcap_identifier)
    zeek_conn_log_file = os.path.join(zeek_log_dir, "conn.json.log")
    zeek_http_log_file = os.path.join(zeek_log_dir, "http.json.log")
    zeek_ftp_log_file = os.path.join(zeek_log_dir, "ftp.json.log")

    # 1. Cargar datos de Zeek conn.log y construir un índice para matching
    zeek_conn_data_by_uid = {}
    # zeek_conn_data_by_flow_key contendrá: {(5-tupla, ts_int): uid}
    # Opcionalmente, para búsquedas con tolerancia de tiempo: {(5-tupla): [(ts_int, uid), ...]}
    zeek_conn_data_by_flow_key_bucket = defaultdict(list)

    if os.path.exists(zeek_conn_log_file):
        logging.info(f"Cargando {zeek_conn_log_file}...")
        with open(zeek_conn_log_file, 'r') as f:
            for line_num, line in enumerate(f):
                if line.startswith('#'): continue
                try:
                    log = json.loads(line)
                    uid = log.get('uid')
                    if uid:
                        data_to_store = {
                            'zeek_service': log.get('service', '-'),
                            'zeek_is_sm_ips_ports': log.get('is_sm_ips_ports_custom', False),
                            'zeek_swin': log.get('swin_initial_custom', 0),
                            'zeek_dwin': log.get('dwin_initial_custom', 0),
                            'zeek_ct_srv_src': log.get('ct_srv_src_custom', 0),
                            'zeek_ct_srv_dst': log.get('ct_srv_dst_custom', 0),
                            'zeek_ct_dst_ltm': log.get('ct_dst_ltm_custom', 0),
                            'zeek_ct_src_ltm': log.get('ct_src_ltm_custom', 0),
                            'zeek_ct_src_dport_ltm': log.get('ct_src_dport_ltm_custom', 0),
                            'zeek_ct_dst_sport_ltm': log.get('ct_dst_sport_ltm_custom', 0),
                            'zeek_ct_dst_src_ltm': log.get('ct_dst_src_ltm_custom', 0),
                            # Campos para matching
                            'zeek_orig_h': log.get('id', {}).get('orig_h'),
                            'zeek_orig_p': str(log.get('id', {}).get('orig_p')),
                            'zeek_resp_h': log.get('id', {}).get('resp_h'),
                            'zeek_resp_p': str(log.get('id', {}).get('resp_p')),
                            'zeek_proto': log.get('proto', log.get('id', {}).get('proto', '-')).lower(),
                            'zeek_ts': float(log.get('ts', 0.0))
                        }
                        zeek_conn_data_by_uid[uid] = data_to_store
                        
                        # Para el índice de matching: (saddr, sport, daddr, dport, proto) -> lista de (timestamp, uid)
                        flow_5_tuple = (data_to_store['zeek_orig_h'], data_to_store['zeek_orig_p'],
                                        data_to_store['zeek_resp_h'], data_to_store['zeek_resp_p'],
                                        data_to_store['zeek_proto'])
                        zeek_conn_data_by_flow_key_bucket[flow_5_tuple].append((data_to_store['zeek_ts'], uid))
                except json.JSONDecodeError:
                    logging.warning(f"Saltando línea JSON malformada en {zeek_conn_log_file}:{line_num+1}")
                except KeyError as e:
                    logging.warning(f"Clave faltante en entrada de conn.log: {e} - Línea: {line.strip()}")
    else:
        logging.warning(f"Archivo no encontrado: {zeek_conn_log_file}")

    # 2. Cargar y agregar datos de Zeek http.log (como antes)
    http_aggs = defaultdict(lambda: {'ct_flw_http_mthd': 0, 'trans_depth_sum': 0, 'res_bdy_len_sum': 0})
    if os.path.exists(zeek_http_log_file):
        logging.info(f"Cargando {zeek_http_log_file}...")
        # ... (lógica de http_aggs como en tu script anterior) ...
        with open(zeek_http_log_file, 'r') as f:
            for line_num, line in enumerate(f):
                if line.startswith('#'): continue
                try:
                    log = json.loads(line)
                    uid = log.get('uid')
                    if uid:
                        if log.get('method') in ['GET', 'POST']: http_aggs[uid]['ct_flw_http_mthd'] += 1
                        http_aggs[uid]['trans_depth_sum'] += log.get('trans_depth', 0)
                        http_aggs[uid]['res_bdy_len_sum'] += log.get('resp_body_len', 0)
                except json.JSONDecodeError: logging.warning(f"Saltando línea JSON malformada en {zeek_http_log_file}:{line_num+1}")

    # 3. Cargar y agregar datos de Zeek ftp.log (como antes)
    ftp_aggs = defaultdict(lambda: {'is_ftp_login_attempt': False, 'ct_ftp_cmd': 0, '_user_seen': False})
    if os.path.exists(zeek_ftp_log_file):
        logging.info(f"Cargando {zeek_ftp_log_file}...")
        # ... (lógica de ftp_aggs como en tu script anterior) ...
        with open(zeek_ftp_log_file, 'r') as f:
            for line_num, line in enumerate(f):
                if line.startswith('#'): continue
                try:
                    log = json.loads(line)
                    uid = log.get('uid')
                    if uid:
                        ftp_aggs[uid]['ct_ftp_cmd'] += 1
                        cmd = log.get('command', '').upper()
                        if cmd == 'USER': ftp_aggs[uid]['_user_seen'] = True
                        elif cmd == 'PASS' and ftp_aggs[uid]['_user_seen']:
                            ftp_aggs[uid]['is_ftp_login_attempt'] = True # Indica intento de login
                except json.JSONDecodeError: logging.warning(f"Saltando línea JSON malformada en {zeek_ftp_log_file}:{line_num+1}")

    # 4. Procesar CSV de Argus y fusionar
    merged_records_count = 0
    if os.path.exists(argus_csv_file):
        logging.info(f"Procesando y fusionando {argus_csv_file}...")
        with open(argus_csv_file, 'r', newline='') as f_argus:
            # Asume que 'ra -n' NO se usa, y la primera línea es la cabecera
            # Si 'ra -n' SÍ se usa, pasa fieldnames=ARGUS_FIELD_NAMES
            # Es más robusto si 'ra' SIEMPRE produce cabecera.
            reader = csv.DictReader(f_argus)
            for argus_row in reader:
                # Crear un registro final inicializado con defaults según feature_order
                final_record = {key: None for key in feature_order} # O usa 0, "-", etc. como default

                # Popular con datos de Argus (mapea nombres de Argus a nombres UNSW-NB15)
                final_record['srcip'] = argus_row.get('saddr')
                final_record['sport'] = argus_row.get('sport')
                final_record['dstip'] = argus_row.get('daddr')
                final_record['dsport'] = argus_row.get('dport') # Nombre en UNSW-NB15
                final_record['proto'] = argus_row.get('proto','-').lower()
                final_record['state'] = argus_row.get('state','-')
                final_record['dur'] = float(argus_row.get('dur', 0.0))
                final_record['sbytes'] = int(argus_row.get('bytes', 0)) # Asumiendo 'bytes' es sbytes si dir no se usa para diferenciar
                # Si Argus da sbytes/dbytes explícitamente, úsalos:
                # final_record['sbytes'] = int(argus_row.get('sbytes', 0))
                final_record['dbytes'] = int(argus_row.get('dbytes', 0))
                final_record['sttl'] = int(argus_row.get('sttl', 0))
                final_record['dttl'] = int(argus_row.get('dttl', 0))
                final_record['sloss'] = int(argus_row.get('sloss', 0))
                final_record['dloss'] = int(argus_row.get('dloss', 0))
                final_record['sload'] = float(argus_row.get('sload', 0.0))
                final_record['dload'] = float(argus_row.get('dload', 0.0))
                final_record['spkts'] = int(argus_row.get('pkts', 0)) # Similar a sbytes
                # final_record['spkts'] = int(argus_row.get('spkts', 0))
                final_record['dpkts'] = int(argus_row.get('dpkts', 0))
                final_record['stcpb'] = int(argus_row.get('stcpb', 0))
                final_record['dtcpb'] = int(argus_row.get('dtcpb', 0))
                final_record['smeansz'] = int(argus_row.get('smeansz', 0))
                final_record['dmeansz'] = int(argus_row.get('dmeansz', 0))
                final_record['sjit'] = float(argus_row.get('sjit', 0.0))
                final_record['djit'] = float(argus_row.get('djit', 0.0))
                argus_stime_float = float(argus_row.get('stime', 0.0))
                final_record['stime'] = int(argus_stime_float)
                final_record['ltime'] = int(float(argus_row.get('ltime', 0.0)))
                final_record['sintpkt'] = float(argus_row.get('sintpkt', 0.0))
                final_record['dintpkt'] = float(argus_row.get('dintpkt', 0.0))
                final_record['tcprtt'] = float(argus_row.get('tcprtt', 0.0))
                final_record['synack'] = float(argus_row.get('synack', 0.0))
                final_record['ackdat'] = float(argus_row.get('ackdat', 0.0))

                # Lógica de Matching con Zeek
                matched_zeek_uid = None
                argus_flow_5_tuple = (final_record['srcip'], final_record['sport'],
                                      final_record['dstip'], final_record['dsport'],
                                      final_record['proto'])

                if argus_flow_5_tuple in zeek_conn_data_by_flow_key_bucket:
                    for zeek_ts, uid in zeek_conn_data_by_flow_key_bucket[argus_flow_5_tuple]:
                        if abs(argus_stime_float - zeek_ts) <= TIMESTAMP_MATCH_TOLERANCE:
                            matched_zeek_uid = uid
                            break
                
                # Poblar con datos de Zeek si hay match
                if matched_zeek_uid and matched_zeek_uid in zeek_conn_data_by_uid:
                    z_conn = zeek_conn_data_by_uid[matched_zeek_uid]
                    final_record['service'] = z_conn.get('zeek_service', '-')
                    final_record['is_sm_ips_ports'] = 1 if z_conn.get('zeek_is_sm_ips_ports') else 0
                    final_record['swin'] = z_conn.get('zeek_swin', 0)
                    final_record['dwin'] = z_conn.get('zeek_dwin', 0)
                    final_record['ct_srv_src'] = z_conn.get('zeek_ct_srv_src', 0)
                    final_record['ct_srv_dst'] = z_conn.get('zeek_ct_srv_dst', 0)
                    final_record['ct_dst_ltm'] = z_conn.get('zeek_ct_dst_ltm', 0)
                    final_record['ct_src_ltm'] = z_conn.get('zeek_ct_src_ltm', 0)
                    final_record['ct_src_dport_ltm'] = z_conn.get('zeek_ct_src_dport_ltm', 0)
                    final_record['ct_dst_sport_ltm'] = z_conn.get('zeek_ct_dst_sport_ltm', 0)
                    final_record['ct_dst_src_ltm'] = z_conn.get('zeek_ct_dst_src_ltm', 0)

                    z_http = http_aggs.get(matched_zeek_uid, defaultdict(int))
                    final_record['ct_flw_http_mthd'] = z_http.get('ct_flw_http_mthd',0)
                    final_record['trans_depth'] = z_http.get('trans_depth_sum',0)
                    final_record['res_bdy_len'] = z_http.get('res_bdy_len_sum',0)
                    
                    z_ftp = ftp_aggs.get(matched_zeek_uid, defaultdict(int))
                    final_record['is_ftp_login'] = 1 if z_ftp.get('is_ftp_login_attempt') else 0
                    final_record['ct_ftp_cmd'] = z_ftp.get('ct_ftp_cmd',0)
                else: # Defaults para campos de Zeek si no hay match
                    final_record['service'] = argus_row.get('service', '-') # Intentar obtener de Argus si está
                    # ... (y el resto de los campos de Zeek con sus defaults 0 o '-')

                # Calcular ct_state_ttl (usa campos de Argus)
                final_record['ct_state_ttl'] = calculate_ct_state_ttl(
                    argus_row.get('state'), argus_row.get('sttl'), argus_row.get('dttl')
                )
                
                # Asegurar que todos los campos en feature_order estén presentes (con defaults si es necesario)
                # y crear una lista ordenada para enviar, o enviar el dict directamente.
                output_dict_to_redis = {feat: final_record.get(feat, 0) for feat in feature_order}
                # El default '0' puede no ser apropiado para todos los campos (ej. strings como 'service')
                # Ajusta el default según el tipo esperado o usa None y maneja en el consumidor.
                # Para strings, un default de "-" podría ser mejor que 0.
                # Ejemplo de default más específico:
                for feat in feature_order:
                    if feat not in output_dict_to_redis or output_dict_to_redis[feat] is None:
                        if any(s in feat.lower() for s in ['ip', 'proto', 'service', 'state', 'attack_cat']):
                            output_dict_to_redis[feat] = "-"
                        else:
                            output_dict_to_redis[feat] = 0
                
                try:
                    r.rpush(MERGED_DATA_REDIS_KEY, json.dumps(output_dict_to_redis))
                    merged_records_count += 1
                except Exception as e:
                    logging.error(f"Error enviando a Redis: {e}. Record: {output_dict_to_redis}")

            logging.info(f"Proceso de fusión completado. {merged_records_count} registros enviados a Redis (key: {MERGED_DATA_REDIS_KEY}).")
    else:
        logging.error(f"ERROR: No se encontró el archivo CSV de Argus en {argus_csv_file}")

if __name__ == '__main__':
    # Cargar el orden de las características primero
    # Asume que model_feature_order.json está en el mismo directorio que este script.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    feature_order_file_path = os.path.join(script_dir, MODEL_FEATURE_ORDER_FILE)
    
    model_features = load_model_feature_order(feature_order_file_path)
    if model_features is None:
        logging.error("No se pudo cargar el orden de las características. Saliendo.")
        exit(1)

    parser = argparse.ArgumentParser(description="Fusiona CSV de Argus y logs JSON de Zeek, y envía a Redis.")
    parser.add_argument('--pcap_id', required=True, help="Identificador base del PCAP para encontrar archivos de entrada.")
    parser.add_argument('--argus_csv_template', default="/input_argus_data/<pcap_identifier>.argus.csv", help="Plantilla de ruta para CSV de Argus.")
    parser.add_argument('--zeek_logs_dir_template', default="/input_zeek_logs/<pcap_identifier>_zeeklogs", help="Plantilla de ruta para directorio de logs de Zeek.")
    
    args = parser.parse_args()
    process_and_merge_data(args.pcap_id, args.argus_csv_template, args.zeek_logs_dir_template, model_features)