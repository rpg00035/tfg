#!/usr/bin/env python


import cupy as cp, rmm, joblib, json, os, sys, signal, time, redis, ipaddress, requests
from threading import Thread
from queue import Queue, Empty
from datetime import datetime
import numpy as np

# ══════════════════════════════ CONFIG ═══════════════════════════════
REDIS_HOST       = os.getenv("ML_REDIS_HOST", "34.175.47.103")
REDIS_PORT       = int(os.getenv("ML_REDIS_PORT", 6379))
REDIS_QUEUE_NAME = os.getenv("ML_REDIS_QUEUE", "merge_data_stream")

CSV_COLUMNS = (
    "stime,proto,saddr,sport,daddr,dport,state,ltime,spkts,dpkts,sbytes,dbytes,"
    "sttl,dttl,sload,dload,sloss,dloss,sintpkt,dintpkt,sjit,djit,stcpb,dtcpb,"
    "tcprtt,synack,ackdat,smeansz,dmeansz,dur,"
    "ct_state_ttl,ct_flw_http_mthd,is_ftp_login,ct_ftp_cmd,"
    "ct_srv_src,ct_srv_dst,ct_dst_ltm,ct_src_ltm,"
    "ct_src_dport_ltm,ct_dst_sport_ltm,ct_dst_src_ltm" 
)
COLS    = CSV_COLUMNS.split(',')
COL_IDX = {c: i for i, c in enumerate(COLS)}

NUMERIC_COLS     = [
    "sport","dport","dur","sbytes","dbytes","sttl","dttl","sloss","dloss",
    "sload","dload","spkts","dpkts","stcpb","dtcpb","smeansz","dmeansz",
    "sjit","djit","stime","ltime","sintpkt","dintpkt","tcprtt","synack","ackdat"
]
CATEGORICAL_COLS = ["proto", "state"]

BATCH_SIZE       = int(os.getenv("GPU_BATCH", 1024))
QUEUE_MAXSIZE    = 16384
ATTACK_THRESHOLD = 0.70
LOG_FILE_ATTACKS = "potentially_malicious_saddr.log"
keep_running     = True

# ═════════════ Redes excluidas ═════════════
NETWORKS = {
    "gcloud"    : [],
    "aws"       : [],
    "ggen"      : [],
    "canonical" : [
        ipaddress.ip_network("185.125.188.0/22"),
        ipaddress.ip_network("91.189.88.0/21")
    ],
    "suse"      : [ipaddress.ip_network("195.135.223.0/24")]
}
CACHE_HOURS = 24
_last_fetch = {}

def fetch_ranges(url, key, list_key, cidr_key):
    now = datetime.utcnow()
    if key in _last_fetch and (now - _last_fetch[key]).seconds < CACHE_HOURS * 3600:
        return
    try:
        data = requests.get(url, timeout=8).json()
        NETWORKS[key] = [
            ipaddress.ip_network(x[cidr_key], strict=False)
            for x in data[list_key] if cidr_key in x
        ]
        _last_fetch[key] = now
        print(f"[INFO] {key}: {len(NETWORKS[key])} rangos IPv4 cargados.")
    except Exception as e:
        print(f"[WARN] Fetch {key}: {e}", file=sys.stderr)

def ip_in_net(key, ipstr):
    try:
        ip = ipaddress.ip_address(ipstr)
        return any(ip in net for net in NETWORKS[key])
    except ValueError:
        return False

# ═════════════ Variables que se llenarán en load_artifacts ═════════════
rf_cuml    = None
feat_order = []
feat2idx   = {}
str_maps   = {}
gpu_buf    = None        # <-- “reservaremos” gpu_buf en load_artifacts()

def load_artifacts():
    global rf_cuml, feat_order, feat2idx, str_maps, gpu_buf, gpu_predict, fil_model

    rf_cuml    = joblib.load("random_forest_gpu_model.pkl")
    feat_order = json.load(open("model_feature_order.json"))
    feat2idx   = {f:i for i, f in enumerate(feat_order)}

    for cat in CATEGORICAL_COLS:
        str_maps[cat] = json.load(
            open(f"string_indexer_maps/string_indexer_{cat}_map.json"))

    try:
        fil_model = rf_cuml.convert_to_fil(
            output_class = False,
            algo = "NAIVE",
            storage_type = "SOA"
         )
        print("[INFO] FIL NAIVE activo (modelo pequeño)")
        gpu_predict = fil_model.predict_proba
    except Exception as e:
        print(f"[WARN] FIL NAIVE falló ({e}); usaré RF nativo")
        gpu_predict = rf_cuml.predict_proba

    MAX_ROWS = int(os.getenv("GPU_BATCH_MAX", 2048)) 
    n_cols   = len(feat_order)
    gpu_buf  = cp.empty((MAX_ROWS, n_cols), dtype=cp.float32)


def str2f(txt):
    try:
        return float(txt)
    except:
        return 0.0

def build_gpu_batch(lines):
    """
    Rellena gpu_buf[0:n, :] con los datos de `lines` y devuelve la vista
    de solo las primeras n filas (evitando copia).
    """
    n = len(lines)
    # Limpiar solo la parte que vamos a usar
    gpu_buf[:n].fill(0.0)

    for r, line in enumerate(lines):
        f = line.split(',')

        # Escribir columnas numéricas
        for col in NUMERIC_COLS:
            name = "dsport" if (col == "dport" and "dsport" in feat2idx) else col
            idx = feat2idx.get(name, -1)
            if idx >= 0:
                gpu_buf[r, idx] = str2f(f[COL_IDX[col]])

        # Escribir columnas categóricas
        for cat in CATEGORICAL_COLS:
            idx = feat2idx[f"{cat}_index"]
            gpu_buf[r, idx] = str_maps[cat].get(f[COL_IDX[cat]], len(str_maps[cat]))

    return gpu_buf[:n]  # Vista de tamaño (n, n_cols)


# ─────────────── Parte de inferencia GPU / RMM ─────────────────
# Ajustamos el pool de RMM para no quedarnos sin VRAM

free , total = cp.cuda.Device(0).mem_info
rmm.reinitialize(
    pool_allocator=True,
    initial_pool_size = 1 * 1024**3,   # 1 GiB de arranque
    maximum_pool_size = None           # sin límite: que use toda la tarjeta
)

from rmm.allocators.cupy import rmm_cupy_allocator
cp.cuda.set_allocator(rmm_cupy_allocator)

# Después de cargar el modelo en load_artifacts(), ya tendremos
# rf_cuml y feat_order; ahora intentamos convertir a FIL
from cuml.ensemble import RandomForestClassifier   # solo por tipado

# Defino gpu_predict más abajo, después de llamar a load_artifacts()


# ───────────── logging de ataques, impresión bonita ─────────────────
def write_attack(sip, sport, dip, dport):
    with open(LOG_FILE_ATTACKS, "a") as fh:
        fh.write(f"{sip}:{sport} -> {dip}:{dport}\n")

def pretty(fields, prob, is_attack, latency, reason=""):
    sip, dip = fields[COL_IDX['saddr']], fields[COL_IDX['daddr']]
    sp , dp  = fields[COL_IDX['sport']], fields[COL_IDX['dport']]
    arrow    = f"{sip}:{sp} -> {dip}:{dp}"
    if is_attack and not reason:
        tag  = "🚨" if prob >= ATTACK_THRESHOLD else "⚠️"
        dest = sys.stderr if prob >= ATTACK_THRESHOLD else sys.stdout
        print(f"{tag} Ataque conf={prob:.3f} {arrow} lat={latency:.3f}s", file=dest)
    elif is_attack:
        print(f"⏩ IGNORADO({reason}) {arrow} lat={latency:.3f}s", file=sys.stdout)
    else:
        print(f"✅ Normal conf={prob:.3f} lat={latency:.3f}s {arrow}", file=sys.stdout)


# ───────────── Reader Redis (hilo) ─────────────────
def redis_reader(q: Queue):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    p = r.pipeline()
    while keep_running:
        p.brpop(REDIS_QUEUE_NAME, timeout=1)
        for itm in p.execute(False):
            if itm:
                q.put(itm[1])
    q.put(None)


# ───────────── Procesado en lotes ─────────────────
def process_batch(lines):
    # 1) Construir batch GPU (evitamos nuevos allocs gracias a gpu_buf)
    gpu_mat = build_gpu_batch(lines)

    # 2) Predict_proba en GPU (FIL si está disponible, o cuML nativo)
    proba_gpu = gpu_predict(gpu_mat)[:, 1]

    # Liberar cualquier bloque no usado en los pools (opcionales, pero ayudan):
    cp.get_default_memory_pool().free_all_blocks()
    cp.get_default_pinned_memory_pool().free_all_blocks()

    # 3) Pasar solo las probabilidades al host
    proba_cpu = cp.asnumpy(proba_gpu)
    now       = time.time()

    # 4) Iterar y detectar/excluir rangos (igual que antes)
    for raw, p, atk in zip(lines, proba_cpu, proba_gpu >= 0.5):
        f    = raw.split(',')
        sip, dip = f[COL_IDX['saddr']], f[COL_IDX['daddr']]
        sp , dp  = f[COL_IDX['sport']], f[COL_IDX['dport']]

        try:
            latency = now - float(f[COL_IDX['stime']])
        except ValueError:
            latency = 0.0

        
        reason = ""
        
        if sip == "169.254.169.254" or dip == "169.254.169.254":
            reason = "Meta"
        elif ip_in_net("gcloud", sip) or ip_in_net("gcloud", dip):
            reason = "GCloud"
        elif ip_in_net("aws", sip) or ip_in_net("aws", dip):
            reason = "AWS"
        elif ip_in_net("ggen", sip) or ip_in_net("ggen", dip):
            reason = "Google"
        elif ip_in_net("canonical", sip) or ip_in_net("canonical", dip):
            reason = "Canonical"
        elif ip_in_net("suse", sip) or ip_in_net("suse", dip):
            reason = "SUSE"

        pretty(f, float(p), bool(atk), latency, reason)
        if atk and not reason:
            write_attack(sip, sp, dip, dp)


def main():
    # 1) Cargar modelo y mapas → también reserva gpu_buf
    load_artifacts()

    # 2) Cargar rangos de IPs “cloud”, “aws”, “ggen”… etc.
    fetch_ranges("https://www.gstatic.com/ipranges/cloud.json",  "gcloud",   "prefixes", "ipv4Prefix")
    fetch_ranges("https://ip-ranges.amazonaws.com/ip-ranges.json", "aws",  "prefixes", "ip_prefix")
    fetch_ranges("https://www.gstatic.com/ipranges/goog.json",  "ggen",    "prefixes", "ipv4Prefix")

    # 3) Poner en marcha el hilo lector de Redis
    q = Queue(maxsize=QUEUE_MAXSIZE)
    Thread(target=redis_reader, args=(q,), daemon=True).start()

    buf = []
    while keep_running:
        try:
            line = q.get(timeout=1)
            if line is None:
                break
            buf.append(line)
            if len(buf) >= BATCH_SIZE:
                process_batch(buf)
                buf.clear()
        except Empty:
            if buf:
                process_batch(buf)
                buf.clear()


if __name__ == "__main__":
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: globals().__setitem__("keep_running", False))

    print("[INFO] IDS GPU batch listo")
    main()
    print("[INFO] Fin.")
