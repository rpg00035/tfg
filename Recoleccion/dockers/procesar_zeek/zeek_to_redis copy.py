#!/usr/bin/env python3
"""
Lee en modo tail -F conn.log, http.log y ftp.log de Zeek.
· Mantiene tres cachés (con TTL) indexadas por uid.
· Cuando existen piezas suficientes para un flujo, construye un
  registro único enriquecido y lo envía a Redis.
"""

import os, json, redis, time, logging, argparse, pathlib, socket, collections

LOG_DIR   = "/output_zeek/current"
TARGETS   = {"conn.log": "conn", "http.log": "http", "ftp.log": "ftp"}
CACHE_TTL = 30           # segundos que se conservan las entradas incompletas
PRUNE_EACH = 300         # cada N líneas limpiamos cachés caducadas

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [zeek_to_redis] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ---------- tail helpers ----------------------------------------------------
def follow(path, kind):
    fh = open(path, "r")
    fh.seek(0, os.SEEK_END)
    inode = os.fstat(fh.fileno()).st_ino

    while True:
        line = fh.readline()
        if line:
            if not line.startswith("#"):
                yield kind, line.rstrip("\n")
            continue

        # sin más datos: comprueba rotación
        time.sleep(0.1)
        try:
            st = os.stat(path)
        except FileNotFoundError:
            # aún no existe el nuevo, seguimos esperando
            continue

        if st.st_ino != inode:
            # el fichero ha sido rotado: reabrimos
            fh.close()
            fh = open(path, "r")
            inode = st.st_ino
            fh.seek(0, os.SEEK_END)

def wait_logs():
    """Espera a que al menos uno de los .log exista y devuelve generadores."""
    gens = []
    while not gens:
        for fname, kind in TARGETS.items():
            full = pathlib.Path(LOG_DIR, fname)
            if full.exists():
                gens.append(follow(str(full), kind))
        time.sleep(0.5)
    return gens

# ---------- main ------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis_host", default=os.getenv("REDIS_HOST", "redis"))
    ap.add_argument("--redis_port", type=int, default=int(os.getenv("REDIS_PORT", 6379)))
    ap.add_argument("--redis_key",  default=os.getenv("REDIS_QUEUE_ZEEK", "zeek_data_stream"))
    ap.add_argument("--use_stream", action="store_true")
    args = ap.parse_args()

    try:
        r = redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=False)
        r.ping()
    except (redis.ConnectionError, socket.error):
        logging.exception("Redis no disponible")
        return
    logging.info("Publicando en %s:%s/%s (%s)", args.redis_host, args.redis_port,
                 args.redis_key, "XADD" if args.use_stream else "RPUSH")

    # --- cachés por uid -----------------------------------------------------
    Entry = collections.namedtuple("Entry", "data ts")
    conn_cache, http_cache, ftp_cache = {}, {}, {}
    line_cnt = 0

    def prune():
        now = time.time()
        for cache in (conn_cache, http_cache, ftp_cache):
            for uid in list(cache.keys()):
                if now - cache[uid].ts > CACHE_TTL:
                    del cache[uid]

    # --- loop ---------------------------------------------------------------
    for kind, line in (l for tail in wait_logs() for l in tail):
        line_cnt += 1
        try:
            rec = json.loads(line)

            # Construye la clave compuesta
            orig_h = rec.get("id.orig_h")
            orig_p = rec.get("id.orig_p")
            resp_h = rec.get("id.resp_h")
            resp_p = rec.get("id.resp_p")
            if not (orig_h and orig_p and resp_h and resp_p):
                continue
            
            key = f"{orig_h}:{orig_p}:{resp_h}:{resp_p}"
            
            now = time.time()

            # ---------- clasifica por tipo -------------------------------------
            if kind == "conn":
                conn_cache[key] = Entry(rec, now)
            elif kind == "http":
                http_cache[key] = Entry(rec, now)
            elif kind == "ftp":
                ftp_cache[key] = Entry(rec, now)

            # ---------- ¿podemos fusionar? -------------------------------------
            ready_conn = conn_cache.get(key)
            if not ready_conn:
                # si llega http/ftp antes que conn, esperamos a conn
                if line_cnt % PRUNE_EACH == 0:
                    prune()
                continue

            # conn existe -> construimos registro enriquecido
            merged = ready_conn.data.copy()

            http_extra = http_cache.pop(key, None)
            if http_extra:
                h = http_extra.data
                merged["trans_depth"]       = h.get("trans_depth", 0)
                merged["response_body_len"] = h.get("response_body_len", 0)
                merged["method"]            = h.get("method", "")

            ftp_extra = ftp_cache.pop(key, None)
            if ftp_extra:
                f = ftp_extra.data
                merged["user"]     = f.get("user", "")
                merged["password"] = f.get("password", "")
                merged["command"]  = f.get("command", "")

            # lista service → cadena simple (opcional)
            if isinstance(merged.get("service"), list) and merged["service"]:
                merged["service"] = merged["service"][0]

            # envía a Redis
            payload = json.dumps(merged).encode()
            if args.use_stream:
                r.xadd(args.redis_key, {"data": payload})
            else:
                r.rpush(args.redis_key, payload)

            # borra el conn ya procesado
            del conn_cache[key]

        except json.JSONDecodeError:
            logging.debug("Descartado JSON inválido: %s", line)
            continue
        except (redis.ConnectionError, socket.error) as e:
            logging.error("Error de conexión con Redis, reintentando...: %s", e)
            time.sleep(5)
            continue
        except Exception as e:
            logging.exception("Error inesperado procesando línea: %s", line)
            continue
        
        # mantenimiento periódico
        if line_cnt % PRUNE_EACH == 0:
            prune()

if __name__ == "__main__":
    main()
