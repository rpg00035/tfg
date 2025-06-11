#!/usr/bin/env python3
"""
merge_argus_zeek_v2.py  —  Fusiona en caliente los flujos de Argus y Zeek.
-----------------------------------------------------------------------
• Consume dos colas Redis (Argus y Zeek) tal y como ya hacía el script
  original.
• Correlaciona los eventos usando la clave compuesta
      (stime≈ts, proto, saddr, sport, daddr, dport),
  tolerando ±1e-4 s entre stime (Argus) y ts (Zeek).
• Al encontrar match:
    → Parte del JSON de Argus y añade:
         service (si existe en Zeek o "-"),
         ct_srv_src, ct_srv_dst, ct_dst_ltm, ct_src_ltm,
         ct_src_dport_ltm, ct_dst_sport_ltm, ct_dst_src_ltm
    → Escribe el resultado en <OUTPUT_DIR>/merge/<ts>/merge_conn.jsonl
• Sin match inmediato:
    → Guarda el mensaje en la cola circular correspondiente
      (tamaño configurable, p.e. 100 000).
• Vuelve a intentar correlacionar cada vez que llega un nuevo mensaje.

Dependencias: redis, python-dateutil.
"""
from __future__ import annotations

import argparse, collections, json, logging, os, sys, time
from typing import Deque, Any, Dict, Optional, Tuple
from collections import Counter, deque

import redis
from dateutil import parser as dtparser

# Columnas CSV que espera tu consumidor (igual que CSV_COLUMNS en tu script Python)
ML_CSV_COLUMNS = (
    "stime,proto,saddr,sport,daddr,dport,state,ltime,spkts,dpkts,sbytes,dbytes,"
    "sttl,dttl,sload,dload,sloss,dloss,sintpkt,dintpkt,sjit,djit,stcpb,dtcpb,"
    "tcprtt,synack,ackdat,smeansz,dmeansz,dur,ct_state_ttl,ct_flw_http_mthd,"
    "is_ftp_login,ct_ftp_cmd,ct_srv_src,ct_srv_dst,ct_dst_ltm,ct_src_ltm,"
    "ct_src_dport_ltm,ct_dst_sport_ltm,ct_dst_src_ltlm"
)
ML_COLS = ML_CSV_COLUMNS.split(',')

# --- Configurables -----------------------------------------------------------
LAST_100: Deque[dict] = deque(maxlen=100)

Key5 = Tuple[Any, Any, Any, Any, Any]
MAP_COUNT_HTTP: Counter[Key5, int] = Counter()
MAP_COUNT_FTP: Counter[Key5, int] = Counter()
HTTP_ACC: dict[Key5, dict] = {}

ZEOK_EXTRA = (
    "ct_srv_src","ct_srv_dst","ct_dst_ltm","ct_src_ltm",
    "ct_src_dport_ltm","ct_dst_sport_ltm","ct_dst_src_ltm",
)

OUTPUT_FIELDS = [
    "saddr","sport","daddr","dport","proto","state","dur","sbytes","dbytes",
    "sttl","dttl","sloss","dloss","service","sload","dload","spkts","dpkts",
    "stcpb","dtcpb","smeansz","dmeansz","trans_depth","response_body_len","sjit",
    "djit","stime","ltime","sintpkt","dintpkt","tcprtt","synack","ackdat",
    "is_sm_ips_ports","ct_flw_http_mthd","is_ftp_login","ct_ftp_cmd","ct_srv_src",
    "ct_srv_dst","ct_dst_ltm","ct_src_ltm","ct_src_dport_ltm","ct_dst_sport_ltm",
    "ct_dst_src_ltm"
]

# --- Helper ------------------------------------------------------------------

def to_float(ts_val):
    """Convierte ts/stime a float segundos (epoch)."""
    if isinstance(ts_val, (int, float)):
        return float(ts_val)
    if isinstance(ts_val, str):
        try:
            return float(ts_val)
        except ValueError:
            return dtparser.parse(ts_val).timestamp()
    raise TypeError(f"No puedo convertir {ts_val!r} a float")

def cast_port(val: Any) -> int:
    if val is None: return 0
    if isinstance(val, str) and val.lower().startswith("0x"):
        try: return int(val, 16)
        except: pass
    try: return int(val)
    except: return 0

def build_key(argus: Optional[dict] = None, zeek: Optional[dict] = None) -> tuple:
    if argus:
        proto = str(argus.get("proto", "")).lower()
        saddr = argus.get("saddr")
        daddr = argus.get("daddr")
        if proto == "icmp":
            return (proto, saddr, daddr)
        return (
            proto,
            saddr,
            cast_port(argus.get("sport")),
            daddr,
            cast_port(argus.get("dport")),
        )
    if zeek:
        zlog = zeek.get("zeek_log", "").lower()
        if zlog in ("http", "ftp"):
            proto = "tcp"
        else:
            proto = str(zeek.get("proto", "")).lower()
        orig_h = zeek.get("id.orig_h")
        resp_h = zeek.get("id.resp_h")
        if proto == "icmp":
            return (proto, orig_h, resp_h)
        return (
            proto,
            orig_h,
            cast_port(zeek.get("id.orig_p")),
            resp_h,
            cast_port(zeek.get("id.resp_p")),
        )
    raise ValueError("Se necesita argus o zeek")

# --- Main --------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Fusiona flujos Argus+Zeek en caliente")
    ap.add_argument("--redis_host", default=os.getenv("REDIS_HOST", "127.0.0.1"))
    ap.add_argument("--redis_port", type=int, default=int(os.getenv("REDIS_PORT", 6379)))
    ap.add_argument("--argus_queue", default=os.getenv("REDIS_QUEUE_ARGUS", "argus_data_stream"))
    ap.add_argument("--zeek_queue", default=os.getenv("REDIS_QUEUE_ZEEK", "zeek_data_stream"))
    ap.add_argument("--merge_queue", default=os.getenv("REDIS_QUEUE_MERGE", "merge_data_stream"))
    ap.add_argument("--output_dir", default=os.getenv("OUTPUT_DIR", "/app/output_logs"))
    ap.add_argument("--queue_size", type=int, default=int(os.getenv("QUEUE_SIZE", 100000)), help="Tamaño máximo de las colas internas de sin-match")
    ap.add_argument("--flush_each", action="store_true")
    ap.add_argument("--log_level", default=os.getenv("LOG_LEVEL", "INFO"))
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        r = redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=False)
        r.ping()
    except redis.exceptions.RedisError as exc:
        logging.error("❌ Redis connection failed: %s", exc)
        sys.exit(1)

    ts_run = time.strftime("%Y%m%d_%H%M%S")
    
    # --- Zeek: único fichero JSON Lines ---
    zeek_dir = os.path.join(args.output_dir, "zeek")
    os.makedirs(zeek_dir, exist_ok=True)
    zeek_log_path = os.path.join(zeek_dir, f"{ts_run}.jsonl")
    zeek_fh = open(zeek_log_path, "a", buffering=1)
    logging.info("Fichero Zeek JSON creado: %s", zeek_log_path)

    # --- Argus: único fichero JSON Lines ---
    argus_dir = os.path.join(args.output_dir, "argus") # Directorio para los logs de Argus
    os.makedirs(argus_dir, exist_ok=True)
    argus_log_path = os.path.join(argus_dir, f"{ts_run}.jsonl") 
    argus_fh = open(argus_log_path, "a", buffering=1)
    logging.info("Fichero Argus JSON creado: %s", argus_log_path)
    
    # --- Merge: único fichero JSON Lines ---
    merge_dir = os.path.join(args.output_dir, "merge")
    os.makedirs(merge_dir, exist_ok=True)
    merge_log_path = os.path.join(merge_dir, f"{ts_run}.jsonl")
    merge_fh = open(merge_log_path, "a", buffering=1)
    logging.info("Escribiendo flujos fusionados JSON en: %s", merge_log_path)
    
    # --- Perdidos: único fichero JSON Lines ---
    lost_dir = os.path.join(args.output_dir, "perdidos", ts_run)
    os.makedirs(lost_dir, exist_ok=True)
    path_a = os.path.join(lost_dir, "argus.log")
    path_z = os.path.join(lost_dir, "zeek.log")

    # Colas circulares para sin-match
    argus_cache: Deque[Tuple[float, tuple, dict]] = collections.deque(maxlen=args.queue_size)
    zeek_cache: Deque[Tuple[float, tuple, dict]] = collections.deque(maxlen=args.queue_size)
    
    def dump_deques():
        # Reescribe archivos de registros de colas perdidas
        with open(path_a, "w", buffering=1) as af:
            for _, rec in argus_cache:
                af.write(json.dumps(rec) + "\n")
        with open(path_z, "w", buffering=1) as zf:
            for _, rec in zeek_cache:
                zf.write(json.dumps(rec) + "\n")


    def try_match_from_caches(key: tuple, src: str, keep_on_match: bool = False) -> Optional[dict]:
        other = zeek_cache if src == "argus" else argus_cache
        for idx, (ok, rec) in enumerate(other):
            if key == ok:
                if not keep_on_match:
                    other.rotate(-idx)
                    other.popleft()
                    dump_deques()
                return rec
        return None
    
    def calc_latency(data):
        current_time = time.time()
        original = data.get("stime")
        if original is not None:
            try:
                return f"{(current_time - to_float(original)):.4f}s"
            except TypeError:
                return "ErrorConvTiempo"
        return "N/A"
    
    def connection_features(rec: dict, history: Deque[dict]) -> dict:
        # Aseguro que ltime en el registro corriente es int
        if "ltime" in rec:
            try:
                rec["ltime"] = int(rec["ltime"])
            except Exception:
                pass

        saddr, daddr = rec.get("saddr"), rec.get("daddr")
        sport, dport = cast_port(rec.get("sport")), cast_port(rec.get("dport"))
        service = rec.get("service", "-")

        def same(field_vals):
            cnt = 0
            for h in history:
                if "ltime" in h:
                    try:
                        h["ltime"] = int(h["ltime"])
                    except Exception:
                        pass
                if all(h.get(f) == v for f, v in field_vals):
                    cnt += 1
            return cnt

        return {
            "ct_srv_src": same([("service", service), ("saddr", saddr), ("ltime", rec.get("ltime"))]),
            "ct_srv_dst": same([("service", service), ("daddr", daddr), ("ltime", rec.get("ltime"))]),
            "ct_dst_ltm": same([("daddr", daddr), ("ltime", rec.get("ltime"))]),
            "ct_src_ltm": same([("saddr", saddr), ("ltime", rec.get("ltime"))]),
            "ct_src_dport_ltm": same([("saddr", saddr), ("dport", dport), ("ltime", rec.get("ltime"))]),
            "ct_dst_sport_ltm": same([("daddr", daddr), ("sport", sport), ("ltime", rec.get("ltime"))]),
            "ct_dst_src_ltm": same([("saddr", saddr), ("daddr", daddr), ("ltime", rec.get("ltime"))]),
        }
        
    def merge_records(argus_j: dict, zeek_j: dict):
        merged = argus_j.copy()
        
        # 1. is_sm_ips_ports siempre
        merged["is_sm_ips_ports"] = int(
            merged.get("saddr") == merged.get("daddr")
            and cast_port(merged.get("sport")) == cast_port(merged.get("dport"))
        )

        key = build_key(argus=merged)
        # 2. Inicializamos a 0 todos los campos “no comunes”
        merged["trans_depth"] = 0
        merged["response_body_len"] = 0
        merged["ct_flw_http_mthd"] = 0
        merged["is_ftp_login"] = 0
        merged["ct_ftp_cmd"] = 0

        # 3. Ajuste según tipo de log de Zeek
        if  zeek_j["zeek_log"] == "http":
            # ➜ HTTP
            MAP_COUNT_HTTP[key] += 1
            merged["service"] = "http"
            merged["trans_depth"] = int(zeek_j.get("trans_depth", 0))
            merged["response_body_len"] = int(zeek_j.get("response_body_len", 0))

            # ct_flw_http_mthd: contamos en el buffer HTTP
            merged["ct_flw_http_mthd"] = MAP_COUNT_HTTP[key]
            
        elif zeek_j["zeek_log"] == "ftp":
            # ➜ FTP
            merged["service"] = "ftp"
            
            user = zeek_j.get("user", "")
            passwd = zeek_j.get("password", "")

            if isinstance(user, str): user = user.strip()
            if isinstance(passwd, str): passwd = passwd.strip()

            merged["is_ftp_login"] = int(bool(user) and bool(passwd))

            # ct_ftp_cmd: contamos en el buffer FTP
            cmd = zeek_j.get("command", "")
            if isinstance(cmd, str) and cmd.strip():
                MAP_COUNT_FTP[key] += 1
                
            merged["ct_ftp_cmd"] = MAP_COUNT_FTP[key]
            
        else:
            # ➜ CONN
            merged["service"] = zeek_j.get("service", "-")
        
        # 4. Ahora calculamos los 7 contadores CT* usando el histórico de conexiones
        ct = connection_features(merged, LAST_100)
        for k in ZEOK_EXTRA:
            merged[k] = ct[k]

        # 5. Escritura y alineamiento de campos
        ordered = { key: merged.get(key) for key in OUTPUT_FIELDS }
        merge_fh.write(json.dumps(ordered) + "\n")
        if args.flush_each:
            merge_fh.flush()
            os.fsync(merge_fh.fileno())
            
        # 6) Publicación en Redis para GPU (como CSV)
        csv_line = ",".join(str(merged.get(c,"")) for c in ML_COLS)
        r.lpush(args.merge_queue, csv_line)

        # 6. Registramos en el buffer global (para contar conexiones futuras)
        LAST_100.append(merged)

    # --- Bucle principal -----------------------------------------------------
    skip_first_argus = True
    while True:
        processed = False
        # Argus primero
        payload_a = r.lpop(args.argus_queue)
        if payload_a:
            processed = True
            if skip_first_argus:
                skip_first_argus = False
                logging.info("Omitiendo cabecera de Argus")
            else:
                try:
                    a_data = json.loads(payload_a.decode())
                    for t in ("stime", "ltime"):
                        if t in a_data:
                            try:
                                a_data[t] = int(round(to_float(a_data[t])))
                            except Exception:
                                pass
                    argus_fh.write(json.dumps(a_data) + "\n")
                    if args.flush_each:
                        argus_fh.flush()
                        os.fsync(argus_fh.fileno())
                        
                    proto = str(a_data.get("proto", "")).lower()
                    if proto == "tcp":
                        key_a = build_key(argus=a_data)

                        # Si había acumulación HTTP para esta key → merge final
                        if key_a in HTTP_ACC:
                            final = HTTP_ACC.pop(key_a)
                            z_final = final["last_z"]
                            # Sobreescribimos con los valores agregados
                            z_final["trans_depth"]       = final["max_depth"]
                            z_final["response_body_len"] = final["sum_len"]
                            merge_records(a_data, z_final)

                        else:
                            # Si no era un HTTP pendiente, seguimos con el proceso normal
                            z_match = try_match_from_caches(key_a, "argus")
                            if z_match:
                                merge_records(a_data, z_match)
                            else:
                                argus_cache.append((key_a, a_data))
                                dump_deques()
                                
                    elif proto not in ("tcp", "udp", "icmp"):
                        a_data["is_sm_ips_ports"] = int(
                            a_data.get("saddr") == a_data.get("daddr")
                            and cast_port(a_data.get("sport")) == cast_port(a_data.get("dport"))
                        )

                        a_data["trans_depth"] = 0
                        a_data["response_body_len"] = 0
                        a_data["ct_flw_http_mthd"] = 0
                        a_data["is_ftp_login"] = 0
                        a_data["ct_ftp_cmd"] = 0

                        ct = connection_features(a_data, LAST_100)
                        a_data.update(ct)
                        
                        ordered = { key: a_data.get(key) for key in OUTPUT_FIELDS }
                        merge_fh.write(json.dumps(ordered) + "\n")
                        if args.flush_each:
                            merge_fh.flush(); os.fsync(merge_fh.fileno())

                        LAST_100.append(a_data)
                        csv_line = ",".join(str(a_data.get(c,"")) for c in ML_COLS)
                        r.lpush(args.merge_queue, csv_line)
                        continue 
                    else:
                        key_a = build_key(argus=a_data)
                        z_match = try_match_from_caches(key_a, "argus")
                        if z_match:
                            merge_records(a_data, z_match)
                        else:
                            argus_cache.append((key_a, a_data))
                            dump_deques()
                except Exception as e:
                    logging.error("Error procesando Argus: %s", e)

        # Luego Zeek
        payload_z = r.lpop(args.zeek_queue)
        if payload_z:
            processed = True
            try:
                z_data = json.loads(payload_z.decode())
                zeek_fh.write(json.dumps(z_data) + "\n")
                if args.flush_each:
                    zeek_fh.flush()
                    os.fsync(zeek_fh.fileno())
                    
                key_z = build_key(zeek=z_data)
                
                zeek_type = z_data.get("zeek_log", "").lower()
                if zeek_type == "http":
                    # 1) Acumula response_body_len y guarda el mensaje de mayor trans_depth
                    depth = int(z_data.get("trans_depth", 0))
                    body_len = int(z_data.get("response_body_len", 0))

                    acc = HTTP_ACC.setdefault(key_z, {"sum_len": 0, "max_depth": 0, "last_z": None})
                    acc["sum_len"] += body_len
                    if depth > acc["max_depth"]:
                        acc["max_depth"] = depth
                        
                    acc["last_z"] = z_data.copy()

                    zeek_cache.append((key_z, z_data))
                    dump_deques()
                else:
                    keep = zeek_type == "ftp"
                    a_match = try_match_from_caches(key_z, "zeek", keep_on_match=keep)
                    if a_match:
                        merge_records(a_match, z_data)
                    else:
                        zeek_cache.append((key_z, z_data))
                        dump_deques()
            except Exception as e:
                logging.error("Error procesando Zeek: %s", e)

        if not processed:
            time.sleep(0.05)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Ctrl-C — saliendo.")
