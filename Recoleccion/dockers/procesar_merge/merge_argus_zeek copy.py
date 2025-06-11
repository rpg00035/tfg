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

import argparse, collections, io, json, logging, os, sys, time
from typing import Deque, Any, Optional, Tuple

import redis
from dateutil import parser as dtparser

# --- Configurables -----------------------------------------------------------
LAST_100: Deque[dict] = collections.deque(maxlen=100)

LAST_100_HTTP: Deque[dict]   = collections.deque(maxlen=100)
LAST_100_FTP:  Deque[dict]   = collections.deque(maxlen=100)

TIME_TOL = 1e-3
ZEOK_EXTRA = (
    "ct_srv_src",
    "ct_srv_dst",
    "ct_dst_ltm",
    "ct_src_ltm",
    "ct_src_dport_ltm",
    "ct_dst_sport_ltm",
    "ct_dst_src_ltm",
)

OUTPUT_FIELDS = [
    "srcip","sport","dstip","dport","proto","state","dur","sbytes","dbytes",
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

def cast_port(val: Any) -> Optional[int]:
    if val is None:
        return 0

    # Caso especial: hexadecimal
    if isinstance(val, str) and val.lower().startswith("0x"):
        try:
            return int(val, 16)
        except ValueError:
            logging.warning(
                "⚠️ No se pudo convertir el hexadecimal '%s' a entero. Intentando decimal...", 
                val
            )
            # si falla, caerá al int(val) genérico más abajo

    try:
        return int(val)
    except (TypeError, ValueError):
        logging.warning(
            "⚠️ No se pudo convertir el valor de puerto '%s' (tipo: %s) a entero. Devolviendo 0.",
            val, type(val).__name__
        )
        return 0




def build_key(argus: Optional[dict] = None, zeek: Optional[dict] = None):
    if argus:
        proto = str(argus.get("proto", "")).lower()
        t = to_float(argus.get("stime"))
        saddr = argus.get("saddr")
        daddr = argus.get("daddr")
        if proto == "icmp":
            base = (proto, saddr, daddr)
        else:
            base = (
                proto,
                saddr,
                cast_port(argus.get("sport")),
                daddr,
                cast_port(argus.get("dport")),
            )
    elif zeek:
        proto = str(zeek.get("proto", "")).lower()
        t = to_float(zeek.get("ts"))
        orig_h = zeek.get("id.orig_h")
        resp_h = zeek.get("id.resp_h")
        if proto == "icmp":
            base = (proto, orig_h, resp_h)
        else:
            base = (
                proto,
                orig_h,
                cast_port(zeek.get("id.orig_p")),
                resp_h,
                cast_port(zeek.get("id.resp_p")),
            )
    else:
        raise ValueError("Se necesita argus o zeek")
    
    return t, base


# --- Main --------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Fusiona flujos Argus+Zeek en caliente")
    ap.add_argument("--redis_host", default=os.getenv("REDIS_HOST", "127.0.0.1"))
    ap.add_argument("--redis_port", type=int, default=int(os.getenv("REDIS_PORT", 6379)))
    ap.add_argument("--argus_queue", default=os.getenv("REDIS_QUEUE_ARGUS", "argus_data_stream"))
    ap.add_argument("--zeek_queue", default=os.getenv("REDIS_QUEUE_ZEEK", "zeek_data_stream"))
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
            for _, _, rec in argus_cache:
                af.write(json.dumps(rec) + "\n")
        with open(path_z, "w", buffering=1) as zf:
            for _, _, rec in zeek_cache:
                zf.write(json.dumps(rec) + "\n")


    def try_match_from_caches(t: float, key: tuple, src: str):
        """Busca coincidencia en la otra cache (src = 'argus'|'zeek')."""
        other = zeek_cache if src == "argus" else argus_cache
        for idx, (ot, ok, od) in enumerate(other):
            if key == ok and abs(t - ot) <= TIME_TOL:
                other.rotate(-idx); other.popleft()  # elimina
                dump_deques()
                return od
        return None
    
    def calc_latency(data):
        current_time = time.time()
        original_stime_argus = data.get("stime")
        if original_stime_argus is not None:
            try:
                latency = current_time - to_float(original_stime_argus)
                return f"{latency:.4f}s"
            except TypeError:
                return "ErrorConvTiempo"
        return "N/A"


    def general_purpose_features(rec: dict) -> dict:
        gp = {}
        proto = str(rec.get("proto", "")).lower()

        # Siempre calculamos service e is_sm_ips_ports
        gp["service"] = rec.get("service", "-")
        gp["is_sm_ips_ports"] = int(
            rec.get("saddr") == rec.get("daddr") and
            cast_port(rec.get("sport")) == cast_port(rec.get("dport"))
        )

        # Si es ICMP, resto a 0
        if proto == "icmp":
            gp["trans_depth"]       = 0
            gp["response_body_len"] = 0
            gp["ct_flw_http_mthd"]  = 0
            gp["is_ftp_login"]      = 0
            gp["ct_ftp_cmd"]        = 0
            return gp

        # ————————————————————————————————————————————————————————————————
        # Para TCP/UDP/HTTP/FTP, mantenemos el cálculo completo:
        gp["trans_depth"]       = int(rec.get("trans_depth", 0))
        gp["response_body_len"] = int(rec.get("response_body_len", 0))

        # HTTP method counter
        method = rec.get("method")
        if gp["service"] == "http" and method:
            key_http = (
                rec.get("saddr"), cast_port(rec.get("sport")),
                rec.get("daddr"), cast_port(rec.get("dport")),
                method.upper()
            )
            gp["ct_flw_http_mthd"] = sum(
                1 for h in LAST_100_HTTP
                if (h.get("saddr"), cast_port(h.get("sport")),
                    h.get("daddr"), cast_port(h.get("dport")),
                    h.get("method", "").upper()) == key_http
            )
            LAST_100_HTTP.append(rec)
        else:
            gp["ct_flw_http_mthd"] = 0

        # FTP login flag
        gp["is_ftp_login"] = int(
            gp["service"] == "ftp" and
            bool(rec.get("user")) and bool(rec.get("password"))
        )

        # FTP command counter
        command = rec.get("command")
        if gp["service"] == "ftp" and command:
            key_ftp = (
                rec.get("saddr"), cast_port(rec.get("sport")),
                rec.get("daddr"), cast_port(rec.get("dport")),
                command.upper()
            )
            gp["ct_ftp_cmd"] = sum(
                1 for h in LAST_100_FTP
                if (h.get("saddr"), cast_port(h.get("sport")),
                    h.get("daddr"), cast_port(h.get("dport")),
                    h.get("command", "").upper()) == key_ftp
            )
            LAST_100_FTP.append(rec)
        else:
            gp["ct_ftp_cmd"] = 0

        return gp

    
    def connection_features(rec: dict, history: Deque[dict]) -> dict:
        """
        Calcula los 7 contadores CT* para `rec` usando los 100 últimos registros en `history`.
        """
        saddr = rec.get("saddr")
        daddr = rec.get("daddr")
        sport = rec.get("sport")
        dport = rec.get("dport")
        service = rec.get("service", "-")
        is_sm_ips_ports = int(rec.get("saddr") == rec.get("daddr") and
            cast_port(rec.get("sport")) == cast_port(rec.get("dport")))

        def same(field_vals):
            return sum(1 for h in history if all(h.get(f) == v for f, v in field_vals))

        return {
            "service": service,
            "trans_depth": 0,
            "response_body_len": 0,
            "is_sm_ips_ports": is_sm_ips_ports,
            "ct_flw_http_mthd": 0,
            "is_ftp_login": 0,
            "ct_ftp_cmd": 0,
            "ct_srv_src": same([("service", service), ("saddr", saddr)]),
            "ct_srv_dst": same([("service", service), ("daddr", daddr)]),
            "ct_dst_ltm": same([("daddr", daddr)]),
            "ct_src_ltm": same([("saddr", saddr)]),
            "ct_src_dport_ltm": same([("saddr", saddr), ("dport", dport)]),
            "ct_dst_sport_ltm": same([("daddr", daddr), ("sport", sport)]),
            "ct_dst_src_ltm": same([("saddr", saddr), ("daddr", daddr)]),
        }
        
    def merge_records(argus_j: dict, zeek_j: dict):
        merged = argus_j.copy()
        for k in ZEOK_EXTRA:
            if k in zeek_j:
                merged[k] = zeek_j[k]

        merged["service"] = zeek_j.get("service", "-")

        merged.update(general_purpose_features(merged))
        
        ordered = { key: merged.get(key) for key in OUTPUT_FIELDS }
        
        latency_str = calc_latency(argus_j)
                
        merge_fh.write(json.dumps(ordered) + "\n")
        logging.debug("Datos combinados: %s. Tamaño de la cache Argus: %d, Zeek: %d. Latencia fusión: %s",
                    ordered, len(argus_cache), len(zeek_cache), latency_str)
        if args.flush_each:
            merge_fh.flush()
            os.fsync(merge_fh.fileno())

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
                    argus_fh.write(json.dumps(a_data) + "\n")
                    if args.flush_each:
                        argus_fh.flush()
                        os.fsync(argus_fh.fileno())
                    if a_data.get("stime") is None:
                        logging.warning("Argus sin 'stime', se omite: %r", payload_a)
                    else:
                        proto = str(a_data.get("proto", "")).lower()
                        if proto not in ("tcp", "udp", "icmp"):
                            ct = connection_features(a_data, LAST_100)
                            a_data.update(ct)
                            
                            ordered = { key: a_data.get(key) for key in OUTPUT_FIELDS }
                            merge_fh.write(json.dumps(ordered) + "\n")
                            if args.flush_each:
                                merge_fh.flush(); os.fsync(merge_fh.fileno())

                            LAST_100.append(a_data)
                            latency_str = calc_latency(a_data)
                            logging.debug("Protocolo distinto. Datos sin combinar: %s. Latencia: %s", ordered, latency_str)
                            continue 
                        else:
                            t_a, key_a = build_key(argus=a_data)
                            z_match = try_match_from_caches(t_a, key_a, "argus")
                            if z_match:
                                merge_records(a_data, z_match)
                            else:
                                argus_cache.append((t_a, key_a, a_data))
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
                if z_data.get("ts") is None:
                    logging.warning("Zeek sin 'ts', se omite: %r", z)
                else:
                    t_z, key_z = build_key(zeek=z_data)
                    a_match = try_match_from_caches(t_z, key_z, "zeek")
                    if a_match:
                        merge_records(a_match, z_data)
                    else:
                        zeek_cache.append((t_z, key_z, z_data))
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
