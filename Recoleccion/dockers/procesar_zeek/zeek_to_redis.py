#!/usr/bin/env python3
"""
Lee en modo tail -F conn.log, http.log y ftp.log de Zeek usando subprocess.
Cada log corre en su propio hilo leyendo directamente de `tail -F`.
Al encontrar JSON válido, lo envía a Redis.
"""

import os
import json
import redis
import time
import logging
import argparse
import socket
import threading
import subprocess

LOG_DIR = "/output_zeek/current"
TARGETS = {
    "conn.log": "conn",
    "http.log": "http",
    "ftp.log": "ftp",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [zeek_to_redis] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def tail_worker(path: str, kind: str, r: redis.Redis, redis_key: str, use_stream: bool):
    cmd = ["tail", "-n", "0", "-F", path]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    logging.info("Arrancado tail -F %s → hilo %s", path, kind)
    for raw in proc.stdout:
        line = raw.rstrip("\n")
        if not line or line.startswith("#"):
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            logging.debug("JSON inválido (%s): %s", kind, line)
            continue

        rec["zeek_log"] = kind
        payload = json.dumps(rec).encode()
        if use_stream:
            r.xadd(redis_key, {"data": payload})
        else:
            r.rpush(redis_key, payload)
        logging.debug("Enviado %s → Redis (%d bytes)", kind, len(payload))
    proc.stdout.close()
    proc.wait()

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

    mode = "XADD" if args.use_stream else "RPUSH"
    logging.info("Publicando en %s:%s/%s (%s)",
                 args.redis_host, args.redis_port, args.redis_key, mode)

    # Lanzamos un hilo **en cuanto** aparezca cada fichero,
    # sin bloquear el arranque de los demás.
    started = {}  # kind → Thread
    while len(started) < len(TARGETS):
        for fname, kind in TARGETS.items():
            if kind in started:
                continue
            path = os.path.join(LOG_DIR, fname)
            if os.path.isfile(path):
                t = threading.Thread(
                    target=tail_worker,
                    args=(path, kind, r, args.redis_key, args.use_stream),
                    daemon=True
                )
                t.start()
                started[kind] = t
                logging.info("Hilo iniciado para %s (%s)", kind, path)
        time.sleep(0.5)

    # Una vez estén todos lanzados, mantenemos el proceso vivo.
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Ctrl-C recibido, saliendo…")

if __name__ == "__main__":
    main()
