#!/usr/bin/env python3
"""
Tail -F de los logs JSON de Zeek y los envía a Redis Stream (XADD) o List (RPUSH).
Solo publica líneas JSON válidas; ignora comentarios '#'.
"""
import os, json, redis, time, logging, argparse

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

LOG_DIR   = "/output_zeek"
TARGETS   = ["conn.json.log", "http.json.log", "ftp.json.log"]

def follow(fname):
    with open(fname, "r") as f:
        f.seek(0, 2)  # EOF
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.1)
                continue
            if line.startswith("#"):   # comentarios Zeek
                continue
            yield line

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--redis_host", default=os.getenv("REDIS_HOST", "redis"))
    p.add_argument("--redis_port", type=int,
                   default=int(os.getenv("REDIS_PORT", 6379)))
    p.add_argument("--redis_key",  default=os.getenv("REDIS_QUEUE_ZEEK",
                                                     "zeek_data_stream"))
    p.add_argument("--use_stream", action="store_true",
                   help="Usa XADD en lugar de RPUSH")
    args = p.parse_args()

    r = redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=False)
    r.ping()

    tails = [follow(os.path.join(LOG_DIR, t)) for t in TARGETS if
             os.path.exists(os.path.join(LOG_DIR, t))]

    while True:
        for g in tails:
            try:
                line = next(g)
            except StopIteration:
                continue
            try:
                doc = json.loads(line)
                payload = json.dumps(doc).encode()
                if args.use_stream:
                    r.xadd(args.redis_key, {"data": payload})
                else:
                    r.rpush(args.redis_key, payload)
            except json.JSONDecodeError:
                logging.warning("JSON inválido descartado")

if __name__ == "__main__":
    main()
