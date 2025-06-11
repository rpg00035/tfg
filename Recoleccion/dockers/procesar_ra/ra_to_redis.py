#!/usr/bin/env python3
# filepath: /home/ruben/TFG/Recoleccion/dockers/procesar_ra/ra_to_redis.py
import os, sys, csv, json, redis, argparse, logging, socket

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [ra_to_redis] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--redis_host", default=os.getenv("REDIS_HOST", "redis"))
    p.add_argument("--redis_port", type=int, default=int(os.getenv("REDIS_PORT", 6379)))
    p.add_argument("--redis_key",  default=os.getenv("REDIS_QUEUE_ARGUS", "argus_data_stream"))
    args = p.parse_args()

    # Obtener el orden definido en RA_FIELDS
    field_list = os.getenv("RA_FIELDS")
    if not field_list:
        logging.error("RA_FIELDS no definido ⇒ salgo.")
        sys.exit(1)

    fieldnames = [f.strip() for f in field_list.split(",")]

    try:
        r = redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=False)
        r.ping()
        logging.info("Conectado a Redis %s:%s (key=%s)", args.redis_host, args.redis_port, args.redis_key)
    except (redis.ConnectionError, socket.error) as e:
        logging.exception("¿Redis caído?: %s", e)
        sys.exit(2)

    # Leer stdin como CSV con los fieldnames
    reader = csv.DictReader(sys.stdin, fieldnames=fieldnames)
    total = 0

    for row in reader:
        # Reconstruir la fila con el orden correcto
        row_ordered = {fn: row.get(fn, "") for fn in fieldnames}
        r.rpush(args.redis_key, json.dumps(row_ordered).encode())
        total += 1

        if total % 100 == 0:
            logging.info("Enviadas %d filas; última stime=%s", total, row_ordered.get("stime"))

if __name__ == "__main__":
    main()