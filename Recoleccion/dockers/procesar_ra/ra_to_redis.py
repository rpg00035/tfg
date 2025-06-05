#!/usr/bin/env python3

import os, sys, csv, json, redis, argparse, logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--redis_host", default=os.getenv("REDIS_HOST", "redis"))
    p.add_argument("--redis_port", type=int,
                   default=int(os.getenv("REDIS_PORT", 6379)))
    p.add_argument("--redis_key",  default=os.getenv("REDIS_QUEUE_ARGUS",
                                                     "argus_data_stream"))
    args = p.parse_args()

    field_list = os.getenv("RA_FIELDS")
    if not field_list:
        logging.error("RA_FIELDS no definido")
        sys.exit(1)
    fieldnames = [f.strip() for f in field_list.split(",")]

    r = redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=False)
    r.ping()

    reader = csv.DictReader(sys.stdin, fieldnames=fieldnames)
    pipe, batch = r.pipeline(), 0

    for row in reader:
        pipe.rpush(args.redis_key, json.dumps(row).encode())
        batch += 1
        if batch >= 500:        
            pipe.execute()
            batch = 0
    if batch:
        pipe.execute()

if __name__ == "__main__":
    main()
