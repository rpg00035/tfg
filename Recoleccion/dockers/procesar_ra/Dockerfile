FROM debian:latest

# Instala ra (en paquete argus-client), socat y Python
RUN apt-get update && \
    apt-get install -y --no-install-recommends argus-client socat python3 python3-pip && \
    pip3 install --break-system-packages redis && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copiamos scripts
COPY ra_to_redis.py /app/ra_to_redis.py
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh 

WORKDIR /app
ENTRYPOINT ["/entrypoint.sh"]
