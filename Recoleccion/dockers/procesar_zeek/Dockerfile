FROM zeek/zeek:latest

# Paquetes extra: socat para el pipe TCP, python3 y redis-py
RUN apt-get update && \
    apt-get install -y --no-install-recommends socat python3 python3-pip && \
    pip3 install --break-system-packages redis && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copiamos scripts y utilidades
COPY local.zeek /usr/local/zeek/share/zeek/site/local.zeek
COPY zeek_to_redis.py /app/zeek_to_redis.py
COPY wait-for-it.sh /app/wait-for-it.sh 
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /app/wait-for-it.sh

# Directorio donde Zeek escribirá sus logs JSON
WORKDIR /output_zeek

ENTRYPOINT ["/entrypoint.sh"]
