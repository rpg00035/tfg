FROM zeek/zeek:latest

ENV DEBIAN_FRONTEND=noninteractive

ARG ZEEK_HOME=/usr/local/zeek     # ← ruta real en la imagen

# Paquetes extra: socat para el pipe TCP, python3 y redis-py
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip procps iproute2 ca-certificates htop && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --break-system-packages --no-cache-dir redis

COPY node.cfg /usr/local/zeek/etc/node.cfg
RUN printf 'mailto =\nLogDir = /output_zeek\n' \
    > /usr/local/zeek/etc/zeekctl.cfg

COPY local.zeek ${ZEEK_HOME}/share/zeek/site/local.zeek
COPY zeek_to_redis.py /usr/local/bin/zeek_to_redis.py
RUN chmod +x /usr/local/bin/zeek_to_redis.py

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
WORKDIR /output_zeek
ENTRYPOINT ["/entrypoint.sh"]
