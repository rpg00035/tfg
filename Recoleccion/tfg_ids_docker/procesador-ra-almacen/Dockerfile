FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Paso 1: Actualizar y intentar instalar argus-server (que debería traer argus-clients)
RUN apt-get update -y && \
    apt-get upgrade -y --no-install-recommends && \
    echo "--- [APT DEBUG] Intentando instalar argus-server (que depende de argus-clients) y procps ---" && \
    apt-get install -y --no-install-recommends argus-server procps && \
    echo "--- [APT DEBUG] argus-server y procps instalados (si no hubo error) ---"

# Paso 2: Instalar Python y pip (si argus-server se instaló bien)
RUN echo "--- [APT DEBUG] Intentando instalar python3 y python3-pip ---" && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip && \
    echo "--- [APT DEBUG] python3 y python3-pip instalados ---"

# Paso 3: Instalar paquetes de Python usando pip3
RUN pip3 install --no-cache-dir redis

# Paso 4: Limpiar caché de apt
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY ra_to_redis.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Asegúrate de que tu entrypoint.sh usa 'python3'
ENTRYPOINT ["/entrypoint.sh"]
