FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Actualizar, instalar dependencias del sistema y de Python, luego limpiar en una sola capa RUN
RUN apt-get update -y && \
    apt-get upgrade -y --no-install-recommends && \
    echo "--- [APT DEBUG] Instalando argus-client, procps, python3, python3-pip ---" && \
    apt-get install -y --no-install-recommends \
        argus-client \
        procps \
        python3 \
        python3-pip && \
    echo "--- [APT DEBUG] Paquetes del sistema instalados ---" && \
    echo "--- [PIP DEBUG] Instalando redis para python3 ---" && \
    pip3 install --no-cache-dir redis && \
    echo "--- [PIP DEBUG] redis para python3 instalado ---" && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY ra_to_redis.py .
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# El entrypoint.sh debe usar 'python3' para ejecutar ra_to_redis.py
ENTRYPOINT ["/entrypoint.sh"]