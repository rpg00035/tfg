FROM debian:bullseye-slim

# Instalar dependencias y Argus
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    argus-server socat \
    && rm -rf /var/lib/apt/lists/*

COPY wait-for-it.sh /app/wait-for-it.sh 
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /app/wait-for-it.sh

ENTRYPOINT ["/entrypoint.sh"]
