FROM debian:latest

ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependencias y Argus
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      argus-server procps tcpdump iproute2 \     
      wget ca-certificates build-essential \
      libpcap-dev bison flex \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 561/udp

ENTRYPOINT ["/entrypoint.sh"]
