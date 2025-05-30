FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq 
RUN apt-get install -y -qq tcpdump
RUN apt-get install -y -qq argus-server
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 561/udp
EXPOSE 561/tcp 

ENTRYPOINT ["/entrypoint.sh"]
