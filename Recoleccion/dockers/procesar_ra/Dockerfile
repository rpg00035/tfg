FROM debian:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      argus-client \   
      redis-tools \   
      python3 \
      python3-pip \
      procps \        
      wget \
      ca-certificates \
      iproute2 \      
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --break-system-packages --no-cache-dir redis

COPY ra_to_redis.py /app/ra_to_redis.py
RUN chmod +x /app/ra_to_redis.py

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh 

WORKDIR /app
ENTRYPOINT ["/entrypoint.sh"]
