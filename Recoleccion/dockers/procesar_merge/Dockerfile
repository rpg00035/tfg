FROM python:latest

WORKDIR /app

COPY merge_argus_zeek.py /app/merge_argus_zeek.py
COPY model_feature_order.json /app/model_feature_order.json

RUN apt-get update && apt-get install -y util-linux && pip install --no-cache-dir redis pandas

ENTRYPOINT ["taskset","-c","3","python","/app/merge_argus_zeek.py"]