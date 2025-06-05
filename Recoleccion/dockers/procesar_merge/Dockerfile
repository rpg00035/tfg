FROM python:latest

WORKDIR /app

COPY merge_argus_zeek.py /app/merge_argus_zeek.py
COPY model_feature_order.json /app/model_feature_order.json

RUN pip install --no-cache-dir redis pandas # O las dependencias que necesite

ENTRYPOINT ["python", "/app/merge_argus_zeek.py"]