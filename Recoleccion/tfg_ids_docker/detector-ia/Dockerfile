# 1. Usar una imagen base de NVIDIA RAPIDS
# Elige una etiqueta compatible con tu GPU, drivers NVIDIA en el host, y la versión de cuML usada en el entrenamiento.
# Ejemplo: rapidsai/rapidsai-core:23.10-cuda11.8-runtime-ubuntu22.04
# Consulta https://hub.docker.com/r/rapidsai/rapidsai-core/tags para más opciones.
FROM rapidsai/base:25.02-cuda12.0-py3.12

WORKDIR /app

# 2. Instalar dependencias adicionales.
RUN conda install -y -c conda-forge redis-py && conda clean -afy


# 3. Copiar los archivos de tu aplicación
COPY ml_processor.py .
COPY random_forest_gpu_model.pkl .
COPY model_feature_order.json .

# 4. Crear el directorio para los mapas y copiar los mapas
RUN mkdir -p ./string_indexer_maps
COPY string_indexer_maps/string_indexer_proto_map.json ./string_indexer_maps/
COPY string_indexer_maps/string_indexer_state_map.json ./string_indexer_maps/

# Variables de entorno (algunas ya podrían estar en la imagen base de RAPIDS, pero no hace daño definirlas)
ENV REDIS_HOST="redis"
ENV REDIS_PORT="6379"
ENV REDIS_QUEUE="argus_flow_data"
ENV COLUMN_NAMES="stime,proto,sport,dport,state,ltime,spkts,dpkts,sbytes,dbytes,sttl,dttl,sload,dload,sloss,dloss,sintpkt,dintpkt,sjit,djit,stcpb,dtcpb,tcprtt,synack,ackdat,smeansz,dmeansz,dur"

# Estas variables son cruciales para que Docker exponga la GPU al contenedor
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility

ENTRYPOINT ["python", "/app/ml_processor.py"]