# 1. Usar una imagen base de NVIDIA RAPIDS
# Elige una etiqueta compatible con tu GPU, drivers NVIDIA en el host, y la versión de cuML usada en el entrenamiento.
# Ejemplo: rapidsai/rapidsai-core:23.10-cuda11.8-runtime-ubuntu22.04
# Consulta https://hub.docker.com/r/rapidsai/rapidsai-core/tags para más opciones.
FROM rapidsai/rapidsai-core:23.06-cuda11.8-runtime-ubuntu22.04-py3.10

WORKDIR /app

# 2. Instalar dependencias adicionales.
# La imagen base de RAPIDS ya incluye Python, cuML, pandas, numpy, joblib, etc.
# Solo necesitas instalar lo que falte, como redis-py.
# Puedes usar conda (preferido en entornos RAPIDS) o pip.
# Eliminar la copia y uso de requirements.txt si era para un entorno de CPU.
RUN conda install -y -c conda-forge redis-py
# Si tienes otros paquetes pequeños que solo están en pip y son necesarios:
# RUN pip install paquete_adicional1 paquete_adicional2

# 3. Copiar los archivos de tu aplicación
COPY ml_processor.py .
COPY random_forest_gpu_model.pkl . # Tu modelo entrenado con cuML
COPY model_feature_order.json .    # El JSON que define el orden de las características

# 4. Crear el directorio para los mapas y copiar los mapas
# Asumiendo que en tu directorio 'detector-ia/' (en el host) tienes una subcarpeta 'string_indexer_maps/'
# con los siguientes archivos JSON generados por tu script de entrenamiento:
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
