#!/usr/bin/env bash

# 1. Activar el entorno 'rapids' localizado en tu carpeta de proyecto
#    Usamos "source" para apuntar al script activate dentro de la instalación local de Conda.
source "/home/ruben/TFG/Recoleccion/tfg_ids_docker/detector-ia/enter/bin/activate" rapids  # 

# 2. Cambiar al directorio donde están los scripts Python (opcional si ya estás allí)
cd "/home/ruben/TFG/Recoleccion/tfg_ids_docker/detector-ia"                              # 

# 3. Ejecutar el script Python deseado 
python ml_processor.py                                                                 # 

# 4. Desactivar el entorno al finalizar (opcional)
conda deactivate                                                                           # 
