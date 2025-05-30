#!/bin/bash

echo "-----------------------------------------------------------------------"
echo "PASO 1: Deteniendo cualquier instancia previa..."
echo "-----------------------------------------------------------------------"
docker-compose down -v # El -v también elimina volúmenes anónimos

echo "-----------------------------------------------------------------------"
echo "PASO 2: Reconstruyendo todas las imágenes (sin caché para asegurar cambios)"
echo "-----------------------------------------------------------------------"
docker-compose build
BUILD_STATUS=$?

if [ $BUILD_STATUS -ne 0 ]; then
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo "ERROR: El build de Docker Compose falló. Saliendo."
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    exit $BUILD_STATUS
fi

echo "-----------------------------------------------------------------------"
echo "PASO 3: Levantando todos los servicios en segundo plano..."
echo "-----------------------------------------------------------------------"
docker-compose up -d # -d para detached (segundo plano)

# Pequeña pausa para que los servicios se estabilicen
sleep 10

echo "-----------------------------------------------------------------------"
echo "PASO 4: Verificando el estado de los contenedores..."
echo "-----------------------------------------------------------------------"
docker-compose ps

echo "-----------------------------------------------------------------------"
echo "PASO 5: Mostrando logs (Ctrl+C para detener los logs, no los servicios)"
echo "         Observa si hay errores y si los datos fluyen."
echo "-----------------------------------------------------------------------"
echo "Logs de TODOS los servicios (Ctrl+C para salir de esta vista de logs):"
docker-compose logs -f &
LOGS_PID=$! # Captura el PID del proceso de logs en segundo plano

echo ""
echo ">>> ACCIONES A REALIZAR AHORA: <<<"
echo "1. Abre una NUEVA TERMINAL."
echo "2. En la nueva terminal, genera algo de tráfico de red en tu interfaz 'enp6s0'."
echo "   Ejemplos:"
echo "   - ping google.com -c 20"
echo "   - Abre un navegador y visita algunas páginas web."
echo "   - Si tienes otra máquina en tu red, intenta hacerle ping o acceder a algún servicio."
echo "3. Mientras generas tráfico, observa estos logs combinados."
echo "4. Después de unos minutos de generar tráfico, puedes detener los logs combinados"
echo "   presionando Ctrl+C en ESTA terminal (donde se ejecuta el script)."
echo "   Los servicios Docker seguirán corriendo en segundo plano."
echo ""
echo ">>> QUÉ BUSCAR EN LOS LOGS: <<<"
echo "  [captura-argus]: Debería estar 'listening'. No debería haber errores de 'stat /entrypoint.sh'."
echo "  [procesador-ra-almacen]: Debería conectarse a Argus y Redis. Si hay tráfico, debería"
echo "                         mostrar que procesa flujos y los envía a Redis (si tienes prints para ello)."
echo "  [detector-ia]: Debería conectarse a Redis y, si hay datos en la cola,"
echo "                 debería empezar a procesar los flujos y aplicar tu modelo."
echo "                 Busca mensajes de 'Modelo cargado', 'Mapeo cargado', 'Predicción...'"
echo "  [redis]: Debería mostrar conexiones y actividad si los otros servicios lo usan."
echo ""
echo "Presiona Ctrl+C para detener la visualización de logs combinados cuando hayas terminado de probar."
wait $LOGS_PID # Espera a que el comando 'docker-compose logs -f' termine (cuando presionas Ctrl+C)

echo "-----------------------------------------------------------------------"
echo "PASO 6: Verificación adicional (opcional, en otra terminal)"
echo "-----------------------------------------------------------------------"
echo "Puedes verificar la cola de Redis (si tienes redis-cli instalado en el host):"
echo "  redis-cli ping"
echo "  redis-cli LLEN argus_flow_data"
echo "  redis-cli LRANGE argus_flow_data 0 5"
echo ""
echo "Para ver logs de un servicio específico (en otra terminal):"
echo "  docker-compose logs -f detector-ia"
echo "  docker-compose logs -f procesador-ra-almacen"
echo ""
echo "-----------------------------------------------------------------------"
echo "PASO 7: Para detener todos los servicios cuando hayas terminado:"
echo "  docker-compose down"
echo "-----------------------------------------------------------------------"
echo "Script finalizado."