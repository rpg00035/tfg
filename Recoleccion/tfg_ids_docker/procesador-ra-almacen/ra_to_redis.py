import sys
import redis
import os
import signal
import time # Para logging periódico

# Configuración de Redis (desde variables de entorno o valores por defecto)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_QUEUE_NAME = os.getenv("REDIS_QUEUE", "argus_flow_data")

# --- Configuración de Logging ---
# Imprimir una muestra de la línea cada N líneas procesadas.
# Pon 1 para imprimir todas, 0 o None para no imprimir muestras.
PRINT_SAMPLE_EVERY_N_LINES = 100
# Intervalo en segundos para imprimir el contador total de líneas.
LOG_COUNT_INTERVAL_SECONDS = 10

# Bandera para controlar el bucle principal y contador de líneas
running = True
line_count = 0
last_log_time = time.time()

def signal_handler(signum, frame):
    global running
    # Usar sys.stderr para logs en Docker es buena práctica
    print(f"Señal {signum} recibida, deteniendo ra_to_redis.py... (Total líneas procesadas: {line_count})", file=sys.stderr)
    running = False

# Registrar manejadores de señales para cierre grácil
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Cambiar nombre de variable r a r_client para más claridad
r_client = None
try:
    print(f"Conectando a Redis en {REDIS_HOST}:{REDIS_PORT}, cola: {REDIS_QUEUE_NAME}", file=sys.stderr)
    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    r_client.ping() # Verificar conexión
    print("Conexión a Redis exitosa.", file=sys.stderr)
except redis.exceptions.ConnectionError as e:
    print(f"Error CRÍTICO al conectar con Redis: {e}", file=sys.stderr)
    sys.exit(1) # Salir si no se puede conectar a Redis

print("Esperando datos CSV de Argus/ra desde stdin...", file=sys.stderr)
try:
    while running:
        line = sys.stdin.readline()
        if not line: # EOF, el stream de entrada (ra | awk) ha terminado
            print("EOF recibido de stdin. El proceso 'ra' probablemente ha terminado o no hay más datos.", file=sys.stderr)
            break
        
        line = line.strip()
        if line: # Solo procesar si la línea no está vacía después de strip
            try:
                r_client.lpush(REDIS_QUEUE_NAME, line)
                line_count += 1
                
                # Imprimir una muestra de la línea procesada
                if PRINT_SAMPLE_EVERY_N_LINES and (line_count % PRINT_SAMPLE_EVERY_N_LINES == 1 or PRINT_SAMPLE_EVERY_N_LINES == 1):
                    print(f"[DEBUG ra_to_redis.py] Pushed (Línea #{line_count}): '{line[:200]}...'", file=sys.stderr) # Muestra los primeros 200 caracteres
                
                # Imprimir contador total periódicamente
                current_time = time.time()
                if current_time - last_log_time >= LOG_COUNT_INTERVAL_SECONDS:
                    print(f"[INFO ra_to_redis.py] Total líneas enviadas a Redis hasta ahora: {line_count}", file=sys.stderr)
                    last_log_time = current_time
                    
            except redis.exceptions.RedisError as e:
                print(f"Error al enviar a Redis: {e}. Línea: '{line}'", file=sys.stderr)
        # No es necesario un sleep aquí porque sys.stdin.readline() es bloqueante.
except KeyboardInterrupt:
    print("Interrupción de teclado, deteniendo ra_to_redis.py.", file=sys.stderr)
except Exception as e_main_loop:
    print(f"Error inesperado en el bucle principal de ra_to_redis.py: {e_main_loop}", file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
finally:
    print(f"ra_to_redis.py ha finalizado. Total líneas procesadas y enviadas a Redis: {line_count}", file=sys.stderr)
    running = False # Asegurar que cualquier otro bucle (si lo hubiera) termine