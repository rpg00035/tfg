import sys
import redis
import os
import signal

# Configuración de Redis (desde variables de entorno o valores por defecto)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_QUEUE_NAME = os.getenv("REDIS_QUEUE", "argus_flow_data")

# Bandera para controlar el bucle principal, para cierre grácil
running = True

def signal_handler(signum, frame):
    global running
    print(f"Señal {signum} recibida, deteniendo ra_to_redis.py...")
    running = False

# Registrar manejadores de señales para cierre grácil
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

try:
    print(f"Conectando a Redis en {REDIS_HOST}:{REDIS_PORT}, cola: {REDIS_QUEUE_NAME}")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    r.ping() # Verificar conexión
    print("Conexión a Redis exitosa.")
except redis.exceptions.ConnectionError as e:
    print(f"Error al conectar con Redis: {e}", file=sys.stderr)
    sys.exit(1)

print("Esperando datos CSV de Argus/ra desde stdin...")
try:
    while running:
        # sys.stdin.readline() es bloqueante, bueno para un stream
        # Si el proceso padre (ra | awk) termina, readline() devolverá ''
        line = sys.stdin.readline()
        if not line: # EOF, el stream de entrada ha terminado
            print("EOF recibido de stdin, finalizando.")
            break
        
        line = line.strip()
        if line:
            try:
                r.lpush(REDIS_QUEUE_NAME, line)
                # print(f"Pushed: {line}", file=sys.stderr) # Descomentar para debugging intensivo
            except redis.exceptions.RedisError as e:
                print(f"Error al enviar a Redis: {e}. Línea: {line}", file=sys.stderr)
        # No es necesario un sleep aquí porque readline() es bloqueante.
except KeyboardInterrupt: # Aunque ya tenemos signal_handler, por si acaso.
    print("Interrupción de teclado, deteniendo ra_to_redis.py.")
finally:
    print("ra_to_redis.py ha finalizado.")
    running = False # Asegurar que cualquier otro bucle termine
