import sys
import os
import signal
import threading
import time

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
NUM_FILTER_HOUR_WORKERS = int(os.environ.get('NUM_FILTER_HOUR_WORKERS', '2'))

# ID del worker (auto-detectado del hostname o env var)
def get_worker_id():
    worker_id_env = os.environ.get('WORKER_ID')
    if worker_id_env is not None:
        return int(worker_id_env)
    
    import socket, re
    hostname = socket.gethostname()
    match = re.search(r'[-_](\d+)$', hostname)
    if match:
        return int(match.group(1)) - 1
    return 0

WORKER_ID = get_worker_id()

INPUT_EXCHANGES = {"transactions_hour_amount": [f"worker_{WORKER_ID}", "eos"]}  # routing keys específicas
OUTPUT_EXCHANGES = {
    "transactions_hour_amount": ["transactions_amount"]      # exchange para transactions filtradas por amount
}
ROUTING_KEY = "amount"                   # routing para topic
THRESHOLD = 75.0

def filter_by_amount(rows, threshold: float):
    """Mantiene filas con final_amount >= threshold."""
    filtered = []
    for r in rows:
        try:
            fa = r.get("final_amount")
            if fa is None or fa == "":
                continue
            # final_amount puede venir como string; convertir con float
            if isinstance(fa, str):
                fa = float(fa)
            if fa >= threshold:
                filtered.append(r)
        except Exception:
            continue
    return filtered

# Control de EOS por sesión - necesitamos recibir EOS de todos los workers de filter_hour para cada sesión
eos_count_per_session = {}  # {session_id: count}
eos_lock = threading.Lock()

def on_message(body, source_exchange):
    header, rows = deserialize_message(body)
    session_id = header.get("session_id", "unknown")
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        with eos_lock:
            # Inicializar contador para esta sesión si no existe
            if session_id not in eos_count_per_session:
                eos_count_per_session[session_id] = 0
            eos_count_per_session[session_id] += 1
            
            session_count = eos_count_per_session[session_id]
            print(f"[FilterAmount] EOS recibido para sesión {session_id} ({session_count}/{NUM_FILTER_HOUR_WORKERS})")
            
            # Solo reenviar EOS cuando hayamos recibido de TODOS los workers de filter_hour para esta sesión
            if session_count >= NUM_FILTER_HOUR_WORKERS:
                print(f"[FilterAmount Worker {WORKER_ID}] EOS recibido de todas las fuentes para sesión {session_id}. Reenviando downstream...")
                eos_msg = serialize_message([], header)
                output_exchanges = OUTPUT_EXCHANGES[source_exchange]
                for exchange_name in output_exchanges:
                    mq_outputs[exchange_name].send(eos_msg)
                print(f"[FilterAmount] EOS reenviado para sesión {session_id}")
                
                # Limpiar contador de esta sesión después de un delay
                def delayed_cleanup():
                    with eos_lock:
                        if session_id in eos_count_per_session:
                            del eos_count_per_session[session_id]
                            print(f"[FilterAmount Worker {WORKER_ID}] Sesión {session_id} limpiada")
                
                cleanup_thread = threading.Thread(target=delayed_cleanup, daemon=True)
                cleanup_thread.start()
        return
    
    # Procesamiento normal
    filtered = filter_by_amount(rows, THRESHOLD)
    
    if filtered:
        out_msg = serialize_message(filtered, header)
        output_exchanges = OUTPUT_EXCHANGES[source_exchange]
        for exchange_name in output_exchanges:
            mq_outputs[exchange_name].send(out_msg)

if __name__ == "__main__":
    print(f"[FilterAmount] Iniciando worker {WORKER_ID}...")
    shutdown_requested = False
    mq_connections = []
    
    def signal_handler(signum, frame):
        global shutdown_requested
        print(f"[FilterAmount] Señal {signum} recibida, cerrando...")
        shutdown_requested = True
        for mq in mq_connections:
            try:
                mq.stop_consuming()
            except:
                pass
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: múltiples exchanges con routing keys específicas
    mq_connections = []
    for exchange_name, route_keys in INPUT_EXCHANGES.items():
        mq_in = MessageMiddlewareExchange(RABBIT_HOST, exchange_name, route_keys)
        mq_connections.append((mq_in, exchange_name))

    # Salida: múltiples exchanges según el tipo de datos
    mq_outputs = {}
    for input_exchange, output_exchanges in OUTPUT_EXCHANGES.items():
        for exchange_name in output_exchanges:
            mq_outputs[exchange_name] = MessageMiddlewareExchange(RABBIT_HOST, exchange_name, [ROUTING_KEY])

    print(f"[*] FilterWorkerAmount esperando mensajes de {INPUT_EXCHANGES}...")
    try:
        # Procesar cada exchange en un hilo separado
        import threading
        
        def consume_exchange(mq_in, exchange_name):
            try:
                print(f"[FilterAmount] Iniciando consumo de {exchange_name}...")
                # Crear una función wrapper que pase el source_exchange
                def on_message_wrapper(body):
                    return on_message(body, exchange_name)
                mq_in.start_consuming(on_message_wrapper)
            except Exception as e:
                print(f"[FilterAmount] Error consumiendo {exchange_name}: {e}")
        
        threads = []
        for mq_in, exchange_name in mq_connections:
            thread = threading.Thread(target=consume_exchange, args=(mq_in, exchange_name))
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        print(f"[FilterAmount Worker {WORKER_ID}] Worker iniciado, esperando mensajes de múltiples sesiones...")
        print(f"[FilterAmount Worker {WORKER_ID}] El worker continuará procesando múltiples clientes")
        
        # Loop principal - solo termina por señal
        while not shutdown_requested:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n[FilterAmount] Interrupción recibida")
    finally:
        try:
            for mq_in in mq_connections:
                mq_in.close()
        except:
            pass
        try:
            for mq_out in mq_outputs.values():
                mq_out.close()
        except:
            pass
        print("[x] FilterWorkerAmount detenido")
