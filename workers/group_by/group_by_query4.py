import sys
import os
import signal
import threading
from collections import defaultdict
import time

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
NUM_FILTER_YEAR_WORKERS = int(os.environ.get('NUM_FILTER_YEAR_WORKERS', '3'))

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

INPUT_EXCHANGE = "transactions_year_query4"  # exchange dedicado para query4
INPUT_ROUTING_KEYS = [f"worker_{WORKER_ID}", "eos"]  # routing keys específicas
OUTPUT_EXCHANGE = "transactions_query4"      # exchange de salida para query 4
ROUTING_KEY = "query4"                       # routing para topic

def group_by_store_and_user(rows):
    """Agrupa por (store_id, user_id) y cuenta transacciones para Query 4."""
    metrics = defaultdict(lambda: {
        'transaction_count': 0
    })
    
    for r in rows:
        # Validar datos requeridos
        store_id = r.get("store_id")
        user_id = r.get("user_id")
        
        if not store_id or not store_id.strip() or not user_id or not user_id.strip():
            continue
        
        try:
            normalized_store_id = store_id.strip()
            normalized_user_id = user_id.strip()
            
            # Clave compuesta: (store_id, user_id)
            key = (normalized_store_id, normalized_user_id)
            
            # Contar transacciones por cliente en cada sucursal
            metrics[key]['transaction_count'] += 1
            
        except Exception:
            # Ignorar filas con datos inválidos
            continue
    
    return metrics

# Control de EOS por sesión - necesitamos recibir EOS de todos los workers de filter_year por cada sesión
eos_count = {}  # {session_id: count}
eos_lock = threading.Lock()
batches_sent = 0 

def on_message(body):
    global eos_count
    global batches_sent
    header, rows = deserialize_message(body)
    session_id = header.get("session_id", "unknown")
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        with eos_lock:
            # Inicializar contador para esta sesión si no existe
            if session_id not in eos_count:
                eos_count[session_id] = 0
            eos_count[session_id] += 1
            print(f"[GroupByQuery4] EOS recibido para sesión {session_id} ({eos_count[session_id]}/{NUM_FILTER_YEAR_WORKERS})")
            
            # Solo reenviar EOS cuando hayamos recibido de TODOS los workers de filter_year para esta sesión
            if eos_count[session_id] >= NUM_FILTER_YEAR_WORKERS:
                print(f"[GroupByQuery4] EOS recibido de TODOS los workers para sesión {session_id}. Reenviando downstream...")
                eos_msg = serialize_message([], header)  # Mantiene session_id en header
                mq_out.send(eos_msg)
                print(f"[GroupByQuery4] EOS reenviado para sesión {session_id}")
                
                # Limpiar contador EOS para esta sesión después de un delay
                def delayed_cleanup():
                    time.sleep(30)  # Esperar 30 segundos antes de limpiar
                    with eos_lock:
                        if session_id in eos_count:
                            del eos_count[session_id]
                            print(f"[GroupByQuery4] Sesión {session_id} limpiada de contadores EOS")
                
                cleanup_thread = threading.Thread(target=delayed_cleanup, daemon=True)
                cleanup_thread.start()
        return
    
    # Procesamiento normal
    total_in = len(rows)
    
    # Agrupar por (store_id, user_id) y contar transacciones
    store_user_metrics = group_by_store_and_user(rows)
    
    batch_records = []
    for (store_id, user_id), metrics in store_user_metrics.items():
        if metrics['transaction_count'] > 0:
            # Crear un registro único con las métricas de (store, user)
            query4_record = {
                'store_id': store_id,
                'user_id': user_id,
                'transaction_count': metrics['transaction_count']
            }
            batch_records.append(query4_record)
    
    
    batch_header = header.copy() if header else {}
    batch_header["batch_size"] = str(len(batch_records))
    
    out_msg = serialize_message(batch_records, batch_header)
    mq_out.send(out_msg)
    batches_sent += 1

    
    if batches_sent <= 3 or batches_sent % 10000 == 0:
        unique_stores = len(set(store_id for store_id, _ in store_user_metrics.keys()))
        unique_users = len(set(user_id for _, user_id in store_user_metrics.keys()))
        
        print(f"[GroupByQuery4] batches_sent={batches_sent} in={total_in} created={len(store_user_metrics)} stores={unique_stores} users={unique_users}")

if __name__ == "__main__":
    print(f"[GroupByQuery4] Iniciando worker {WORKER_ID}...")
    shutdown_requested = False
    
    def signal_handler(signum, frame):
        global shutdown_requested
        print(f"[GroupByQuery4] Señal {signum} recibida, cerrando...")
        shutdown_requested = True
        mq_in.stop_consuming()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: suscripción al exchange del filtro por año con routing keys específicas
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, INPUT_ROUTING_KEYS)
    
    # Salida: exchange para datos agregados de query 4
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] GroupByQuery4 worker esperando mensajes...")
    print(f"[*] Esperando EOS de {NUM_FILTER_YEAR_WORKERS} workers de filter_year")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        print("\n[GroupByQuery4] Interrupción recibida")
    finally:
        try:
            mq_in.close()
        except:
            pass
        try:
            mq_out.close()
        except:
            pass
        print("[x] GroupByQuery4 worker detenido")
