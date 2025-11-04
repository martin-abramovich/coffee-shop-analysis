import sys
import os
import signal
import threading
from collections import defaultdict


sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message


RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
WORKER_ID = os.environ.get('WORKER_ID')

INPUT_EXCHANGE = "transactions_year_query4"  # exchange dedicado para query4
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


batches_sent = 0 

def on_message(body):
    global batches_sent
    header, rows = deserialize_message(body)
    
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
    

    out_msg = serialize_message(batch_records, header)
    group_by_queue.send(out_msg)
    batches_sent += 1

    
    if batches_sent <= 3 or batches_sent % 10000 == 0:
        total_in = len(rows)
        unique_stores = len(set(store_id for store_id, _ in store_user_metrics.keys()))
        unique_users = len(set(user_id for _, user_id in store_user_metrics.keys()))
        
        print(f"[GroupByQuery4] batches_sent={batches_sent} in={total_in} created={len(store_user_metrics)} stores={unique_stores} users={unique_users}")

if __name__ == "__main__":
    print(f"[GroupByQuery4] Iniciando worker {WORKER_ID}...")
    shutdown_event = threading.Event()
    
    def signal_handler(signum, frame):
        print(f"[GroupByQuery4] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
        try:
            year_trans_queue.stop_consuming()
        except Exception as e: 
            print(f"Error al parar el consumo: {e}")
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: suscripción al exchange del filtro por año con routing keys específicas
    year_trans_exchange = MessageMiddlewareExchange(RABBIT_HOST, "transactions_year", ["transactions_year"])
    year_trans_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_year_q4")
    
    # Salida: exchange para datos agregados de query 4
    group_by_queue = MessageMiddlewareQueue(RABBIT_HOST, "group_by_q4")
    
    print("[*] GroupByQuery4 worker esperando mensajes...")
    try:
        year_trans_queue.start_consuming(on_message)
    except KeyboardInterrupt:
        print("\n[GroupByQuery4] Interrupción recibida")
        shutdown_event.set()
    finally:
        for mq in [group_by_queue, year_trans_exchange, year_trans_queue]:
            try:
                mq.delete()
            except Exception as e:
                print(f"Error al eliminar conexión: {e}")
    
        # Cerrar conexiones
        for mq in [group_by_queue, year_trans_exchange, year_trans_queue]:
            try:
                mq.close()
            except Exception as e:
                print(f"Error al cerrar conexión: {e}")
        print("[x] GroupByQuery4 worker detenido")
