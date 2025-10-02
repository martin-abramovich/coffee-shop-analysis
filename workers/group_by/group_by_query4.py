import sys
import os
import signal
from collections import defaultdict

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
INPUT_EXCHANGE = "transactions_year"     # exchange del filtro por año
INPUT_ROUTING_KEY = "year"               # routing key del filtro por año
OUTPUT_EXCHANGE = "transactions_query4"  # exchange de salida para query 4
ROUTING_KEY = "query4"                   # routing para topic

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

def on_message(body):
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        # Log compacto al recibir EOS
        print("[GroupByQuery4] EOS recibido, reenviando aguas abajo")
        # Reenviar EOS a workers downstream
        eos_msg = serialize_message([], header)
        mq_out.send(eos_msg)
        print("[GroupByQuery4] EOS reenviado a workers downstream")
        return
    
    # Procesamiento normal
    total_in = len(rows)
    
    # Agrupar por (store_id, user_id) y contar transacciones
    store_user_metrics = group_by_store_and_user(rows)
    
    # OPTIMIZACIÓN: Enviar como BATCH en lugar de mensajes individuales
    batch_records = []
    total_transactions = 0
    
    for (store_id, user_id), metrics in store_user_metrics.items():
        if metrics['transaction_count'] > 0:
            # Crear un registro único con las métricas de (store, user)
            query4_record = {
                'store_id': store_id,
                'user_id': user_id,
                'transaction_count': metrics['transaction_count']
            }
            batch_records.append(query4_record)
            total_transactions += metrics['transaction_count']
    
    # Enviar como UN SOLO BATCH grande
    if batch_records:
        # Agregar información del grupo al header
        batch_header = header.copy() if header else {}
        batch_header["group_by"] = "store_user"
        batch_header["group_size"] = len(batch_records)
        batch_header["metrics_type"] = "query4_aggregated"
        batch_header["batch_type"] = "grouped_metrics"
        
        # Enviar como batch grande
        out_msg = serialize_message(batch_records, batch_header)
        mq_out.send(out_msg)
    
    unique_stores = len(set(store_id for store_id, _ in store_user_metrics.keys()))
    unique_users = len(set(user_id for _, user_id in store_user_metrics.keys()))
    
    print(f"[GroupByQuery4] in={total_in} created={len(store_user_metrics)} sent=1_batch stores={unique_stores} users={unique_users} tx_total={total_transactions}")

if __name__ == "__main__":
    shutdown_requested = False
    
    def signal_handler(signum, frame):
        global shutdown_requested
        print(f"[GroupByQuery4] Señal {signum} recibida, cerrando...")
        shutdown_requested = True
        mq_in.stop_consuming()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: suscripción al exchange del filtro por año
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Salida: exchange para datos agregados de query 4
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] GroupByQuery4 worker esperando mensajes...")
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
