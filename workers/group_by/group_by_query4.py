import sys
import os
import signal
import threading
from collections import defaultdict

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
INPUT_EXCHANGE = "transactions_year_query4"  # exchange dedicado para query4
INPUT_ROUTING_KEY = "year"                   # routing key del filtro por año
OUTPUT_EXCHANGE = "transactions_query4"      # exchange de salida para query 4
ROUTING_KEY = "query4"                       # routing para topic

# Número de workers upstream de filter_year (deben coincidir con NUM_FILTER_YEAR_WORKERS del gateway)
NUM_FILTER_YEAR_WORKERS = int(os.environ.get('NUM_FILTER_YEAR_WORKERS', '3'))

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

# Control de EOS - necesitamos recibir EOS de todos los workers de filter_year
eos_count = 0
eos_lock = threading.Lock()

def on_message(body):
    global eos_count
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        with eos_lock:
            eos_count += 1
            print(f"[GroupByQuery4] EOS recibido ({eos_count}/{NUM_FILTER_YEAR_WORKERS})")
            
            # Solo reenviar EOS cuando hayamos recibido de TODOS los workers de filter_year
            if eos_count >= NUM_FILTER_YEAR_WORKERS:
                print(f"[GroupByQuery4] ✅ EOS recibido de TODOS los workers. Reenviando downstream...")
                eos_msg = serialize_message([], header)
                mq_out.send(eos_msg)
                print("[GroupByQuery4] EOS reenviado a workers downstream")
        return
    
    # Procesamiento normal
    total_in = len(rows)
    
    # Agrupar por (store_id, user_id) y contar transacciones
    store_user_metrics = group_by_store_and_user(rows)
    
    # OPTIMIZACIÓN: Enviar de a BATCHES de 100 registros para evitar mensajes gigantes
    BATCH_SIZE = 1000
    batch_records = []
    total_transactions = 0
    batches_sent = 0
    
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
            
            # Enviar cuando alcanzamos el tamaño del batch
            if len(batch_records) >= BATCH_SIZE:
                batch_header = header.copy() if header else {}
                batch_header["group_by"] = "store_user"
                batch_header["batch_size"] = str(len(batch_records))
                batch_header["metrics_type"] = "query4_aggregated"
                batch_header["batch_type"] = "grouped_metrics"
                
                out_msg = serialize_message(batch_records, batch_header)
                mq_out.send(out_msg)
                batches_sent += 1
                batch_records = []  # Limpiar para el siguiente batch
    
    # Enviar batch residual si queda algo
    if batch_records:
        batch_header = header.copy() if header else {}
        batch_header["group_by"] = "store_user"
        batch_header["batch_size"] = str(len(batch_records))
        batch_header["metrics_type"] = "query4_aggregated"
        batch_header["batch_type"] = "grouped_metrics"
        batch_header["is_last_chunk"] = "true"
        
        out_msg = serialize_message(batch_records, batch_header)
        mq_out.send(out_msg)
        batches_sent += 1
    
    unique_stores = len(set(store_id for store_id, _ in store_user_metrics.keys()))
    unique_users = len(set(user_id for _, user_id in store_user_metrics.keys()))
    
    print(f"[GroupByQuery4] in={total_in} created={len(store_user_metrics)} sent={batches_sent}_batches stores={unique_stores} users={unique_users} tx_total={total_transactions}")

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
