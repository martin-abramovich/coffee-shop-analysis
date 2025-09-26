from collections import defaultdict
from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = "localhost"
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
        print("[GroupByQuery4] End of Stream recibido. Reenviando...")
        # Reenviar EOS a workers downstream
        eos_msg = serialize_message([], header)
        mq_out.send(eos_msg)
        print("[GroupByQuery4] EOS reenviado a workers downstream")
        return
    
    # Procesamiento normal
    total_in = len(rows)
    
    # Agrupar por (store_id, user_id) y contar transacciones
    store_user_metrics = group_by_store_and_user(rows)
    
    # Enviar cada combinación (store, user) por separado
    groups_sent = 0
    total_transactions = 0
    
    for (store_id, user_id), metrics in store_user_metrics.items():
        if metrics['transaction_count'] > 0:
            # Crear un registro único con las métricas de (store, user)
            query4_record = {
                'store_id': store_id,
                'user_id': user_id,
                'transaction_count': metrics['transaction_count']
            }
            
            # Agregar información del grupo al header
            group_header = header.copy() if header else {}
            group_header["group_by"] = "store_user"
            group_header["group_key"] = f"{store_id}|{user_id}"
            group_header["store_id"] = store_id
            group_header["user_id"] = user_id
            group_header["group_size"] = 1
            group_header["metrics_type"] = "query4_aggregated"
            
            # Enviar como lista con un solo elemento
            out_msg = serialize_message([query4_record], group_header)
            mq_out.send(out_msg)
            groups_sent += 1
            
            # Acumular total para logging
            total_transactions += metrics['transaction_count']
    
    unique_stores = len(set(store_id for store_id, _ in store_user_metrics.keys()))
    unique_users = len(set(user_id for _, user_id in store_user_metrics.keys()))
    
    print(f"[GroupByQuery4] in={total_in} combinations_created={len(store_user_metrics)} groups_sent={groups_sent}")
    print(f"[GroupByQuery4] unique_stores={unique_stores} unique_users={unique_users} total_transactions={total_transactions}")

if __name__ == "__main__":
    # Entrada: suscripción al exchange del filtro por año
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Salida: exchange para datos agregados de query 4
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] GroupByQuery4 worker esperando mensajes...")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        mq_in.stop_consuming()
        mq_in.close()
        mq_out.close()
        print("[x] GroupByQuery4 worker detenido")
