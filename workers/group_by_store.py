from collections import defaultdict
from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = "localhost"
INPUT_EXCHANGE = "transactions_year"     # exchange del filtro por año
INPUT_ROUTING_KEY = "year"               # routing key del filtro por año
OUTPUT_EXCHANGE = "transactions_store"   # exchange de salida para store grouping
ROUTING_KEY = "store"                    # routing para topic

def group_by_store_id(rows):
    """Agrupa las transacciones por store_id."""
    store_groups = defaultdict(list)
    
    for r in rows:
        store_id = r.get("store_id")
        if store_id is not None:
            store_groups[store_id].append(r)
    
    return store_groups

def on_message(body):
    header, rows = deserialize_message(body)
    total_in = len(rows)
    
    # Agrupar por store_id
    store_groups = group_by_store_id(rows)
    
    # Enviar cada grupo por separado
    groups_sent = 0
    for store_id, store_rows in store_groups.items():
        if store_rows:
            # Agregar información del grupo al header
            group_header = header.copy() if header else {}
            group_header["group_by"] = "store_id"
            group_header["group_key"] = str(store_id)
            group_header["group_size"] = len(store_rows)
            
            out_msg = serialize_message(store_rows, group_header)
            mq_out.send(out_msg)
            groups_sent += 1
    
    print(f"[GroupByStore] in={total_in} groups_created={len(store_groups)} groups_sent={groups_sent}")

if __name__ == "__main__":
    # Entrada: suscripción al exchange del filtro por año
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Salida: exchange para datos agrupados por store
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] GroupByStore worker esperando mensajes...")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        mq_in.stop_consuming()
        mq_in.close()
        mq_out.close()
        print("[x] GroupByStore worker detenido")
