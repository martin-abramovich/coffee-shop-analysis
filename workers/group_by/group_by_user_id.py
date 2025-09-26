from collections import defaultdict
from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = "localhost"
INPUT_EXCHANGE = "transactions_store"    # exchange del group by store
INPUT_ROUTING_KEY = "store"              # routing key del group by store
OUTPUT_EXCHANGE = "transactions_user"    # exchange de salida para user grouping
ROUTING_KEY = "user"                     # routing para topic

def group_by_user_id(rows):
    """Agrupa las transacciones por user_id."""
    user_groups = defaultdict(list)
    
    for r in rows:
        user_id = r.get("user_id")
        if user_id is not None:
            user_groups[user_id].append(r)
    
    return user_groups

def on_message(body):
    header, rows = deserialize_message(body)
    total_in = len(rows)
    
    # Obtener información del grupo anterior (store)
    previous_group_by = header.get("group_by", "unknown") if header else "unknown"
    previous_group_key = header.get("group_key", "unknown") if header else "unknown"
    
    # Agrupar por user_id
    user_groups = group_by_user_id(rows)
    
    # Enviar cada grupo por separado
    groups_sent = 0
    for user_id, user_rows in user_groups.items():
        if user_rows:
            # Agregar información del grupo al header
            group_header = header.copy() if header else {}
            group_header["previous_group_by"] = previous_group_by
            group_header["previous_group_key"] = previous_group_key
            group_header["group_by"] = "user_id"
            group_header["group_key"] = str(user_id)
            group_header["group_size"] = len(user_rows)
            
            out_msg = serialize_message(user_rows, group_header)
            mq_out.send(out_msg)
            groups_sent += 1
    
    print(f"[GroupByUser] in={total_in} from_store={previous_group_key} users_created={len(user_groups)} groups_sent={groups_sent}")

if __name__ == "__main__":
    # Entrada: suscripción al exchange del group by store
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Salida: exchange para datos agrupados por user
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] GroupByUser worker esperando mensajes...")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        mq_in.stop_consuming()
        mq_in.close()
        mq_out.close()
        print("[x] GroupByUser worker detenido")
