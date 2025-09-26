from collections import defaultdict
from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = "localhost"
INPUT_EXCHANGE = "transactions_month"    # exchange del group by month
INPUT_ROUTING_KEY = "month"              # routing key del group by month
OUTPUT_EXCHANGE = "transactions_item"    # exchange de salida para item grouping
ROUTING_KEY = "item"                     # routing para topic

def group_by_item_name(rows):
    """Agrupa las transacciones por item_name."""
    item_groups = defaultdict(list)
    
    for r in rows:
        item_name = r.get("item_name")
        if item_name is not None and item_name.strip():  # Verificar que no esté vacío
            # Normalizar el nombre del item (quitar espacios extra)
            normalized_name = item_name.strip()
            item_groups[normalized_name].append(r)
    
    return item_groups

def on_message(body):
    header, rows = deserialize_message(body)
    total_in = len(rows)
    
    # Obtener información del grupo anterior (month)
    previous_group_by = header.get("group_by", "unknown") if header else "unknown"
    previous_group_key = header.get("group_key", "unknown") if header else "unknown"
    
    # Agrupar por item_name
    item_groups = group_by_item_name(rows)
    
    # Enviar cada grupo por separado
    groups_sent = 0
    for item_name, item_rows in item_groups.items():
        if item_rows:
            # Agregar información del grupo al header
            group_header = header.copy() if header else {}
            group_header["previous_group_by"] = previous_group_by
            group_header["previous_group_key"] = previous_group_key
            group_header["group_by"] = "item_name"
            group_header["group_key"] = str(item_name)
            group_header["group_size"] = len(item_rows)
            
            out_msg = serialize_message(item_rows, group_header)
            mq_out.send(out_msg)
            groups_sent += 1
    
    print(f"[GroupByItem] in={total_in} from_month={previous_group_key} items_created={len(item_groups)} groups_sent={groups_sent}")

if __name__ == "__main__":
    # Entrada: suscripción al exchange del group by month
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Salida: exchange para datos agrupados por item
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] GroupByItem worker esperando mensajes...")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        mq_in.stop_consuming()
        mq_in.close()
        mq_out.close()
        print("[x] GroupByItem worker detenido")
