from collections import defaultdict
import threading
from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = "localhost"

# Inputs: múltiples exchanges
INPUT_YEAR_EXCHANGE = "transactions_year"    # para Query 4
INPUT_YEAR_ROUTING_KEY = "year"
INPUT_HOUR_EXCHANGE = "transactions_hour"    # para Query 3  
INPUT_HOUR_ROUTING_KEY = "hour"

# Outputs: diferentes exchanges según el input
OUTPUT_STORE_EXCHANGE = "transactions_store"      # para Query 4 (después de year)
OUTPUT_STORE_HOUR_EXCHANGE = "transactions_store_hour"  # para Query 3 (después de hour)
ROUTING_KEY_STORE = "store"
ROUTING_KEY_STORE_HOUR = "store_hour"

def group_by_store_id(rows):
    """Agrupa las transacciones por store_id."""
    store_groups = defaultdict(list)
    
    for r in rows:
        store_id = r.get("store_id")
        if store_id is not None:
            store_groups[store_id].append(r)
    
    return store_groups

def create_message_handler(output_exchange, routing_key, pipeline_name):
    """Factory function para crear handlers específicos según el pipeline."""
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
                group_header["pipeline"] = pipeline_name
                
                out_msg = serialize_message(store_rows, group_header)
                output_exchange.send(out_msg)
                groups_sent += 1
        
        print(f"[GroupByStore-{pipeline_name}] in={total_in} groups_created={len(store_groups)} groups_sent={groups_sent}")
    
    return on_message

if __name__ == "__main__":
    # Configuración de múltiples inputs y outputs
    
    # Input 1: Para Query 4 (después de filter_year) - Top 3 clientes por sucursal
    mq_in_year = MessageMiddlewareExchange(RABBIT_HOST, INPUT_YEAR_EXCHANGE, [INPUT_YEAR_ROUTING_KEY])
    mq_out_store = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_STORE_EXCHANGE, [ROUTING_KEY_STORE])
    
    # Input 2: Para Query 3 (después de filter_hour) - TPV por semestre por sucursal
    mq_in_hour = MessageMiddlewareExchange(RABBIT_HOST, INPUT_HOUR_EXCHANGE, [INPUT_HOUR_ROUTING_KEY])
    mq_out_store_hour = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_STORE_HOUR_EXCHANGE, [ROUTING_KEY_STORE_HOUR])
    
    # Crear handlers específicos para cada pipeline
    handler_query4 = create_message_handler(mq_out_store, ROUTING_KEY_STORE, "Query4")
    handler_query3 = create_message_handler(mq_out_store_hour, ROUTING_KEY_STORE_HOUR, "Query3")
    
    print("[*] GroupByStore worker esperando mensajes de múltiples exchanges...")
    print("    - Query 4: transactions_year -> transactions_store (Top 3 clientes)")
    print("    - Query 3: transactions_hour -> transactions_store_hour (TPV por semestre)")
    
    try:
        # Iniciar consumo en threads separados
        thread_year = threading.Thread(target=lambda: mq_in_year.start_consuming(handler_query4))
        thread_hour = threading.Thread(target=lambda: mq_in_hour.start_consuming(handler_query3))
        
        thread_year.daemon = True
        thread_hour.daemon = True
        
        thread_year.start()
        thread_hour.start()
        
        # Mantener el programa vivo
        thread_year.join()
        thread_hour.join()
        
    except KeyboardInterrupt:
        print("\n[!] Cerrando worker...")
        mq_in_year.stop_consuming()
        mq_in_hour.stop_consuming()
        mq_in_year.close()
        mq_in_hour.close()
        mq_out_store.close()
        mq_out_store_hour.close()
        print("[x] GroupByStore worker detenido")
