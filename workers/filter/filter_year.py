from configparser import Error
import sys
import os
import signal
import threading


# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message
from common.healthcheck import start_healthcheck_server

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
WORKER_ID = int(os.environ.get('WORKER_ID'))


# INPUT_EXCHANGES = {
#     "transactions_raw": [f"worker_{WORKER_ID}", "eos"],  # routing keys: worker específico + eos
#     "transaction_items_raw": [f"worker_{WORKER_ID}", "eos"]
# }

# OUTPUT_EXCHANGES = {
#     "transactions_raw": {
#         "scalable": [
#             ("transactions_year", NUM_FILTER_HOUR_WORKERS),  # Para filter_hour
#             ("transactions_year_query4", NUM_GROUP_BY_QUERY4_WORKERS)  # Para group_by_query4
#         ],
#     },
#     "transaction_items_raw": {
#         "scalable": [
#             ("transaction_items_year_query2", NUM_GROUP_BY_QUERY2_WORKERS)  # Para group_by_query2
#         ],
#     }
# }

def filter_by_year(rows):
    """Mantiene filas con created_at entre 2024 y 2025 (inclusive)."""
    filtered = []
    for r in rows:
        created = r.get("created_at")
        if not created:
            continue
        try:
            # Soporta formatos ISO (del gateway) y 'YYYY-MM-DD HH:MM:SS'
            year = int(created[:4])
            if 2024 <= year <= 2025:
                filtered.append(r)
        except Exception:
            continue
    return filtered


def on_trans_message(body): 
    try:
        header, rows = deserialize_message(body)

        if (header.get("is_eos") == "true"): 
            print("SE RECIBIO EOS") 

        filtered = filter_by_year(rows)    

        out_msg = serialize_message(filtered, header)

        filter_trans_exchange.send(out_msg)
    except Exception as e:
        print(f"[FilterYear] Error al procesar mensaje: {e}")
        
def on_trans_item_message(body): 
    try: 
        header, rows = deserialize_message(body)

        if (header.get("is_eos") == "true"): 
            print("SE RECIBIO EOS") 

        filtered = filter_by_year(rows)    
        
        out_msg = serialize_message(filtered, header)
        filter_trans_item_queue.send(out_msg)
    except Exception as e:
        print(f"[FilterYear] Error al procesar mensaje de transaction_items: {e}")
        
def filter_transaccion(trans_queue: MessageMiddlewareQueue):
    try:
        trans_queue.start_consuming(on_trans_message)
    except KeyboardInterrupt: 
        return
    except Exception as e:
        print(f"[FilterYear] Error en trans_queue: {e}")


def filter_transaccion_item(trans_items_queue: MessageMiddlewareQueue):
    try:
        trans_items_queue.start_consuming(on_trans_item_message)
    except Exception as e:
        print(f"[FilterYear] Error en trans_items_queue: {e}")

if __name__ == "__main__":
    print(f"[FilterYear] Iniciando worker {WORKER_ID}...")

    shutdown_event = threading.Event()
    
    # Iniciar servidor de healthcheck UDP
    healthcheck_port = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
    start_healthcheck_server(port=healthcheck_port, node_name=f"filter_year_{WORKER_ID}", shutdown_event=shutdown_event)
    print(f"[FilterYear Worker {WORKER_ID}] Healthcheck server iniciado en puerto UDP {healthcheck_port}")

    def signal_handler(signum, frame):
        print(f"[FilterYear Worker {WORKER_ID}] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
        for mq in [trans_queue, trans_item_queue]:
            try:
                mq.stop_consuming()
            except Exception as e:
                print(f"Error al parar el consumo: {e}")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try: 
        trans_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_raw")
        trans_item_queue = MessageMiddlewareQueue(RABBIT_HOST, "transaction_items_raw")

        filter_trans_exchange = MessageMiddlewareExchange(RABBIT_HOST, "transactions_year", ["transactions_year"])
        filter_trans_queue_q1 = MessageMiddlewareQueue(RABBIT_HOST, "transactions_year_q1")
        filter_trans_queue_q1.bind("transactions_year", "transactions_year")
        filter_trans_queue_q4 = MessageMiddlewareQueue(RABBIT_HOST, "transactions_year_q4")
        filter_trans_queue_q4.bind("transactions_year", "transactions_year")

        filter_trans_item_queue = MessageMiddlewareQueue(RABBIT_HOST, "transaction_items_year")
    except Exception as e:
        print(f"[FilterYear Worker {WORKER_ID}] Error al crear conexiones: {e}")
        sys.exit(1)
        
    print(f"[*] FilterWorkerYear {WORKER_ID} conexiones creadas")

    try:    
        threads = []
        trans_thread  = threading.Thread(target=filter_transaccion, args=(trans_queue,))
        trans_items_thread = threading.Thread(target=filter_transaccion_item, args=(trans_item_queue,))
        
        trans_thread.start()
        trans_items_thread.start()
        
        
        print(f"[FilterYear Worker {WORKER_ID}] Worker iniciado, esperando mensajes de múltiples sesiones...")
        
        while not shutdown_event.is_set():
            trans_thread.join(timeout=1)
            trans_items_thread.join(timeout=1)
            if not trans_thread.is_alive() and not trans_items_thread.is_alive():
                break
            
    except KeyboardInterrupt:
        print(f"\n[FilterYear Worker {WORKER_ID}] Interrupción recibida")
        shutdown_event.set()
    finally:
        # Detener consumo
        for mq in [trans_queue, trans_item_queue]:
            try:
                mq.stop_consuming()
            except Exception as e:
                print(f"Error al parar el consumo: {e}")
                
        # Cerrar conexiones
        for mq in [trans_queue, trans_item_queue, filter_trans_exchange, filter_trans_item_queue, filter_trans_queue_q1, filter_trans_queue_q4]:
            try:
                mq.close()
            except Exception as e:
                print(f"Error al cerrar conexión: {e}")
            
        print(f"[x] FilterWorkerYear {WORKER_ID} detenido")
