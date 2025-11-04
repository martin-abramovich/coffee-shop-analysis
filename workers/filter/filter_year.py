from configparser import Error
import sys
import os
import signal
import threading


# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
WORKER_ID = int(os.environ.get('WORKER_ID'))

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
    header, rows = deserialize_message(body)
    
    if (header.get("is_eos") == "true"): 
        print("SE RECIBIO EOS") 
    
    filtered = filter_by_year(rows)    
    
    out_msg = serialize_message(filtered, header)
    
    filter_trans_exchange.send(out_msg)

def on_trans_item_message(body): 
    header, rows = deserialize_message(body)
        
    filtered = filter_by_year(rows)    
    
    out_msg = serialize_message(filtered, header)
    
    filter_trans_item_queue.send(out_msg)

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

    def signal_handler(signum, frame):
        print(f"[FilterYear Worker {WORKER_ID}] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
        for mq in [trans_queue, trans_item_queue]:
            try:
                mq.stop_consuming()
            except Exception as e:
                print(f"Error al parar el consumo: {e}")

    # Registrar señales
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Instanciar colas/exchanges
    trans_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_raw")
    trans_item_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_items_raw")

    filter_trans_exchange = MessageMiddlewareQueue(RABBIT_HOST, "transactions_year")
    filter_trans_item_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_year")

    print(f"[*] FilterWorkerYear {WORKER_ID} conexiones creadas")

    try:    
        threads = []
        trans_thread  = threading.Thread(target=filter_transaccion, args=(trans_queue,))
        trans_items_thread = threading.Thread(target=filter_transaccion, args=(trans_item_queue,))
        
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
                
        for mq in [trans_queue, trans_item_queue, filter_trans_exchange, filter_trans_item_queue]:
            try:
                mq.delete()
            except Exception as e:
                print(f"Error al eliminar conexión: {e}")
    
        # Cerrar conexiones
        for mq in [trans_queue, trans_item_queue, filter_trans_exchange, filter_trans_item_queue]:
            try:
                mq.close()
            except Exception as e:
                print(f"Error al cerrar conexión: {e}")
            
        print(f"[x] FilterWorkerYear {WORKER_ID} detenido")
