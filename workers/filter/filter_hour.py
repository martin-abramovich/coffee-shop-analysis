import sys
import os
import signal
import threading
import time
from datetime import datetime

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
WORKER_ID = os.environ.get('WORKER_ID')

# Exchanges de entrada - cada worker escucha su routing key específica + eos
# INPUT_EXCHANGES = {
#     "transactions_year": [f"worker_{WORKER_ID}", "eos"],
# }

# # Exchanges de salida - diferenciamos por destino (scalable = round-robin)
# OUTPUT_EXCHANGES = {
#     "transactions_year": {
#         "scalable": [
#             ("transactions_hour_amount", NUM_FILTER_AMOUNT_WORKERS),  # Para filter_amount
#             ("transactions_hour", NUM_GROUP_BY_QUERY3_WORKERS)  # Para group_by_query3
#         ]
#     },
# }

# Ventana horaria (inclusive)
START_HOUR = 6   # 06:00
END_HOUR = 23    # 23:00

def parse_hour(created_at: str) -> int:
    """Extrae la hora de created_at. Soporta ISO y 'YYYY-MM-DD HH:MM:SS'."""
    if not created_at:
        raise ValueError("created_at vacío")
    try:
        return int(created_at[11:13])
    except Exception:
        # Último intento parseando
        try:
            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            return dt.hour
        except Exception:
            dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            return dt.hour

def filter_by_hour(rows):
    """Mantiene filas cuyo created_at esté entre START_HOUR y END_HOUR (inclusive)."""
    filtered = []
    for r in rows:
        created = r.get("created_at")
        if not created:
            continue
        try:
            h = parse_hour(created)
            if START_HOUR <= h <= END_HOUR:
                filtered.append(r)
        except Exception:
            continue
    return filtered


def on_message(body):
    header, rows = deserialize_message(body)
    
    if (header.get("is_eos") == "true"):
        print("SE RECIBIO EOS con batch_id:", header.get("batch_id"))
        
    filtered = filter_by_hour(rows)
    
    out_msg = serialize_message(filtered, header)

    hour_trans_exchange.send(out_msg)

if __name__ == "__main__":
    print(f"[FilterHour] Iniciando worker {WORKER_ID}...")
    shutdown_event = threading.Event()
    
    def signal_handler(signum, frame):
        print(f"[FilterHour] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
        try:
            year_trans_queue.stop_consuming()
        except Exception as e: 
            print(f"Error al parar el consumo: {e}")
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada
    year_trans_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_year")
    
    # Saida
    hour_trans_exchange = MessageMiddlewareExchange(RABBIT_HOST, "transactions_hour", ["transactions_hour"])
    hour_trans_queue_q1 = MessageMiddlewareQueue(RABBIT_HOST, "transactions_hour_q1")
    hour_trans_queue_q1.bind("transactions_hour", "transactions_hour")
    hour_trans_queue_q3 = MessageMiddlewareQueue(RABBIT_HOST, "transactions_hour_q3")
    hour_trans_queue_q3.bind("transactions_hour", "transactions_hour")

    print(f"[*] FilterWorkerHour conexiones creadas ...")
    try:
        print(f"[FilterWorkerHour {WORKER_ID}] Worker iniciado, esperando mensajes de múltiples sesiones...")
        year_trans_queue.start_consuming(on_message)
            
    except KeyboardInterrupt:
        print("\n[FilterHour] Interrupción recibida")
        shutdown_event.set()
    finally:    
        for mq in [year_trans_queue, hour_trans_exchange, hour_trans_queue_q1, hour_trans_queue_q3]:
            try:
                mq.delete()
            except Exception as e:
                print(f"Error al eliminar conexión: {e}")
    
        # Cerrar conexiones
        for mq in [year_trans_queue, hour_trans_exchange, hour_trans_queue_q1, hour_trans_queue_q3]:
            try:
                mq.close()
            except Exception as e:
                print(f"Error al cerrar conexión: {e}")
                
        print("[x] FilterWorkerHour detenido")
