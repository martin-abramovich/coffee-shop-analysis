import sys
import os
import signal
import threading
import time
import traceback

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message
from common.healthcheck import start_healthcheck_server

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
WORKER_ID = os.environ.get('WORKER_ID')

# INPUT_EXCHANGES = {"transactions_hour_amount": [f"worker_{WORKER_ID}", "eos"]}  # routing keys específicas
# OUTPUT_EXCHANGES = {
#     "transactions_hour_amount": ["transactions_amount"]      # exchange para transactions filtradas por amount
# }

THRESHOLD = 75.0

def filter_by_amount(rows, threshold: float):
    """Mantiene filas con final_amount >= threshold."""
    filtered = []
    for r in rows:
        try:
            fa = r.get("final_amount")
            if fa is None or fa == "":
                continue
            # final_amount puede venir como string; convertir con float
            if isinstance(fa, str):
                fa = float(fa)
                
            if fa >= threshold:
                filtered.append({
                    "transaction_id": r.get("transaction_id"),
                    "final_amount": fa
                })
                
        except Exception:
            continue
        
    return filtered


def on_message(body):
    try:
        header, rows = deserialize_message(body)
        
        if header.get("is_eos") == "true": 
            print("SE RECIBIO EOS")
            
        filtered = filter_by_amount(rows, THRESHOLD)
        
        out_msg = serialize_message(filtered, header)
        
        amount_trans_queue.send(out_msg)
    except Exception as e: 
        print(f"[FilterAmount] Error procesando el mensaje de transactions: {e}")
        print(traceback.format_exc())
        
if __name__ == "__main__":
    print(f"[FilterAmount] Iniciando worker {WORKER_ID}...")
    shutdown_event = threading.Event()
    
    # Iniciar servidor de healthcheck UDP
    healthcheck_port = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
    start_healthcheck_server(port=healthcheck_port, node_name=f"filter_amount_{WORKER_ID}", shutdown_event=shutdown_event)
    print(f"[FilterAmount Worker {WORKER_ID}] Healthcheck server iniciado en puerto UDP {healthcheck_port}")

    
    def signal_handler(signum, frame):
        print(f"[FilterAmount] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
        try:
            hour_trans_queue.stop_consuming()
        except Exception as e: 
            print(f"Error al parar el consumo: {e}")
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    hour_trans_exchange = MessageMiddlewareExchange(RABBIT_HOST, "transactions_hour", ["transactions_hour"])
    hour_trans_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_hour_q1")
    hour_trans_queue.bind("transactions_hour", "transactions_hour")
    
    amount_trans_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_amount")
   
    print(f"[*] FilterWorkerAmount conexiones creadas ...")
    try:
        print(f"[FilterAmount Worker {WORKER_ID}] Worker iniciado, esperando mensajes de múltiples sesiones...")
        hour_trans_queue.start_consuming(on_message)
                    
    except KeyboardInterrupt:
        print("\n[FilterAmount] Interrupción recibida")
    finally:
        try:
            hour_trans_queue.stop_consuming()
        except Exception as e: 
            print(f"Error al parar el consumo: {e}")
        
        # Cerrar conexiones
        for mq in [amount_trans_queue, hour_trans_queue, hour_trans_exchange]:
            try:
                mq.close()
            except Exception as e:
                print(f"Error al cerrar conexión: {e}")
                
        print("[x] FilterWorkerAmount detenido")
