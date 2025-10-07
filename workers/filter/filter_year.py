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
NUM_FILTER_HOUR_WORKERS = int(os.environ.get('NUM_FILTER_HOUR_WORKERS', '2'))
NUM_GROUP_BY_QUERY2_WORKERS = int(os.environ.get('NUM_GROUP_BY_QUERY2_WORKERS', '2'))
NUM_GROUP_BY_QUERY4_WORKERS = int(os.environ.get('NUM_GROUP_BY_QUERY4_WORKERS', '2'))

# ID del worker (0, 1, 2, ...) - se obtiene del nombre del contenedor o env var
def get_worker_id():
    worker_id_env = os.environ.get('WORKER_ID')
    if worker_id_env is not None:
        return int(worker_id_env)
    
    import socket, re
    hostname = socket.gethostname()
    match = re.search(r'[-_](\d+)$', hostname)
    if match:
        return int(match.group(1)) - 1
    return 0

WORKER_ID = get_worker_id()

# Wrapper para round-robin
class RoundRobinExchange:
    def __init__(self, exchange, num_workers):
        self.exchange = exchange
        self.num_workers = num_workers
        self.current = 0
    
    def send(self, msg):
        routing_key = f"worker_{self.current}"
        self.exchange.channel.basic_publish(
            exchange=self.exchange.exchange_name,
            routing_key=routing_key,
            body=msg
        )
        self.current = (self.current + 1) % self.num_workers
    
    def send_eos(self, msg):
        # Broadcast EOS a todos
        self.exchange.channel.basic_publish(
            exchange=self.exchange.exchange_name,
            routing_key="eos",
            body=msg
        )

# Exchanges de entrada (ahora son exchanges, no queues)
INPUT_EXCHANGES = {
    "transactions_raw": [f"worker_{WORKER_ID}", "eos"],  # routing keys: worker específico + eos
    "transaction_items_raw": [f"worker_{WORKER_ID}", "eos"]
}

# Exchanges de salida - diferenciamos por destino (scalable = round-robin)
OUTPUT_EXCHANGES = {
    "transactions_raw": {
        "scalable": [
            ("transactions_year", NUM_FILTER_HOUR_WORKERS),  # Para filter_hour
            ("transactions_year_query4", NUM_GROUP_BY_QUERY4_WORKERS)  # Para group_by_query4
        ],
        "broadcast": []  # Ya no hay broadcast
    },
    "transaction_items_raw": {
        "scalable": [
            ("transaction_items_year", NUM_FILTER_HOUR_WORKERS),  # Para filter_hour
            ("transaction_items_year_query2", NUM_GROUP_BY_QUERY2_WORKERS)  # Para group_by_query2
        ],
        "broadcast": []  # Ya no hay broadcast
    }
}

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

# Estadísticas globales para logging eficiente
stats = {"processed": 0, "filtered": 0, "batches": 0}

def on_message(body, source_queue):
    header, rows = deserialize_message(body)
    session_id = header.get("session_id", "unknown")
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print(f"[FilterYear] 🔚 EOS desde {source_queue} (sesión {session_id}). Stats: {stats['batches']} batches, {stats['processed']} in, {stats['filtered']} out")
        # Reenviar EOS a workers downstream usando los exchanges correctos
        eos_msg = serialize_message([], header)
        output_exchanges = OUTPUT_EXCHANGES[source_queue]
        for exchange_name in output_exchanges:
            mq_outputs[exchange_name].send(eos_msg)
        return
    
    # Procesamiento normal
    stats["batches"] += 1
    stats["processed"] += len(rows)
    filtered = filter_by_year(rows)
    stats["filtered"] += len(filtered)
    
    if filtered:
        out_msg = serialize_message(filtered, header)
        output_exchanges = OUTPUT_EXCHANGES[source_queue]
        for exchange_name in output_exchanges:
            mq_outputs[exchange_name].send(out_msg)
    
    # Log solo cada 1000 batches
    if stats["batches"] % 1000 == 0:
        print(f"[FilterYear] {stats['batches']} batches | {stats['processed']} in | {stats['filtered']} out")

if __name__ == "__main__":
    print(f"[FilterYear] Iniciando worker {WORKER_ID}...")
    
    # Control de EOS por sesión - necesitamos recibir EOS de todas las fuentes para cada sesión
    eos_received = {}  # {session_id: {source: True}}
    eos_lock = threading.Lock()
    shutdown_event = threading.Event()
    
    def check_session_eos_received(session_id, source):
        with eos_lock:
            if session_id not in eos_received:
                eos_received[session_id] = set()
            eos_received[session_id].add(source)
            
            # Verificar si esta sesión completó todas las fuentes
            if len(eos_received[session_id]) == len(INPUT_EXCHANGES):
                print(f"[FilterYear Worker {WORKER_ID}] ✅ EOS recibido de todas las fuentes para sesión {session_id}")
                return True
        return False
    
    def cleanup_completed_session(session_id):
        """Limpia los datos de una sesión completada"""
        with eos_lock:
            if session_id in eos_received:
                del eos_received[session_id]
                print(f"[FilterYear Worker {WORKER_ID}] Sesión {session_id} limpiada")
    
    def signal_handler(signum, frame):
        print(f"[FilterYear Worker {WORKER_ID}] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: múltiples exchanges (transactions y transaction_items)
    # Cada worker escucha su routing key específica + "eos" para broadcast
    mq_connections = []
    for exchange_name, route_keys in INPUT_EXCHANGES.items():
        mq_in = MessageMiddlewareExchange(RABBIT_HOST, exchange_name, route_keys)
        mq_connections.append((mq_in, exchange_name))

    # Salida: crear exchanges escalables (round-robin)
    mq_outputs_scalable = {}
    
    for input_exchange, destinations in OUTPUT_EXCHANGES.items():
        # Exchanges escalables (round-robin)
        for exchange_name, num_workers in destinations["scalable"]:
            route_keys = [f"worker_{i}" for i in range(num_workers)] + ["eos"]
            raw_exchange = MessageMiddlewareExchange(RABBIT_HOST, exchange_name, route_keys)
            mq_outputs_scalable[exchange_name] = RoundRobinExchange(raw_exchange, num_workers)

    # Modificar on_message para manejar EOS correctamente por sesión
    def enhanced_on_message(body, source_exchange):
        header, rows = deserialize_message(body)
        session_id = header.get("session_id", "unknown")
        
        # Verificar si es mensaje de End of Stream
        if header.get("is_eos") == "true":
            print(f"[FilterYear Worker {WORKER_ID}] 🔚 EOS recibido desde {source_exchange} (sesión {session_id})")
            
            # Verificar si esta sesión completó todas las fuentes
            session_complete = check_session_eos_received(session_id, source_exchange)
            
            # Reenviar EOS a workers downstream (broadcast a todos)
            eos_msg = serialize_message([], header)
            destinations = OUTPUT_EXCHANGES[source_exchange]
            
            # EOS a exchanges escalables (broadcast)
            for exchange_name, _ in destinations["scalable"]:
                mq_outputs_scalable[exchange_name].send_eos(eos_msg)
            
            print(f"[FilterYear Worker {WORKER_ID}] EOS enviado para sesión {session_id}")
            
            # Si esta sesión completó todas las fuentes, limpiar después de un delay
            if session_complete:
                # Programar limpieza de la sesión después de un delay
                def delayed_cleanup():
                    time.sleep(10)  # Esperar 10 segundos antes de limpiar
                    cleanup_completed_session(session_id)
                
                cleanup_thread = threading.Thread(target=delayed_cleanup, daemon=True)
                cleanup_thread.start()
            
            # El worker continúa esperando más sesiones - NO termina
            return
        
        # Procesamiento normal
        filtered = filter_by_year(rows)
        if filtered:
            out_msg = serialize_message(filtered, header)
            destinations = OUTPUT_EXCHANGES[source_exchange]
            
            # Enviar a exchanges escalables (round-robin)
            for exchange_name, _ in destinations["scalable"]:
                mq_outputs_scalable[exchange_name].send(out_msg)

    print(f"[*] FilterWorkerYear {WORKER_ID} esperando mensajes de {list(INPUT_EXCHANGES.keys())}...")
    print(f"[*] Routing keys: worker_{WORKER_ID} + eos")
    print(f"[*] Necesita recibir EOS de todas las fuentes para terminar")
    
    try:
        def consume_exchange(mq_in, exchange_name):
            try:
                print(f"[FilterYear Worker {WORKER_ID}] 🚀 Iniciando consumo de {exchange_name}...")
                def on_message_wrapper(body):
                    return enhanced_on_message(body, exchange_name)
                mq_in.start_consuming(on_message_wrapper)
            except Exception as e:
                if not shutdown_event.is_set():
                    print(f"[FilterYear Worker {WORKER_ID}] ❌ Error consumiendo {exchange_name}: {e}")
        
        threads = []
        for mq_in, exchange_name in mq_connections:
            thread = threading.Thread(target=consume_exchange, args=(mq_in, exchange_name))
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # Esperar indefinidamente - el worker NO termina después de EOS
        # Solo termina por señal externa (SIGTERM, SIGINT)
        print(f"[FilterYear Worker {WORKER_ID}] ✅ Worker iniciado, esperando mensajes de múltiples sesiones...")
        print(f"[FilterYear Worker {WORKER_ID}] 💡 El worker continuará procesando múltiples clientes")
        
        # Loop principal - solo termina por señal
        while not shutdown_event.is_set():
            time.sleep(1)
        
        print(f"[FilterYear Worker {WORKER_ID}] ✅ Terminando por señal externa")
            
    except KeyboardInterrupt:
        print(f"\n[FilterYear Worker {WORKER_ID}] Interrupción recibida")
    finally:
        # Detener consumo
        for mq, _ in mq_connections:
            try:
                mq.stop_consuming()
            except:
                pass
        
        # Cerrar conexiones
        try:
            for mq_in, _ in mq_connections:
                mq_in.close()
        except:
            pass
        try:
            for mq_out in mq_outputs.values():
                mq_out.close()
        except:
            pass
        print(f"[x] FilterWorkerYear {WORKER_ID} detenido")
