import sys
import os
import signal
import threading
from datetime import datetime

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')

# Número de workers upstream de filter_year
NUM_FILTER_YEAR_WORKERS = int(os.environ.get('NUM_FILTER_YEAR_WORKERS', '3'))

# Número de workers downstream de filter_amount
NUM_FILTER_AMOUNT_WORKERS = int(os.environ.get('NUM_FILTER_AMOUNT_WORKERS', '2'))
NUM_GROUP_BY_QUERY3_WORKERS = int(os.environ.get('NUM_GROUP_BY_QUERY3_WORKERS', '2'))

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

# ID del worker (auto-detectado del hostname o env var)
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

# Exchanges de entrada - cada worker escucha su routing key específica + eos
INPUT_EXCHANGES = {
    "transactions_year": [f"worker_{WORKER_ID}", "eos"],
    "transaction_items_year": [f"worker_{WORKER_ID}", "eos"]
}

# Exchanges de salida - diferenciamos por destino (scalable = round-robin)
OUTPUT_EXCHANGES = {
    "transactions_year": {
        "scalable": [
            ("transactions_hour_amount", NUM_FILTER_AMOUNT_WORKERS),  # Para filter_amount
            ("transactions_hour", NUM_GROUP_BY_QUERY3_WORKERS)  # Para group_by_query3
        ]
    },
    "transaction_items_year": {
        "scalable": [
            ("transaction_items_hour", NUM_GROUP_BY_QUERY3_WORKERS)  # Para group_by_query3
        ]
    }
}

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

# Control de EOS - necesitamos recibir EOS de cada worker de filter_year por cada exchange
eos_count_per_exchange = {}
eos_lock = threading.Lock()

def on_message(body, source_exchange):
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        with eos_lock:
            # Incrementar contador de EOS para este exchange
            if source_exchange not in eos_count_per_exchange:
                eos_count_per_exchange[source_exchange] = 0
            eos_count_per_exchange[source_exchange] += 1
            
            total_eos = sum(eos_count_per_exchange.values())
            expected_eos = NUM_FILTER_YEAR_WORKERS * len(INPUT_EXCHANGES)
            
            print(f"[FilterHour] 🔚 EOS recibido desde {source_exchange} ({eos_count_per_exchange[source_exchange]}/{NUM_FILTER_YEAR_WORKERS})")
            print(f"[FilterHour] Total EOS: {total_eos}/{expected_eos}")
            
            # Solo reenviar EOS cuando hayamos recibido de TODOS los workers de TODOS los exchanges
            all_complete = all(
                eos_count_per_exchange.get(ex, 0) >= NUM_FILTER_YEAR_WORKERS 
                for ex in INPUT_EXCHANGES.keys()
            )
            
            if all_complete:
                print(f"[FilterHour Worker {WORKER_ID}] ✅ EOS recibido de TODOS los workers. Reenviando downstream...")
                # Reenviar EOS a TODOS los outputs (broadcast)
                eos_msg = serialize_message([], header)
                
                for input_exchange, destinations in OUTPUT_EXCHANGES.items():
                    # EOS a exchanges escalables (broadcast)
                    for exchange_name, _ in destinations["scalable"]:
                        mq_outputs_scalable[exchange_name].send_eos(eos_msg)
                        print(f"[FilterHour Worker {WORKER_ID}] EOS broadcast a {exchange_name}")
                
                shutdown_event.set()
        return
    
    # Procesamiento normal
    filtered = filter_by_hour(rows)
    
    if filtered:
        out_msg = serialize_message(filtered, header)
        destinations = OUTPUT_EXCHANGES[source_exchange]
        
        # Enviar a exchanges escalables (round-robin)
        for exchange_name, _ in destinations["scalable"]:
            mq_outputs_scalable[exchange_name].send(out_msg)

if __name__ == "__main__":
    print(f"[FilterHour] Iniciando worker {WORKER_ID}...")
    shutdown_requested = False
    shutdown_event = threading.Event()
    mq_connections = []
    
    def signal_handler(signum, frame):
        global shutdown_requested
        print(f"[FilterHour] Señal {signum} recibida, cerrando...")
        shutdown_requested = True
        for mq in mq_connections:
            try:
                mq.stop_consuming()
            except:
                pass
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: múltiples exchanges con routing keys específicas
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

    print(f"[*] FilterWorkerHour esperando mensajes de {INPUT_EXCHANGES}...")
    try:
        # Procesar cada exchange en un hilo separado
        def consume_exchange(mq_in, exchange_name):
            try:
                print(f"[FilterHour] Iniciando consumo de {exchange_name}...")
                # Crear una función wrapper que pase el source_exchange
                def on_message_wrapper(body):
                    return on_message(body, exchange_name)
                mq_in.start_consuming(on_message_wrapper)
            except Exception as e:
                print(f"[FilterHour] Error consumiendo {exchange_name}: {e}")
        
        threads = []
        for mq_in, exchange_name in mq_connections:
            thread = threading.Thread(target=consume_exchange, args=(mq_in, exchange_name))
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # Esperar shutdown event
        shutdown_event.wait()
            
    except KeyboardInterrupt:
        print("\n[FilterHour] Interrupción recibida")
    finally:
        try:
            for mq_in in mq_connections:
                mq_in.close()
        except:
            pass
        try:
            for mq_out in mq_outputs.values():
                mq_out.close()
        except:
            pass
        print("[x] FilterWorkerHour detenido")
