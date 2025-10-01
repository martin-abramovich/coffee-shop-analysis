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
INPUT_QUEUES = ["transactions_raw", "transaction_items_raw"]  # ambas colas del Gateway
OUTPUT_EXCHANGES = {
    "transactions_raw": ["transactions_year"],      # exchange para transactions filtradas
    "transaction_items_raw": ["transaction_items_year"]  # exchange para transaction_items filtradas
}
ROUTING_KEY = "year"                   # clave de ruteo para el fanout

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

def on_message(body, source_queue):
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print(f"[FilterByYear] End of Stream recibido desde {source_queue}. Reenviando...")
        # Reenviar EOS a workers downstream usando los exchanges correctos
        eos_msg = serialize_message([], header)
        output_exchanges = OUTPUT_EXCHANGES[source_queue]
        for exchange_name in output_exchanges:
            mq_outputs[exchange_name].send(eos_msg)
        print(f"[FilterByYear] EOS reenviado a {output_exchanges}")
        return
    
    # Procesamiento normal
    filtered = filter_by_year(rows)
    if filtered:
        out_msg = serialize_message(filtered, header)
        output_exchanges = OUTPUT_EXCHANGES[source_queue]
        for exchange_name in output_exchanges:
            mq_outputs[exchange_name].send(out_msg)
        print(f"[FilterByYear] Procesadas {len(filtered)} registros de {len(rows)} desde {source_queue} → {output_exchanges}")

if __name__ == "__main__":
    # Control de EOS - necesitamos recibir EOS de todas las fuentes
    eos_received = set()
    eos_lock = threading.Lock()
    shutdown_event = threading.Event()
    
    def check_all_eos_received():
        with eos_lock:
            if len(eos_received) == len(INPUT_QUEUES):
                print("[FilterYear] ✅ EOS recibido de todas las fuentes. Terminando...")
                shutdown_event.set()
                return True
        return False
    
    def signal_handler(signum, frame):
        print(f"[FilterYear] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: múltiples colas (transactions y transaction_items)
    mq_connections = []
    for queue_name in INPUT_QUEUES:
        mq_in = MessageMiddlewareQueue(RABBIT_HOST, queue_name)
        mq_connections.append(mq_in)

    # Salida: múltiples exchanges según el tipo de datos
    mq_outputs = {}
    for queue_name, exchange_names in OUTPUT_EXCHANGES.items():
        for exchange_name in exchange_names:
            mq_outputs[exchange_name] = MessageMiddlewareExchange(RABBIT_HOST, exchange_name, [ROUTING_KEY])

    # Modificar on_message para manejar EOS correctamente
    def enhanced_on_message(body, source_queue):
        header, rows = deserialize_message(body)
        
        # Verificar si es mensaje de End of Stream
        if header.get("is_eos") == "true":
            print(f"[FilterYear] 🔚 EOS recibido desde {source_queue}")
            
            with eos_lock:
                eos_received.add(source_queue)
                print(f"[FilterYear] EOS recibido de: {eos_received} (esperando: {set(INPUT_QUEUES)})")
            
            # Reenviar EOS a workers downstream
            eos_msg = serialize_message([], header)
            output_exchanges = OUTPUT_EXCHANGES[source_queue]
            for exchange_name in output_exchanges:
                mq_outputs[exchange_name].send(eos_msg)
            print(f"[FilterYear] EOS reenviado a {output_exchanges}")
            
            # Verificar si hemos recibido EOS de todas las fuentes
            check_all_eos_received()
            return
        
        # Procesamiento normal
        filtered = filter_by_year(rows)
        if filtered:
            out_msg = serialize_message(filtered, header)
            output_exchanges = OUTPUT_EXCHANGES[source_queue]
            for exchange_name in output_exchanges:
                mq_outputs[exchange_name].send(out_msg)
            print(f"[FilterYear] ✅ Procesadas {len(filtered)}/{len(rows)} desde {source_queue} → {output_exchanges}")

    print(f"[*] FilterWorkerYear esperando mensajes de {INPUT_QUEUES}...")
    print(f"[*] Necesita recibir EOS de todas las fuentes para terminar")
    
    try:
        def consume_queue(mq_in, queue_name):
            try:
                print(f"[FilterYear] 🚀 Iniciando consumo de {queue_name}...")
                def on_message_wrapper(body):
                    return enhanced_on_message(body, queue_name)
                mq_in.start_consuming(on_message_wrapper)
            except Exception as e:
                if not shutdown_event.is_set():
                    print(f"[FilterYear] ❌ Error consumiendo {queue_name}: {e}")
        
        threads = []
        for i, mq_in in enumerate(mq_connections):
            thread = threading.Thread(target=consume_queue, args=(mq_in, INPUT_QUEUES[i]))
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # Esperar hasta recibir EOS de todas las fuentes o timeout
        timeout = 300  # 5 minutos timeout
        if shutdown_event.wait(timeout):
            print("[FilterYear] ✅ Terminando por EOS completo o señal")
        else:
            print(f"[FilterYear] ⏰ Timeout después de {timeout}s, terminando...")
            
    except KeyboardInterrupt:
        print("\n[FilterYear] Interrupción recibida")
    finally:
        # Detener consumo
        for mq in mq_connections:
            try:
                mq.stop_consuming()
            except:
                pass
        
        # Cerrar conexiones
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
        print("[x] FilterWorkerYear detenido")
