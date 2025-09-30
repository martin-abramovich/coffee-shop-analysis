import sys
import os
import signal
from datetime import datetime

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
INPUT_EXCHANGES = ["transactions_year", "transaction_items_year"]  # exchanges del filtro por año
INPUT_ROUTING_KEY = "year"               # routing key del filtro por año
OUTPUT_EXCHANGES = {
    "transactions_year": ["transactions_hour"],      # exchange para transactions filtradas por hora
    "transaction_items_year": ["transaction_items_hour"]  # exchange para transaction_items filtradas por hora
}
ROUTING_KEY = "hour"                     # routing para topic

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

def on_message(body, source_exchange):
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print(f"[FilterByHour] End of Stream recibido desde {source_exchange}. Reenviando...")
        # Reenviar EOS a workers downstream usando los exchanges correctos
        eos_msg = serialize_message([], header)
        output_exchanges = OUTPUT_EXCHANGES[source_exchange]
        for exchange_name in output_exchanges:
            mq_outputs[exchange_name].send(eos_msg)
        print(f"[FilterByHour] EOS reenviado a {output_exchanges}")
        return
    
    # Procesamiento normal
    total_in = len(rows)
    filtered = filter_by_hour(rows)
    if filtered:
        out_msg = serialize_message(filtered, header)
        output_exchanges = OUTPUT_EXCHANGES[source_exchange]
        for exchange_name in output_exchanges:
            mq_outputs[exchange_name].send(out_msg)
    kept = len(filtered)
    dropped = total_in - kept
    print(f"[FilterHour] in={total_in} kept={kept} dropped={dropped} window=[{START_HOUR}-{END_HOUR}] desde {source_exchange}")

if __name__ == "__main__":
    shutdown_requested = False
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
    
    # Entrada: múltiples exchanges (transactions_year y transaction_items_year)
    mq_connections = []
    for exchange_name in INPUT_EXCHANGES:
        mq_in = MessageMiddlewareExchange(RABBIT_HOST, exchange_name, [INPUT_ROUTING_KEY])
        mq_connections.append(mq_in)

    # Salida: múltiples exchanges según el tipo de datos
    mq_outputs = {}
    for input_exchange, output_exchanges in OUTPUT_EXCHANGES.items():
        for exchange_name in output_exchanges:
            mq_outputs[exchange_name] = MessageMiddlewareExchange(RABBIT_HOST, exchange_name, [ROUTING_KEY])

    print(f"[*] FilterWorkerHour esperando mensajes de {INPUT_EXCHANGES}...")
    try:
        # Procesar cada exchange en un hilo separado
        import threading
        
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
        for i, mq_in in enumerate(mq_connections):
            thread = threading.Thread(target=consume_exchange, args=(mq_in, INPUT_EXCHANGES[i]))
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # Esperar a que todos los hilos terminen
        for thread in threads:
            thread.join()
            
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
