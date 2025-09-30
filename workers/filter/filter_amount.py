import sys
import os
import signal

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
INPUT_EXCHANGES = ["transactions_hour"]  # solo transactions_hour (transaction_items no pasa por amount)
INPUT_ROUTING_KEY = "hour"               # routing key del filtro por hora
OUTPUT_EXCHANGES = {
    "transactions_hour": ["transactions_amount"]      # exchange para transactions filtradas por amount
}
ROUTING_KEY = "amount"                   # routing para topic
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
                filtered.append(r)
        except Exception:
            continue
    return filtered

def on_message(body, source_exchange):
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print(f"[FilterByAmount] End of Stream recibido desde {source_exchange}. Reenviando...")
        # Reenviar EOS a workers downstream usando los exchanges correctos
        eos_msg = serialize_message([], header)
        output_exchanges = OUTPUT_EXCHANGES[source_exchange]
        for exchange_name in output_exchanges:
            mq_outputs[exchange_name].send(eos_msg)
        print(f"[FilterByAmount] EOS reenviado a {output_exchanges}")
        return
    
    # Procesamiento normal
    total_in = len(rows)
    filtered = filter_by_amount(rows, THRESHOLD)
    if filtered:
        out_msg = serialize_message(filtered, header)
        output_exchanges = OUTPUT_EXCHANGES[source_exchange]
        for exchange_name in output_exchanges:
            mq_outputs[exchange_name].send(out_msg)
    kept = len(filtered)
    dropped = total_in - kept
    print(f"[FilterAmount] in={total_in} kept={kept} dropped={dropped} threshold>={THRESHOLD} desde {source_exchange}")

if __name__ == "__main__":
    shutdown_requested = False
    mq_connections = []
    
    def signal_handler(signum, frame):
        global shutdown_requested
        print(f"[FilterAmount] Señal {signum} recibida, cerrando...")
        shutdown_requested = True
        for mq in mq_connections:
            try:
                mq.stop_consuming()
            except:
                pass
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: múltiples exchanges
    mq_connections = []
    for exchange_name in INPUT_EXCHANGES:
        mq_in = MessageMiddlewareExchange(RABBIT_HOST, exchange_name, [INPUT_ROUTING_KEY])
        mq_connections.append(mq_in)

    # Salida: múltiples exchanges según el tipo de datos
    mq_outputs = {}
    for input_exchange, output_exchanges in OUTPUT_EXCHANGES.items():
        for exchange_name in output_exchanges:
            mq_outputs[exchange_name] = MessageMiddlewareExchange(RABBIT_HOST, exchange_name, [ROUTING_KEY])

    print(f"[*] FilterWorkerAmount esperando mensajes de {INPUT_EXCHANGES}...")
    try:
        # Procesar cada exchange en un hilo separado
        import threading
        
        def consume_exchange(mq_in, exchange_name):
            try:
                print(f"[FilterAmount] Iniciando consumo de {exchange_name}...")
                # Crear una función wrapper que pase el source_exchange
                def on_message_wrapper(body):
                    return on_message(body, exchange_name)
                mq_in.start_consuming(on_message_wrapper)
            except Exception as e:
                print(f"[FilterAmount] Error consumiendo {exchange_name}: {e}")
        
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
        print("\n[FilterAmount] Interrupción recibida")
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
        print("[x] FilterWorkerAmount detenido")
