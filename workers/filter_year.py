from datetime import datetime
from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = "localhost"
INPUT_QUEUE = "transactions_raw"       # la cola donde el Gateway manda los batches
OUTPUT_EXCHANGE = "transactions_year"  # exchange donde publicamos lo filtrado
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

def on_message(body):
    header, rows = deserialize_message(body)
    filtered = filter_by_year(rows)
    if filtered:
        out_msg = serialize_message(filtered, header)
        mq_out.send(out_msg)

if __name__ == "__main__":
    # Entrada: cola directa (balanceo entre workers)
    mq_in = MessageMiddlewareQueue(RABBIT_HOST, INPUT_QUEUE)

    # Salida: exchange (fanout a todos los pipelines que usen año)
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])

    print("[*] FilterWorkerYear esperando mensajes...")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        mq_in.stop_consuming()
        mq_in.close()
        mq_out.close()
        print("[x] Worker detenido")
