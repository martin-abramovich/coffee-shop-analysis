from datetime import datetime
from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = "localhost"
INPUT_EXCHANGE = "transactions_year"     # exchange del filtro por año
INPUT_ROUTING_KEY = "year"               # routing key del filtro por año
OUTPUT_EXCHANGE = "transactions_hour"    # exchange de salida para horas
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

def on_message(body):
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print("[FilterByHour] End of Stream recibido. Reenviando...")
        # Reenviar EOS a workers downstream
        eos_msg = serialize_message([], header)
        mq_out.send(eos_msg)
        print("[FilterByHour] EOS reenviado a workers downstream")
        return
    
    # Procesamiento normal
    total_in = len(rows)
    filtered = filter_by_hour(rows)
    if filtered:
        out_msg = serialize_message(filtered, header)
        mq_out.send(out_msg)
    kept = len(filtered)
    dropped = total_in - kept
    print(f"[FilterHour] in={total_in} kept={kept} dropped={dropped} window=[{START_HOUR}-{END_HOUR}]")

if __name__ == "__main__":
    # Entrada: suscripción al exchange del filtro por año
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])

    # Salida: exchange (fanout/route)
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])

    print("[*] FilterWorkerHour esperando mensajes...")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        mq_in.stop_consuming()
        mq_in.close()
        mq_out.close()
        print("[x] Worker detenido")
