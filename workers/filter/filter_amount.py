from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = "localhost"
INPUT_EXCHANGE = "transactions_hour"     # exchange del filtro por hora
INPUT_ROUTING_KEY = "hour"               # routing key del filtro por hora
OUTPUT_EXCHANGE = "transactions_amount"  # exchange de salida para amount
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

def on_message(body):
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print("[FilterByAmount] End of Stream recibido. Reenviando...")
        # Reenviar EOS a workers downstream
        eos_msg = serialize_message([], header)
        mq_out.send(eos_msg)
        print("[FilterByAmount] EOS reenviado a workers downstream")
        return
    
    # Procesamiento normal
    total_in = len(rows)
    filtered = filter_by_amount(rows, THRESHOLD)
    if filtered:
        out_msg = serialize_message(filtered, header)
        mq_out.send(out_msg)
    kept = len(filtered)
    dropped = total_in - kept
    print(f"[FilterAmount] in={total_in} kept={kept} dropped={dropped} threshold>={THRESHOLD}")

if __name__ == "__main__":
    # Entrada: suscripción al exchange del filtro por hora
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Salida: exchange para datos filtrados por amount
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])

    print("[*] FilterWorkerAmount esperando mensajes...")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        mq_in.stop_consuming()
        mq_in.close()
        mq_out.close()
        print("[x] Worker detenido")
