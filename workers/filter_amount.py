from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración (hardcodeada por ahora) ---
RABBIT_HOST = "localhost"
INPUT_QUEUE = "transactions_raw"
OUTPUT_EXCHANGE = "transactions_amount"
ROUTING_KEY = "amount"
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
    total_in = len(rows)
    filtered = filter_by_amount(rows, THRESHOLD)
    if filtered:
        out_msg = serialize_message(filtered, header)
        mq_out.send(out_msg)
    kept = len(filtered)
    dropped = total_in - kept
    print(f"[FilterAmount] in={total_in} kept={kept} dropped={dropped} threshold>={THRESHOLD}")

if __name__ == "__main__":
    mq_in = MessageMiddlewareQueue(RABBIT_HOST, INPUT_QUEUE)
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])

    print("[*] FilterWorkerAmount esperando mensajes...")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        mq_in.stop_consuming()
        mq_in.close()
        mq_out.close()
        print("[x] Worker detenido")
