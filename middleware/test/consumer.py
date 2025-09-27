from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import os

MODE = os.getenv("MODE", "queue")
QUEUE_NAME = os.getenv("QUEUE_NAME", "test_queue")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "test_exchange")
ROUTE_KEYS = os.getenv("ROUTE_KEYS", "test.key").split(",")

def on_message(body):
    print(f"[{MODE.upper()} CONSUMER] Recibido: {body.decode()}", flush=True)

if __name__ == "__main__":
    if MODE == "queue":
        consumer = MessageMiddlewareQueue("rabbitmq", QUEUE_NAME)
    else:
        consumer = MessageMiddlewareExchange("rabbitmq", EXCHANGE_NAME, ROUTE_KEYS)

    try:
        print(f"[{MODE.upper()} CONSUMER] Esperando mensajes...", flush=True)
        consumer.start_consuming(on_message)
    except KeyboardInterrupt:
        print(f"[{MODE.upper()} CONSUMER] Deteniendo consumo...", flush=True)
        consumer.stop_consuming()
        consumer.close()
