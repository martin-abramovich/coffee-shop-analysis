from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
import os, time

MODE = os.getenv("MODE", "queue")
QUEUE_NAME = os.getenv("QUEUE_NAME", "test_queue")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "test_exchange")
ROUTE_KEYS = os.getenv("ROUTE_KEYS", "test.key").split(",")

if MODE == "queue":
    producer = MessageMiddlewareQueue("rabbitmq", QUEUE_NAME)
else:
    producer = MessageMiddlewareExchange("rabbitmq", EXCHANGE_NAME, ROUTE_KEYS)

messages = int(os.getenv("N_MESSAGES", "1"))
for i in range(messages):
    msg = f"msg-{i}"
    producer.send(msg.encode())
    print(f"[PRODUCER] Enviado: {msg}", flush=True)
    time.sleep(0.1)

producer.close()
