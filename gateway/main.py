import socket
import logging
import sys
import os

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from gateway.server import handle_client
from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from gateway.results_handler import start_results_handler

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

LISTEN_HOST = "0.0.0.0"  # Para escuchar conexiones TCP
PORT = 9000  # Puerto para recibir del cliente
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')  # Hostname de RabbitMQ

def main():
    """
    Función principal del gateway.
    Escucha conexiones TCP del cliente y procesa los datos recibidos.
    """
    logger.info("Iniciando Gateway...")
 
    # Inicializar conexiones de salida al middleware por tipo de entidad
    mq_map = {}
    try:
        # Queues simples
        mq_map["transactions"] = MessageMiddlewareQueue(host=RABBITMQ_HOST, queue_name="transactions_raw")
        mq_map["transaction_items"] = MessageMiddlewareQueue(host=RABBITMQ_HOST, queue_name="transaction_items_raw")
        mq_map["users"] = MessageMiddlewareQueue(host=RABBITMQ_HOST, queue_name="users_raw")
        mq_map["menu_items"] = MessageMiddlewareQueue(host=RABBITMQ_HOST, queue_name="menu_items_raw")
        
        # Exchange solo para stores
        mq_map["stores"] = MessageMiddlewareExchange(
            host=RABBITMQ_HOST, 
            exchange_name="stores_raw",
            route_keys=["q3", "q4"]  # Q3 y Q4 consumen de este exchange
        )
    except Exception as e:
        logger.error(f"No se pudo inicializar colas/exchanges del middleware: {e}")
        return
    
    # Iniciar handler de resultados en threads separados
    logger.info("Iniciando handler de resultados...")
    start_results_handler()

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((LISTEN_HOST, PORT))
            s.listen(5)  # Permitir hasta 5 conexiones en cola
            logger.info(f"[GATEWAY] Escuchando en {LISTEN_HOST}:{PORT}")
            logger.info("[GATEWAY] Listo para recibir datos del cliente")

            while True:
                try:
                    conn, addr = s.accept()
                    logger.info(f"[GATEWAY] Nueva conexión desde {addr}")
                    handle_client(conn, addr, mq_map)
                except KeyboardInterrupt:
                    logger.info("Interrupción recibida, cerrando servidor...")
                    break
                except Exception as e:
                    logger.error(f"Error manejando conexión: {e}")
                    continue
                    
    except Exception as e:
        logger.error(f"Error en servidor: {e}")
    finally:
        # Cerrar todas las conexiones de colas
        for _k, _mq in mq_map.items():
            try:
                _mq.close()
            except Exception:
                pass
        logger.info("Gateway cerrado")

if __name__ == "__main__":
    main()
