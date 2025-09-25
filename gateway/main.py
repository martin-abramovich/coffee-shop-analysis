import socket
import logging
import sys
import os
from server import handle_client
from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

HOST = "0.0.0.0"
PORT = 9000  # Puerto para recibir del cliente

def main():
    """
    Función principal del gateway.
    Escucha conexiones TCP del cliente y procesa los datos recibidos.
    """
    logger.info("Iniciando Gateway...")

    # Inicializar conexiones de salida al middleware por tipo de entidad
    mq_map = {}
    try:
        # Exchanges para datos compartidos (fanout a múltiples workers)
        mq_map["transactions"] = MessageMiddlewareExchange(
            host="localhost", 
            exchange_name="transactions_raw",
            route_keys=["q1", "q4"]  # Q1 (ID & Amount) y Q4 (Birthday)
        )
        mq_map["transaction_items"] = MessageMiddlewareExchange(
            host="localhost", 
            exchange_name="transaction_items_raw",
            route_keys=["q2", "q3"]  # Q2 (Productos más vendidos) y Q3 (TPV)
        )
        # Queues simples para datos de lookup (usados solo para joins al final)
        mq_map["users"] = MessageMiddlewareQueue(host="localhost", queue_name="users_raw")
        mq_map["stores"] = MessageMiddlewareQueue(host="localhost", queue_name="stores_raw")
        mq_map["menu_items"] = MessageMiddlewareQueue(host="localhost", queue_name="menu_items_raw")
    except Exception as e:
        logger.error(f"No se pudo inicializar colas/exchanges del middleware: {e}")
        return

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((HOST, PORT))
            s.listen(5)  # Permitir hasta 5 conexiones en cola
            logger.info(f"[GATEWAY] Escuchando en {HOST}:{PORT}")
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
