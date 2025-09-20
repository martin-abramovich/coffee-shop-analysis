import socket
import logging
from server import handle_client
from common.messaging import TCPMessaging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

HOST = "0.0.0.0"
PORT = 9000  # Puerto para recibir del cliente
MIDDLEWARE_HOST = "localhost"
MIDDLEWARE_PORT = 8000  # Puerto para enviar al middleware

def main():
    """
    Función principal del gateway.
    Escucha conexiones TCP del cliente y envía datos procesados al middleware.
    """
    logger.info("Iniciando Gateway...")
    
    # Configurar conexión al middleware
    middleware = TCPMessaging(host=MIDDLEWARE_HOST, port=MIDDLEWARE_PORT)
    
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((HOST, PORT))
            s.listen(5)  # Permitir hasta 5 conexiones en cola
            logger.info(f"[GATEWAY] Escuchando en {HOST}:{PORT}")
            logger.info(f"[GATEWAY] Enviando datos procesados a {MIDDLEWARE_HOST}:{MIDDLEWARE_PORT}")

            while True:
                try:
                    conn, addr = s.accept()
                    logger.info(f"[GATEWAY] Nueva conexión desde {addr}")
                    handle_client(conn, addr, middleware)
                except KeyboardInterrupt:
                    logger.info("Interrupción recibida, cerrando servidor...")
                    break
                except Exception as e:
                    logger.error(f"Error manejando conexión: {e}")
                    continue
                    
    except Exception as e:
        logger.error(f"Error en servidor: {e}")
    finally:
        middleware.disconnect()
        logger.info("Gateway cerrado")

if __name__ == "__main__":
    main()
