import socket
import logging
from server import handle_client

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
                    handle_client(conn, addr)
                except KeyboardInterrupt:
                    logger.info("Interrupción recibida, cerrando servidor...")
                    break
                except Exception as e:
                    logger.error(f"Error manejando conexión: {e}")
                    continue
                    
    except Exception as e:
        logger.error(f"Error en servidor: {e}")
    finally:
        logger.info("Gateway cerrado")

if __name__ == "__main__":
    main()
