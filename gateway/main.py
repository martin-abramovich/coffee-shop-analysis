import socket
import logging
import sys
import os
import threading
import signal
import time

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from gateway.server import handle_client, active_sessions, sessions_lock
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

# Control de shutdown global
shutdown_event = threading.Event()
active_threads = []
threads_lock = threading.Lock()

def signal_handler(signum, frame):
    """Maneja señales para graceful shutdown"""
    logger.info(f"Señal {signum} recibida. Iniciando cierre ordenado...")
    shutdown_event.set()


def handle_client_wrapper(conn, addr, mq_map):
    """Wrapper para manejar cliente con manejo de errores"""
    try:
        handle_client(conn, addr, mq_map)
    except Exception as e:
        logger.error(f"Error no manejado en cliente {addr}: {e}")
    finally:
        try:
            conn.close()
        except:
            pass
        logger.info(f"Thread para cliente {addr} terminado")

def main():
    """
    Función principal del gateway.
    Escucha conexiones TCP de múltiples clientes y procesa los datos recibidos concurrentemente.
    """
    logger.info("Iniciando Gateway...")
 
    NUM_FILTER_YEAR_WORKERS = int(os.environ.get('NUM_FILTER_YEAR_WORKERS', '3'))
    
    # Inicializar conexiones de salida al middleware por tipo de entidad
    mq_map = {}
    try:
        # Usamos routing keys worker_0, worker_1, ... worker_N-1 para round-robin
        # y "eos" para broadcast de EOS a todos los workers
        filter_year_route_keys = [f"worker_{i}" for i in range(NUM_FILTER_YEAR_WORKERS)] + ["eos"]
        
        mq_map["transactions"] = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name="transactions_raw",
            route_keys=filter_year_route_keys
        )
        mq_map["transaction_items"] = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name="transaction_items_raw",
            route_keys=filter_year_route_keys
        )
        
        # Queues simples para otros tipos
        mq_map["users"] = MessageMiddlewareQueue(host=RABBITMQ_HOST, queue_name="users_raw")
        mq_map["menu_items"] = MessageMiddlewareQueue(host=RABBITMQ_HOST, queue_name="menu_items_raw")
        
        # Exchange para stores (Q3 y Q4)
        mq_map["stores"] = MessageMiddlewareExchange(
            host=RABBITMQ_HOST, 
            exchange_name="stores_raw",
            route_keys=["q3", "q4"]  # Q3 y Q4 consumen de este exchange
        )
        
        # Guardar configuración de escalado en mq_map para usar en server.py
        mq_map["_config"] = {
            "num_filter_year_workers": NUM_FILTER_YEAR_WORKERS
        }
        
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

            # Configurar manejadores de señales
            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGINT, signal_handler)
            
            logger.info("Gateway listo para múltiples clientes concurrentes")
            
            while not shutdown_event.is_set():
                try:
                    # Usar timeout para poder verificar shutdown_event
                    s.settimeout(1.0)
                    try:
                        conn, addr = s.accept()
                        logger.info(f"[GATEWAY] Nueva conexión desde {addr}")
                        
                        # Crear thread para manejar cliente
                        client_thread = threading.Thread(
                            target=handle_client_wrapper,
                            args=(conn, addr, mq_map),
                            name=f"Client-{addr[0]}:{addr[1]}"
                        )
                        client_thread.daemon = True
                        client_thread.start()
                        
                        # Registrar thread activo
                        with threads_lock:
                            active_threads.append(client_thread)
                        
                        logger.info(f"Thread creado para cliente {addr}")
                        
                    except socket.timeout:
                        # Timeout normal, continuar
                        continue
                        
                except KeyboardInterrupt:
                    logger.info("Interrupción recibida, cerrando servidor...")
                    shutdown_event.set()
                    break
                except Exception as e:
                    if not shutdown_event.is_set():
                        logger.error(f"Error aceptando conexión: {e}")
                    continue
            
            # Esperar a que terminen todos los threads de clientes
            logger.info("Esperando que terminen todos los clientes...")
            with threads_lock:
                for thread in active_threads:
                    if thread.is_alive():
                        logger.info(f"Esperando thread {thread.name}...")
                        thread.join(timeout=10)
            
            logger.info("Todos los clientes han terminado")
                    
    except Exception as e:
        logger.error(f"Error en servidor: {e}")
        shutdown_event.set()
    finally:
        # Cerrar todas las conexiones de colas
        logger.info("Cerrando conexiones de middleware...")
        for _k, _mq in mq_map.items():
            if _k != "_config":  # Saltar entrada de configuración
                try:
                    _mq.close()
                    logger.debug(f"Conexión {_k} cerrada")
                except Exception as e:
                    logger.error(f"Error cerrando conexión {_k}: {e}")
        
        logger.info("Gateway cerrado completamente")

if __name__ == "__main__":
    main()
