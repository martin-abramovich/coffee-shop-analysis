import socket
import logging
import sys
import os
import threading
import signal
import time
import json

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from common.logger import init_log
from gateway.server import handle_client, active_sessions, sessions_lock, set_shutdown_event
from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from gateway.results_handler import start_results_handler
from common.healthcheck import start_healthcheck_server

logger = init_log("Gateway")

LISTEN_HOST = "0.0.0.0"  # Para escuchar conexiones TCP
PORT = 9000  # Puerto para recibir del cliente
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')  # Hostname de RabbitMQ
MAX_CONCURRENT_CLIENTS = int(os.environ.get('MAX_CONCURRENT_CLIENTS', '5'))

# Control de shutdown
shutdown_event = threading.Event()
active_threads = []
threads_lock = threading.Lock()
client_slots = threading.BoundedSemaphore(MAX_CONCURRENT_CLIENTS)

def signal_handler(signum, frame):
    """Maneja señales para graceful shutdown"""
    logger.info(f"Señal {signum} recibida. Iniciando cierre ordenado...")
    shutdown_event.set()


def handle_client_wrapper(conn, addr):
    """Wrapper para manejar cliente con manejo de errores"""
    try:
        handle_client(conn, addr)
    except Exception as e:
        logger.error(f"Error no manejado en cliente {addr}: {e}")
    finally:
        try:
            conn.close()
        except:
            pass
        finally:
            client_slots.release()
        logger.info(f"Thread para cliente {addr} terminado")


def reject_client_connection(conn):
    """Envía una respuesta de rechazo cuando se alcanza el límite de clientes."""
    try:
        response_body = {
            "status": "ERROR",
            "error": "MAX_CLIENTS_REACHED",
            "message": (
                f"El gateway alcanzó el máximo de {MAX_CONCURRENT_CLIENTS} clientes simultáneos. "
                "Intente nuevamente más tarde."
            )
        }
        payload = json.dumps(response_body).encode('utf-8')
        header = len(payload).to_bytes(4, byteorder='big')
        conn.sendall(header + payload)
    except Exception as send_err:
        logger.debug(f"No se pudo notificar rechazo de conexión: {send_err}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def main():
    """
    Función principal del gateway.
    Escucha conexiones TCP de múltiples clientes y procesa los datos recibidos concurrentemente.
    """
    logger.info("Iniciando Gateway...")
    
    set_shutdown_event(shutdown_event)
    
    # Iniciar servidor de healthcheck UDP
    healthcheck_port = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
    healthcheck_thread = start_healthcheck_server(port=healthcheck_port, node_name="gateway", shutdown_event=shutdown_event)
    logger.info(f"Healthcheck server iniciado en puerto UDP {healthcheck_port}")
 
    # Iniciar handler de resultados en threads separados
    logger.info("Iniciando handler de resultados...")
    results_mq_connections = start_results_handler(shutdown_event)

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((LISTEN_HOST, PORT))
            s.listen(5)
            logger.info(f"[GATEWAY] Escuchando en {LISTEN_HOST}:{PORT}")
            logger.info("[GATEWAY] Listo para recibir datos del cliente")

            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGINT, signal_handler)
            
            logger.info("Gateway listo para múltiples clientes concurrentes")
            
            while not shutdown_event.is_set():
                try:
                    s.settimeout(1.0)
                    try:
                        conn, addr = s.accept()
                        logger.info(f"[GATEWAY] Nueva conexión desde {addr}")

                        if not client_slots.acquire(blocking=False):
                            logger.warning(
                                "[GATEWAY] Límite de clientes simultáneos alcanzado (%d). Rechazando %s",
                                MAX_CONCURRENT_CLIENTS,
                                addr,
                            )
                            reject_client_connection(conn)
                            continue

                        # Crear thread para manejar cliente
                        try:
                            client_thread = threading.Thread(
                                target=handle_client_wrapper,
                                args=(conn, addr),
                                name=f"Client-{addr[0]}:{addr[1]}"
                            )
                            client_thread.daemon = False  # Non-daemon para graceful shutdown
                            client_thread.start()
                            
                            with threads_lock:
                                active_threads.append(client_thread)
                        except Exception as thread_error:
                            # Si falla la creación o inicio del thread, cerrar conn aquí
                            client_slots.release()
                            logger.error(f"No se pudo iniciar thread para {addr}: {thread_error}")
                            try:
                                conn.close()
                            except:
                                pass
                            continue
                        
                        # Thread creado para cliente
                        
                    except socket.timeout:
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
                        # Esperando thread
                        thread.join(timeout=10)
            
            logger.info("Todos los clientes han terminado")
            
            # Cerrar ResultsHandler
            if results_mq_connections:
                logger.info("Cerrando ResultsHandler...")
                for query_name, mq in results_mq_connections.items():
                    try:
                        mq.stop_consuming()
                        logger.info(f"Deteniendo consumo de {query_name}")
                    except Exception as e:
                        logger.error(f"Error deteniendo {query_name}: {e}")
                
                for query_name, mq in results_mq_connections.items():
                    try:
                        mq.close()
                        logger.info(f"Conexión {query_name} cerrada")
                    except Exception as e:
                        logger.error(f"Error cerrando {query_name}: {e}")
                    
    except Exception as e:
        logger.error(f"Error en servidor: {e}")
        shutdown_event.set()
    finally:
        logger.info("Gateway cerrado completamente")

if __name__ == "__main__":
    main()
