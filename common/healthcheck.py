"""
Módulo común para healthcheck UDP.
Cada nodo (gateway o worker) debe iniciar un servidor UDP que responda a pings de monitores.
"""
import socket
import threading
import logging
import os

logger = logging.getLogger(__name__)

# Puerto por defecto para healthcheck UDP
DEFAULT_HEALTHCHECK_PORT = int(os.environ.get('HEALTHCHECK_PORT', '8888'))

# Mensaje de respuesta estándar
HEALTH_RESPONSE = b"OK"


def start_healthcheck_server(port=None, node_name="node", shutdown_event=None):
    """
    Inicia un servidor UDP que responde a healthchecks.
    
    Args:
        port: Puerto UDP donde escuchar (default: HEALTHCHECK_PORT env var o 8888)
        node_name: Nombre del nodo para logging
        shutdown_event: threading.Event para detener el servidor
    
    Returns:
        threading.Thread: Thread del servidor (ya iniciado)
    """
    if port is None:
        port = DEFAULT_HEALTHCHECK_PORT
    
    if shutdown_event is None:
        shutdown_event = threading.Event()
    
    def healthcheck_listener():
        """Thread que escucha peticiones UDP de healthcheck"""
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('0.0.0.0', port))
            sock.settimeout(1.0)  # Timeout para poder verificar shutdown_event periódicamente
            
            logger.info(f"[{node_name}] Healthcheck UDP server iniciado en puerto {port}")
            
            while not shutdown_event.is_set():
                try:
                    data, addr = sock.recvfrom(1024)
                    # Responder con mensaje de salud
                    sock.sendto(HEALTH_RESPONSE, addr)
                    logger.debug(f"[{node_name}] Healthcheck respondido a {addr}")
                except socket.timeout:
                    # Timeout normal, continuar loop para verificar shutdown_event
                    continue
                except Exception as e:
                    if not shutdown_event.is_set():
                        logger.error(f"[{node_name}] Error en healthcheck server: {e}")
                    continue
            
            logger.info(f"[{node_name}] Healthcheck UDP server deteniéndose...")
            
        except Exception as e:
            logger.error(f"[{node_name}] Error iniciando healthcheck server: {e}")
        finally:
            if sock:
                try:
                    sock.close()
                except:
                    pass
            logger.info(f"[{node_name}] Healthcheck UDP server cerrado")
    
    thread = threading.Thread(target=healthcheck_listener, name=f"Healthcheck-{node_name}", daemon=True)
    thread.start()
    return thread

