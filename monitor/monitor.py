"""
Monitor que verifica la salud de los nodos (gateway y workers) mediante UDP healthchecks.
Envía paquetes UDP periódicamente y detecta nodos caídos si no responden después de N intentos.
"""
import socket
import time
import threading
import logging
import sys
import os
import signal
from typing import Dict, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración
HEALTHCHECK_PORT = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
CHECK_INTERVAL = float(os.environ.get('CHECK_INTERVAL', '5.0'))  # Segundos entre checks
TIMEOUT = float(os.environ.get('TIMEOUT', '2.0'))  # Timeout para respuesta UDP
MAX_FAILED_ATTEMPTS = int(os.environ.get('MAX_FAILED_ATTEMPTS', '3'))  # Intentos antes de marcar como caído

# Mensaje de healthcheck
HEALTHCHECK_REQUEST = b"PING"
HEALTHCHECK_RESPONSE = b"OK"


@dataclass
class NodeStatus:
    """Estado de un nodo"""
    name: str
    host: str
    port: int
    is_up: bool = True
    failed_attempts: int = 0
    last_check: datetime = field(default_factory=datetime.now)
    last_success: datetime = field(default_factory=datetime.now)
    last_failure: datetime = None


class HealthMonitor:
    """Monitor que verifica la salud de múltiples nodos"""
    
    def __init__(self, nodes: List[Tuple[str, str, int]], check_interval: float = CHECK_INTERVAL,
                 timeout: float = TIMEOUT, max_failed_attempts: int = MAX_FAILED_ATTEMPTS):
        """
        Args:
            nodes: Lista de tuplas (nombre, host, puerto) para cada nodo
            check_interval: Intervalo entre checks (segundos)
            timeout: Timeout para respuesta UDP (segundos)
            max_failed_attempts: Intentos fallidos antes de marcar como caído
        """
        self.nodes: Dict[str, NodeStatus] = {}
        for name, host, port in nodes:
            self.nodes[name] = NodeStatus(name=name, host=host, port=port)
        
        self.check_interval = check_interval
        self.timeout = timeout
        self.max_failed_attempts = max_failed_attempts
        self.shutdown_event = threading.Event()
        self.lock = threading.Lock()
        
    def check_node(self, node: NodeStatus) -> bool:
        """
        Verifica un nodo individual enviando un paquete UDP.
        
        Returns:
            True si el nodo responde correctamente, False en caso contrario
        """
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(self.timeout)
            
            # Enviar healthcheck
            sock.sendto(HEALTHCHECK_REQUEST, (node.host, node.port))
            
            # Esperar respuesta
            data, addr = sock.recvfrom(1024)
            
            if data == HEALTHCHECK_RESPONSE:
                return True
            else:
                logger.warning(f"[{node.name}] Respuesta inesperada: {data}")
                return False
                
        except socket.timeout:
            logger.debug(f"[{node.name}] Timeout esperando respuesta")
            return False
        except Exception as e:
            logger.debug(f"[{node.name}] Error en healthcheck: {e}")
            return False
        finally:
            if sock:
                try:
                    sock.close()
                except:
                    pass
    
    def check_all_nodes(self):
        """Verifica todos los nodos y actualiza su estado"""
        for node_name, node in self.nodes.items():
            is_healthy = self.check_node(node)
            
            with self.lock:
                node.last_check = datetime.now()
                
                if is_healthy:
                    node.failed_attempts = 0
                    node.last_success = datetime.now()
                    
                    # Si estaba caído y ahora está arriba, reportar recuperación
                    if not node.is_up:
                        logger.info(f"✓ [{node.name}] NODO RECUPERADO - {node.host}:{node.port}")
                        node.is_up = True
                    else:
                        logger.debug(f"✓ [{node.name}] OK - {node.host}:{node.port}")
                else:
                    node.failed_attempts += 1
                    node.last_failure = datetime.now()
                    
                    # Si supera el umbral, marcar como caído
                    if node.failed_attempts >= self.max_failed_attempts and node.is_up:
                        logger.error(f"✗ [{node.name}] NODO CAÍDO - {node.host}:{node.port} "
                                   f"(fallos consecutivos: {node.failed_attempts})")
                        node.is_up = False
                    else:
                        logger.warning(f"⚠ [{node.name}] Fallo {node.failed_attempts}/{self.max_failed_attempts} - "
                                     f"{node.host}:{node.port}")
    
    def monitor_loop(self):
        """Loop principal del monitor"""
        logger.info(f"Monitor iniciado. Verificando {len(self.nodes)} nodos cada {self.check_interval}s")
        logger.info(f"Timeout: {self.timeout}s, Max fallos: {self.max_failed_attempts}")
        
        while not self.shutdown_event.is_set():
            try:
                self.check_all_nodes()
                self.print_status()
                
                # Esperar hasta el próximo check
                self.shutdown_event.wait(self.check_interval)
                
            except Exception as e:
                logger.error(f"Error en monitor loop: {e}")
                time.sleep(1)
    
    def print_status(self):
        """Imprime el estado actual de todos los nodos"""
        with self.lock:
            up_count = sum(1 for n in self.nodes.values() if n.is_up)
            down_count = len(self.nodes) - up_count
            
            if down_count > 0:
                logger.info(f"Estado: {up_count} arriba, {down_count} caídos")
                for node in self.nodes.values():
                    if not node.is_up:
                        logger.info(f"  ✗ {node.name} ({node.host}:{node.port}) - "
                                  f"Fallos: {node.failed_attempts}, "
                                  f"Último check: {node.last_check.strftime('%H:%M:%S')}")
    
    def start(self):
        """Inicia el monitor en un thread separado"""
        thread = threading.Thread(target=self.monitor_loop, name="HealthMonitor", daemon=False)
        thread.start()
        return thread
    
    def stop(self):
        """Detiene el monitor"""
        self.shutdown_event.set()


def load_nodes_from_env() -> List[Tuple[str, str, int]]:
    """
    Carga la lista de nodos desde variables de entorno.
    Formato: NODES="nombre1:host1:puerto1,nombre2:host2:puerto2,..."
    """
    nodes_str = os.environ.get('NODES', '')
    if not nodes_str:
        # Configuración por defecto basada en docker-compose
        return [
            ('gateway', 'gateway', HEALTHCHECK_PORT),
            ('filter_year_0', 'filter_year_0', HEALTHCHECK_PORT),
            ('filter_year_1', 'filter_year_1', HEALTHCHECK_PORT),
            ('filter_year_2', 'filter_year_2', HEALTHCHECK_PORT),
            ('filter_hour_0', 'filter_hour_0', HEALTHCHECK_PORT),
            ('filter_hour_1', 'filter_hour_1', HEALTHCHECK_PORT),
            ('filter_amount_0', 'filter_amount_0', HEALTHCHECK_PORT),
            ('filter_amount_1', 'filter_amount_1', HEALTHCHECK_PORT),
            ('group_by_query2_0', 'group_by_query2_0', HEALTHCHECK_PORT),
            ('group_by_query2_1', 'group_by_query2_1', HEALTHCHECK_PORT),
            ('group_by_query3_0', 'group_by_query3_0', HEALTHCHECK_PORT),
            ('group_by_query3_1', 'group_by_query3_1', HEALTHCHECK_PORT),
            ('group_by_query4_0', 'group_by_query4_0', HEALTHCHECK_PORT),
            ('group_by_query4_1', 'group_by_query4_1', HEALTHCHECK_PORT),
            ('aggregator_query1', 'aggregator_query1', HEALTHCHECK_PORT),
            ('aggregator_query2', 'aggregator_query2', HEALTHCHECK_PORT),
            ('aggregator_query3', 'aggregator_query3', HEALTHCHECK_PORT),
            ('aggregator_query4', 'aggregator_query4', HEALTHCHECK_PORT),
        ]
    
    nodes = []
    for node_str in nodes_str.split(','):
        parts = node_str.strip().split(':')
        if len(parts) == 3:
            name, host, port = parts
            nodes.append((name, host, int(port)))
        elif len(parts) == 2:
            # Si no se especifica puerto, usar el default
            name, host = parts
            nodes.append((name, host, HEALTHCHECK_PORT))
    
    return nodes


def main():
    """Función principal del monitor"""
    logger.info("Iniciando Health Monitor...")
    
    # Cargar nodos a monitorear
    nodes = load_nodes_from_env()
    logger.info(f"Nodos a monitorear: {len(nodes)}")
    for name, host, port in nodes:
        logger.info(f"  - {name}: {host}:{port}")
    
    # Crear y configurar monitor
    monitor = HealthMonitor(
        nodes=nodes,
        check_interval=CHECK_INTERVAL,
        timeout=TIMEOUT,
        max_failed_attempts=MAX_FAILED_ATTEMPTS
    )
    
    # Manejar señales para shutdown graceful
    def signal_handler(signum, frame):
        logger.info(f"Señal {signum} recibida. Deteniendo monitor...")
        monitor.stop()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Iniciar monitor (bloquea hasta shutdown)
        monitor.monitor_loop()
    except KeyboardInterrupt:
        logger.info("Interrupción recibida")
        monitor.stop()
    finally:
        logger.info("Monitor detenido")


if __name__ == "__main__":
    main()

