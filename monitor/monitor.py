"""
Monitor redundante que verifica la salud de los nodos (gateway y workers) mediante UDP healthchecks.
Implementa algoritmo Bully para elección de líder y sincronización de estado entre réplicas.
El monitor líder puede revivir contenedores caídos usando Docker CLI.
"""
import socket
import time
import threading
import logging
import sys
import os
import signal
import subprocess
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from monitor.monitor_comm import MonitorCommunication
from monitor.monitor_protocol import MessageType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración
HEALTHCHECK_PORT = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
CHECK_INTERVAL = float(os.environ.get('CHECK_INTERVAL', '5.0'))
TIMEOUT = float(os.environ.get('TIMEOUT', '2.0'))
MAX_FAILED_ATTEMPTS = int(os.environ.get('MAX_FAILED_ATTEMPTS', '3'))

# Configuración de monitores
MONITOR_ID = int(os.environ.get('MONITOR_ID', '1'))
MONITOR_TCP_PORT = int(os.environ.get('MONITOR_TCP_PORT', '9999'))
HEARTBEAT_INTERVAL = float(os.environ.get('HEARTBEAT_INTERVAL', '3.0'))
LEADER_TIMEOUT = float(os.environ.get('LEADER_TIMEOUT', '10.0'))  # Tiempo sin heartbeat antes de considerar líder caído

# Mensaje de healthcheck
HEALTHCHECK_REQUEST = b"PING"
HEALTHCHECK_RESPONSE = b"OK"


class MonitorState(Enum):
    """Estados del monitor"""
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


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
    container_id: Optional[str] = None  # ID del contenedor Docker
    revival_attempts: int = 0  # Intentos de revival
    
    def to_dict(self):
        """Convierte a dict para serialización"""
        return {
            'name': self.name,
            'host': self.host,
            'port': self.port,
            'is_up': self.is_up,
            'failed_attempts': self.failed_attempts,
            'last_check': self.last_check.isoformat() if isinstance(self.last_check, datetime) else self.last_check,
            'last_success': self.last_success.isoformat() if isinstance(self.last_success, datetime) else self.last_success,
            'last_failure': self.last_failure.isoformat() if isinstance(self.last_failure, datetime) else self.last_failure if self.last_failure else None,
            'container_id': self.container_id,
            'revival_attempts': self.revival_attempts
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        """Crea desde dict"""
        obj = cls(
            name=data['name'],
            host=data['host'],
            port=data['port'],
            is_up=data['is_up'],
            failed_attempts=data['failed_attempts'],
            container_id=data.get('container_id'),
            revival_attempts=data.get('revival_attempts', 0)
        )
        # Manejar last_check
        last_check_val = data.get('last_check')
        if last_check_val and last_check_val != 'None' and last_check_val is not None:
            try:
                if isinstance(last_check_val, str):
                    obj.last_check = datetime.fromisoformat(last_check_val)
                else:
                    obj.last_check = last_check_val
            except (ValueError, TypeError):
                obj.last_check = datetime.now()
        
        # Manejar last_success
        last_success_val = data.get('last_success')
        if last_success_val and last_success_val != 'None' and last_success_val is not None:
            try:
                if isinstance(last_success_val, str):
                    obj.last_success = datetime.fromisoformat(last_success_val)
                else:
                    obj.last_success = last_success_val
            except (ValueError, TypeError):
                obj.last_success = datetime.now()
        
        # Manejar last_failure (puede ser None)
        last_failure_val = data.get('last_failure')
        if last_failure_val and last_failure_val != 'None' and last_failure_val is not None:
            try:
                if isinstance(last_failure_val, str):
                    obj.last_failure = datetime.fromisoformat(last_failure_val)
                else:
                    obj.last_failure = last_failure_val
            except (ValueError, TypeError):
                obj.last_failure = None
        else:
            obj.last_failure = None
        
        return obj


class RedundantHealthMonitor:
    """Monitor redundante con algoritmo Bully para elección de líder"""
    
    def __init__(self, monitor_id: int, nodes: List[Tuple[str, str, int]], 
                 all_monitors: List[Tuple[int, str, int]], tcp_port: int):
        """
        Args:
            monitor_id: ID único de este monitor
            nodes: Lista de nodos a monitorear (nombre, host, puerto UDP)
            all_monitors: Lista de todos los monitores (id, host, puerto TCP)
            tcp_port: Puerto TCP donde este monitor escucha
        """
        self.monitor_id = monitor_id
        self.nodes: Dict[str, NodeStatus] = {}
        for name, host, port in nodes:
            self.nodes[name] = NodeStatus(name=name, host=host, port=port)
        
        self.state = MonitorState.FOLLOWER
        self.current_leader_id: Optional[int] = None
        self.last_leader_heartbeat: Optional[float] = None
        self.election_in_progress = False
        
        # Comunicación entre monitores
        self.comm = MonitorCommunication(monitor_id, tcp_port, all_monitors)
        self.comm.register_handler(MessageType.HEARTBEAT, self._handle_heartbeat)
        self.comm.register_handler(MessageType.ELECTION, self._handle_election)
        self.comm.register_handler(MessageType.ANSWER, self._handle_answer)
        self.comm.register_handler(MessageType.COORDINATOR, self._handle_coordinator)
        self.comm.register_handler(MessageType.STATE_SYNC, self._handle_state_sync)
        self.comm.register_handler(MessageType.STATE_REQUEST, self._handle_state_request)
        
        self.shutdown_event = threading.Event()
        self.lock = threading.Lock()
        self.answer_received = threading.Event()
        
        # Configuración
        self.check_interval = CHECK_INTERVAL
        self.timeout = TIMEOUT
        self.max_failed_attempts = MAX_FAILED_ATTEMPTS
        self.heartbeat_interval = HEARTBEAT_INTERVAL
        self.leader_timeout = LEADER_TIMEOUT
    
    def _handle_heartbeat(self, message, addr):
        """Maneja heartbeat del líder"""
        if message.sender_id == self.current_leader_id:
            with self.lock:
                self.last_leader_heartbeat = time.time()
                logger.debug(f"[Monitor {self.monitor_id}] Heartbeat recibido del líder {message.sender_id}")
    
    def _handle_election(self, message, addr):
        """Maneja mensaje de elección"""
        if message.sender_id < self.monitor_id:
            # Responder con ANSWER si tenemos ID mayor
            self.comm.send_message(message.sender_id, MessageType.ANSWER)
            logger.info(f"Recibida elección de monitor {message.sender_id}, respondiendo con ANSWER")
            
            # Iniciar nuestra propia elección si no está en progreso
            if not self.election_in_progress:
                threading.Thread(target=self._start_election, daemon=True).start()
    
    def _handle_answer(self, message, addr):
        """Maneja respuesta a nuestra elección"""
        if message.sender_id > self.monitor_id:
            logger.info(f"Recibido ANSWER de monitor {message.sender_id} (ID mayor)")
            self.answer_received.set()
    
    def _handle_coordinator(self, message, addr):
        """Maneja mensaje de nuevo coordinador"""
        new_leader = message.sender_id
        with self.lock:
            self.current_leader_id = new_leader
            self.state = MonitorState.FOLLOWER
            self.last_leader_heartbeat = time.time()
            self.election_in_progress = False
        logger.info(f"Nuevo líder elegido: monitor {new_leader}")
    
    def _handle_state_sync(self, message, addr):
        """Maneja sincronización de estado del líder"""
        if message.sender_id == self.current_leader_id and message.data:
            try:
                nodes_data = message.data.get('nodes', {})
                with self.lock:
                    for name, node_data in nodes_data.items():
                        if name in self.nodes:
                            # Actualizar estado del nodo
                            node = NodeStatus.from_dict(node_data)
                            self.nodes[name] = node
                logger.debug(f"Estado sincronizado desde líder {message.sender_id}")
            except Exception as e:
                logger.error(f"Error sincronizando estado: {e}")
    
    def _handle_state_request(self, message, addr):
        """Maneja solicitud de estado (solo el líder responde)"""
        if self.state == MonitorState.LEADER:
            self._send_state_sync()
    
    def _start_election(self):
        """Inicia el algoritmo Bully de elección"""
        with self.lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
            self.state = MonitorState.CANDIDATE
        
        logger.info(f"Iniciando elección (ID: {self.monitor_id})")
        
        # Enviar ELECTION a monitores con ID mayor
        self.answer_received.clear()
        responded = self.comm.broadcast_to_higher(MessageType.ELECTION)
        
        # Esperar respuesta (timeout corto)
        time.sleep(1.0)
        
        if not self.answer_received.is_set():
            # No recibimos respuesta de IDs mayores, somos el líder
            with self.lock:
                self.state = MonitorState.LEADER
                self.current_leader_id = self.monitor_id
                self.last_leader_heartbeat = time.time()
                self.election_in_progress = False
            
            logger.info(f"✓ Monitor {self.monitor_id} es ahora el LÍDER")
            
            # Notificar a todos los otros monitores
            self.comm.broadcast_to_all(MessageType.COORDINATOR)
        else:
            # Alguien con ID mayor respondió, esperar que complete la elección
            with self.lock:
                self.state = MonitorState.FOLLOWER
                self.election_in_progress = False
            logger.info(f"Monitor con ID mayor respondió, esperando nuevo líder...")
    
    def _check_leader_alive(self):
        """Verifica si el líder está vivo"""
        if self.state == MonitorState.LEADER:
            return True
        
        if self.current_leader_id is None:
            return False
        
        with self.lock:
            if self.last_leader_heartbeat is None:
                return False
            
            time_since_heartbeat = time.time() - self.last_leader_heartbeat
            if time_since_heartbeat > self.leader_timeout:
                return False
        
        return True
    
    def _send_heartbeat(self):
        """Envía heartbeat a otros monitores (solo si es líder)"""
        if self.state == MonitorState.LEADER:
            self.comm.broadcast_to_all(MessageType.HEARTBEAT)
    
    def _send_state_sync(self):
        """Envía estado actual a otros monitores (solo si es líder)"""
        if self.state == MonitorState.LEADER:
            with self.lock:
                nodes_data = {name: node.to_dict() for name, node in self.nodes.items()}
            
            self.comm.broadcast_to_all(MessageType.STATE_SYNC, {'nodes': nodes_data})
    
    def get_container_id_by_name(self, container_name: str) -> Optional[str]:
        """Obtiene el ID del contenedor por nombre usando Docker CLI"""
        try:
            result = subprocess.run(
                ['docker', 'ps', '-a', '--filter', f'name={container_name}', '--format', '{{.ID}}'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip().split('\n')[0]  # Primera línea
            return None
        except Exception as e:
            logger.error(f"Error obteniendo container ID para {container_name}: {e}")
            return None
    
    def get_container_ids_by_label(self, label: str = 'role=node') -> Dict[str, str]:
        """Obtiene todos los contenedores con un label específico (nombre -> ID)"""
        try:
            result = subprocess.run(
                ['docker', 'ps', '-a', '--filter', f'label={label}', '--format', '{{.Names}}:{{.ID}}'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                containers = {}
                for line in result.stdout.strip().split('\n'):
                    if ':' in line:
                        name, cid = line.split(':', 1)
                        containers[name] = cid
                return containers
            return {}
        except Exception as e:
            logger.error(f"Error obteniendo contenedores con label {label}: {e}")
            return {}
    
    def is_container_running(self, container_id: str) -> bool:
        """Verifica si un contenedor está corriendo usando Docker CLI"""
        try:
            result = subprocess.run(
                ['docker', 'inspect', '-f', '{{.State.Running}}', container_id],
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.returncode == 0 and result.stdout.strip() == 'true'
        except Exception as e:
            logger.debug(f"Error verificando si contenedor {container_id} está corriendo: {e}")
            return False
    
    def start_container(self, container_id: str, container_name: str) -> bool:
        """Intenta levantar un contenedor usando Docker CLI"""
        try:
            logger.info(f"[REVIVAL] Intentando levantar contenedor {container_name} ({container_id})...")
            result = subprocess.run(
                ['docker', 'start', container_id],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                logger.info(f"[REVIVAL] ✓ Contenedor {container_name} levantado exitosamente")
                return True
            else:
                logger.error(f"[REVIVAL] ✗ Error al levantar {container_name}: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"[REVIVAL] ✗ Excepción al levantar {container_name}: {e}")
            return False
    
    def check_node(self, node: NodeStatus) -> bool:
        """Verifica un nodo individual enviando un paquete UDP"""
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(self.timeout)
            sock.sendto(HEALTHCHECK_REQUEST, (node.host, node.port))
            data, addr = sock.recvfrom(1024)
            return data == HEALTHCHECK_RESPONSE
        except:
            return False
        finally:
            if sock:
                try:
                    sock.close()
                except:
                    pass
    
    def check_all_nodes(self):
        """Verifica todos los nodos y revive contenedores caídos (solo si es líder)"""
        if self.state != MonitorState.LEADER:
            return
        
        for node_name, node in self.nodes.items():
            is_healthy = self.check_node(node)
            
            with self.lock:
                node.last_check = datetime.now()
                
                if is_healthy:
                    node.failed_attempts = 0
                    node.last_success = datetime.now()
                    if not node.is_up:
                        logger.info(f"✓ [{node.name}] NODO RECUPERADO - {node.host}:{node.port}")
                        node.is_up = True
                        node.revival_attempts = 0  # Reset revival attempts on recovery
                else:
                    node.failed_attempts += 1
                    node.last_failure = datetime.now()
                    if node.failed_attempts >= self.max_failed_attempts and node.is_up:
                        logger.error(f"✗ [{node.name}] NODO CAÍDO - {node.host}:{node.port} "
                                        f"(fallos: {node.failed_attempts})")
                        node.is_up = False
                        
                        # Intentar revivir el contenedor usando Docker
                        self._attempt_container_revival(node)
    
    def _attempt_container_revival(self, node: NodeStatus):
        """Intenta revivir un contenedor caído usando Docker CLI"""
        # Obtener container ID si no lo tenemos
        if not node.container_id:
            # Intentar con diferentes variantes del nombre
            container_id = None
            
            # Variante 1: Nombre original
            container_id = self.get_container_id_by_name(node.name)
            
            # Variante 2: Nombre con guiones en lugar de underscores (container_name style)
            if not container_id:
                name_with_dashes = node.name.replace('_', '-')
                container_id = self.get_container_id_by_name(name_with_dashes)
            
            # Variante 3: Con prefijo coffee- (container_name completo)
            if not container_id:
                name_with_prefix = f"coffee-{node.name.replace('_', '-')}"
                container_id = self.get_container_id_by_name(name_with_prefix)
            
            # Variante 4: Nombre del host
            if not container_id and node.host != node.name:
                container_id = self.get_container_id_by_name(node.host)
            
            if container_id:
                node.container_id = container_id
                logger.info(f"[REVIVAL] Contenedor encontrado: {node.name} -> {container_id[:12]}")
            else:
                logger.warning(f"[REVIVAL] No se encontró contenedor para {node.name}")
                return
        
        # Verificar si el contenedor está corriendo a nivel Docker
        if self.is_container_running(node.container_id):
            logger.debug(f"[REVIVAL] Contenedor {node.name} está corriendo en Docker, "
                        "pero no responde healthcheck")
            return
        
        # Intentar levantar el contenedor
        node.revival_attempts += 1
        logger.info(f"[REVIVAL] Intento #{node.revival_attempts} de revivir {node.name}")
        
        success = self.start_container(node.container_id, node.name)
        
        if success:
            # Esperar un poco para que el contenedor se inicialice
            time.sleep(2)
            # Verificar si ahora responde healthcheck
            if self.check_node(node):
                logger.info(f"[REVIVAL] ✓✓ {node.name} revivido y respondiendo!")
                node.is_up = True
                node.failed_attempts = 0
                node.revival_attempts = 0
            else:
                logger.warning(f"[REVIVAL] Contenedor {node.name} levantado pero aún no responde healthcheck")
    
    def update_container_mapping(self):
        """Actualiza el mapeo de nombres de nodo a IDs de contenedor"""
        if self.state != MonitorState.LEADER:
            return
        
        # Obtener todos los contenedores con label role=node
        containers = self.get_container_ids_by_label('role=node')
        
        with self.lock:
            for node_name, node in self.nodes.items():
                # Buscar el contenedor por nombre con diferentes estrategias
                for container_name, container_id in containers.items():
                    # Normalizar nombres para comparación (reemplazar - por _ y _ por -)
                    normalized_node = node.name.replace('_', '-').replace('-', '_')
                    normalized_container = container_name.replace('_', '-').replace('-', '_')
                    
                    # Estrategias de match:
                    # 1. Match exacto del nombre del nodo
                    if node.name in container_name:
                        node.container_id = container_id
                        logger.debug(f"Mapeado {node.name} -> {container_name} ({container_id[:12]})")
                        break
                    
                    # 2. Match con nombre normalizado (underscores ↔ guiones)
                    if normalized_node in normalized_container:
                        node.container_id = container_id
                        logger.debug(f"Mapeado {node.name} -> {container_name} ({container_id[:12]})")
                        break
                    
                    # 3. Match del nombre del nodo con guiones reemplazados
                    node_with_dashes = node.name.replace('_', '-')
                    if node_with_dashes in container_name:
                        node.container_id = container_id
                        logger.debug(f"Mapeado {node.name} -> {container_name} ({container_id[:12]})")
                        break
                    
                    # 4. Match del host name
                    if node.host in container_name:
                        node.container_id = container_id
                        logger.debug(f"Mapeado {node.name} -> {container_name} ({container_id[:12]})")
                        break
    
    def leader_loop(self):
        """Loop del líder: verifica nodos y sincroniza estado"""
        while not self.shutdown_event.is_set() and self.state == MonitorState.LEADER:
            try:
                self.check_all_nodes()
                self._send_state_sync()
                self.print_status()
                self.shutdown_event.wait(self.check_interval)
            except Exception as e:
                logger.error(f"Error en leader loop: {e}")
                time.sleep(1)
    
    def heartbeat_loop(self):
        """Loop para enviar heartbeats (solo líder)"""
        while not self.shutdown_event.is_set():
            try:
                if self.state == MonitorState.LEADER:
                    self._send_heartbeat()
                self.shutdown_event.wait(self.heartbeat_interval)
            except Exception as e:
                logger.error(f"Error en heartbeat loop: {e}")
                time.sleep(1)
    
    def follower_loop(self):
        """Loop del follower: verifica líder y solicita estado si es necesario"""
        while not self.shutdown_event.is_set() and self.state != MonitorState.LEADER:
            try:
                if not self._check_leader_alive():
                    logger.warning(f"Líder {self.current_leader_id} no responde, iniciando elección...")
                    threading.Thread(target=self._start_election, daemon=True).start()
                
                # Solicitar estado periódicamente si no tenemos uno reciente
                if self.current_leader_id:
                    self.comm.send_message(self.current_leader_id, MessageType.STATE_REQUEST)
                
                self.shutdown_event.wait(self.check_interval)
            except Exception as e:
                logger.error(f"Error en follower loop: {e}")
                time.sleep(1)
    
    def print_status(self):
        """Imprime el estado actual"""
        with self.lock:
            state_str = self.state.value
            leader_str = f"Líder: {self.current_leader_id}" if self.current_leader_id else "Sin líder"
            up_count = sum(1 for n in self.nodes.values() if n.is_up)
            down_count = len(self.nodes) - up_count
            
            if self.state == MonitorState.LEADER:
                logger.info(f"[{state_str}] {leader_str} | Nodos: {up_count}↑ {down_count}↓")
                if down_count > 0:
                    for node in self.nodes.values():
                        if not node.is_up:
                            logger.info(f"  ✗ {node.name} ({node.host}:{node.port})")
    
    def start(self):
        """Inicia el monitor"""
        try:
            # Iniciar servidor TCP
            self.comm.start_server()
            
            # Iniciar elección inicial (el primero que se conecta intenta ser líder)
            time.sleep(2.0)  # Esperar que otros monitores se conecten
            threading.Thread(target=self._start_election, daemon=True).start()
            
            # Esperar a que se elija un líder e inicializar mapeo de contenedores
            time.sleep(3.0)
            if self.state == MonitorState.LEADER:
                logger.info("Inicializando mapeo de contenedores Docker...")
                self.update_container_mapping()
            
            # Iniciar loops en threads separados
            threading.Thread(target=self.heartbeat_loop, daemon=True).start()
            threading.Thread(target=self._main_loop, daemon=True).start()
            
            # Mantener el thread principal vivo
            while not self.shutdown_event.is_set():
                self.shutdown_event.wait(1.0)
        except Exception as e:
            logger.error(f"[Monitor {self.monitor_id}] Error crítico en start: {e}", exc_info=True)
            raise
    
    def _main_loop(self):
        """Loop principal que ejecuta leader_loop o follower_loop según el estado"""
        while not self.shutdown_event.is_set():
            try:
                if self.state == MonitorState.LEADER:
                    self.leader_loop()
                else:
                    self.follower_loop()
            except Exception as e:
                logger.error(f"[Monitor {self.monitor_id}] Error en loop principal: {e}", exc_info=True)
                time.sleep(1)
    
    def stop(self):
        """Detiene el monitor"""
        self.shutdown_event.set()
        self.comm.stop()


def load_nodes_from_env() -> List[Tuple[str, str, int]]:
    """Carga lista de nodos desde variables de entorno"""
    nodes_str = os.environ.get('NODES', '')
    if not nodes_str:
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
            name, host = parts
            nodes.append((name, host, HEALTHCHECK_PORT))
    return nodes


def load_monitors_from_env() -> List[Tuple[int, str, int]]:
    """Carga lista de monitores desde variables de entorno"""
    monitors_str = os.environ.get('MONITORS', '')
    if not monitors_str:
        # Default: 3 monitores (usar nombres de servicio de Docker Compose)
        # Los nombres de servicio en docker-compose son monitor_1, monitor_2, monitor_3
        return [
            (1, 'monitor_1', 9999),
            (2, 'monitor_2', 10000),
            (3, 'monitor_3', 10001),
        ]
    
    monitors = []
    for monitor_str in monitors_str.split(','):
        parts = monitor_str.strip().split(':')
        if len(parts) == 3:
            mid, host, port = parts
            monitors.append((int(mid), host, int(port)))
    return monitors


def main():
    """Función principal del monitor"""
    monitor_id = MONITOR_ID
    logger.info(f"Iniciando Monitor Redundante (ID: {monitor_id})...")
    
    # Cargar configuración
    nodes = load_nodes_from_env()
    all_monitors = load_monitors_from_env()
    
    logger.info(f"Monitor ID: {monitor_id}")
    logger.info(f"Nodos a monitorear: {len(nodes)}")
    logger.info(f"Monitores totales: {len(all_monitors)}")
    
    # Crear monitor
    monitor = RedundantHealthMonitor(
        monitor_id=monitor_id,
        nodes=nodes,
        all_monitors=all_monitors,
        tcp_port=MONITOR_TCP_PORT
    )
    
    # Manejar señales
    def signal_handler(signum, frame):
        logger.info(f"Señal {signum} recibida. Deteniendo monitor...")
        monitor.stop()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        monitor.start()
    except KeyboardInterrupt:
        logger.info("Interrupción recibida")
        monitor.stop()
    finally:
        logger.info("Monitor detenido")


if __name__ == "__main__":
    main()
