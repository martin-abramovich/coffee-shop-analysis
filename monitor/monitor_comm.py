"""
Módulo de comunicación entre monitores.
Usa TCP para garantizar confiabilidad en la comunicación entre monitores.
Serialización manual sin JSON (siguiendo protocol.py).
"""
import socket
import threading
import logging
import time
from typing import Dict, List, Optional, Tuple

from monitor.monitor_protocol import MessageType, encode_message, decode_message

logger = logging.getLogger(__name__)


class MonitorMessage:
    """Mensaje entre monitores (wrapper para compatibilidad)"""
    def __init__(self, message_type: MessageType, sender_id: int, timestamp: float, data: Optional[Dict] = None):
        self.type = message_type
        self.sender_id = sender_id
        self.timestamp = timestamp
        self.data = data


class MonitorCommunication:
    """Maneja la comunicación TCP entre monitores"""
    
    def __init__(self, monitor_id: int, port: int, all_monitors: List[Tuple[int, str, int]]):
        """
        Args:
            monitor_id: ID único de este monitor (usado para elección)
            port: Puerto TCP donde este monitor escucha
            all_monitors: Lista de (id, host, port) de todos los monitores
        """
        self.monitor_id = monitor_id
        self.port = port
        self.all_monitors = all_monitors  # Todos los monitores incluyendo este
        self.other_monitors = [(mid, host, p) for mid, host, p in all_monitors if mid != monitor_id]
        
        self.server_socket = None
        self.running = False
        self.lock = threading.Lock()
        
        # Callbacks para manejar mensajes recibidos
        self.message_handlers = {}
        
    def register_handler(self, message_type: MessageType, handler):
        """Registra un handler para un tipo de mensaje"""
        self.message_handlers[message_type] = handler
    
    def start_server(self):
        """Inicia el servidor TCP para recibir mensajes de otros monitores"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', self.port))
        self.server_socket.listen(10)
        self.server_socket.settimeout(1.0)
        self.running = True
        
        logger.info(f"[Monitor {self.monitor_id}] Servidor TCP iniciado en puerto {self.port}")
        
        def accept_connections():
            while self.running:
                try:
                    conn, addr = self.server_socket.accept()
                    # Manejar conexión en thread separado
                    thread = threading.Thread(
                        target=self._handle_connection,
                        args=(conn, addr),
                        daemon=True
                    )
                    thread.start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        logger.error(f"[Monitor {self.monitor_id}] Error aceptando conexión: {e}")
        
        thread = threading.Thread(target=accept_connections, daemon=True)
        thread.start()
        return thread
    
    def _read_full_message(self, conn: socket.socket) -> Optional[bytes]:
        """
        Lee un mensaje completo del socket.
        Primero lee 4 bytes para saber el tamaño total del mensaje.
        """
        try:
            # Leer tamaño del mensaje (4 bytes)
            size_bytes = b''
            while len(size_bytes) < 4:
                chunk = conn.recv(4 - len(size_bytes))
                if not chunk:
                    return None
                size_bytes += chunk
            
            message_size = int.from_bytes(size_bytes, byteorder='big', signed=False)
            
            # Leer el mensaje completo
            message_data = b''
            while len(message_data) < message_size:
                chunk = conn.recv(message_size - len(message_data))
                if not chunk:
                    return None
                message_data += chunk
            
            return message_data
        except Exception as e:
            logger.debug(f"[Monitor {self.monitor_id}] Error leyendo mensaje: {e}")
            return None
    
    def _handle_connection(self, conn: socket.socket, addr: Tuple[str, int]):
        """Maneja una conexión entrante"""
        try:
            # Leer mensaje completo
            message_data = self._read_full_message(conn)
            if not message_data:
                return
            
            # Decodificar mensaje
            try:
                message_type, sender_id, timestamp, data = decode_message(message_data)
                
                # Crear wrapper para compatibilidad
                msg = MonitorMessage(message_type, sender_id, timestamp, data)
                
                # Llamar handler correspondiente
                if message_type in self.message_handlers:
                    self.message_handlers[message_type](msg, addr)
                else:
                    logger.warning(f"[Monitor {self.monitor_id}] Sin handler para mensaje tipo {message_type}")
                    
            except Exception as e:
                logger.error(f"[Monitor {self.monitor_id}] Error decodificando mensaje de {addr}: {e}")
                
        except Exception as e:
            logger.error(f"[Monitor {self.monitor_id}] Error manejando conexión de {addr}: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
    
    def send_message(self, target_id: int, message_type: MessageType, data: Optional[Dict] = None) -> bool:
        """
        Envía un mensaje a otro monitor.
        
        Returns:
            True si el mensaje se envió exitosamente, False en caso contrario
        """
        # Buscar el monitor objetivo
        target = None
        for mid, host, port in self.all_monitors:
            if mid == target_id:
                target = (host, port)
                break
        
        if not target:
            logger.warning(f"[Monitor {self.monitor_id}] Monitor {target_id} no encontrado")
            return False
        
        host, port = target
        
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)  # Timeout corto para no bloquear
            sock.connect((host, port))
            
            # Codificar mensaje
            message_bytes = encode_message(message_type, self.monitor_id, time.time(), data)
            
            # Enviar tamaño del mensaje primero (4 bytes)
            size_bytes = len(message_bytes).to_bytes(4, byteorder='big', signed=False)
            sock.sendall(size_bytes)
            
            # Enviar mensaje
            sock.sendall(message_bytes)
            return True
            
        except socket.timeout:
            logger.debug(f"[Monitor {self.monitor_id}] Timeout enviando a monitor {target_id}")
            return False
        except Exception as e:
            logger.debug(f"[Monitor {self.monitor_id}] Error enviando a monitor {target_id}: {e}")
            return False
        finally:
            if sock:
                try:
                    sock.close()
                except:
                    pass
    
    def broadcast_to_higher(self, message_type: MessageType, data: Optional[Dict] = None) -> List[int]:
        """
        Envía un mensaje a todos los monitores con ID mayor.
        
        Returns:
            Lista de IDs que respondieron exitosamente
        """
        responded = []
        for mid, host, port in self.other_monitors:
            if mid > self.monitor_id:
                if self.send_message(mid, message_type, data):
                    responded.append(mid)
        return responded
    
    def broadcast_to_all(self, message_type: MessageType, data: Optional[Dict] = None) -> List[int]:
        """
        Envía un mensaje a todos los otros monitores.
        
        Returns:
            Lista de IDs que recibieron el mensaje exitosamente
        """
        responded = []
        for mid, host, port in self.other_monitors:
            if self.send_message(mid, message_type, data):
                responded.append(mid)
        return responded
    
    def stop(self):
        """Detiene el servidor"""
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
