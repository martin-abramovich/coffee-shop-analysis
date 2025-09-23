"""
Módulo de mensajería TCP simple para comunicación con el middleware
"""
import socket
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class TCPMessaging:
    """
    Cliente TCP simple para enviar mensajes al middleware
    """
    
    def __init__(self, host: str = "localhost", port: int = 8000):
        self.host = host
        self.port = port
        self.connection = None
        
    def connect(self):
        """Establece conexión TCP con el middleware"""
        try:
            self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connection.connect((self.host, self.port))
            logger.info(f"Conectado al middleware en {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Error conectando al middleware: {e}")
            raise
    
    def disconnect(self):
        """Cierra la conexión con el middleware"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Desconectado del middleware")
    
    def send(self, message: str):
        """
        Envía un mensaje al middleware via TCP
        
        Args:
            message: Mensaje a enviar (string)
        """
        if not self.connection:
            self.connect()
        
        try:
            # Enviar el mensaje con delimitador
            data = message + ";"
            self.connection.sendall(data.encode("utf-8"))
            logger.debug(f"Mensaje enviado al middleware: {len(message)} caracteres")
            
        except Exception as e:
            logger.error(f"Error enviando mensaje al middleware: {e}")
            # Intentar reconectar
            try:
                self.disconnect()
                self.connect()
                data = message + ";"
                self.connection.sendall(data.encode("utf-8"))
                logger.info("Mensaje enviado después de reconectar")
            except Exception as e2:
                logger.error(f"Error crítico enviando mensaje: {e2}")
                raise
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
