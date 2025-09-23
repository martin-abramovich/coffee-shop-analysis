import socket
from typing import List, Union
from .protocol import encode_batch
from .entities import Transactions, TransactionItems, Users, Stores, MenuItem

class Client:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        """Establece conexión con el servidor"""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))

    def send_batch(self, entities: List[Union[Transactions, TransactionItems, Users, Stores, MenuItem]]):
        """
        Envía un batch de entidades del mismo tipo usando el protocolo binario.
        """
        if not self.sock:
            raise RuntimeError("Cliente no conectado. Llama connect() primero.")
        
        data = encode_batch(entities)
        self.sock.sendall(data)

    def send_batches(self, batches: List[List[Union[Transactions, TransactionItems, Users, Stores, MenuItem]]]):
        """
        Envía múltiples batches de entidades.
        """
        for batch in batches:
            self.send_batch(batch)

    def receive_response(self) -> str:
        """
        Bloquea hasta recibir la respuesta final del servidor.
        Asumimos que el servidor envía primero 4 bytes (header) con el tamaño del mensaje.
        """
        if not self.sock:
            raise RuntimeError("Cliente no conectado. Llama connect() primero.")
            
        raw_len = self._recv_exact(4)
        msg_len = int.from_bytes(raw_len, byteorder="big")
        data = self._recv_exact(msg_len)
        return data.decode("utf-8")

    def _recv_exact(self, n: int) -> bytes:
        """Recibe exactamente n bytes (evita short read)."""
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Connection closed unexpectedly")
            buf += chunk
        return buf

    def close(self):
        """Cierra la conexión"""
        if self.sock:
            self.sock.close()
            self.sock = None

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
