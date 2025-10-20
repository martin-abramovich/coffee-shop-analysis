import socket
import threading
from typing import List, Union
from .protocol import encode_batch
from .entities import Transactions, TransactionItems, Users, Stores, MenuItem

class Client:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock = None
        self._shutdown_requested = False
        self._lock = threading.Lock()

    def connect(self):
        """Establece conexión con el servidor"""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        
        # Verificar si el servidor rechaza la conexión inmediatamente
        # El servidor puede enviar un mensaje de rechazo si está lleno
        # Usamos un timeout de 2 segundos para dar tiempo a que el servidor responda
        self.sock.settimeout(2.0)  # Timeout de 2 segundos para detectar rechazo
        try:
            # Intentar leer 4 bytes (longitud del mensaje)
            raw_len = self.sock.recv(4)
            if raw_len:
                # Hay datos disponibles, probablemente es un rechazo
                msg_len = int.from_bytes(raw_len, byteorder="big")
                
                # Leer el mensaje completo
                data = self._recv_exact(msg_len)
                message = data.decode("utf-8")
                
                # Si el mensaje comienza con "REJECTED", es un rechazo
                if message.startswith("REJECTED"):
                    self.sock.close()
                    self.sock = None
                    raise ConnectionRefusedError(message)
            elif raw_len == b'':
                # El servidor cerró la conexión sin enviar mensaje
                self.sock.close()
                self.sock = None
                raise ConnectionRefusedError("REJECTED: Servidor rechazó la conexión (cerró inmediatamente)")
        except socket.timeout:
            # No hay datos disponibles en 2 segundos, la conexión fue aceptada normalmente
            pass
        except ConnectionRefusedError:
            # Propagar el error de rechazo
            raise
        except Exception as e:
            # Cualquier otro error durante la verificación
            if self.sock:
                self.sock.close()
                self.sock = None
            raise ConnectionError(f"Error verificando conexión: {e}")
        finally:
            # Restaurar el socket a modo bloqueante sin timeout
            if self.sock:
                self.sock.settimeout(None)

    def send_batch(self, entities: List[Union[Transactions, TransactionItems, Users, Stores, MenuItem]]):
        """
        Envía un batch de entidades del mismo tipo usando el protocolo binario.
        """
        with self._lock:
            if self._shutdown_requested:
                raise RuntimeError("Cliente en proceso de cierre.")
            
            if not self.sock:
                raise RuntimeError("Cliente no conectado. Llama connect() primero.")
            
            try:
                data = encode_batch(entities)
                self.sock.sendall(data)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
                # La conexión fue cerrada por el servidor
                self.sock = None
                raise ConnectionRefusedError(
                    "REJECTED: El servidor cerró la conexión. "
                    "Probablemente se alcanzó el límite de clientes concurrentes."
                ) from e
            except OSError as e:
                # Manejar errores de socket en general
                if e.errno in (32, 104, 54):  # EPIPE, ECONNRESET, ECONNRESET (macOS)
                    self.sock = None
                    raise ConnectionRefusedError(
                        "REJECTED: El servidor cerró la conexión. "
                        "Probablemente se alcanzó el límite de clientes concurrentes."
                    ) from e
                raise

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
        with self._lock:
            if self._shutdown_requested:
                raise RuntimeError("Cliente en proceso de cierre.")
                
            if not self.sock:
                raise RuntimeError("Cliente no conectado. Llama connect() primero.")
                
            # Señalar al servidor que no enviaremos más datos (half-close de escritura)
            try:
                self.sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass

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

    def request_shutdown(self):
        """Solicita el cierre ordenado del cliente"""
        with self._lock:
            self._shutdown_requested = True
            print("\n🔄 Solicitud de cierre recibida. Cerrando conexión...")

    def close(self):
        """Cierra la conexión"""
        with self._lock:
            if self.sock:
                try:
                    # Intentar cerrar elegantemente
                    self.sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    # Socket ya cerrado o en mal estado
                    pass
                finally:
                    self.sock.close()
                    self.sock = None
                    print("✓ Conexión cerrada correctamente")

    def is_shutdown_requested(self) -> bool:
        """Verifica si se solicitó el cierre"""
        with self._lock:
            return self._shutdown_requested

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
