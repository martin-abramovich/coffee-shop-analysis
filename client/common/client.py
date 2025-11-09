import socket
import threading

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

    def send_batch(self, batch):
        """
        Envía un batch de entidades del mismo tipo usando el protocolo binario.
        """
        with self._lock:
            if self._shutdown_requested:
                raise RuntimeError("Cliente en proceso de cierre.")
            
            if not self.sock:
                raise RuntimeError("Cliente no conectado. Llama connect() primero.")
            
            self.sock.sendall(batch)


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
