import socket
import threading

class Client:
    def __init__(self, host: str, port: int, stop_event: threading.Event):
        self.host = host
        self.port = port
        self.sock = None
        self.stop_event = stop_event
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
            if self.stop_event.is_set():
                return 
            
            if not self.sock:
                raise RuntimeError("Cliente no conectado. Llama connect() primero.")
            
            self.sock.sendall(batch)


    def receive_response(self) -> str:
        """
        Bloquea hasta recibir la respuesta final del servidor.
        Asumimos que el servidor envía primero 4 bytes (header) con el tamaño del mensaje.
        """
        with self._lock:
            if self.stop_event.is_set():
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
        """Recibe exactamente n bytes (evita short read). Respeta stop_event."""
        buf = b""
        # Configurar timeout para que recv() no bloquee indefinidamente
        self.sock.settimeout(1.0)
        
        while len(buf) < n:
            # Verificar si se solicitó detener
            if self.stop_event.is_set():
                raise RuntimeError("Cliente detenido por usuario")
            
            try:
                chunk = self.sock.recv(n - len(buf))
                if not chunk:
                    raise ConnectionError("Connection closed unexpectedly")
                buf += chunk
            except socket.timeout:
                # Timeout esperado, continuar esperando
                continue
            
        return buf

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


    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
