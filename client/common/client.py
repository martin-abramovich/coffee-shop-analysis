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
        self.sock.settimeout(1.0)
        self.sock.connect((self.host, self.port))

    def send_batch(self, batch):
        """
        Envía un batch de entidades del mismo tipo usando el protocolo binario.
        Retorna True si se envió exitosamente, False si se canceló por stop_event.
        Lanza excepciones de red si falla la comunicación.
        """
        with self._lock:
            if self.stop_event.is_set():
                return False
            
            if not self.sock:
                raise RuntimeError("Cliente no conectado. Llama connect() primero.")
            
            # sendall() maneja short writes automáticamente
            # pero puede lanzar excepciones que el caller debe manejar
            self.sock.sendall(batch)
            return True


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
        """
        Recibe exactamente n bytes (evita short read).
        Maneja timeout para revisar el stop_event.
        """
        buf = b""
        while len(buf) < n:
            if self.stop_event.is_set():
                # Si el evento de cierre está activado, salimos.
                raise RuntimeError("Cierre solicitado durante la recepción.")
                
            try:
                # El recv puede lanzar socket.timeout
                chunk = self.sock.recv(n - len(buf))
            except socket.timeout:
                # Si hay timeout, volvemos a la parte superior del bucle
                # para revisar el stop_event antes de reintentar.
                continue 
            except OSError as e:
                # Capturar otros errores de socket (como ConnectionResetError)
                raise ConnectionError(f"Error de red durante la recepción: {e}")

            if not chunk:
                # El servidor cerró la conexión
                raise ConnectionError("Connection closed unexpectedly")
            
            buf += chunk
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
