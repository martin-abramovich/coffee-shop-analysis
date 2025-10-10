import socket
import struct
import sys
import os
import threading
import time
import uuid
from datetime import datetime, timezone

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def log_with_timestamp(message):
    """Función para logging con timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] {message}")

from gateway.processor import process_batch_by_type
from gateway.serializer import serialize_message

# Wrapper para manejar round-robin en exchanges escalables
class ScalableExchangeWrapper:
    """Wrapper que distribuye mensajes con round-robin y hace broadcast de EOS
    
    Cada thread tiene su propia instancia, por lo que NO necesita locks.
    """
    def __init__(self, exchange, num_workers):
        self.exchange = exchange
        self.num_workers = num_workers
        self.current_worker = 0
    
    def send(self, msg, is_eos=False):
        """Envía mensaje con round-robin o broadcast si es EOS"""
        if is_eos:
            # Broadcast: enviar a todos los workers usando routing key "eos"
            # Cada worker estará escuchando tanto su routing key específica como "eos"
            self.exchange.channel.basic_publish(
                exchange=self.exchange.exchange_name,
                routing_key="eos",
                body=msg
            )
        else:
            # Round-robin: enviar solo al worker actual
            routing_key = f"worker_{self.current_worker}"
            self.exchange.channel.basic_publish(
                exchange=self.exchange.exchange_name,
                routing_key=routing_key,
                body=msg
            )
            # Avanzar al siguiente worker
            self.current_worker = (self.current_worker + 1) % self.num_workers
    
    def close(self):
        self.exchange.close()

HOST = "0.0.0.0"
PORT = 9000

# Tipos de entidades según el protocolo
ENTITY_TYPES = {
    0: "transactions",
    1: "transaction_items", 
    2: "users",
    3: "stores",
    4: "menu_items"
}

def read_string(data, offset):
    """Lee un string del buffer: 4 bytes de longitud + string"""
    if offset + 4 > len(data):
        raise ValueError("Datos insuficientes para leer longitud de string")
    
    length = int.from_bytes(data[offset:offset+4], byteorder='big')
    
    offset += 4
    if offset + length > len(data):
        raise ValueError("Datos insuficientes para leer string")
    string_data = data[offset:offset+length].decode('utf-8')
    return string_data, offset + length

def read_uint32(data, offset):
    """Lee un uint32 de 4 bytes (big-endian, sin struct)"""
    if offset + 4 > len(data):
        raise ValueError("Datos insuficientes para leer uint32")
    value = int.from_bytes(data[offset:offset+4], byteorder='big', signed=False)
    return value, offset + 4

def read_float(data, offset):
    """Lee un float de 4 bytes (IEEE 754, big-endian, sin struct)"""
    if offset + 4 > len(data):
        raise ValueError("Datos insuficientes para leer float")
    import array, sys
    raw = data[offset:offset+4]
    # Si la plataforma es little-endian, invierte los bytes
    if sys.byteorder == 'little':
        raw = raw[::-1]
    arr = array.array('f')
    arr.frombytes(raw)
    return arr[0], offset + 4
    
def read_int(data, offset):
    """Lee un int (usamos uint32 del cliente) de 4 bytes"""
    return read_uint32(data, offset)

def read_uint64(data, offset):
    """Lee un uint64 de 8 bytes (big-endia)"""
    if offset + 8 > len(data):
        raise ValueError("Datos insuficientes para leer uint64")
    value = int.from_bytes(data[offset:offset+8], byteorder='big', signed=False)
    return value, offset + 8

def read_bool(data, offset):
    """Lee un bool de 1 byte"""
    if offset + 1 > len(data):
        raise ValueError("Datos insuficientes para leer bool")
    value = data[offset] != 0
    return value, offset + 1

def read_datetime_as_iso(data, offset):
    """Lee un timestamp uint64 y lo devuelve como ISO UTC string"""
    ts, new_offset = read_uint64(data, offset)
    if ts == 0:
        # Tratar 0 como "sin fecha"
        return "", new_offset
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.isoformat(), new_offset

def read_date_as_iso(data, offset):
    """Lee un date como timestamp (mismo formato) y devuelve YYYY-MM-DD"""
    ts, new_offset = read_uint64(data, offset)
    if ts == 0:
        # Tratar 0 como "sin fecha"
        return "", new_offset
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.date().isoformat(), new_offset

def parse_batch(data):
    """
    Parsea un batch binario según el protocolo:
    - 4 bytes: cantidad de items
    - 1 byte: tipo de entidad (0-4)
    - Para cada item: campos según el tipo
    """
    offset = 0
    
    # Leer cantidad de items (4 bytes)
    if len(data) < 4:
        raise ValueError("Datos insuficientes para leer cantidad de items")
    
    item_count, offset = read_uint32(data, offset)
    
    # Leer tipo de entidad (1 byte)
    if len(data) < offset + 1:
        raise ValueError("Datos insuficientes para leer tipo de entidad")
    
    entity_type_byte = data[offset]
    offset += 1
    
    if entity_type_byte not in ENTITY_TYPES:
        raise ValueError(f"Tipo de entidad inválido: {entity_type_byte}")
    
    entity_type = ENTITY_TYPES[entity_type_byte]
    
    # Parsear items según el tipo
    items = []
    for i in range(item_count):
        # Si falla el parseo de un item, propagamos el error para que el caller
        # pueda decidir si esperar más datos (caso datos insuficientes)
        item, offset = parse_item(data, offset, entity_type)
        items.append(item)
    
    return entity_type, items, offset

def parse_item(data, offset, entity_type):
    """Parsea un item individual según su tipo"""
    item = {}
    
    if entity_type == "transactions":
        # transaction_id (str), store_id (str), payment_method (str), voucher_id (str),
        # user_id (str), original_amount (f32), discount_applied (f32), final_amount (f32), created_at (u64)
        trans_id, offset = read_string(data, offset)
        store_id, offset = read_string(data, offset)
        _payment_method, offset = read_string(data, offset)
        _voucher_id, offset = read_string(data, offset)
        user_id, offset = read_string(data, offset)
        _original_amount, offset = read_float(data, offset)
        _discount_applied, offset = read_float(data, offset)
        final_amount, offset = read_float(data, offset)
        created_at_iso, offset = read_datetime_as_iso(data, offset)
        # Conservar solo los campos necesarios para el reducer
        item['transaction_id'] = trans_id
        item['store_id'] = store_id
        item['user_id'] = user_id
        item['final_amount'] = final_amount
        item['created_at'] = created_at_iso
        
    elif entity_type == "transaction_items":
        # transaction_id (str), item_id (str), quantity (u32), unit_price (f32), subtotal (f32), created_at (u64)
        trans_id, offset = read_string(data, offset)
        item_id, offset = read_string(data, offset)
        quantity, offset = read_int(data, offset)
        _unit_price, offset = read_float(data, offset)
        subtotal, offset = read_float(data, offset)
        created_at_iso, offset = read_datetime_as_iso(data, offset)
        item['transaction_id'] = trans_id
        item['item_id'] = item_id
        item['quantity'] = quantity
        item['subtotal'] = subtotal
        item['created_at'] = created_at_iso
        
    elif entity_type == "users":
        # user_id (str), gender (str), birthdate (u64), registered_at (u64)
        user_id, offset = read_string(data, offset)
        _gender, offset = read_string(data, offset)
        birthdate_iso, offset = read_date_as_iso(data, offset)
        _registered_at_iso, offset = read_datetime_as_iso(data, offset)
        item['user_id'] = user_id
        item['birthdate'] = birthdate_iso
        
    elif entity_type == "stores":
        # store_id (str), store_name (str), street (str), postal_code (str), city (str), state (str), latitude (f32), longitude (f32)
        store_id, offset = read_string(data, offset)
        store_name, offset = read_string(data, offset)
        _street, offset = read_string(data, offset)
        _postal_code, offset = read_string(data, offset)
        _city, offset = read_string(data, offset)
        _state, offset = read_string(data, offset)
        _lat, offset = read_float(data, offset)
        _lon, offset = read_float(data, offset)
        item['store_id'] = store_id
        item['store_name'] = store_name
        
    elif entity_type == "menu_items":
        # item_id (str), item_name (str), category (str), price (f32), is_seasonal (1B),
        # has_available_from (1B), [available_from (u64)], has_available_to (1B), [available_to (u64)]
        item_id, offset = read_string(data, offset)
        item_name, offset = read_string(data, offset)
        _category, offset = read_string(data, offset)
        price, offset = read_float(data, offset)
        _is_seasonal, offset = read_bool(data, offset)
        has_from, offset = read_bool(data, offset)
        if has_from:
            _available_from, offset = read_date_as_iso(data, offset)
        has_to, offset = read_bool(data, offset)
        if has_to:
            _available_to, offset = read_date_as_iso(data, offset)
        item['item_id'] = item_id
        item['item_name'] = item_name
        # Incluir price para pasar validación en gateway.processor.validate_menu_item
        item['price'] = price
    
    return item, offset

def send_eos_to_worker_simple(session_id, entity_type, thread_mq_map, scalable_exchanges):
    """
    Envía EOS a un worker específico tan pronto como termina de procesar ese tipo de entidad.
    Usa las conexiones del thread (thread_mq_map) que son seguras sin locks adicionales.
    """
    # Preparar mensaje EOS con ID de sesión
    eos_msg = serialize_message(
        [], 
        stream_id=f"session_{session_id}",
        batch_id=f"s{session_id}_EOS_{entity_type}", 
        is_batch_end=True,
        is_eos=True,
        session_id=session_id
    )
    
    try:
        # Usar wrapper escalable si existe para hacer broadcast de EOS
        if entity_type in scalable_exchanges:
            scalable_exchanges[entity_type].send(eos_msg, is_eos=True)
            print(f"[GATEWAY] Sesión {session_id}: ✅ EOS enviado a workers de {entity_type}")
        else:
            # Usar conexión del thread (thread-safe por diseño, sin lock necesario)
            thread_mq_map[entity_type].send(eos_msg)
            print(f"[GATEWAY] Sesión {session_id}: ✅ EOS enviado a {entity_type}")
    except Exception as e:
        print(f"[GATEWAY] Sesión {session_id}: ❌ Error enviando EOS a {entity_type}: {e}")


# Control global de sesiones activas
active_sessions = {}
sessions_lock = threading.Lock()

class ClientSession:
    """Representa una sesión de cliente con su propio contexto"""
    def __init__(self, session_id, addr):
        self.session_id = session_id
        self.addr = addr
        self.start_time = time.time()
        self.batch_count = 0
        self.total_processed = 0
        self.is_active = True
        
    def get_stats(self):
        duration = time.time() - self.start_time
        return {
            'session_id': self.session_id,
            'addr': str(self.addr),
            'duration': duration,
            'batch_count': self.batch_count,
            'total_processed': self.total_processed,
            'is_active': self.is_active
        }

def handle_client(conn, addr, mq_map):
    # Generar ID único para esta sesión
    session_id = str(uuid.uuid4())[:8]
    session = ClientSession(session_id, addr)
    
    # Registrar sesión activa
    with sessions_lock:
        active_sessions[session_id] = session
    
    log_with_timestamp(f"[GATEWAY] Nueva sesión {session_id} desde {addr}")
    log_with_timestamp(f"[GATEWAY] Sesiones activas: {len(active_sessions)}")
    
    buffer = b""
    batch_id = 0
    
    # Extraer configuración de escalado
    config = mq_map.get("_config", {})
    num_filter_year_workers = config.get("num_filter_year_workers", 1)
    
    # CREAR CONEXIONES PROPIAS PARA ESTE THREAD (no compartir conexiones)
    # Esto evita problemas de "Channel is closed" en concurrencia
    from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
    import os
    
    RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    filter_year_route_keys = [f"worker_{i}" for i in range(num_filter_year_workers)] + ["eos"]
    
    # Crear conexiones exclusivas para este thread
    thread_mq_map = {}
    try:
        thread_mq_map["transactions"] = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name="transactions_raw",
            route_keys=filter_year_route_keys
        )
        thread_mq_map["transaction_items"] = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name="transaction_items_raw",
            route_keys=filter_year_route_keys
        )
        thread_mq_map["users"] = MessageMiddlewareQueue(
            host=RABBITMQ_HOST, 
            queue_name="users_raw"
        )
        thread_mq_map["menu_items"] = MessageMiddlewareQueue(
            host=RABBITMQ_HOST, 
            queue_name="menu_items_raw"
        )
        thread_mq_map["stores"] = MessageMiddlewareExchange(
            host=RABBITMQ_HOST,
            exchange_name="stores_raw",
            route_keys=["q3", "q4"]
        )
        
        log_with_timestamp(f"[GATEWAY] Sesión {session_id}: Conexiones RabbitMQ creadas para este thread")
        
    except Exception as e:
        log_with_timestamp(f"[GATEWAY] Sesión {session_id}: Error creando conexiones: {e}")
        conn.close()
        return
    
    # Crear wrappers escalables con las conexiones de este thread
    scalable_exchanges = {}
    scalable_exchanges["transactions"] = ScalableExchangeWrapper(
        thread_mq_map["transactions"], 
        num_filter_year_workers
    )
    scalable_exchanges["transaction_items"] = ScalableExchangeWrapper(
        thread_mq_map["transaction_items"], 
        num_filter_year_workers
    )
    
    try:
        while True:
            data = conn.recv(4096)  # Buffer más grande para datos binarios
            if not data:
                break

            buffer += data

            # Procesar batches completos
            while len(buffer) >= 5:  # Mínimo: 4 bytes (cantidad) + 1 byte (tipo)
                try:
                    # Intentar parsear el batch y obtener el offset final real
                    entity_type, items, end_offset = parse_batch(buffer)
                    
                    # Remover el batch procesado del buffer exactamente
                    buffer = buffer[end_offset:]
                    
                    # Procesar el batch
                    batch_id += 1
                    session.batch_count += 1
                    processed_items = process_batch_by_type(items, entity_type)
                    
                    if processed_items:
                        session.total_processed += len(processed_items)
                        # Solo imprimir cada 1000 batches para reducir logs
                        if batch_id % 1000 == 0:
                            print(f"[GATEWAY] Sesión {session_id}: Batch {batch_id} de tipo {entity_type} procesado ({len(processed_items)} registros)")
                            print(f"[GATEWAY] Sesión {session_id}: Total procesado hasta ahora: {session.total_processed} registros")

                        # Serializar y enviar al middleware con ID de sesión
                        msg = serialize_message(
                            processed_items,
                            stream_id=f"session_{session_id}",
                            batch_id=f"s{session_id}_b{batch_id:04d}",
                            is_batch_end=True,
                            is_eos=False,
                            session_id=session_id
                        )
                        
                        # Usar wrapper escalable si existe, sino usar mq_map directamente
                        if entity_type in scalable_exchanges:
                            # Enviar con round-robin para exchanges escalables
                            try:
                                scalable_exchanges[entity_type].send(msg, is_eos=False)
                            except Exception as e:
                                print(f"[GATEWAY] Sesión {session_id}: Error enviando al middleware ({entity_type}): {e}")
                        else:
                            # Envío normal para colas/exchanges no escalables (usando conexiones del thread)
                            target_mq = thread_mq_map.get(entity_type)
                            if not target_mq:
                                print(f"[GATEWAY] No hay cola configurada para tipo {entity_type}, descartando batch")
                            else:
                                try:
                                    # Usar conexión propia del thread (thread-safe por diseño)
                                    target_mq.send(msg)
                                except Exception as e:
                                    print(f"[GATEWAY] Error enviando al middleware ({entity_type}): {e}")
                                    # Si es error de conexión, intentar reconectar
                                    if "conexión" in str(e).lower() or "connection" in str(e).lower():
                                        print(f"[GATEWAY] Intentando reconectar {entity_type}...")
                                        try:
                                            # Recrear la conexión en el thread
                                            if entity_type == "stores":
                                                thread_mq_map[entity_type] = MessageMiddlewareExchange(
                                                    host=RABBITMQ_HOST, 
                                                    exchange_name="stores_raw",
                                                    route_keys=["q3", "q4"]
                                                )
                                            else:
                                                thread_mq_map[entity_type] = MessageMiddlewareQueue(
                                                    host=RABBITMQ_HOST, 
                                                    queue_name=f"{entity_type}_raw"
                                                )
                                            print(f"[GATEWAY] Reconectado {entity_type}, reintentando envío...")
                                            target_mq = thread_mq_map[entity_type]
                                            target_mq.send(msg)
                                            print(f"[GATEWAY] Reenvío exitoso para {entity_type}")
                                        except Exception as retry_e:
                                            print(f"[GATEWAY] Error en reconexión de {entity_type}: {retry_e}")
                                            # Continuar sin este mensaje

                except ValueError as e:
                    # Si faltan datos del batch, esperar más datos sin limpiar el buffer
                    msg = str(e)
                    if "Datos insuficientes" in msg:
                        break
                    else:
                        print(f"[GATEWAY] Error procesando batch: {e}")
                        buffer = b""
                        break
                except Exception as e:
                    print(f"[GATEWAY] Error procesando batch: {e}")
                    # En caso de error, limpiar el buffer y continuar
                    buffer = b""
                    break

    except Exception as e:
        print(f"[GATEWAY] Sesión {session_id}: Error en conexión con {addr}: {e}")
    finally:
        # Marcar sesión como inactiva
        session.is_active = False
        
        print(f"[GATEWAY] Sesión {session_id}: Enviando EOS a workers...")
        
        # Enviar EOS a cada tipo de worker que procesamos
        for entity_type in ["transactions", "transaction_items", "users", "stores", "menu_items"]:
            if entity_type in thread_mq_map:
                send_eos_to_worker_simple(session_id, entity_type, thread_mq_map, scalable_exchanges)
        
        
        # Enviar ACK final al cliente (4 bytes longitud + payload UTF-8)
        try:
            summary = f"OK session={session_id} batches={session.batch_count} total_records={session.total_processed}"
            payload = summary.encode('utf-8')
            header = len(payload).to_bytes(4, byteorder='big')
            conn.sendall(header + payload)
        except Exception:
            pass
        # Cerrar conexiones del thread
        print(f"[GATEWAY] Sesión {session_id}: Cerrando conexiones RabbitMQ...")
        for entity_type, mq in thread_mq_map.items():
            try:
                mq.close()
            except Exception as e:
                print(f"[GATEWAY] Sesión {session_id}: Error cerrando {entity_type}: {e}")
        
        try:
            conn.close()
        finally:
            # Remover sesión de las activas
            with sessions_lock:
                if session_id in active_sessions:
                    del active_sessions[session_id]
            
            duration = time.time() - session.start_time
            print(f"[GATEWAY] Sesión {session_id} cerrada. Duración: {duration:.2f}s, Total procesado: {session.total_processed} registros en {session.batch_count} batches")
            print(f"[GATEWAY] Sesiones activas restantes: {len(active_sessions)}")
