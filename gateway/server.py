import socket
import struct
from datetime import datetime, timezone
from processor import process_batch_by_type
from serializer import serialize_message

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
    
    length = struct.unpack('>I', data[offset:offset+4])[0]
    offset += 4
    
    if offset + length > len(data):
        raise ValueError("Datos insuficientes para leer string")
    
    string_data = data[offset:offset+length].decode('utf-8')
    return string_data, offset + length

def read_uint32(data, offset):
    """Lee un uint32 de 4 bytes (big-endian)"""
    if offset + 4 > len(data):
        raise ValueError("Datos insuficientes para leer uint32")
    value = struct.unpack('>I', data[offset:offset+4])[0]
    return value, offset + 4

def read_float(data, offset):
    """Lee un float de 4 bytes"""
    if offset + 4 > len(data):
        raise ValueError("Datos insuficientes para leer float")
    
    value = struct.unpack('>f', data[offset:offset+4])[0]
    return value, offset + 4

def read_int(data, offset):
    """Lee un int (usamos uint32 del cliente) de 4 bytes"""
    # El cliente envía cantidad como uint32, usamos el mismo formato
    return read_uint32(data, offset)

def read_uint64(data, offset):
    """Lee un uint64 de 8 bytes (big-endian)"""
    if offset + 8 > len(data):
        raise ValueError("Datos insuficientes para leer uint64")
    value = struct.unpack('>Q', data[offset:offset+8])[0]
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
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.isoformat(), new_offset

def read_date_as_iso(data, offset):
    """Lee un date como timestamp (mismo formato) y devuelve YYYY-MM-DD"""
    ts, new_offset = read_uint64(data, offset)
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
        _price, offset = read_float(data, offset)
        _is_seasonal, offset = read_bool(data, offset)
        has_from, offset = read_bool(data, offset)
        if has_from:
            _available_from, offset = read_date_as_iso(data, offset)
        has_to, offset = read_bool(data, offset)
        if has_to:
            _available_to, offset = read_date_as_iso(data, offset)
        item['item_id'] = item_id
        item['item_name'] = item_name
    
    return item, offset

def handle_client(conn, addr, mq_map):
    print(f"[GATEWAY] Conexión de {addr}")
    buffer = b""
    batch_id = 0
    total_processed = 0
    batch_count = 0
    
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
                    batch_count += 1
                    processed_items = process_batch_by_type(items, entity_type)
                    
                    if processed_items:
                        total_processed += len(processed_items)
                        print(f"[GATEWAY] Batch {batch_id} de tipo {entity_type} procesado ({len(processed_items)} registros)")
                        print(f"[GATEWAY] Total procesado hasta ahora: {total_processed} registros")

                        # Serializar y enviar al middleware
                        msg = serialize_message(
                            processed_items,
                            stream_id="default",
                            batch_id=f"b{batch_id:04d}",
                            is_batch_end=True,
                            is_eos=False
                        )
                        # Seleccionar cola por tipo de entidad
                        target_mq = mq_map.get(entity_type)
                        if not target_mq:
                            print(f"[GATEWAY] No hay cola configurada para tipo {entity_type}, descartando batch")
                        else:
                            try:
                                target_mq.send(msg)
                            except Exception as e:
                                print(f"[GATEWAY] Error enviando al middleware ({entity_type}): {e}")

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
        print(f"[GATEWAY] Error en conexión con {addr}: {e}")
    finally:
        # Enviar EOS (End of Stream) a todas las colas cuando termina el cliente
        print(f"[GATEWAY] Enviando End of Stream a todas las colas...")
        for entity_type, mq in mq_map.items():
            try:
                eos_msg = serialize_message(
                    [], 
                    stream_id="default",
                    batch_id="EOS", 
                    is_batch_end=True,
                    is_eos=True
                )
                mq.send(eos_msg)
                print(f"[GATEWAY] EOS enviado a cola {entity_type}")
            except Exception as e:
                print(f"[GATEWAY] Error enviando EOS a {entity_type}: {e}")
        
        # Enviar ACK final al cliente (4 bytes longitud + payload UTF-8)
        try:
            summary = f"OK batches={batch_count} total_records={total_processed}"
            payload = summary.encode('utf-8')
            header = len(payload).to_bytes(4, byteorder='big')
            conn.sendall(header + payload)
        except Exception:
            pass
        try:
            conn.close()
        finally:
            print(f"[GATEWAY] Conexión con {addr} cerrada. Total procesado: {total_processed} registros en {batch_count} batches")
