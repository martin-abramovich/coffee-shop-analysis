import socket
import struct
from processor import process_batch_by_type

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

def read_float(data, offset):
    """Lee un float de 4 bytes"""
    if offset + 4 > len(data):
        raise ValueError("Datos insuficientes para leer float")
    
    value = struct.unpack('>f', data[offset:offset+4])[0]
    return value, offset + 4

def read_int(data, offset):
    """Lee un int de 4 bytes"""
    if offset + 4 > len(data):
        raise ValueError("Datos insuficientes para leer int")
    
    value = struct.unpack('>i', data[offset:offset+4])[0]
    return value, offset + 4

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
    
    item_count = struct.unpack('>I', data[offset:offset+4])[0]
    offset += 4
    
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
        try:
            item, offset = parse_item(data, offset, entity_type)
            items.append(item)
        except Exception as e:
            print(f"[GATEWAY] Error parseando item {i}: {e}")
            continue
    
    return entity_type, items

def parse_item(data, offset, entity_type):
    """Parsea un item individual según su tipo"""
    item = {}
    
    if entity_type == "transactions":
        # transaction_id (string), store_id (string), user_id (string), 
        # final_amount (float), created_at (string)
        item['transaction_id'], offset = read_string(data, offset)
        item['store_id'], offset = read_string(data, offset)
        item['user_id'], offset = read_string(data, offset)
        item['final_amount'], offset = read_float(data, offset)
        item['created_at'], offset = read_string(data, offset)
        
    elif entity_type == "transaction_items":
        # transaction_id (string), item_id (string), quantity (int), 
        # subtotal (float), created_at (string)
        item['transaction_id'], offset = read_string(data, offset)
        item['item_id'], offset = read_string(data, offset)
        item['quantity'], offset = read_int(data, offset)
        item['subtotal'], offset = read_float(data, offset)
        item['created_at'], offset = read_string(data, offset)
        
    elif entity_type == "users":
        # user_id (string), birthdate (string)
        item['user_id'], offset = read_string(data, offset)
        item['birthdate'], offset = read_string(data, offset)
        
    elif entity_type == "stores":
        # store_id (string), store_name (string)
        item['store_id'], offset = read_string(data, offset)
        item['store_name'], offset = read_string(data, offset)
        
    elif entity_type == "menu_items":
        # item_id (string), item_name (string)
        item['item_id'], offset = read_string(data, offset)
        item['item_name'], offset = read_string(data, offset)
    
    return item, offset

def handle_client(conn, addr):
    print(f"[GATEWAY] Conexión de {addr}")
    buffer = b""
    batch_id = 0
    total_processed = 0
    
    try:
        while True:
            data = conn.recv(4096)  # Buffer más grande para datos binarios
            if not data:
                break

            buffer += data

            # Procesar batches completos
            while len(buffer) >= 5:  # Mínimo: 4 bytes (cantidad) + 1 byte (tipo)
                try:
                    # Intentar leer la cantidad de items
                    if len(buffer) < 4:
                        break
                    
                    item_count = struct.unpack('>I', buffer[:4])[0]
                    
                    # Calcular tamaño mínimo del batch
                    # 4 bytes (cantidad) + 1 byte (tipo) + datos de items
                    min_batch_size = 5
                    
                    # Estimación conservadora del tamaño del batch
                    # Asumimos al menos 20 bytes por item (muy conservador)
                    estimated_size = min_batch_size + (item_count * 20)
                    
                    if len(buffer) < estimated_size:
                        # No tenemos suficientes datos, esperar más
                        break
                    
                    # Intentar parsear el batch
                    entity_type, items = parse_batch(buffer)
                    
                    # Calcular el tamaño real del batch parseado
                    # Esto es una aproximación, en un caso real necesitarías
                    # que parse_batch devuelva el offset final
                    batch_size = 5  # header
                    for item in items:
                        for value in item.values():
                            if isinstance(value, str):
                                batch_size += 4 + len(value.encode('utf-8'))
                            else:
                                batch_size += 4
                    
                    # Remover el batch procesado del buffer
                    buffer = buffer[batch_size:]
                    
                    # Procesar el batch
                    batch_id += 1
                    processed_items = process_batch_by_type(items, entity_type)
                    
                    if processed_items:
                        total_processed += len(processed_items)
                        print(f"[GATEWAY] Batch {batch_id} de tipo {entity_type} procesado ({len(processed_items)} registros)")
                        print(f"[GATEWAY] Total procesado hasta ahora: {total_processed} registros")
                        
                        # Aquí podrías guardar los datos procesados en archivos, base de datos, etc.
                        # Por ahora solo los mostramos en consola
                        for item in processed_items:
                            print(f"  - {item}")

                except Exception as e:
                    print(f"[GATEWAY] Error procesando batch: {e}")
                    # En caso de error, limpiar el buffer y continuar
                    buffer = b""
                    break

    except Exception as e:
        print(f"[GATEWAY] Error en conexión con {addr}: {e}")
    finally:
        conn.close()
        print(f"[GATEWAY] Conexión con {addr} cerrada. Total procesado: {total_processed} registros")
