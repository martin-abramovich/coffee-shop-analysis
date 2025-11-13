"""
Protocolo de serialización manual para mensajes entre monitores.
Sigue el patrón de protocol.py sin usar JSON.
"""
from typing import Dict, Optional, Tuple
from enum import Enum


class MessageType(Enum):
    HEARTBEAT = 0
    ELECTION = 1
    ANSWER = 2
    COORDINATOR = 3
    STATE_SYNC = 4
    STATE_REQUEST = 5


def encode_string(s: str) -> bytes:
    """Codifica un string: 4 bytes para tamaño + string en UTF-8"""
    s_bytes = s.encode('utf-8')
    length = len(s_bytes)
    length_bytes = length.to_bytes(4, byteorder='big', signed=False)
    return length_bytes + s_bytes


def decode_string(data: bytes, offset: int) -> Tuple[str, int]:
    """Decodifica un string desde offset"""
    if offset + 4 > len(data):
        raise ValueError("Datos insuficientes para leer longitud de string")
    
    length = int.from_bytes(data[offset:offset+4], byteorder='big', signed=False)
    offset += 4
    
    if offset + length > len(data):
        raise ValueError("Datos insuficientes para leer string")
    
    string_data = data[offset:offset+length].decode('utf-8')
    return string_data, offset + length


def encode_int(i: int) -> bytes:
    """Codifica un int de 4 bytes (unsigned)"""
    if i < 0:
        i = 0
    return i.to_bytes(4, byteorder='big', signed=False)


def decode_int(data: bytes, offset: int) -> Tuple[int, int]:
    """Decodifica un int de 4 bytes desde offset"""
    if offset + 4 > len(data):
        raise ValueError("Datos insuficientes para leer int")
    value = int.from_bytes(data[offset:offset+4], byteorder='big', signed=False)
    return value, offset + 4


def encode_float(f: float) -> bytes:
    """Codifica un float de 8 bytes como double (IEEE 754)"""
    # Usar struct para simplificar, pero manteniendo big-endian
    import struct
    return struct.pack('>d', f)


def decode_float(data: bytes, offset: int) -> Tuple[float, int]:
    """Decodifica un float de 8 bytes desde offset"""
    if offset + 8 > len(data):
        raise ValueError("Datos insuficientes para leer float")
    import struct
    value = struct.unpack('>d', data[offset:offset+8])[0]
    return value, offset + 8


def encode_bool(b: bool) -> bytes:
    """Codifica un bool como 1 byte"""
    return (1 if b else 0).to_bytes(1, byteorder='big', signed=False)


def decode_bool(data: bytes, offset: int) -> Tuple[bool, int]:
    """Decodifica un bool de 1 byte desde offset"""
    if offset + 1 > len(data):
        raise ValueError("Datos insuficientes para leer bool")
    value = data[offset] != 0
    return value, offset + 1


def encode_dict(d: Dict) -> bytes:
    """
    Codifica un diccionario:
    - 4 bytes: cantidad de pares clave-valor
    - Para cada par: string clave + valor (dependiendo del tipo)
    """
    if not d:
        return (0).to_bytes(4, byteorder='big', signed=False)
    
    data = b''
    count = len(d)
    data += count.to_bytes(4, byteorder='big', signed=False)
    
    for key, value in d.items():
        # Codificar clave (string)
        data += encode_string(str(key))
        
        # Codificar valor según tipo
        if isinstance(value, dict):
            # Dict anidado: tipo 0
            data += (0).to_bytes(1, byteorder='big', signed=False)
            data += encode_dict(value)
        elif isinstance(value, list):
            # Lista: tipo 1
            data += (1).to_bytes(1, byteorder='big', signed=False)
            data += encode_list(value)
        elif isinstance(value, str):
            # String: tipo 2
            data += (2).to_bytes(1, byteorder='big', signed=False)
            data += encode_string(value)
        elif isinstance(value, int):
            # Int: tipo 3
            data += (3).to_bytes(1, byteorder='big', signed=False)
            data += encode_int(value)
        elif isinstance(value, float):
            # Float: tipo 4
            data += (4).to_bytes(1, byteorder='big', signed=False)
            data += encode_float(value)
        elif isinstance(value, bool):
            # Bool: tipo 5
            data += (5).to_bytes(1, byteorder='big', signed=False)
            data += encode_bool(value)
        else:
            # Por defecto, convertir a string
            data += (2).to_bytes(1, byteorder='big', signed=False)
            data += encode_string(str(value))
    
    return data


def decode_dict(data: bytes, offset: int) -> Tuple[Dict, int]:
    """Decodifica un diccionario desde offset"""
    if offset + 4 > len(data):
        raise ValueError("Datos insuficientes para leer cantidad de pares")
    
    count = int.from_bytes(data[offset:offset+4], byteorder='big', signed=False)
    offset += 4
    
    result = {}
    for _ in range(count):
        # Decodificar clave
        key, offset = decode_string(data, offset)
        
        # Leer tipo de valor
        if offset + 1 > len(data):
            raise ValueError("Datos insuficientes para leer tipo de valor")
        value_type = data[offset]
        offset += 1
        
        # Decodificar valor según tipo
        if value_type == 0:  # Dict
            value, offset = decode_dict(data, offset)
        elif value_type == 1:  # List
            value, offset = decode_list(data, offset)
        elif value_type == 2:  # String
            value, offset = decode_string(data, offset)
        elif value_type == 3:  # Int
            value, offset = decode_int(data, offset)
        elif value_type == 4:  # Float
            value, offset = decode_float(data, offset)
        elif value_type == 5:  # Bool
            value, offset = decode_bool(data, offset)
        else:
            raise ValueError(f"Tipo de valor desconocido: {value_type}")
        
        result[key] = value
    
    return result, offset


def encode_list(l: list) -> bytes:
    """
    Codifica una lista:
    - 4 bytes: cantidad de elementos
    - Para cada elemento: valor según tipo
    """
    if not l:
        return (0).to_bytes(4, byteorder='big', signed=False)
    
    data = b''
    count = len(l)
    data += count.to_bytes(4, byteorder='big', signed=False)
    
    for value in l:
        # Codificar valor según tipo (mismo formato que dict)
        if isinstance(value, dict):
            data += (0).to_bytes(1, byteorder='big', signed=False)
            data += encode_dict(value)
        elif isinstance(value, list):
            data += (1).to_bytes(1, byteorder='big', signed=False)
            data += encode_list(value)
        elif isinstance(value, str):
            data += (2).to_bytes(1, byteorder='big', signed=False)
            data += encode_string(value)
        elif isinstance(value, int):
            data += (3).to_bytes(1, byteorder='big', signed=False)
            data += encode_int(value)
        elif isinstance(value, float):
            data += (4).to_bytes(1, byteorder='big', signed=False)
            data += encode_float(value)
        elif isinstance(value, bool):
            data += (5).to_bytes(1, byteorder='big', signed=False)
            data += encode_bool(value)
        else:
            data += (2).to_bytes(1, byteorder='big', signed=False)
            data += encode_string(str(value))
    
    return data


def decode_list(data: bytes, offset: int) -> Tuple[list, int]:
    """Decodifica una lista desde offset"""
    if offset + 4 > len(data):
        raise ValueError("Datos insuficientes para leer cantidad de elementos")
    
    count = int.from_bytes(data[offset:offset+4], byteorder='big', signed=False)
    offset += 4
    
    result = []
    for _ in range(count):
        # Leer tipo de valor
        if offset + 1 > len(data):
            raise ValueError("Datos insuficientes para leer tipo de valor")
        value_type = data[offset]
        offset += 1
        
        # Decodificar valor según tipo
        if value_type == 0:  # Dict
            value, offset = decode_dict(data, offset)
        elif value_type == 1:  # List
            value, offset = decode_list(data, offset)
        elif value_type == 2:  # String
            value, offset = decode_string(data, offset)
        elif value_type == 3:  # Int
            value, offset = decode_int(data, offset)
        elif value_type == 4:  # Float
            value, offset = decode_float(data, offset)
        elif value_type == 5:  # Bool
            value, offset = decode_bool(data, offset)
        else:
            raise ValueError(f"Tipo de valor desconocido: {value_type}")
        
        result.append(value)
    
    return result, offset


def encode_message(message_type: MessageType, sender_id: int, timestamp: float, 
                   data: Optional[Dict] = None) -> bytes:
    """
    Codifica un mensaje completo:
    - 1 byte: tipo de mensaje (MessageType enum)
    - 4 bytes: sender_id
    - 8 bytes: timestamp (float)
    - 1 byte: tiene_data (0 o 1)
    - Si tiene_data: dict codificado
    """
    msg_bytes = b''
    
    # Tipo de mensaje (1 byte)
    msg_bytes += message_type.value.to_bytes(1, byteorder='big', signed=False)
    
    # Sender ID (4 bytes)
    msg_bytes += encode_int(sender_id)
    
    # Timestamp (8 bytes)
    msg_bytes += encode_float(timestamp)
    
    # Tiene data (1 byte)
    has_data = data is not None and len(data) > 0
    msg_bytes += encode_bool(has_data)
    
    # Data (si existe)
    if has_data:
        msg_bytes += encode_dict(data)
    
    return msg_bytes


def decode_message(data: bytes) -> Tuple[MessageType, int, float, Optional[Dict]]:
    """
    Decodifica un mensaje completo.
    Returns: (message_type, sender_id, timestamp, data)
    """
    offset = 0
    
    # Tipo de mensaje (1 byte)
    if offset + 1 > len(data):
        raise ValueError("Datos insuficientes para leer tipo de mensaje")
    msg_type_val = data[offset]
    offset += 1
    message_type = MessageType(msg_type_val)
    
    # Sender ID (4 bytes)
    sender_id, offset = decode_int(data, offset)
    
    # Timestamp (8 bytes)
    timestamp, offset = decode_float(data, offset)
    
    # Tiene data (1 byte)
    has_data, offset = decode_bool(data, offset)
    
    # Data (si existe)
    data_dict = None
    if has_data:
        data_dict, offset = decode_dict(data, offset)
    
    return message_type, sender_id, timestamp, data_dict

