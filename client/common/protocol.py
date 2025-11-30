import calendar
import csv
from datetime import datetime, date

# Constantes para tipos de entidad
ENTITY_TYPES = {
    'transactions': 0,
    'transaction_items': 1, 
    'users': 2,
    'stores': 3,
    'menu_items': 4
}

ITEM_INDEX = {
    'item_id': 0,
    'item_name': 1,
    'category': 2,
    'price': 3,
    'is_seasonal': 4,
    'available_from': 5,
    'available_to': 6,
}

STORE_INDEX = {
    "store_id": 0,
    "store_name": 1,
    "street": 2,
    "postal_code": 3,
    "city": 4,
    "state": 5,
    "latitude": 6,
    "longitud": 7,
}

TRANSACTION_ITEM_INDEX = {
    "transaction_id":0,
    "item_id":1,
    "quantity":2,
    "unit_price":3,
    "subtotal":4,
    "created_at":5,
}

TRANSACTION_INDEX = {
    "transaction_id": 0,
    "store_id": 1,
    "payment_method_id": 2,
    "voucher_id": 3,
    "user_id": 4,
    "original_amount": 5,
    "discount_applied": 6,
    "final_amount": 7,
    "created_at": 8,
}

USER_INDEX = {
    "user_id": 0,
    "gender": 1,
    "birthdate": 2,
    "registered_at": 3,
}

def to_int(v):
    try:
        if v in ("", None):
            return 0
        # Convertir a float primero maneja strings como "151.0"
        return int(float(v)) 
    except:
        return 0
        
def encode_string(s: str) -> bytes:
    """Codifica un string: 4 bytes para tamaño + string en UTF-8"""
    s_bytes = s.encode('utf-8')
    # Convertir longitud a 4 bytes big-endian manualmente
    length = len(s_bytes)
    length_bytes = length.to_bytes(4, byteorder='big', signed=False)
    return length_bytes + s_bytes

def encode_float(f: float) -> bytes:
    """Codifica un float de 4 bytes usando representación IEEE 754 manual"""
    # Implementación manual de IEEE 754 single precision
    if f == 0.0:
        return (0).to_bytes(4, byteorder='big', signed=False)
    
    # Manejar signo
    sign = 0 if f >= 0 else 1
    f = abs(f)
    
    # Casos especiales
    if f == float('inf'):
        return ((sign << 31) | 0x7F800000).to_bytes(4, byteorder='big', signed=False)
    if f != f:  # NaN
        return ((sign << 31) | 0x7FC00000).to_bytes(4, byteorder='big', signed=False)
    
    # Normalizar
    if f >= 2.0:
        exponent = 0
        while f >= 2.0:
            f /= 2.0
            exponent += 1
        exponent += 127  # Bias IEEE 754
    elif f < 1.0:
        exponent = 0
        while f < 1.0 and exponent > -126:
            f *= 2.0
            exponent -= 1
        exponent += 127  # Bias IEEE 754
        if exponent <= 0:  # Número denormalizado
            exponent = 0
    else:
        exponent = 127  # f está entre 1.0 y 2.0
    
    # Calcular mantisa (23 bits)
    if exponent > 0:
        mantissa = int((f - 1.0) * (1 << 23))
    else:
        mantissa = int(f * (1 << 23))
    
    # Combinar los bits
    ieee_bits = (sign << 31) | (exponent << 23) | (mantissa & 0x7FFFFF)
    return ieee_bits.to_bytes(4, byteorder='big', signed=False)

def encode_int(i: int) -> bytes:
    """Codifica un int de 4 bytes (signed para manejar negativos)"""
    # Asegurar que sea unsigned positivo, si es negativo usar 0
    if i < 0:
        i = 0
    return i.to_bytes(4, byteorder='big', signed=False)


def encode_bool(b: bool) -> bytes:
    """Codifica bool como 1 byte"""
    return (1 if b else 0).to_bytes(1, byteorder='big', signed=False)

def encode_date_str(date_str: str) -> bytes:
    """Convierte un string 'YYYY-MM-DD' a 8 bytes timestamp"""
    try:
        y, m, d = map(int, date_str.split('-'))
        ts = int(datetime(y, m, d).timestamp())
        if ts < 0:
            ts = 0
        return ts.to_bytes(8, 'big', signed=False)
    except Exception:
        return (0).to_bytes(8, 'big', signed=False)

def encode_datetime_str(datetime_str: str) -> bytes:
    """Convierte un string 'YYYY-MM-DD HH:MM:SS' a 8 bytes timestamp"""
    try:
        # Intentar con formato completo
        dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        ts = int(dt.timestamp())
        if ts < 0:
            ts = 0
        return ts.to_bytes(8, 'big', signed=False)
    except Exception:
        # Intentar solo fecha si falla
        try:
            return encode_date_str(datetime_str)
        except Exception:
            return (0).to_bytes(8, 'big', signed=False)

def encode_date(s) -> bytes:
    """Parse YYYY-MM-DD → epoch (int) sin usar datetime, rapidísimo."""
    y = int(s[0:4])
    m = int(s[5:7])
    d = int(s[8:10])
    return encode_int(calendar.timegm((y, m, d, 0, 0, 0)))

def encode_datetime(s) -> bytes:
    """Parse YYYY-MM-DD HH:MM:SS → epoch sin datetime.strptime (10x más rápido)."""
    y = int(s[0:4])
    m = int(s[5:7])
    d = int(s[8:10])
    H = int(s[11:13])
    M = int(s[14:16])
    S = int(s[17:19])
    #tiene sentido usar int32 porque tenemos hasta 2025, 
    # si estamos en 2050 esto no funciona
    return encode_int(calendar.timegm((y, m, d, H, M, S)))

def encode_transaction(row: list) -> bytes:
    idx = TRANSACTION_INDEX
    
    b = bytearray()
    b.extend(encode_string(row[idx['transaction_id']]))
    b.extend(encode_int(to_int(row[idx['store_id']])))
    b.extend(encode_int(to_int(row[idx['payment_method_id']])))
    b.extend(encode_int(to_int(row[idx['voucher_id']])))
    b.extend(encode_int(to_int(row[idx['user_id']])))
    b.extend(encode_float(float(row[idx['original_amount']])))
    b.extend(encode_float(float(row[idx['discount_applied']])))
    b.extend(encode_float(float(row[idx['final_amount']])))
    b.extend(encode_datetime(row[idx['created_at']]))
    
    return b


def encode_transaction_item(row: list) -> bytes:
    idx = TRANSACTION_ITEM_INDEX
   
    b = bytearray()
    b.extend(encode_string(row[idx['transaction_id']]))
    b.extend(encode_int(int(row[idx['item_id']])))
    b.extend(encode_int(int(row[idx['quantity']])))
    b.extend(encode_float(float(row[idx['unit_price']])))
    b.extend(encode_float(float(row[idx['subtotal']])))
    b.extend(encode_datetime(row[idx['created_at']]))
    
    return b


def encode_user(row: list) -> bytes:
    idx = USER_INDEX
    
    b = bytearray()
    b.extend(encode_int(int(row[idx['user_id']])))
    b.append(1 if row[1] == "male" else 0)
    b.extend(encode_date(row[idx['birthdate']]))
    b.extend(encode_datetime(row[idx['registered_at']]))
    
    return b


def encode_store(row: list) -> bytes:
    """Codifica una tienda desde una fila CSV"""
    idx = STORE_INDEX
    b = bytearray() 
    
    b.extend(encode_int(int(row[idx['store_id']])))
    b.extend( encode_string(row[idx['store_name']]))
    b.extend( encode_string(row[idx['street']]))
    b.extend( encode_int(int(row[idx['postal_code']])))
    b.extend( encode_string(row[idx['city']]))
    b.extend( encode_string(row[idx['state']]))
    b.extend( encode_float(float(row[idx['latitude']])))
    b.extend( encode_float(float(row[idx['longitud']])))
    
    return b
    
def encode_item(row: list) -> bytes:
    idx = ITEM_INDEX
    data = b''
    data += encode_string(row[idx['item_id']])
    data += encode_string(row[idx['item_name']])
    data += encode_string(row[idx['category']])
    data += encode_float(float(row[idx['price']]))
    data += encode_bool(row[idx['is_seasonal']].lower() == 'true')

    for key in ['available_from', 'available_to']:
        val = row[idx[key]].strip()
        if val:
            data += encode_bool(True)
            data += encode_date_str(val)
        else:
            data += encode_bool(False)

    return data
    
def encode_row(row:dict, entity_type:str): 
    if entity_type == "transactions": 
        return encode_transaction(row)
    elif entity_type == 'transaction_items':
        return encode_transaction_item(row)
    elif entity_type == "users": 
        return encode_user(row)
    elif entity_type == "stores": 
        return encode_store(row)
    elif entity_type == "menu_items":
        return encode_item(row)
    else: 
        raise ValueError(f"Tipo de entidad no soportado: {entity_type}")
         
def entity_batch_iterator(csv_path: str, batch_size: int, entity_type: str, batch_id):
    """
    Lee un CSV y genera batches binarios de entidades del tipo especificado.
    batch_size: número máximo de filas por batch
    """
    type_code = ENTITY_TYPES.get(entity_type)
    
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        _headers = next(reader)  # descarta encabezados

        batch_data = bytearray()
        count = 0

        for row in reader:
            try:
                entity_bytes = encode_row(row, entity_type)
                batch_data.extend(entity_bytes) 
                count += 1

                if count >= batch_size:
                    # header: [4 bytes count][1 byte type][4 bytes id]
                    header = count.to_bytes(4, 'big') + type_code.to_bytes(1, 'big') + batch_id[0].to_bytes(4, "big")
                    yield bytes(header + batch_data)
                    batch_data.clear()
                    count = 0

            except (ValueError, KeyError) as e:
                print(f"Advertencia: Error procesando fila {row}: {e}")
                continue

        if count > 0:
            header = count.to_bytes(4, 'big') + type_code.to_bytes(1, 'big') + batch_id[0].to_bytes(4, "big")
            yield bytes(header + batch_data)

def batch_eos(entity_type: str, batch_id: int): 
    type_code = ENTITY_TYPES.get(entity_type)
    
    return (0).to_bytes(4, 'big') + type_code.to_bytes(1, 'big') + batch_id[0].to_bytes(4, "big")