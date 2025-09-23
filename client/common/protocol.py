import struct
import csv
from typing import List, Union, Optional
from datetime import datetime, date
from .entities import Transactions, TransactionItems, Users, Stores, MenuItem

# Constantes para tipos de entidad
ENTITY_TYPES = {
    'transactions': 0,
    'transaction_items': 1, 
    'users': 2,
    'stores': 3,
    'menu_items': 4
}

def encode_string(s: str) -> bytes:
    """Codifica un string: 4 bytes para tamaño + string en UTF-8"""
    s_bytes = s.encode('utf-8')
    return struct.pack('>I', len(s_bytes)) + s_bytes

def encode_float(f: float) -> bytes:
    """Codifica un float de 4 bytes"""
    return struct.pack('>f', f)

def encode_int(i: int) -> bytes:
    """Codifica un int de 4 bytes"""
    return struct.pack('>I', i)

def encode_datetime(dt: datetime) -> bytes:
    """Codifica datetime como timestamp (8 bytes)"""
    timestamp = int(dt.timestamp())
    return struct.pack('>Q', timestamp)

def encode_date(d: date) -> bytes:
    """Codifica date como timestamp (8 bytes)"""
    dt = datetime.combine(d, datetime.min.time())
    return encode_datetime(dt)

def encode_bool(b: bool) -> bytes:
    """Codifica bool como 1 byte"""
    return struct.pack('B', 1 if b else 0)

def encode_transaction(trans: Transactions) -> bytes:
    """Codifica una transacción"""
    data = b''
    data += encode_string(trans.transaction_id)
    data += encode_string(trans.store_id)
    data += encode_string(trans.payment_method)
    data += encode_string(trans.voucher_id)
    data += encode_string(trans.user_id)
    data += encode_float(trans.original_amount)
    data += encode_float(trans.discount_applied)
    data += encode_float(trans.final_amount)
    data += encode_datetime(trans.created_at)
    return data

def encode_transaction_item(item: TransactionItems) -> bytes:
    """Codifica un item de transacción"""
    data = b''
    data += encode_string(item.transaction_id)
    data += encode_string(item.item_id)
    data += encode_int(item.quantity)
    data += encode_float(item.unit_price)
    data += encode_float(item.subtotal)
    data += encode_datetime(item.created_at)
    return data

def encode_user(user: Users) -> bytes:
    """Codifica un usuario"""
    data = b''
    data += encode_string(user.user_id)
    data += encode_string(user.gender)
    data += encode_date(user.birthdate)
    data += encode_datetime(user.registered_at)
    return data

def encode_store(store: Stores) -> bytes:
    """Codifica una tienda"""
    data = b''
    data += encode_string(store.store_id)
    data += encode_string(store.store_name)
    data += encode_string(store.street)
    data += encode_string(store.postal_code)
    data += encode_string(store.city)
    data += encode_string(store.state)
    data += encode_float(store.latitude)
    data += encode_float(store.longitude)
    return data

def encode_menu_item(menu_item: MenuItem) -> bytes:
    """Codifica un item del menú"""
    data = b''
    data += encode_string(menu_item.item_id)
    data += encode_string(menu_item.item_name)
    data += encode_string(menu_item.category)
    data += encode_float(menu_item.price)
    data += encode_bool(menu_item.is_seasonal)
    
    # Manejar fechas opcionales
    if menu_item.available_from:
        data += encode_bool(True)  # Tiene fecha from
        data += encode_date(menu_item.available_from)
    else:
        data += encode_bool(False)  # No tiene fecha from
    
    if menu_item.available_to:
        data += encode_bool(True)  # Tiene fecha to
        data += encode_date(menu_item.available_to)
    else:
        data += encode_bool(False)  # No tiene fecha to
    
    return data

def encode_entity(entity) -> bytes:
    """Codifica una entidad según su tipo"""
    if isinstance(entity, Transactions):
        return encode_transaction(entity)
    elif isinstance(entity, TransactionItems):
        return encode_transaction_item(entity)
    elif isinstance(entity, Users):
        return encode_user(entity)
    elif isinstance(entity, Stores):
        return encode_store(entity)
    elif isinstance(entity, MenuItem):
        return encode_menu_item(entity)
    else:
        raise ValueError(f"Tipo de entidad no soportado: {type(entity)}")

def get_entity_type(entity) -> int:
    """Obtiene el tipo numérico de una entidad"""
    if isinstance(entity, Transactions):
        return ENTITY_TYPES['transactions']
    elif isinstance(entity, TransactionItems):
        return ENTITY_TYPES['transaction_items']
    elif isinstance(entity, Users):
        return ENTITY_TYPES['users']
    elif isinstance(entity, Stores):
        return ENTITY_TYPES['stores']
    elif isinstance(entity, MenuItem):
        return ENTITY_TYPES['menu_items']
    else:
        raise ValueError(f"Tipo de entidad no soportado: {type(entity)}")

def encode_batch(entities: List[Union[Transactions, TransactionItems, Users, Stores, MenuItem]]) -> bytes:
    """
    Codifica un batch de entidades del mismo tipo.
    Formato: [4 bytes: cantidad][1 byte: tipo][datos de entidades...]
    """
    if not entities:
        return struct.pack('>I', 0) + struct.pack('B', 0)  # Batch vacío
    
    # Verificar que todas las entidades sean del mismo tipo
    entity_type = get_entity_type(entities[0])
    for entity in entities[1:]:
        if get_entity_type(entity) != entity_type:
            raise ValueError("Todas las entidades en un batch deben ser del mismo tipo")
    
    # Codificar header
    data = struct.pack('>I', len(entities))  # 4 bytes: cantidad
    data += struct.pack('B', entity_type)    # 1 byte: tipo
    
    # Codificar entidades
    for entity in entities:
        data += encode_entity(entity)
    
    return data

def parse_datetime(dt_str: str) -> datetime:
    """Parsea un string de datetime en formato ISO"""
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except ValueError:
        # Intentar otros formatos comunes
        try:
            return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return datetime.strptime(dt_str, '%Y-%m-%d')

def parse_date(date_str: str) -> date:
    """Parsea un string de fecha"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        return datetime.fromisoformat(date_str).date()

def parse_bool(bool_str: str) -> bool:
    """Parsea un string a booleano"""
    return bool_str.lower() in ('true', '1', 'yes', 'y')

def parse_optional_date(date_str: str) -> Optional[date]:
    """Parsea una fecha opcional"""
    if not date_str or date_str.strip() == '':
        return None
    return parse_date(date_str)

def dict_to_transaction(row: dict) -> Transactions:
    """Convierte un dict de CSV a objeto Transactions"""
    return Transactions(
        transaction_id=row['transaction_id'],
        store_id=row['store_id'],
        payment_method=row['payment_method'],
        voucher_id=row['voucher_id'],
        user_id=row['user_id'],
        original_amount=float(row['original_amount']),
        discount_applied=float(row['discount_applied']),
        final_amount=float(row['final_amount']),
        created_at=parse_datetime(row['created_at'])
    )

def dict_to_transaction_item(row: dict) -> TransactionItems:
    """Convierte un dict de CSV a objeto TransactionItems"""
    return TransactionItems(
        transaction_id=row['transaction_id'],
        item_id=row['item_id'],
        quantity=int(row['quantity']),
        unit_price=float(row['unit_price']),
        subtotal=float(row['subtotal']),
        created_at=parse_datetime(row['created_at'])
    )

def dict_to_user(row: dict) -> Users:
    """Convierte un dict de CSV a objeto Users"""
    return Users(
        user_id=row['user_id'],
        gender=row['gender'],
        birthdate=parse_date(row['birthdate']),
        registered_at=parse_datetime(row['registered_at'])
    )

def dict_to_store(row: dict) -> Stores:
    """Convierte un dict de CSV a objeto Stores"""
    return Stores(
        store_id=row['store_id'],
        store_name=row['store_name'],
        street=row['street'],
        postal_code=row['postal_code'],
        city=row['city'],
        state=row['state'],
        latitude=float(row['latitude']),
        longitude=float(row['longitude'])
    )

def dict_to_menu_item(row: dict) -> MenuItem:
    """Convierte un dict de CSV a objeto MenuItem"""
    return MenuItem(
        item_id=row['item_id'],
        item_name=row['item_name'],
        category=row['category'],
        price=float(row['price']),
        is_seasonal=parse_bool(row['is_seasonal']),
        available_from=parse_optional_date(row.get('available_from', '')),
        available_to=parse_optional_date(row.get('available_to', ''))
    )

def detect_entity_type_from_filename(filename: str) -> str:
    """Detecta el tipo de entidad basado en el nombre del archivo"""
    filename_lower = filename.lower()
    if 'transaction_item' in filename_lower or 'trans_item' in filename_lower:
        return 'transaction_items'
    elif 'transaction' in filename_lower or 'trans' in filename_lower:
        return 'transactions'
    elif 'user' in filename_lower:
        return 'users'
    elif 'store' in filename_lower or 'shop' in filename_lower:
        return 'stores'
    elif 'menu' in filename_lower or 'item' in filename_lower:
        return 'menu_items'
    else:
        raise ValueError(f"No se pudo detectar el tipo de entidad para el archivo: {filename}")

def detect_entity_type_from_headers(headers: List[str]) -> str:
    """Detecta el tipo de entidad basado en las columnas del CSV"""
    headers_set = set(h.lower() for h in headers)
    
    if 'transaction_id' in headers_set and 'item_id' in headers_set and 'quantity' in headers_set:
        return 'transaction_items'
    elif 'transaction_id' in headers_set and 'store_id' in headers_set and 'payment_method' in headers_set:
        return 'transactions'
    elif 'user_id' in headers_set and 'gender' in headers_set and 'birthdate' in headers_set:
        return 'users'
    elif 'store_id' in headers_set and 'store_name' in headers_set and 'latitude' in headers_set:
        return 'stores'
    elif 'item_id' in headers_set and 'item_name' in headers_set and 'category' in headers_set:
        return 'menu_items'
    else:
        raise ValueError(f"No se pudo detectar el tipo de entidad para las columnas: {headers}")

def dict_to_entity(row: dict, entity_type: str) -> Union[Transactions, TransactionItems, Users, Stores, MenuItem]:
    """Convierte un dict a la entidad correspondiente"""
    converters = {
        'transactions': dict_to_transaction,
        'transaction_items': dict_to_transaction_item,
        'users': dict_to_user,
        'stores': dict_to_store,
        'menu_items': dict_to_menu_item
    }
    
    if entity_type not in converters:
        raise ValueError(f"Tipo de entidad no soportado: {entity_type}")
    
    return converters[entity_type](row)

def entity_batch_iterator(csv_path: str, batch_size: int, entity_type: str = None):
    """
    Lee un CSV y genera batches de entidades del tipo especificado.
    Si entity_type es None, intenta detectarlo automáticamente.
    """
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Detectar tipo de entidad si no se especifica
        if entity_type is None:
            # Leer primera fila para detectar tipo
            try:
                first_row = next(reader)
                entity_type = detect_entity_type_from_headers(reader.fieldnames)
                # Volver al inicio del archivo
                f.seek(0)
                reader = csv.DictReader(f)
            except StopIteration:
                return  # Archivo vacío
        
        batch = []
        for row in reader:
            try:
                entity = dict_to_entity(row, entity_type)
                batch.append(entity)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            except (ValueError, KeyError) as e:
                print(f"Advertencia: Error procesando fila {row}: {e}")
                continue
        
        if batch:
            yield batch