import sys
import os
import logging

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from gateway.entities_clean import MenuItemShort, StoreShort, UserShort, Transactions, TransactionItems
from datetime import datetime

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ValidationError(Exception):
    """Excepción para errores de validación de datos"""
    pass

def canonicalize_id(value: str) -> str:
    """Normaliza IDs que puedan venir como números con parte decimal nula.
    Ej: "901388.0" -> "901388". No altera IDs alfanuméricos.
    """
    if not isinstance(value, str):
        value = str(value)
    s = value.strip()
    if '.' in s:
        parts = s.split('.', 1)
        integer_part = parts[0]
        decimal_part = parts[1]
        if decimal_part.strip('0') == '':
            return integer_part
    return s

def validate_transaction(item: dict) -> None:
    """Valida que una transacción tenga todos los campos requeridos y válidos"""
    # user_id es OPCIONAL - algunas transacciones no lo tienen
    required_fields = ['transaction_id', 'store_id', 'final_amount', 'created_at']
    
    for field in required_fields:
        if field not in item or item[field] is None or item[field] == '':
            raise ValidationError(f"Campo requerido '{field}' faltante o vacío")
    
    # Validar que final_amount sea un número positivo
    try:
        amount = float(item['final_amount'])
        if amount < 0:
            raise ValidationError(f"final_amount debe ser positivo, recibido: {amount}")
    except (ValueError, TypeError):
        raise ValidationError(f"final_amount debe ser un número válido, recibido: {item['final_amount']}")
    
    # Normalizar y validar IDs y claves
    if isinstance(item['transaction_id'], str):
        item['transaction_id'] = item['transaction_id'].strip()
    if isinstance(item['store_id'], str):
        item['store_id'] = item['store_id'].strip()
    
    # solo normalizar si existe y no está vacío
    if 'user_id' in item and item['user_id'] and isinstance(item['user_id'], str):
        item['user_id'] = canonicalize_id(item['user_id'])
    elif 'user_id' not in item or not item['user_id']:
        item['user_id'] = ''

    # Validar formato de transaction_id (no vacío)
    if not item['transaction_id']:
        raise ValidationError("transaction_id no puede estar vacío")
    
    # Validar formato de created_at (ISO string)
    try:
        datetime.fromisoformat(item['created_at'].replace('Z', '+00:00'))
    except ValueError:
        raise ValidationError(f"created_at debe ser formato ISO válido, recibido: {item['created_at']}")

def validate_user(item: dict) -> None:
    """Valida que un usuario tenga campos válidos. birthdate es opcional."""
    required_fields = ['user_id']
    
    for field in required_fields:
        if field not in item or item[field] is None or item[field] == '':
            raise ValidationError(f"Campo requerido '{field}' faltante o vacío")
    
    # Normalizar y validar user_id
    if isinstance(item['user_id'], str):
        item['user_id'] = canonicalize_id(item['user_id'])
    if not item['user_id']:
        raise ValidationError("user_id no puede estar vacío")
    
    # Validar formato de birthdate solo si viene presente y no vacío
    if 'birthdate' in item and item['birthdate'] not in (None, ''):
        try:
            datetime.fromisoformat(item['birthdate']).date()
        except ValueError:
            raise ValidationError(f"birthdate debe ser formato ISO date válido, recibido: {item['birthdate']}")

def validate_store(item: dict) -> None:
    """Valida que una tienda tenga todos los campos requeridos y válidos"""
    required_fields = ['store_id', 'store_name']
    
    for field in required_fields:
        if field not in item or item[field] is None or item[field] == '':
            raise ValidationError(f"Campo requerido '{field}' faltante o vacío")
    
    # Normalizar y validar store_id y store_name
    if isinstance(item['store_id'], str):
        item['store_id'] = item['store_id'].strip()
    if isinstance(item['store_name'], str):
        item['store_name'] = item['store_name'].strip()
    if not item['store_name']:
        raise ValidationError("store_name no puede estar vacío")

def validate_menu_item(item: dict) -> None:
    """Valida que un item del menú tenga todos los campos requeridos y válidos"""
    required_fields = ['item_id', 'item_name', 'price']
    
    for field in required_fields:
        if field not in item or item[field] is None or item[field] == '':
            raise ValidationError(f"Campo requerido '{field}' faltante o vacío")
    
    # Validar que price sea un número positivo
    try:
        price = float(item['price'])
        if price < 0:
            raise ValidationError(f"price debe ser positivo, recibido: {price}")
    except (ValueError, TypeError, KeyError):
        raise ValidationError(f"price debe ser un número válido, recibido: {item.get('price')}")
    
    # Validar que item_name no esté vacío
    if not item['item_name'].strip():
        raise ValidationError("item_name no puede estar vacío")

def validate_transaction_item(item: dict) -> None:
    """Valida que un item de transacción tenga todos los campos requeridos y válidos"""
    required_fields = ['transaction_id', 'item_id', 'quantity', 'subtotal', 'created_at']
    
    for field in required_fields:
        if field not in item or item[field] is None or item[field] == '':
            raise ValidationError(f"Campo requerido '{field}' faltante o vacío")
    
    # Validar que quantity sea un entero positivo
    try:
        qty = int(item['quantity'])
        if qty <= 0:
            raise ValidationError(f"quantity debe ser positivo, recibido: {qty}")
    except (ValueError, TypeError):
        raise ValidationError(f"quantity debe ser un entero válido, recibido: {item['quantity']}")
    
    # Validar que subtotal sea un número positivo
    try:
        subtotal = float(item['subtotal'])
        if subtotal < 0:
            raise ValidationError(f"subtotal debe ser positivo, recibido: {subtotal}")
    except (ValueError, TypeError):
        raise ValidationError(f"subtotal debe ser un número válido, recibido: {item['subtotal']}")
    
    # Validar formato de created_at (ISO string)
    try:
        datetime.fromisoformat(item['created_at'].replace('Z', '+00:00'))
    except ValueError:
        raise ValidationError(f"created_at debe ser formato ISO válido, recibido: {item['created_at']}")

def reduce_transaction(row: dict) -> Transactions:
    """Reduce y valida una transacción a la entidad simplificada"""
    validate_transaction(row)
    return Transactions(
        transaction_id=row["transaction_id"],
        store_id=row["store_id"],
        user_id=row["user_id"],
        final_amount=float(row["final_amount"]),
        created_at=row["created_at"],
    )

def reduce_user(row: dict) -> UserShort:
    """Reduce y valida un usuario a la entidad simplificada"""
    validate_user(row)
    return UserShort(
        user_id=row["user_id"],
        birthdate=row["birthdate"],
    )

def reduce_store(row: dict) -> StoreShort:
    """Reduce y valida una tienda a la entidad simplificada"""
    validate_store(row)
    return StoreShort(
        store_id=row["store_id"],
        store_name=row["store_name"],
    )

def reduce_menu_item(row: dict) -> MenuItemShort:
    """Reduce y valida un item del menú a la entidad simplificada"""
    validate_menu_item(row)
    return MenuItemShort(
        item_id=row["item_id"],
        item_name=row["item_name"],
        price=float(row["price"]),
    )

def reduce_transaction_item(row: dict) -> TransactionItems:
    """Reduce y valida un item de transacción a la entidad simplificada"""
    validate_transaction_item(row)
    return TransactionItems(
        transaction_id=row["transaction_id"],
        item_id=row["item_id"],
        quantity=int(row["quantity"]),
        subtotal=float(row["subtotal"]),
        created_at=row["created_at"],
    )


def process_batch_by_type(items: list, data_type: str) -> list:
    """
    Procesa un batch de items según el tipo de datos.
    Aplica validación y manejo de errores robusto.
    """
    if not items:
        return []
    
    processed = []
    validation_errors = 0
    processing_errors = 0
    
    for i, item in enumerate(items):
        try:
            if data_type == "transactions":
                processed.append(reduce_transaction(item))
            elif data_type == "users":
                processed.append(reduce_user(item))
            elif data_type == "stores":
                processed.append(reduce_store(item))
            elif data_type == "menu_items":
                processed.append(reduce_menu_item(item))
            elif data_type == "transaction_items":
                processed.append(reduce_transaction_item(item))
            else:
                logger.warning(f"Tipo de datos desconocido: {data_type}, saltando item {i}")
                continue
                
        except ValidationError as e:
            validation_errors += 1
            logger.warning(f"Validación fallida para item {i} de tipo {data_type}: {e}")
            continue
        except Exception as e:
            processing_errors += 1
            logger.error(f"Error procesando item {i} de tipo {data_type}: {e}")
            continue
    
    # Log de estadísticas del batch (solo si hay errores significativos)
    total_items = len(items)
    successful = len(processed)
    if validation_errors > 0 or processing_errors > 0:
        logger.warning(f"Batch procesado - Tipo: {data_type}, Total: {total_items}, "
                      f"Exitosos: {successful}, Errores validación: {validation_errors}, "
                      f"Errores procesamiento: {processing_errors}")
    
    return processed

