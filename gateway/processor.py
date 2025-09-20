from entities_clean import MenuItemShort, StoreShort, UserShort, Transactions, TransactionItems
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def reduce_transaction(row: dict) -> Transactions:
    return Transactions(
        transaction_id=row["transaction_id"],
        store_id=row["store_id"],
        payment_method=row["payment_method"],
        voucher_id=row["voucher_id"],
        user_id=row["user_id"],
        original_amount=float(row["original_amount"]),
        discount_applied=float(row["discount_applied"]),
        final_amount=float(row["final_amount"]),
        created_at=row["created_at"],
    )

def reduce_user(row: dict) -> UserShort:
    return UserShort(
        user_id=row["user_id"],
        birthdate=row["birthdate"],  # Mantener como string por simplicidad
        registered_at=row["registered_at"],  # Mantener como string por simplicidad
    )

def reduce_store(row: dict) -> StoreShort:
    return StoreShort(
        store_id=row["store_id"],
        store_name=row["store_name"],
        city=row["city"],
        state=row["state"],
    )

def reduce_menu_item(row: dict) -> MenuItemShort:
    return MenuItemShort(
        item_id=row["item_id"],
        item_name=row["item_name"],
        category=row["category"],
        price=float(row["price"]),
    )

def reduce_transaction_item(row: dict) -> TransactionItems:
    return TransactionItems(
        transaction_id=row["transaction_id"],
        item_id=row["item_id"],
        quantity=int(row["quantity"]),
        unit_price=float(row["unit_price"]),
        subtotal=float(row["subtotal"]),
        created_at=row["created_at"],  # Mantener como string por simplicidad
    )

def determine_data_type(row: dict) -> str:
    """
    Determina el tipo de datos basado en las columnas presentes en la fila.
    """
    if not row:
        return "unknown"
    
    # Claves específicas para cada tipo de entidad
    if "transaction_id" in row and "store_id" in row and "user_id" in row:
        if "item_id" in row and "quantity" in row:
            return "transaction_items"
        else:
            return "transactions"
    elif "user_id" in row and "birthdate" in row and "registered_at" in row:
        return "users"
    elif "store_id" in row and "store_name" in row and "city" in row:
        return "stores"
    elif "item_id" in row and "item_name" in row and "category" in row:
        return "menu_items"
    else:
        return "unknown"

def process_data_by_type(rows: list, data_type: str) -> list:
    """
    Procesa las filas según el tipo de datos identificado.
    Aplica validación y limpieza según sea necesario.
    """
    if not rows:
        return []
    
    processed = []
    
    for row in rows:
        try:
            if data_type == "transactions":
                processed.append(reduce_transaction(row))
            elif data_type == "users":
                processed.append(reduce_user(row))
            elif data_type == "stores":
                processed.append(reduce_store(row))
            elif data_type == "menu_items":
                processed.append(reduce_menu_item(row))
            elif data_type == "transaction_items":
                processed.append(reduce_transaction_item(row))
            else:
                logger.warning(f"Tipo de datos desconocido: {data_type}, saltando fila")
                continue
                
        except Exception as e:
            logger.error(f"Error procesando fila {row}: {e}")
            continue
    
    logger.info(f"Procesados {len(processed)} registros de tipo {data_type}")
    return processed

def validate_and_clean_data(row: dict, data_type: str) -> dict:
    """
    Valida y limpia los datos según el tipo.
    Versión simplificada sin conversiones complejas de fechas.
    """
    cleaned_row = {}
    
    for key, value in row.items():
        if value is None or value == "":
            continue
            
        # Limpiar espacios en blanco
        if isinstance(value, str):
            value = value.strip()
            
        # Validaciones básicas por tipo
        if data_type == "transactions":
            if key in ["original_amount", "discount_applied", "final_amount"]:
                try:
                    cleaned_row[key] = float(value)
                except ValueError:
                    logger.warning(f"Valor inválido para {key}: {value}")
                    continue
            else:
                cleaned_row[key] = value
                
        elif data_type == "transaction_items":
            if key in ["quantity"]:
                try:
                    cleaned_row[key] = int(value)
                except ValueError:
                    logger.warning(f"Valor inválido para {key}: {value}")
                    continue
            elif key in ["unit_price", "subtotal"]:
                try:
                    cleaned_row[key] = float(value)
                except ValueError:
                    logger.warning(f"Valor inválido para {key}: {value}")
                    continue
            else:
                cleaned_row[key] = value
                
        elif data_type == "menu_items":
            if key == "price":
                try:
                    cleaned_row[key] = float(value)
                except ValueError:
                    logger.warning(f"Valor inválido para {key}: {value}")
                    continue
            else:
                cleaned_row[key] = value
                
        else:
            cleaned_row[key] = value
    
    return cleaned_row
