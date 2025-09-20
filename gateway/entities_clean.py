# gateway/entities_clean.py
from dataclasses import dataclass

@dataclass
class MenuItemShort:
    item_id: str
    item_name: str
    category: str
    price: float

@dataclass
class StoreShort:
    store_id: str
    store_name: str
    city: str
    state: str

@dataclass
class UserShort:
    user_id: str
    birthdate: str  # Mantener como string por simplicidad
    registered_at: str  # Mantener como string por simplicidad

@dataclass
class TransactionItems:
    transaction_id: str
    item_id: str
    quantity: int
    unit_price: float
    subtotal: float
    created_at: str  # Mantener como string por simplicidad

@dataclass
class Transactions:
    transaction_id: str
    store_id: str
    payment_method: str
    voucher_id: str
    user_id: str
    original_amount: float
    discount_applied: float
    final_amount: float
    created_at: str  # Mantener como string por simplicidad