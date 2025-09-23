# gateway/entities_clean.py
from dataclasses import dataclass

@dataclass
class MenuItemShort:
    item_id: str
    item_name: str

@dataclass
class StoreShort:
    store_id: str
    store_name: str

@dataclass
class UserShort:
    user_id: str
    birthdate: str

@dataclass
class TransactionItems:
    transaction_id: str
    item_id: str
    quantity: int
    subtotal: float
    created_at: str

@dataclass
class Transactions:
    transaction_id: str
    store_id: str
    user_id: str
    final_amount: float
    created_at: str