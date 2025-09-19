from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, List

@dataclass
class MenuItem:
    item_id: str
    item_name: str
    category: str
    price: float
    is_seasonal: bool
    available_from: Optional[date] = None
    available_to: Optional[date] = None

@dataclass
class Stores:
    store_id: str
    store_name: str
    street: str
    postal_code: str
    city: str
    state: str
    latitude: float
    longitude: float

@dataclass
class TransactionItems:
    transaction_id: str
    item_id: str
    quantity: int
    unit_price: float
    subtotal: float
    created_at: datetime

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
    created_at: datetime

@dataclass
class Users:
    user_id: str
    gender: str
    birthdate: date
    registered_at: datetime