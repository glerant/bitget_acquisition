from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class Symbol:
    id: int
    exchange_symbol: str
    base_asset: str
    quote_asset: str
    market_type: str  # e.g. "spot", "futures", "perpetual"
    is_active: bool


@dataclass
class Trade:
    symbol_id: int
    exchange_trade_id: str
    ts_exchange: datetime
    price: float
    quantity: float
    side: str  # "buy" or "sell"
    raw: Dict[str, Any]


@dataclass
class Kline:
    symbol_id: int
    interval: str
    open_time: datetime
    close_time: Optional[datetime]
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: Optional[float]
    trade_count: Optional[int]
    raw: Dict[str, Any]


@dataclass
class OrderBookSnapshot:
    symbol_id: int
    ts_exchange: datetime
    bids: List[List[float]]  # [[price, qty], ...]
    asks: List[List[float]]
    raw: Dict[str, Any]
