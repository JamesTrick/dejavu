from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Optional


class EventType(Enum):
    MARKET = auto()   # New price data arrived
    SIGNAL = auto()   # Strategy generated a signal
    ORDER = auto()    # Order submitted
    FILL = auto()     # Order was executed

class AssetClass(Enum):
    EQUITY = auto()
    OPTION = auto()
    FUTURE = auto()
    FX = auto()

@dataclass
class Event:
    type: EventType
    timestamp: datetime

@dataclass
class MarketEvent(Event):
    """Generic market event, representing a new price update for an asset. It supports OHLCV data and be used for both
    equities and options (with additional fields in OptionMarketEvent)."""
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    asset_class: AssetClass = AssetClass.EQUITY

@dataclass
class OptionMarketEvent(MarketEvent):
    underlying: str = ""
    strike: float = 0.0
    expiry: datetime = field(default_factory=datetime.now)
    option_type: str = "C"   # "C" or "P"
    iv: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None

@dataclass
class Position:
    symbol: str
    quantity: float        # negative = short
    avg_cost: float
    asset_class: AssetClass
    # Options-specific
    underlying: Optional[str] = None
    strike: Optional[float] = None
    expiry: Optional[datetime] = None
    option_type: Optional[str] = None

    def market_value(self, current_price: float) -> float:
        return self.quantity * current_price * self.multiplier

    @property
    def multiplier(self) -> int:
        """Options typically represent 100 shares."""
        return 100 if self.asset_class == AssetClass.OPTION else 1

class OrderType(Enum):
    MARKET = auto()
    LIMIT = auto()
    STOP = auto()

@dataclass
class Order:
    symbol: str
    quantity: float        # positive = buy, negative = sell
    order_type: OrderType
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    asset_class: AssetClass = AssetClass.EQUITY

@dataclass
class FillEvent:
    type:       EventType
    timestamp:  datetime
    symbol:     str
    quantity:   float
    fill_price: float
    commission: float
    multiplier: int
