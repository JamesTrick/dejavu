from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto


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
    CRYPTO = auto()

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
    iv: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None

@dataclass
class Position:
    symbol: str
    quantity: float        # negative = short
    avg_cost: float
    asset_class: AssetClass

    multiplier: float = 1.0

    # Options-specific
    underlying: str | None = None
    strike: float | None = None
    expiry: datetime | None = None
    option_type: str | None = None

    def market_value(self, current_price: float) -> float:
        return self.quantity * current_price * self.multiplier


class OrderType(Enum):
    MARKET = auto()
    LIMIT = auto()
    STOP = auto()

@dataclass
class Order:
    symbol: str
    quantity: float        # positive = buy, negative = sell
    order_type: OrderType
    limit_price: float | None = None
    stop_price: float | None = None
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

@dataclass
class MultiLegOrder:
    """Order class for when there are multiple legs, e.g. for options strategies like strangles, spreads, condors, etc.

    This is helpful for tracking and filling strategies without creating time-gaps between legs.

    If fill_as_unit=True, all legs must be filled together.
    """
    legs: list[Order]
    strategy_type: str   # "strangle", "spread", "condor", etc.
    fill_as_unit:  bool = True
