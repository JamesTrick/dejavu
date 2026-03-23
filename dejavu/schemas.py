import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto


class EventType(Enum):
    MARKET = auto()  # New price data arrived
    SIGNAL = auto()  # Strategy generated a signal
    ORDER = auto()  # Order submitted
    FILL = auto()  # Order was executed


class AssetClass(Enum):
    EQUITY = auto()
    OPTION = auto()
    FUTURE = auto()
    FX = auto()
    CRYPTO = auto()
    COMMODITY = auto()


@dataclass(frozen=True, kw_only=True)
class Instrument:
    symbol: str
    asset_class: AssetClass
    multiplier: float = 1.0


@dataclass(frozen=True, kw_only=True)
class Option(Instrument):
    underlying: str
    strike: float
    expiry: datetime
    option_type: str


@dataclass(slots=True)
class Event:
    type: EventType
    timestamp: datetime


@dataclass(slots=True)
class MarketEvent(Event):
    """Generic market event, representing a new price update for an asset. It supports OHLCV data and be used for both
    equities and options (with additional fields in OptionMarketEvent)."""

    instrument: Instrument
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    bid: float | None = None
    ask: float | None = None

    @property
    def spread(self) -> float | None:
        if self.bid is not None and self.ask is not None:
            return self.ask - self.bid
        return None

    @property
    def mid(self) -> float | None:
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / 2
        return None


@dataclass(slots=True)
class OptionMarketEvent(MarketEvent):
    underlying: str = ""
    strike: float = 0.0
    expiry: datetime = field(default_factory=datetime.now)
    option_type: str = "C"  # "C" or "P"
    iv: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None


@dataclass(slots=True)
class Position:
    instrument: Instrument
    quantity: float  # negative = short
    avg_cost: float

    def market_value(self, current_price: float) -> float:
        return self.quantity * current_price * self.instrument.multiplier


class OrderType(Enum):
    MARKET = auto()
    LIMIT = auto()
    STOP = auto()


@dataclass(slots=True)
class Order:
    instrument: Instrument
    quantity: float  # positive = buy, negative = sell
    order_type: OrderType
    limit_price: float | None = None
    stop_price: float | None = None
    order_id: str = field(default_factory=lambda: uuid.uuid4().hex)


@dataclass
class FillEvent(Event):
    order_id: str
    instrument: Instrument
    quantity: float
    fill_price: float
    commission: float
    bid: float | None = None
    ask: float | None = None

    @property
    def spread_cost(self) -> float | None:
        """How much of the fill cost was due to the spread."""
        if self.bid is None or self.ask is None:
            return None
        mid = (self.bid + self.ask) / 2
        return (
            abs(self.fill_price - mid) * abs(self.quantity) * self.instrument.multiplier
        )


@dataclass
class OrderEvent(Event):
    order: "Order"


@dataclass
class MultiLegOrder:
    """Order class for when there are multiple legs, e.g. for options strategies like strangles, spreads, condors, etc.

    This is helpful for tracking and filling strategies without creating time-gaps between legs.

    If fill_as_unit=True, all legs must be filled together.
    """

    legs: list[Order]
    strategy_type: str  # "strangle", "spread", "condor", etc.
    fill_as_unit: bool = True
    order_id: str = field(default_factory=lambda: uuid.uuid4().hex)


class FillTiming(Enum):
    SAME_BAR = auto()  # fill immediately on current event (good for intraday)
    NEXT_BAR = (
        auto()
    )  # queue and fill on next event for this symbol (realistic for EOD)
