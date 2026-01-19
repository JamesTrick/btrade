from dataclasses import dataclass
from datetime import datetime
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator

import pandas as pd

import backtrader as bt


@dataclass(frozen=True, slots=True)
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    openinterest: float = 0.0

    def to_lines(self) -> dict:
        return {
            'datetime': bt.date2num(self.timestamp),
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'openinterest': 0.0
        }


@dataclass
class BarSeries:
    """Container for multiple bars with metadata"""
    bars: list[Bar]
    symbol: str
    timeframe: str

    def __iter__(self) -> Iterator[Bar]:
        return iter(self.bars)

    def __len__(self) -> int:
        return len(self.bars)

    def to_dataframe(self) -> pd.DataFrame:
        """Optional: convert to pandas if needed"""
        if not self.bars:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        df = pd.DataFrame([vars(b) for b in self.bars])
        return df.set_index("timestamp")


class MarketDataFeed(ABC):
    """Base class for Market data feeds."""

    @abstractmethod
    def fetch(self, symbol: str, start: datetime, end: datetime, **kwargs) -> Iterable[Bar]:
        """Return the historical bars for a given symbol"""


class ValidationError(Exception):
    pass
