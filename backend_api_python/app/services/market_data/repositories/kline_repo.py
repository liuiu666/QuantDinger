"""Kline (OHLCV) repository."""

import logging
from typing import List, Dict, Any, Optional, Tuple

from app.services.market_data.repositories.base import BaseRepository

logger = logging.getLogger(__name__)

KLINE_COLUMNS = [
    "symbol", "timeframe", "open_time", "open", "high", "low", "close",
    "volume", "quote_volume", "trades", "is_final", "source",
]
KLINE_CONFLICT = ["symbol", "timeframe", "open_time"]
KLINE_UPDATE = ["high", "low", "close", "volume", "quote_volume", "trades", "is_final", "updated_at"]


class KlineRepository(BaseRepository):
    def __init__(self):
        super().__init__("qd_klines", "open_time")

    def upsert_klines(self, rows: List[Tuple]) -> int:
        """Batch upsert kline records. Rows must match KLINE_COLUMNS order.
        
        Symbols are normalized (uppercase, no slash) to ensure DB consistency.
        """
        return self.bulk_upsert(
            KLINE_COLUMNS, self._normalize_rows(rows), KLINE_CONFLICT, KLINE_UPDATE
        )

    def get_klines(
        self,
        symbol: str,
        timeframe: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Query klines by symbol, timeframe, and optional time range.
        
        Symbol is normalized by BaseRepository.query_range().
        Timeframe is normalized to lowercase to match DB storage format.
        """
        return self.query_range(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            extra_where="timeframe = %s",
            extra_params=(timeframe.lower(),),
        )

    def get_latest_kline(
        self, symbol: str, timeframe: str
    ) -> Optional[Dict[str, Any]]:
        """Get the most recent kline for a symbol/timeframe."""
        results = self.query_latest(
            symbol=symbol,
            limit=1,
            extra_where="timeframe = %s",
            extra_params=(timeframe.lower(),),
        )
        return results[0] if results else None

    def get_coverage(
        self, symbol: str, timeframe: str
    ) -> Optional[Dict[str, int]]:
        """Get time range coverage for a symbol/timeframe."""
        return self.get_time_range(
            symbol=symbol,
            extra_where="timeframe = %s",
            extra_params=(timeframe.lower(),),
        )

    def to_kline_dict(self, row: Dict) -> Dict[str, Any]:
        """Convert DB row to standard kline format used by the rest of the app."""
        return {
            "time": int(row["open_time"]) // 1000,  # ms -> seconds
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
            "quote_volume": float(row.get("quote_volume", 0)),
        }
