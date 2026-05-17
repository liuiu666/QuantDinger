"""Funding rate history repository."""

import logging
from typing import List, Dict, Any, Optional, Tuple

from app.services.market_data.repositories.base import BaseRepository

logger = logging.getLogger(__name__)

FUNDING_COLUMNS = [
    "symbol", "funding_time", "funding_rate", "mark_price", "index_price", "source",
]
FUNDING_CONFLICT = ["symbol", "funding_time"]


class FundingRateRepository(BaseRepository):
    def __init__(self):
        super().__init__("qd_funding_rates", "funding_time")

    def upsert_rates(self, rows: List[Tuple]) -> int:
        """Batch upsert funding rate records.
        
        Symbols are normalized (uppercase, no slash) to ensure DB consistency.
        """
        return self.bulk_upsert(FUNDING_COLUMNS, self._normalize_rows(rows), FUNDING_CONFLICT)

    def get_history(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        return self.query_range(symbol, start_time, end_time, limit)

    def get_latest_rate(self, symbol: str) -> Optional[Dict[str, Any]]:
        results = self.query_latest(symbol, limit=1)
        return results[0] if results else None
