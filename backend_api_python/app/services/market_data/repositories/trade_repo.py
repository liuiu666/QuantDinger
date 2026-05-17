"""Trade (tick) repository."""

import logging
from typing import List, Dict, Any, Optional, Tuple

from app.services.market_data.repositories.base import BaseRepository

logger = logging.getLogger(__name__)

TRADE_COLUMNS = [
    "symbol", "trade_id", "price", "quantity", "quote_quantity",
    "buyer_is_maker", "trade_time",
]
TRADE_CONFLICT = ["symbol", "trade_id"]


class TradeRepository(BaseRepository):
    def __init__(self):
        super().__init__("qd_trades", "trade_time")

    def insert_trades(self, rows: List[Tuple]) -> int:
        """Batch insert trades. ON CONFLICT DO NOTHING (trades are immutable).
        
        Symbols are normalized (uppercase, no slash) to ensure DB consistency.
        """
        return self.bulk_upsert(TRADE_COLUMNS, self._normalize_rows(rows), TRADE_CONFLICT)

    def get_trades(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        return self.query_range(symbol, start_time, end_time, limit)
