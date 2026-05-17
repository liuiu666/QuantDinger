"""Open interest history repository."""

import logging
from typing import List, Dict, Any, Optional, Tuple

from app.services.market_data.repositories.base import BaseRepository

logger = logging.getLogger(__name__)

OI_COLUMNS = ["symbol", "snapshot_time", "open_interest", "open_interest_usd", "source"]


class OpenInterestRepository(BaseRepository):
    def __init__(self):
        super().__init__("qd_open_interest_hist", "snapshot_time")

    def insert_records(self, rows: List[Tuple]) -> int:
        """Batch insert OI records.
        
        Symbols are normalized (uppercase, no slash) to ensure DB consistency.
        """
        return self.bulk_upsert(OI_COLUMNS, self._normalize_rows(rows), ["symbol", "snapshot_time"])

    def get_history(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        return self.query_range(symbol, start_time, end_time, limit)
