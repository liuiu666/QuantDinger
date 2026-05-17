"""Long/short ratio history repository."""

import logging
from typing import List, Dict, Any, Optional, Tuple

from app.services.market_data.repositories.base import BaseRepository

logger = logging.getLogger(__name__)

LSR_COLUMNS = [
    "symbol", "snapshot_time", "long_short_ratio", "long_account",
    "short_account", "ratio_type", "source",
]
LSR_CONFLICT = ["symbol", "ratio_type", "snapshot_time"]


class LongShortRatioRepository(BaseRepository):
    def __init__(self):
        super().__init__("qd_long_short_ratio", "snapshot_time")

    def upsert_ratios(self, rows: List[Tuple]) -> int:
        """Batch upsert long/short ratio records.
        
        Symbols are normalized (uppercase, no slash) to ensure DB consistency.
        """
        return self.bulk_upsert(LSR_COLUMNS, self._normalize_rows(rows), LSR_CONFLICT)

    def get_history(
        self,
        symbol: str,
        ratio_type: str = "account",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        return self.query_range(
            symbol, start_time, end_time, limit,
            extra_where="ratio_type = %s",
            extra_params=(ratio_type,),
        )
