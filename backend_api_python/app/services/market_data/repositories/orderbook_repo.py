"""Orderbook snapshot repository."""

import json
import logging
from typing import List, Dict, Any, Optional, Tuple

from app.services.market_data.repositories.base import BaseRepository

logger = logging.getLogger(__name__)


class OrderbookRepository(BaseRepository):
    def __init__(self):
        super().__init__("qd_orderbook_snapshots", "snapshot_time")

    def insert_snapshot(
        self, symbol: str, snapshot_time: int, bids: list, asks: list,
        depth_levels: int = 20,
    ) -> None:
        """Insert a single orderbook snapshot.
        
        Symbol is normalized (uppercase, no slash) to ensure DB consistency.
        """
        from app.utils.db_postgres import execute_sql
        execute_sql(
            "INSERT INTO qd_orderbook_snapshots "
            "(symbol, snapshot_time, bids, asks, depth_levels) "
            "VALUES (%s, %s, %s, %s, %s)",
            (self._normalize_symbol(symbol), snapshot_time,
             json.dumps(bids), json.dumps(asks), depth_levels),
        )

    def get_latest(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get the most recent orderbook snapshot for a symbol."""
        results = self.query_latest(symbol, limit=1)
        if results:
            row = results[0]
            # Parse JSONB fields
            if isinstance(row.get("bids"), str):
                row["bids"] = json.loads(row["bids"])
            if isinstance(row.get("asks"), str):
                row["asks"] = json.loads(row["asks"])
            return row
        return None
