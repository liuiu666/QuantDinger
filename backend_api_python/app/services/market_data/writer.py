"""Batch DB writer for WebSocket data.

Accumulates parsed WS messages in a bounded queue and flushes to PostgreSQL
periodically in batches for high-throughput write performance.
"""

import logging
import threading
import time
import json
from collections import deque
from typing import Optional

from app.services.market_data.service import get_market_data_service

logger = logging.getLogger(__name__)

DEFAULT_FLUSH_INTERVAL = 0.5  # seconds
DEFAULT_MAX_QUEUE_SIZE = 10000
DEFAULT_BATCH_SIZE = 1000


class BatchWriter:
    """
    Background thread that drains a queue of parsed WS messages
    and writes them to PostgreSQL in batches.
    """

    def __init__(
        self,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL,
        max_queue_size: int = DEFAULT_MAX_QUEUE_SIZE,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        self._queue: deque = deque(maxlen=max_queue_size)
        self._flush_interval = flush_interval
        self._batch_size = batch_size
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._total_written = 0
        self._total_batches = 0

    @property
    def queue_size(self) -> int:
        return len(self._queue)

    @property
    def stats(self) -> dict:
        return {
            "queue_size": len(self._queue),
            "total_written": self._total_written,
            "total_batches": self._total_batches,
            "running": self._running,
        }

    def start(self) -> None:
        """Start the background writer thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._writer_loop, daemon=True, name="ws-batch-writer"
        )
        self._thread.start()
        logger.info("BatchWriter started")

    def stop(self) -> None:
        """Stop the writer thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        # Final flush
        self._flush()
        logger.info("BatchWriter stopped")

    def enqueue(self, msg_type: str, symbol: str, data: dict) -> None:
        """Add a parsed WS message to the write queue."""
        self._queue.append((msg_type, symbol, data))

    # ── Internal ────────────────────────────────────────────────────────

    def _writer_loop(self) -> None:
        """Main writer loop: flush periodically."""
        while self._running:
            try:
                self._flush()
            except Exception as e:
                logger.error(f"BatchWriter flush error: {e}")
            time.sleep(self._flush_interval)

    def _flush(self) -> None:
        """Drain the queue and write to DB."""
        if not self._queue:
            return

        # Drain up to batch_size items
        klines = []
        trades = []
        funding = []
        depth = []

        count = 0
        while self._queue and count < self._batch_size:
            try:
                msg_type, symbol, data = self._queue.popleft()
            except IndexError:
                break

            count += 1
            if msg_type == "kline":
                klines.append((symbol, data))
            elif msg_type == "trade":
                trades.append((symbol, data))
            elif msg_type == "markPrice":
                funding.append((symbol, data))
            elif msg_type == "depth":
                depth.append((symbol, data))

        if not count:
            return

        service = get_market_data_service()

        # Write klines
        if klines:
            try:
                for symbol, data in klines:
                    service.store_ws_kline(
                        symbol, data.get("interval", "1m"), data
                    )
                self._total_written += len(klines)
            except Exception as e:
                logger.warning(f"BatchWriter kline write error: {e}")

        # Write trades
        if trades:
            try:
                trade_rows = []
                for symbol, data in trades:
                    trade_rows.append((
                        symbol, data["trade_id"], data["price"],
                        data["quantity"], data["quote_quantity"],
                        data["buyer_is_maker"], data["trade_time"],
                    ))
                service.trade_repo.insert_trades(trade_rows)
                self._total_written += len(trade_rows)
            except Exception as e:
                logger.warning(f"BatchWriter trade write error: {e}")

        # Write funding rates
        if funding:
            try:
                for symbol, data in funding:
                    ft = data.get("funding_time")
                    if ft is not None and ft != 0:
                        service.store_funding_rate(symbol, data)
                self._total_written += len(funding)
            except Exception as e:
                logger.warning(f"BatchWriter funding write error: {e}")

        # Write orderbook snapshots
        if depth:
            try:
                for symbol, data in depth:
                    if data.get("bids") and data.get("asks"):
                        service.orderbook_repo.insert_snapshot(
                            symbol, data["update_time"],
                            data["bids"], data["asks"],
                        )
                self._total_written += len(depth)
            except Exception as e:
                logger.warning(f"BatchWriter depth write error: {e}")

        self._total_batches += 1
