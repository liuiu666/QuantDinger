"""WebSocket Manager — asyncio thread bridge and connection orchestrator.

Runs an asyncio event loop in a dedicated daemon thread, manages
BinanceWSConnection instances, and dispatches parsed messages to the
BatchWriter for database persistence.
"""

import asyncio
import logging
import os
import threading
import time
from typing import Dict, List, Optional, Set

from app.services.market_data.ws_connection import BinanceWSConnection, DEFAULT_FUTURES_WS
from app.services.market_data.ws_parser import parse_ws_message
from app.services.market_data.writer import BatchWriter

logger = logging.getLogger(__name__)


class WSManager:
    """
    Orchestrates the WebSocket subsystem:
    - Runs asyncio event loop in a daemon thread
    - Manages multiple BinanceWSConnection instances
    - Parses messages and pushes to BatchWriter
    - Provides thread-safe status interface
    """

    def __init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._connections: List[BinanceWSConnection] = []
        self._writer = BatchWriter()
        self._running = False
        self._subscribed_streams: Set[str] = set()
        self._start_time: float = 0
        self._total_messages: int = 0
        self._type_counts: Dict[str, int] = {}

        # Config from env
        self._base_url = os.environ.get(
            "WS_FUTURES_BASE_URL", DEFAULT_FUTURES_WS
        )
        self._kline_timeframes = ["1m", "5m", "1h", "4h", "1d"]
        self._default_symbols = self._load_default_symbols()

    def _load_default_symbols(self) -> List[str]:
        """Load Crypto symbols from qd_watchlist, fallback to env or defaults."""
        try:
            from app.utils.db_postgres import get_pg_connection
            with get_pg_connection() as pg_conn:
                raw_conn = pg_conn._conn
                raw_cursor = raw_conn.cursor()
                raw_cursor.execute(
                    "SELECT symbol FROM qd_watchlist WHERE market = 'Crypto'"
                )
                rows = raw_cursor.fetchall()
            if rows:
                return [r[0].replace("/", "").lower() for r in rows]
        except Exception as e:
            logger.warning(f"WSManager: failed to load watchlist from DB: {e}")

        # Fallback: env variable
        env_symbols = os.environ.get("WS_KLINE_SYMBOLS", "")
        if env_symbols:
            return [s.strip().lower() for s in env_symbols.split(",") if s.strip()]

        # Last resort: hardcoded defaults
        return [
            "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
            "dogeusdt", "adausdt", "avaxusdt", "dotusdt", "linkusdt",
        ]

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        """Start the WebSocket manager in a background thread."""
        if self._running:
            return

        self._running = True
        self._start_time = time.time()
        self._writer.start()
        self._thread = threading.Thread(
            target=self._run_event_loop, daemon=True, name="ws-manager"
        )
        self._thread.start()
        logger.info("WSManager started")

    def stop(self) -> None:
        """Stop the WebSocket manager."""
        self._running = False
        self._writer.stop()
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("WSManager stopped")

    def get_status(self) -> Dict:
        """Thread-safe status query."""
        connections = [c.stats for c in self._connections]
        return {
            "running": self._running,
            "uptime_seconds": int(time.time() - self._start_time) if self._start_time else 0,
            "total_messages": self._total_messages,
            "subscribed_streams": len(self._subscribed_streams),
            "connections": connections,
            "writer": self._writer.stats,
            "type_counts": self._type_counts,
        }

    def subscribe_symbol(self, symbol: str, streams: List[str]) -> None:
        """Thread-safe: add streams for a symbol."""
        if self._loop and not self._loop.is_closed():
            for stream in streams:
                full_stream = f"{symbol.lower()}@{stream}"
                self._subscribed_streams.add(full_stream)
            asyncio.run_coroutine_threadsafe(
                self._subscribe_on_loop(symbol.lower(), streams), self._loop
            )

    def unsubscribe_symbol(self, symbol: str, streams: List[str]) -> None:
        """Thread-safe: remove streams for a symbol."""
        if self._loop and not self._loop.is_closed():
            for stream in streams:
                full_stream = f"{symbol.lower()}@{stream}"
                self._subscribed_streams.discard(full_stream)
            asyncio.run_coroutine_threadsafe(
                self._unsubscribe_on_loop(symbol.lower(), streams), self._loop
            )

    # ── Internal: runs in asyncio thread ────────────────────────────────

    def _run_event_loop(self) -> None:
        """Target for the daemon thread: create and run an asyncio event loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._start_connections())
            self._loop.call_later(60, self._schedule_symbol_refresh)
            self._loop.run_forever()
        except Exception as e:
            logger.error(f"WSManager event loop error: {e}")
        finally:
            self._loop.close()

    def _schedule_symbol_refresh(self) -> None:
        """Periodically check watchlist for symbol changes."""
        if not self._running:
            return
        try:
            new_symbols = set(self._load_default_symbols())
            current_symbols = set(self._default_symbols)
            if new_symbols != current_symbols:
                added = new_symbols - current_symbols
                removed = current_symbols - new_symbols
                if added:
                    for sym in added:
                        self.subscribe_symbol(sym, ["trade", "bookTicker"])
                    logger.info(f"WSManager: added symbols {added}")
                if removed:
                    for sym in removed:
                        self.unsubscribe_symbol(sym, ["trade", "bookTicker"])
                    logger.info(f"WSManager: removed symbols {removed}")
                self._default_symbols = list(new_symbols)
        except Exception as e:
            logger.warning(f"WSManager symbol refresh error: {e}")
        if self._running:
            self._loop.call_later(60, self._schedule_symbol_refresh)

    async def _start_connections(self) -> None:
        """Create and start WebSocket connections for all configured streams."""
        # Build stream list from default symbols
        # Note: kline/markPrice/aggTrade streams may be blocked by some networks
        # (confirmed: only trade and bookTicker work from China mainland).
        # Kline data should be backfilled via REST API instead.
        all_streams = []
        for symbol in self._default_symbols:
            # Trade stream — tick-by-tick trade data (works on all networks)
            all_streams.append(f"{symbol}@trade")
            # BookTicker — best bid/ask (works on all networks)
            all_streams.append(f"{symbol}@bookTicker")

        self._subscribed_streams.update(all_streams)

        # Split into connections (200 streams max each)
        conn_id = 0
        while all_streams:
            batch = all_streams[:200]
            all_streams = all_streams[200:]
            conn = BinanceWSConnection(
                on_message=self._on_message,
                base_url=self._base_url,
                connection_id=conn_id,
            )
            self._connections.append(conn)
            conn_id += 1
            self._loop.create_task(conn.start(batch))

        logger.info(
            f"WSManager: started {len(self._connections)} connections, "
            f"{len(self._subscribed_streams)} streams"
        )

    async def _subscribe_on_loop(self, symbol: str, streams: list) -> None:
        """Subscribe on the event loop."""
        full_streams = [f"{symbol}@{s}" for s in streams]
        for conn in self._connections:
            if conn.can_add(full_streams):
                await conn.subscribe(full_streams)
                return
        # Need a new connection
        conn_id = len(self._connections)
        conn = BinanceWSConnection(
            on_message=self._on_message,
            base_url=self._base_url,
            connection_id=conn_id,
        )
        self._connections.append(conn)
        self._loop.create_task(conn.start(full_streams))

    async def _unsubscribe_on_loop(self, symbol: str, streams: list) -> None:
        """Unsubscribe on the event loop."""
        full_streams = [f"{symbol}@{s}" for s in streams]
        for conn in self._connections:
            await conn.unsubscribe(full_streams)

    def _on_message(self, data: dict) -> None:
        """Handle a raw WS message (called from asyncio thread)."""
        self._total_messages += 1

        result = parse_ws_message(data)
        if result:
            msg_type, symbol, parsed = result
            self._type_counts[msg_type] = self._type_counts.get(msg_type, 0) + 1
            self._writer.enqueue(msg_type, symbol, parsed)
        else:
            key = f"unhandled_{data.get('e', data.get('stream', 'unknown'))}"
            self._type_counts[key] = self._type_counts.get(key, 0) + 1
            if self._type_counts[key] <= 3:
                stream = data.get("stream", "")
                evt = data.get("e", "")
                logger.warning(
                    f"WSManager: unhandled msg, stream={stream}, e={evt}, "
                    f"keys={list(data.keys())[:8]}, preview={str(data)[:300]}"
                )
