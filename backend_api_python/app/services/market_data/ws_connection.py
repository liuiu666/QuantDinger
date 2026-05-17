"""Binance WebSocket connection manager.

Manages the lifecycle of a single Binance combined stream WebSocket connection,
including connect, reconnect, subscribe/unsubscribe, and message dispatch.
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional, Set

import websockets

logger = logging.getLogger(__name__)

DEFAULT_FUTURES_WS = "wss://fstream.binance.com/ws"
MAX_STREAMS_PER_CONNECTION = 200
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 60.0
HEARTBEAT_TIMEOUT = 30  # seconds without message before reconnect


class BinanceWSConnection:
    """Manages a single WebSocket connection to Binance combined streams."""

    def __init__(
        self,
        on_message: Callable[[dict], None],
        base_url: str = DEFAULT_FUTURES_WS,
        connection_id: int = 0,
    ):
        self._base_url = base_url
        self._connection_id = connection_id
        self._on_message = on_message
        self._subscriptions: Set[str] = set()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._reconnect_delay = RECONNECT_BASE_DELAY
        self._last_message_time = 0.0
        self._message_count = 0
        self._error_count = 0

    @property
    def subscriptions(self) -> Set[str]:
        return self._subscriptions.copy()

    @property
    def stream_count(self) -> int:
        return len(self._subscriptions)

    @property
    def is_connected(self) -> bool:
        if self._ws is None:
            return False
        # websockets >= 13 removed .open; use state or try/except
        try:
            return self._ws.open
        except AttributeError:
            from websockets.protocol import State
            return self._ws.state == State.OPEN

    @property
    def stats(self) -> dict:
        return {
            "connection_id": self._connection_id,
            "connected": self.is_connected,
            "streams": len(self._subscriptions),
            "messages": self._message_count,
            "errors": self._error_count,
            "last_message_age": time.time() - self._last_message_time if self._last_message_time else -1,
        }

    def can_add(self, streams: list) -> bool:
        """Check if we can add more streams without exceeding the limit."""
        return len(self._subscriptions) + len(streams) <= MAX_STREAMS_PER_CONNECTION

    async def start(self, initial_streams: Optional[list] = None) -> None:
        """Start the connection loop."""
        if initial_streams:
            self._subscriptions.update(initial_streams)
        self._running = True
        await self._connection_loop()

    async def stop(self) -> None:
        """Stop the connection."""
        self._running = False
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def subscribe(self, streams: list) -> None:
        """Add streams to the subscription. Sends live subscribe if connected."""
        self._subscriptions.update(streams)
        if self.is_connected:
            await self._send_subscribe(streams)

    async def unsubscribe(self, streams: list) -> None:
        """Remove streams from the subscription."""
        self._subscriptions -= set(streams)
        if self.is_connected:
            await self._send_unsubscribe(streams)

    # ── Internal ────────────────────────────────────────────────────────

    async def _connection_loop(self) -> None:
        """Main connection loop with auto-reconnect."""
        while self._running:
            try:
                await self._connect()
                await self._receive_loop()
            except Exception as e:
                self._error_count += 1
                logger.warning(
                    f"WS connection {self._connection_id} error: {e}, "
                    f"reconnecting in {self._reconnect_delay:.1f}s"
                )

            if self._running:
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, RECONNECT_MAX_DELAY
                )

    async def _connect(self) -> None:
        """Establish the WebSocket connection."""
        streams_str = "/".join(sorted(self._subscriptions))
        if not streams_str:
            logger.info(f"WS connection {self._connection_id}: no streams, waiting")
            await asyncio.sleep(5)
            return

        # Build combined stream URL:
        # Correct format: wss://fstream.binance.com/stream?streams=s1/s2/...
        # Note: base_url may end with /ws (single-stream path); strip it.
        base = self._base_url
        if base.endswith("/ws"):
            base = base[:-3]
        url = f"{base}/stream?streams={streams_str}"
        logger.info(
            f"WS connection {self._connection_id}: connecting "
            f"({len(self._subscriptions)} streams) url_len={len(url)}"
        )
        self._ws = await websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
            max_size=2**22,  # 4MB for large depth snapshots
        )
        self._reconnect_delay = RECONNECT_BASE_DELAY
        self._last_message_time = time.time()
        logger.info(f"WS connection {self._connection_id}: connected")

    async def _receive_loop(self) -> None:
        """Receive and dispatch messages."""
        if not self._ws:
            return

        async for raw_message in self._ws:
            self._message_count += 1
            self._last_message_time = time.time()

            try:
                # websockets may return bytes or str depending on version
                if isinstance(raw_message, bytes):
                    raw_message = raw_message.decode("utf-8")
                data = json.loads(raw_message)
                self._on_message(data)
            except json.JSONDecodeError as e:
                logger.debug(f"WS JSON parse error: {e}")
            except Exception as e:
                logger.debug(f"WS message dispatch error: {e}")

            # Heartbeat check
            if time.time() - self._last_message_time > HEARTBEAT_TIMEOUT:
                logger.warning(
                    f"WS connection {self._connection_id}: heartbeat timeout, reconnecting"
                )
                await self._ws.close()
                return

    async def _send_subscribe(self, streams: list) -> None:
        """Send dynamic subscribe message."""
        if not self._ws:
            return
        msg = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": int(time.time() * 1000),
        }
        try:
            await self._ws.send(json.dumps(msg))
        except Exception as e:
            logger.warning(f"WS subscribe send error: {e}")

    async def _send_unsubscribe(self, streams: list) -> None:
        """Send dynamic unsubscribe message."""
        if not self._ws:
            return
        msg = {
            "method": "UNSUBSCRIBE",
            "params": streams,
            "id": int(time.time() * 1000),
        }
        try:
            await self._ws.send(json.dumps(msg))
        except Exception as e:
            logger.warning(f"WS unsubscribe send error: {e}")
