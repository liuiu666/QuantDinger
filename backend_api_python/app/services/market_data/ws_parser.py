"""Binance WebSocket message parser.

Handles parsing of all Binance stream event types:
- kline (candlestick updates)
- trade (individual trades)
- bookTicker (best bid/ask)
- depth (partial orderbook)
- markPrice (mark price + funding rate)
"""

import logging
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)


def parse_ws_message(data: dict) -> Optional[Tuple[str, str, dict]]:
    """
    Parse a Binance combined stream message.
    Returns (stream_type, symbol, parsed_data) or None.

    Combined stream format:
    {"stream": "btcusdt@kline_1m", "data": {...}}
    """
    if not data:
        return None

    stream = data.get("stream", "")
    payload = data.get("data", data)

    if not stream:
        # Fallback: handle single-stream format (no wrapper)
        # Infer stream type from event type 'e' field
        event_type = data.get("e", "")
        if event_type == "kline":
            k = data.get("k", {})
            symbol = k.get("s", data.get("s", "")).upper()
            interval = k.get("i", "1m")
            parsed = parse_kline_event(data)
            if parsed:
                return ("kline", symbol, parsed)
        elif event_type == "trade":
            symbol = data.get("s", "").upper()
            parsed = parse_trade_event(data)
            if parsed:
                return ("trade", symbol, parsed)
        elif event_type == "bookTicker":
            symbol = data.get("s", "").upper()
            parsed = parse_book_ticker_event(data)
            if parsed:
                return ("bookTicker", symbol, parsed)
        elif event_type == "depthUpdate":
            symbol = data.get("s", "").upper()
            parsed = parse_depth_event(data)
            if parsed:
                return ("depth", symbol, parsed)
        elif event_type == "markPriceUpdate":
            symbol = data.get("s", "").upper()
            parsed = parse_mark_price_event(data)
            if parsed:
                return ("markPrice", symbol, parsed)
        return None

    # Extract stream type
    if "@" not in stream:
        return None

    parts = stream.split("@", 1)
    symbol = parts[0].upper()
    stream_type_full = parts[1]

    if stream_type_full.startswith("kline_"):
        parsed = parse_kline_event(payload)
        if parsed:
            return ("kline", symbol, parsed)
    elif stream_type_full == "trade":
        parsed = parse_trade_event(payload)
        if parsed:
            return ("trade", symbol, parsed)
    elif stream_type_full == "bookTicker":
        parsed = parse_book_ticker_event(payload)
        if parsed:
            return ("bookTicker", symbol, parsed)
    elif stream_type_full.startswith("depth"):
        parsed = parse_depth_event(payload)
        if parsed:
            return ("depth", symbol, parsed)
    elif stream_type_full.startswith("markPrice"):
        parsed = parse_mark_price_event(payload)
        if parsed:
            return ("markPrice", symbol, parsed)

    return None


def parse_kline_event(data: dict) -> Optional[dict]:
    """
    Parse kline event.
    Returns: {open_time, open, high, low, close, volume, quote_volume,
              trades, is_final, interval}
    """
    try:
        k = data.get("k", data)
        return {
            "open_time": k["t"],
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"]),
            "quote_volume": float(k.get("q", 0)),
            "trades": int(k.get("n", 0)),
            "is_final": bool(k.get("x", False)),
            "interval": k.get("i", "1m"),
        }
    except (KeyError, ValueError, TypeError) as e:
        logger.debug(f"parse_kline_event error: {e}")
        return None


def parse_trade_event(data: dict) -> Optional[dict]:
    """
    Parse trade event.
    Returns: {trade_id, price, quantity, quote_quantity, buyer_is_maker, trade_time}
    """
    try:
        return {
            "trade_id": data["t"],
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "quote_quantity": float(data.get("Y", 0)) or float(data["p"]) * float(data["q"]),
            "buyer_is_maker": bool(data.get("m", False)),
            "trade_time": data["T"],
        }
    except (KeyError, ValueError, TypeError) as e:
        logger.debug(f"parse_trade_event error: {e}")
        return None


def parse_book_ticker_event(data: dict) -> Optional[dict]:
    """Parse best bid/ask ticker event."""
    try:
        return {
            "bid_price": float(data["b"]),
            "bid_qty": float(data["B"]),
            "ask_price": float(data["a"]),
            "ask_qty": float(data["A"]),
            "update_time": data.get("T", data.get("E", 0)),
        }
    except (KeyError, ValueError, TypeError) as e:
        logger.debug(f"parse_book_ticker_event error: {e}")
        return None


def parse_depth_event(data: dict) -> Optional[dict]:
    """Parse partial depth event."""
    try:
        bids = [[float(p), float(q)] for p, q in data.get("b", [])]
        asks = [[float(p), float(q)] for p, q in data.get("a", [])]
        return {
            "bids": bids,
            "asks": asks,
            "last_update_id": data.get("lastUpdateId", 0),
            "update_time": data.get("E", 0),
        }
    except (ValueError, TypeError) as e:
        logger.debug(f"parse_depth_event error: {e}")
        return None


def parse_mark_price_event(data: dict) -> Optional[dict]:
    """
    Parse mark price / funding rate event.
    Returns: {mark_price, index_price, funding_rate, funding_time, ...}
    """
    try:
        return {
            "mark_price": float(data.get("p", 0)),
            "index_price": float(data.get("i", 0)),
            "funding_rate": float(data.get("r", 0)),
            "funding_time": data.get("T", 0),
            "estimated_rate": float(data.get("e", 0)) if "e" in data else None,
            "next_funding_time": data.get("C", 0) if "C" in data else None,
        }
    except (ValueError, TypeError) as e:
        logger.debug(f"parse_mark_price_event error: {e}")
        return None
