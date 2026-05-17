"""Market Data API endpoints.

Provides REST API for querying stored market data, managing backfill jobs,
and checking WebSocket connection status.
"""

import logging
from datetime import datetime, timezone

from flask import Blueprint, request, jsonify

from app.services.market_data.service import get_market_data_service
from app.services.market_data.repositories.backfill_job_repo import BackfillJobRepository

logger = logging.getLogger(__name__)

market_data_bp = Blueprint("market_data", __name__)


def _success(data=None, msg="ok"):
    return jsonify({"code": 1, "msg": msg, "data": data})


def _error(msg="error", code=0, status=400):
    return jsonify({"code": code, "msg": msg}), status


@market_data_bp.route("/klines", methods=["GET"])
def get_klines():
    """Query stored kline data from PostgreSQL."""
    symbol = request.args.get("symbol", "").upper()
    timeframe = request.args.get("timeframe", "1m")
    start_time = request.args.get("start_time", type=int)
    end_time = request.args.get("end_time", type=int)
    limit = request.args.get("limit", 500, type=int)

    if not symbol:
        return _error("symbol is required")

    service = get_market_data_service()
    data = service.get_klines(symbol, timeframe, start_time, end_time, limit)
    return _success(data)


@market_data_bp.route("/trades", methods=["GET"])
def get_trades():
    """Query stored trade data."""
    symbol = request.args.get("symbol", "").upper()
    start_time = request.args.get("start_time", type=int)
    end_time = request.args.get("end_time", type=int)
    limit = request.args.get("limit", 500, type=int)

    if not symbol:
        return _error("symbol is required")

    service = get_market_data_service()
    data = service.get_trades(symbol, start_time, end_time, limit)
    return _success(data)


@market_data_bp.route("/funding-rate", methods=["GET"])
def get_funding_rate():
    """Query stored funding rate history."""
    symbol = request.args.get("symbol", "").upper()
    start_time = request.args.get("start_time", type=int)
    end_time = request.args.get("end_time", type=int)
    limit = request.args.get("limit", 500, type=int)

    if not symbol:
        return _error("symbol is required")

    service = get_market_data_service()
    data = service.get_funding_rate_history(symbol, start_time, end_time, limit)
    return _success(data)


@market_data_bp.route("/open-interest", methods=["GET"])
def get_open_interest():
    """Query stored open interest history."""
    symbol = request.args.get("symbol", "").upper()
    start_time = request.args.get("start_time", type=int)
    end_time = request.args.get("end_time", type=int)
    limit = request.args.get("limit", 500, type=int)

    if not symbol:
        return _error("symbol is required")

    service = get_market_data_service()
    data = service.get_open_interest_history(symbol, start_time, end_time, limit)
    return _success(data)


@market_data_bp.route("/long-short-ratio", methods=["GET"])
def get_long_short_ratio():
    """Query stored long/short ratio history."""
    symbol = request.args.get("symbol", "").upper()
    ratio_type = request.args.get("ratio_type", "account")
    start_time = request.args.get("start_time", type=int)
    end_time = request.args.get("end_time", type=int)
    limit = request.args.get("limit", 500, type=int)

    if not symbol:
        return _error("symbol is required")

    service = get_market_data_service()
    data = service.get_long_short_history(symbol, ratio_type, start_time, end_time, limit)
    return _success(data)


@market_data_bp.route("/orderbook", methods=["GET"])
def get_orderbook():
    """Get latest orderbook snapshot."""
    symbol = request.args.get("symbol", "").upper()

    if not symbol:
        return _error("symbol is required")

    service = get_market_data_service()
    data = service.get_latest_orderbook(symbol)
    if data:
        return _success(data)
    return _error("no orderbook data found", status=404)


def _to_ms(val, name):
    """Convert start_time/end_time to Unix milliseconds.

    Accepts:
      - int/float (treated as already ms)
      - ISO 8601 string (e.g. 2026-05-16T00:00:00Z)
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except (ValueError, TypeError):
            raise ValueError(f"{name}: invalid format '{val}', use Unix ms or ISO 8601")
    raise ValueError(f"{name}: unsupported type {type(val)}")


@market_data_bp.route("/backfill", methods=["POST"])
def create_backfill_job():
    """Trigger a new backfill job."""
    body = request.get_json(silent=True) or {}
    symbol = body.get("symbol", "").upper().replace("/", "")
    data_type = body.get("data_type", "kline")
    timeframe = body.get("timeframe", "1m")

    if not symbol:
        return _error("symbol is required")
    if "start_time" not in body or "end_time" not in body:
        return _error("start_time and end_time are required (Unix ms or ISO 8601)")
    if data_type not in ("kline", "trade", "funding", "oi", "lsr"):
        return _error("data_type must be one of: kline, trade, funding, oi, lsr")

    try:
        start_time = _to_ms(body["start_time"], "start_time")
        end_time = _to_ms(body["end_time"], "end_time")
    except ValueError as e:
        return _error(str(e))

    repo = BackfillJobRepository()
    job = repo.create_job(
        symbol=symbol,
        data_type=data_type,
        start_time=start_time,
        end_time=end_time,
        timeframe=timeframe if data_type == "kline" else None,
    )
    if job:
        return _success(job, msg="backfill job created")
    return _error("failed to create backfill job", status=500)


@market_data_bp.route("/backfill/status", methods=["GET"])
def get_backfill_status():
    """List backfill jobs and their status."""
    repo = BackfillJobRepository()
    jobs = repo.get_all_jobs()
    return _success(jobs)


@market_data_bp.route("/ws/status", methods=["GET"])
def get_ws_status():
    """WebSocket connection status."""
    try:
        from flask import current_app
        ws_manager = current_app.extensions.get('ws_manager')
        if ws_manager:
            return _success(ws_manager.get_status())
        return _success({"running": False})
    except Exception as e:
        return _success({"running": False, "error": str(e)})


@market_data_bp.route("/poller/status", methods=["GET"])
def get_poller_status():
    """Scheduled poller status."""
    try:
        from flask import current_app
        poller = current_app.extensions.get('poller')
        if poller:
            return _success(poller.get_status())
        return _success({"running": False})
    except Exception as e:
        return _success({"running": False, "error": str(e)})


@market_data_bp.route("/stats", methods=["GET"])
def get_stats():
    """Data coverage statistics."""
    service = get_market_data_service()
    stats = service.get_data_stats()
    return _success(stats)
