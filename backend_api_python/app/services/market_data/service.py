"""Unified market data service with DB-first, API-fallback query pattern."""

import logging
import time
from typing import List, Dict, Any, Optional

from app.services.market_data.repositories.kline_repo import KlineRepository, KLINE_COLUMNS
from app.services.market_data.repositories.trade_repo import TradeRepository
from app.services.market_data.repositories.orderbook_repo import OrderbookRepository
from app.services.market_data.repositories.funding_repo import FundingRateRepository
from app.services.market_data.repositories.open_interest_repo import OpenInterestRepository
from app.services.market_data.repositories.long_short_repo import LongShortRatioRepository
from app.services.market_data.repositories.backfill_job_repo import BackfillJobRepository

logger = logging.getLogger(__name__)


class MarketDataService:
    """
    Unified query interface for market data.
    Strategy: query local PostgreSQL first, fall back to external API if insufficient.
    """

    def __init__(self):
        self.kline_repo = KlineRepository()
        self.trade_repo = TradeRepository()
        self.orderbook_repo = OrderbookRepository()
        self.funding_repo = FundingRateRepository()
        self.oi_repo = OpenInterestRepository()
        self.lsr_repo = LongShortRatioRepository()
        self.backfill_repo = BackfillJobRepository()

    # ── Klines ──────────────────────────────────────────────────────────

    def get_klines(
        self,
        symbol: str,
        timeframe: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 300,
    ) -> List[Dict[str, Any]]:
        """
        Get klines from local DB. Returns list in standard format:
        [{"time": int(seconds), "open": float, ...}]
        Returns empty list if no data in DB.
        """
        try:
            rows = self.kline_repo.get_klines(
                symbol=symbol,
                timeframe=timeframe,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )
            if rows:
                return [self.kline_repo.to_kline_dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"MarketDataService.get_klines DB error: {e}")
        return []

    def get_klines_with_fallback(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 300,
        before_time: Optional[int] = None,
        after_time: Optional[int] = None,
        market: str = "Crypto",
        exchange_id: Optional[str] = None,
        market_type: Optional[str] = None,
        max_age_seconds: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        DB-first kline query with API fallback.
        If DB has enough data, return it. Otherwise fall back to DataSourceFactory.
        On API success, persist to DB asynchronously.

        When requesting latest data (no before_time), automatically applies
        a freshness gate: if the latest candle is older than the timeframe-
        specific threshold, the API fallback is triggered to get fresh data.

        Args:
            max_age_seconds: if set, DB data must have a latest candle within this
                             many seconds to be considered fresh.  Stale DB data
                             triggers the API fallback.
        """
        # Convert seconds to ms for DB query
        start_ms = int(after_time * 1000) if after_time else None
        end_ms = int(before_time * 1000) if before_time else None

        # Try local DB first
        db_klines = self.get_klines(symbol, timeframe, start_ms, end_ms, limit)
        db_klines_fresh = None  # will hold DB data that passes the freshness gate
        if db_klines and len(db_klines) >= limit * 0.8:
            # Auto-detect freshness threshold when requesting latest data
            if before_time is None and max_age_seconds is None:
                max_age_seconds = self._guess_max_age(timeframe)
            # Freshness gate: if caller specified max_age, check latest candle age
            if max_age_seconds is not None:
                latest_time = db_klines[-1].get("time", 0)
                if latest_time <= 0 or (time.time() - latest_time) >= max_age_seconds:
                    logger.debug(
                        f"DB kline stale for {symbol}/{timeframe} "
                        f"(age={int(time.time() - latest_time)}s > {max_age_seconds}s), "
                        f"falling back to API"
                    )
                else:
                    db_klines_fresh = db_klines
            else:
                db_klines_fresh = db_klines
            if db_klines_fresh:
                return db_klines_fresh

        # Fallback to DataSourceFactory (existing CCXT path)
        try:
            from app.data_sources.factory import DataSourceFactory
            api_klines = DataSourceFactory.get_kline(
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                limit=limit,
                before_time=before_time,
                after_time=after_time,
                exchange_id=exchange_id,
                market_type=market_type,
            )
            # If primary exchange (spot) didn't have the symbol, try Binance Futures
            if not api_klines and market and market.lower() == "crypto":
                logger.debug(
                    f"Primary CCXT fallback returned no data for {symbol}, "
                    f"trying Binance Futures (swap)"
                )
                api_klines = DataSourceFactory.get_kline(
                    market=market,
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=limit,
                    before_time=before_time,
                    after_time=after_time,
                    exchange_id="binance",
                    market_type="swap",
                )
            # Persist to DB in background
            if api_klines:
                self._persist_klines_async(symbol, timeframe, api_klines)
            return api_klines if api_klines else db_klines
        except Exception as e:
            logger.warning(f"MarketDataService API fallback error: {e}")
            return db_klines

    @staticmethod
    def _guess_max_age(timeframe: str) -> int:
        """Guess a reasonable max age (seconds) for a given kline timeframe.
        
        When requesting latest data, the latest candle shouldn't be older than
        ~2x the candle duration. This ensures the API fallback fills gaps
        between poller cycles.
        """
        mapping = {
            "1m": 120, "3m": 360, "5m": 600, "15m": 1800,
            "30m": 3600, "1h": 7200, "2h": 14400, "4h": 28800,
            "6h": 43200, "8h": 57600, "12h": 86400,
            "1d": 172800, "1w": 1209600, "1M": 2592000,
        }
        return mapping.get(timeframe, 300)

    def _persist_klines_async(
        self, symbol: str, timeframe: str, klines: List[Dict[str, Any]]
    ) -> None:
        """Convert kline dicts to DB rows and persist (fire-and-forget)."""
        try:
            rows = []
            for k in klines:
                open_time_ms = k["time"] * 1000
                rows.append((
                    symbol, timeframe, open_time_ms,
                    k.get("open", 0), k.get("high", 0), k.get("low", 0),
                    k.get("close", 0), k.get("volume", 0),
                    k.get("quote_volume", 0), k.get("trades", 0),
                    True, "rest",
                ))
            if rows:
                self.kline_repo.upsert_klines(rows)
        except Exception as e:
            logger.warning(f"_persist_klines_async error: {e}")

    def store_ws_kline(self, symbol: str, timeframe: str, kline_data: dict) -> None:
        """Store a single kline from WebSocket data."""
        try:
            k = kline_data
            row = ((
                symbol, timeframe, k["open_time"],
                k.get("open", 0), k.get("high", 0), k.get("low", 0),
                k.get("close", 0), k.get("volume", 0),
                k.get("quote_volume", 0), k.get("trades", 0),
                k.get("is_final", True), "ws",
            ),)
            self.kline_repo.upsert_klines(list(row))
        except Exception as e:
            logger.warning(f"store_ws_kline error: {e}")

    # ── Trades ──────────────────────────────────────────────────────────

    def get_trades(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        try:
            return self.trade_repo.get_trades(symbol, start_time, end_time, limit)
        except Exception as e:
            logger.warning(f"MarketDataService.get_trades error: {e}")
            return []

    def store_ws_trades(self, rows: list) -> None:
        """Store trade records from WebSocket."""
        try:
            if rows:
                self.trade_repo.insert_trades(rows)
        except Exception as e:
            logger.warning(f"store_ws_trades error: {e}")

    # ── Orderbook ───────────────────────────────────────────────────────

    def get_latest_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            return self.orderbook_repo.get_latest(symbol)
        except Exception as e:
            logger.warning(f"MarketDataService.get_latest_orderbook error: {e}")
            return None

    def store_book_ticker(self, symbol: str, data: dict) -> None:
        """Store best bid/ask ticker from WebSocket (upsert, one row per symbol)."""
        try:
            from app.utils.db_postgres import execute_sql
            execute_sql(
                "INSERT INTO qd_orderbook_ticker "
                "(symbol, bid_price, bid_qty, ask_price, ask_qty, update_time) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (symbol) DO UPDATE SET "
                "bid_price = EXCLUDED.bid_price, bid_qty = EXCLUDED.bid_qty, "
                "ask_price = EXCLUDED.ask_price, ask_qty = EXCLUDED.ask_qty, "
                "update_time = EXCLUDED.update_time, updated_at = NOW()",
                (
                    self.orderbook_repo._normalize_symbol(symbol),
                    data.get("bid_price", 0), data.get("bid_qty", 0),
                    data.get("ask_price", 0), data.get("ask_qty", 0),
                    data.get("update_time", 0),
                ),
            )
        except Exception as e:
            logger.warning(f"MarketDataService.store_book_ticker error: {e}")

    # ── Funding Rate ────────────────────────────────────────────────────

    def get_funding_rate_history(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        try:
            return self.funding_repo.get_history(symbol, start_time, end_time, limit)
        except Exception as e:
            logger.warning(f"MarketDataService.get_funding_rate_history error: {e}")
            return []

    def store_funding_rate(self, symbol: str, data: dict) -> None:
        """Store a single funding rate record."""
        try:
            row = [(
                symbol, data["funding_time"], data["funding_rate"],
                data.get("mark_price", 0), data.get("index_price", 0),
                data.get("source", "ws"),
            )]
            self.funding_repo.upsert_rates(row)
        except Exception as e:
            logger.warning(f"store_funding_rate error: {e}")

    # ── Open Interest ───────────────────────────────────────────────────

    def get_open_interest_history(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        try:
            return self.oi_repo.get_history(symbol, start_time, end_time, limit)
        except Exception as e:
            logger.warning(f"MarketDataService.get_open_interest_history error: {e}")
            return []

    # ── Long/Short Ratio ────────────────────────────────────────────────

    def get_long_short_history(
        self,
        symbol: str,
        ratio_type: str = "account",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        try:
            return self.lsr_repo.get_history(symbol, ratio_type, start_time, end_time, limit)
        except Exception as e:
            logger.warning(f"MarketDataService.get_long_short_history error: {e}")
            return []

    # ── Stats ───────────────────────────────────────────────────────────

    def get_data_stats(self) -> Dict[str, Any]:
        """Get overall data coverage statistics."""
        stats = {}
        for repo_info in [
            ("klines", self.kline_repo),
            ("trades", self.trade_repo),
            ("funding_rates", self.funding_repo),
            ("open_interest", self.oi_repo),
            ("long_short_ratio", self.lsr_repo),
        ]:
            name, repo = repo_info
            try:
                from app.utils.db_postgres import execute_sql
                result = execute_sql(
                    f"SELECT COUNT(*) as total, "
                    f"COUNT(DISTINCT symbol) as symbols "
                    f"FROM {repo.table_name}"
                )
                if result:
                    stats[name] = {
                        "total_records": result[0]["total"],
                        "unique_symbols": result[0]["symbols"],
                    }
            except Exception:
                stats[name] = {"total_records": 0, "unique_symbols": 0}
        return stats


# ── Singleton ───────────────────────────────────────────────────────────

_service: Optional[MarketDataService] = None


def get_market_data_service() -> MarketDataService:
    global _service
    if _service is None:
        _service = MarketDataService()
    return _service
