"""Scheduled REST poller for Binance market data.

Periodically fetches klines, funding rates, open interest, and long/short
ratios via Binance Futures REST API and persists them to PostgreSQL.

Runs as a daemon thread with its own asyncio event loop, following the
same pattern as BackfillService and WSManager.
"""

import asyncio
import logging
import os
import threading
import time
from typing import Dict, List, Optional

import aiohttp

from app.services.market_data.repositories.kline_repo import KlineRepository
from app.services.market_data.repositories.funding_repo import FundingRateRepository
from app.services.market_data.repositories.open_interest_repo import OpenInterestRepository
from app.services.market_data.repositories.long_short_repo import LongShortRatioRepository

logger = logging.getLogger(__name__)

BINANCE_FAPI = "https://fapi.binance.com"
REQUEST_TIMEOUT = 15
TICK_INTERVAL = 10  # seconds between main loop ticks


class ScheduledPoller:
    """Periodically polls Binance REST API for market data."""

    def __init__(self):
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = False
        self._start_time: float = 0
        self._first_run = True

        # Repositories
        self._kline_repo = KlineRepository()
        self._funding_repo = FundingRateRepository()
        self._oi_repo = OpenInterestRepository()
        self._lsr_repo = LongShortRatioRepository()

        # Config from env
        self._rate_limit = int(os.environ.get("POLLER_RATE_LIMIT_PER_SEC", "8"))
        self._initial_hours = int(os.environ.get("POLLER_INITIAL_BACKFILL_HOURS", "6"))
        self._symbols = self._load_crypto_symbols()
        self._kline_timeframes = [
            tf.strip()
            for tf in os.environ.get("POLLER_KLINE_INTERVALS", "1m,5m,1h,4h,1d").split(",")
            if tf.strip()
        ]

        # Per-task status tracking
        self._task_status: Dict[str, Dict] = {}
        for name, interval in [
            ("kline", 60),   # 1 min — reduced from 300s for fresher data
            ("funding", 3600),
            ("oi_hist", 300),
            ("lsr", 3600),
        ]:
            self._task_status[name] = {
                "interval_sec": interval,
                "last_poll_time": 0,
                "next_poll_time": 0,
                "last_record_count": 0,
                "total_records": 0,
                "error_count": 0,
                "last_error": None,
                "last_error_time": None,
            }

    # ── Symbol loading ──────────────────────────────────────────────

    @staticmethod
    def _load_crypto_symbols() -> List[str]:
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
            logger.warning(f"Poller: failed to load watchlist from DB: {e}")

        # Fallback: env variable
        env_symbols = os.environ.get("POLLER_SYMBOLS", "")
        if env_symbols:
            return [s.strip().lower() for s in env_symbols.split(",") if s.strip()]

        # Last resort: hardcoded defaults
        return [
            "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
            "dogeusdt", "adausdt", "avaxusdt", "dotusdt", "linkusdt",
        ]

    # ── Public lifecycle ────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._start_time = time.time()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="scheduled-poller"
        )
        self._thread.start()
        logger.info(
            f"ScheduledPoller started: {len(self._symbols)} symbols, "
            f"{len(self._kline_timeframes)} timeframes"
        )

    def stop(self) -> None:
        self._running = False
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("ScheduledPoller stopped")

    def get_status(self) -> Dict:
        return {
            "running": self._running,
            "uptime_seconds": int(time.time() - self._start_time) if self._start_time else 0,
            "first_run": self._first_run,
            "symbols": self._symbols,
            "kline_timeframes": self._kline_timeframes,
            "tasks": self._task_status,
        }

    # ── Internal: daemon thread ─────────────────────────────────────

    def _run_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._worker())
        except Exception as e:
            logger.error(f"ScheduledPoller event loop error: {e}")
        finally:
            self._loop.close()

    async def _worker(self) -> None:
        """Main async worker: long-lived session + scheduling loop."""
        self._last_symbol_refresh = 0
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        ) as session:
            while self._running:
                now = time.time()

                # Refresh symbols from watchlist every 60s
                if now - self._last_symbol_refresh >= 60:
                    self._refresh_symbols()
                    self._last_symbol_refresh = now

                if self._should_run("kline", now):
                    await self._poll_klines(session)

                if self._should_run("oi_hist", now):
                    await self._poll_oi_hist(session)

                if self._should_run("funding", now):
                    await self._poll_funding(session)

                if self._should_run("lsr", now):
                    await self._poll_lsr(session)

                self._first_run = False
                await asyncio.sleep(TICK_INTERVAL)

    def _refresh_symbols(self) -> None:
        """Reload symbols from watchlist if changed."""
        new_symbols = self._load_crypto_symbols()
        if new_symbols != self._symbols:
            added = set(new_symbols) - set(self._symbols)
            removed = set(self._symbols) - set(new_symbols)
            self._symbols = new_symbols
            if added or removed:
                logger.info(
                    f"Poller symbols refreshed: {len(new_symbols)} symbols"
                    + (f" +{added}" if added else "")
                    + (f" -{removed}" if removed else "")
                )
            # Trigger backfill for newly added symbols
            if added and self._loop and not self._loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    self._backfill_new_symbols(list(added)), self._loop
                )

    async def _backfill_new_symbols(self, symbols: List[str]) -> None:
        """Backfill historical data for newly added symbols."""
        logger.info(f"Poller: backfilling history for {symbols}")
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        ) as session:
            for symbol in symbols:
                try:
                    await self._backfill_symbol(session, symbol)
                except Exception as e:
                    logger.warning(f"Poller backfill failed for {symbol}: {e}")

    async def _backfill_symbol(self, session: aiohttp.ClientSession, symbol: str) -> None:
        """Backfill one symbol: klines + funding + OI + LSR."""
        rows = []

        # Klines: fetch initial_hours of history
        for tf in self._kline_timeframes:
            try:
                limit = self._kline_first_run_limit(tf)
                params = {
                    "symbol": symbol.upper(),
                    "interval": tf,
                    "limit": limit,
                    "startTime": int((time.time() - self._initial_hours * 3600) * 1000),
                }
                data = await self._fetch_json(
                    session, f"{BINANCE_FAPI}/fapi/v1/klines", params
                )
                if data:
                    for item in data:
                        rows.append((
                            symbol.upper(), tf, item[0],
                            float(item[1]), float(item[2]),
                            float(item[3]), float(item[4]),
                            float(item[5]), float(item[7]),
                            int(item[8]), True, "poller_backfill",
                        ))
                await self._rate_sleep()
            except Exception as e:
                logger.warning(f"Poller backfill kline {symbol}/{tf}: {e}")

        if rows:
            try:
                self._kline_repo.upsert_klines(rows)
                logger.info(f"Poller backfill: {symbol} kline {len(rows)} rows")
            except Exception as e:
                logger.warning(f"Poller backfill kline write error: {e}")

        # Funding rate: fetch recent 100
        try:
            data = await self._fetch_json(
                session, f"{BINANCE_FAPI}/fapi/v1/fundingRate",
                {"symbol": symbol.upper(), "limit": 100},
            )
            if data:
                fr_rows = []
                for item in data:
                    fr_rows.append((
                        symbol.upper(), int(item["fundingTime"]),
                        float(item["fundingRate"]),
                        0, 0, "poller_backfill",
                    ))
                if fr_rows:
                    self._funding_repo.upsert_rates(fr_rows)
                    logger.info(f"Poller backfill: {symbol} funding {len(fr_rows)} rows")
            await self._rate_sleep()
        except Exception as e:
            logger.warning(f"Poller backfill funding {symbol}: {e}")

        # OI history
        try:
            data = await self._fetch_json(
                session, f"{BINANCE_FAPI}/futures/data/openInterestHist",
                {"symbol": symbol.upper(), "period": "5m", "limit": 30},
            )
            if data:
                oi_rows = []
                for item in data:
                    oi_rows.append((
                        symbol.upper(), int(item["timestamp"]),
                        float(item["sumOpenInterest"]),
                        float(item["sumOpenInterestValue"]),
                        "poller_backfill",
                    ))
                if oi_rows:
                    self._oi_repo.insert_records(oi_rows)
                    logger.info(f"Poller backfill: {symbol} OI {len(oi_rows)} rows")
            await self._rate_sleep()
        except Exception as e:
            logger.warning(f"Poller backfill OI {symbol}: {e}")

        # LSR
        try:
            data = await self._fetch_json(
                session, f"{BINANCE_FAPI}/futures/data/globalLongShortAccountRatio",
                {"symbol": symbol.upper(), "period": "1h", "limit": 30},
            )
            if data:
                lsr_rows = []
                for item in data:
                    lsr_rows.append((
                        symbol.upper(), int(item["timestamp"]),
                        float(item["longShortRatio"]),
                        float(item["longAccount"]),
                        float(item["shortAccount"]),
                        "account", "poller_backfill",
                    ))
                if lsr_rows:
                    self._lsr_repo.upsert_ratios(lsr_rows)
                    logger.info(f"Poller backfill: {symbol} LSR {len(lsr_rows)} rows")
            await self._rate_sleep()
        except Exception as e:
            logger.warning(f"Poller backfill LSR {symbol}: {e}")

        logger.info(f"Poller backfill complete for {symbol}")

    def _should_run(self, task_name: str, now: float) -> bool:
        """Check if a task is due to run."""
        status = self._task_status[task_name]
        # First run: always run immediately
        if self._first_run and status["last_poll_time"] == 0:
            return True
        return now >= status["next_poll_time"]

    def _mark_done(self, task_name: str, record_count: int) -> None:
        """Update status after a successful poll."""
        now = time.time()
        status = self._task_status[task_name]
        status["last_poll_time"] = now
        status["next_poll_time"] = now + status["interval_sec"]
        status["last_record_count"] = record_count
        status["total_records"] += record_count

    def _record_error(self, task_name: str, error: str) -> None:
        status = self._task_status[task_name]
        status["error_count"] += 1
        status["last_error"] = error
        status["last_error_time"] = time.time()

    # ── Rate limiting & fetch helpers ───────────────────────────────

    async def _rate_sleep(self) -> None:
        await asyncio.sleep(1.0 / self._rate_limit)

    async def _fetch_json(self, session: aiohttp.ClientSession, url: str, params: Dict) -> Optional[object]:
        """Fetch JSON from Binance with retry and 429 handling."""
        for attempt in range(3):
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", "60"))
                        logger.warning(f"Poller: 429 rate limit, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    return await resp.json()
            except aiohttp.ClientError as e:
                wait = min(2 ** (attempt + 1), 30)
                logger.warning(f"Poller fetch error (attempt {attempt+1}/3): {e}, retry in {wait}s")
                await asyncio.sleep(wait)
        return None

    # ── Kline polling ───────────────────────────────────────────────

    async def _poll_klines(self, session: aiohttp.ClientSession) -> None:
        all_rows = []
        for symbol in self._symbols:
            for tf in self._kline_timeframes:
                try:
                    limit = self._kline_first_run_limit(tf) if self._first_run else 2
                    params = {"symbol": symbol.upper(), "interval": tf, "limit": limit}
                    if self._first_run:
                        params["startTime"] = int(
                            (time.time() - self._initial_hours * 3600) * 1000
                        )

                    data = await self._fetch_json(
                        session, f"{BINANCE_FAPI}/fapi/v1/klines", params
                    )
                    if data:
                        for item in data:
                            all_rows.append((
                                symbol.upper(), tf, item[0],
                                float(item[1]), float(item[2]),
                                float(item[3]), float(item[4]),
                                float(item[5]), float(item[7]),
                                int(item[8]), True, "poller",
                            ))
                    await self._rate_sleep()
                except Exception as e:
                    logger.warning(f"Poller kline {symbol}/{tf}: {e}")
                    self._record_error("kline", str(e))

        if all_rows:
            try:
                self._kline_repo.upsert_klines(all_rows)
            except Exception as e:
                logger.warning(f"Poller kline write error: {e}")
                self._record_error("kline", str(e))
        self._mark_done("kline", len(all_rows))
        logger.info(f"Poller: kline done, {len(all_rows)} rows")

    def _kline_first_run_limit(self, tf: str) -> int:
        """Calculate how many candles to fetch on first run."""
        candles_per_hour = {"1m": 60, "5m": 12, "15m": 4, "1h": 1, "4h": 1, "1d": 1}
        return min(candles_per_hour.get(tf, 1) * self._initial_hours, 1500)

    # ── Funding rate polling ────────────────────────────────────────

    async def _poll_funding(self, session: aiohttp.ClientSession) -> None:
        all_rows = []
        # Batch fetch premiumIndex for mark_price / index_price enrichment
        mark_prices = {}
        for symbol in self._symbols:
            try:
                data = await self._fetch_json(
                    session, f"{BINANCE_FAPI}/fapi/v1/premiumIndex",
                    {"symbol": symbol.upper()},
                )
                if data:
                    mark_prices[symbol.upper()] = {
                        "mark": float(data.get("markPrice", 0)),
                        "index": float(data.get("indexPrice", 0)),
                    }
                await self._rate_sleep()
            except Exception as e:
                logger.warning(f"Poller premiumIndex {symbol}: {e}")

        # Fetch funding rates
        for symbol in self._symbols:
            try:
                limit = 10 if self._first_run else 3
                data = await self._fetch_json(
                    session, f"{BINANCE_FAPI}/fapi/v1/fundingRate",
                    {"symbol": symbol.upper(), "limit": limit},
                )
                if data:
                    mp = mark_prices.get(symbol.upper(), {})
                    for item in data:
                        all_rows.append((
                            symbol.upper(), item["fundingTime"],
                            float(item["fundingRate"]),
                            mp.get("mark", 0), mp.get("index", 0),
                            "poller",
                        ))
                await self._rate_sleep()
            except Exception as e:
                logger.warning(f"Poller funding {symbol}: {e}")
                self._record_error("funding", str(e))

        if all_rows:
            try:
                self._funding_repo.upsert_rates(all_rows)
            except Exception as e:
                logger.warning(f"Poller funding write error: {e}")
                self._record_error("funding", str(e))
        self._mark_done("funding", len(all_rows))
        logger.info(f"Poller: funding done, {len(all_rows)} rows")

    # ── Open Interest polling ───────────────────────────────────────

    async def _poll_oi_hist(self, session: aiohttp.ClientSession) -> None:
        all_rows = []
        for symbol in self._symbols:
            # 1) Current OI snapshot
            try:
                data = await self._fetch_json(
                    session, f"{BINANCE_FAPI}/fapi/v1/openInterest",
                    {"symbol": symbol.upper()},
                )
                if data:
                    all_rows.append((
                        symbol.upper(), int(time.time() * 1000),
                        float(data.get("openInterest", 0)), 0, "poller",
                    ))
                await self._rate_sleep()
            except Exception as e:
                logger.warning(f"Poller OI current {symbol}: {e}")

            # 2) Historical OI snapshots (5m granularity, last 30 records)
            try:
                data = await self._fetch_json(
                    session, f"{BINANCE_FAPI}/futures/data/openInterestHist",
                    {"symbol": symbol.upper(), "period": "5m", "limit": 30},
                )
                if data:
                    for item in data:
                        all_rows.append((
                            symbol.upper(), item["timestamp"],
                            float(item.get("sumOpenInterest", 0)),
                            float(item.get("sumOpenInterestValue", 0)),
                            "poller",
                        ))
                await self._rate_sleep()
            except Exception as e:
                logger.warning(f"Poller OI hist {symbol}: {e}")
                self._record_error("oi_hist", str(e))

        if all_rows:
            try:
                self._oi_repo.insert_records(all_rows)
            except Exception as e:
                logger.warning(f"Poller OI write error: {e}")
                self._record_error("oi_hist", str(e))
        self._mark_done("oi_hist", len(all_rows))
        logger.info(f"Poller: OI done, {len(all_rows)} rows")

    # ── Long/Short Ratio polling ────────────────────────────────────

    async def _poll_lsr(self, session: aiohttp.ClientSession) -> None:
        all_rows = []
        period = "1h" if not self._first_run else "5m"
        limit = 30
        for symbol in self._symbols:
            try:
                data = await self._fetch_json(
                    session,
                    f"{BINANCE_FAPI}/futures/data/globalLongShortAccountRatio",
                    {"symbol": symbol.upper(), "period": period, "limit": limit},
                )
                if data:
                    for item in data:
                        all_rows.append((
                            symbol.upper(), item["timestamp"],
                            float(item.get("longShortRatio", 1.0)),
                            float(item.get("longAccount", 0.5)),
                            float(item.get("shortAccount", 0.5)),
                            "account", "poller",
                        ))
                await self._rate_sleep()
            except Exception as e:
                logger.warning(f"Poller LSR {symbol}: {e}")
                self._record_error("lsr", str(e))

        if all_rows:
            try:
                self._lsr_repo.upsert_ratios(all_rows)
            except Exception as e:
                logger.warning(f"Poller LSR write error: {e}")
                self._record_error("lsr", str(e))
        self._mark_done("lsr", len(all_rows))
        logger.info(f"Poller: LSR done, {len(all_rows)} rows")
