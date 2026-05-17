"""Historical data backfill service.

Runs in a daemon thread with its own asyncio event loop.
Fetches historical data from Binance REST API and persists to PostgreSQL.
Supports resumable jobs via cursor tracking.
"""

import asyncio
import logging
import os
import threading
import time
from typing import List, Optional

import aiohttp

from app.services.market_data.repositories.backfill_job_repo import BackfillJobRepository
from app.services.market_data.repositories.kline_repo import KlineRepository, KLINE_COLUMNS
from app.services.market_data.repositories.trade_repo import TradeRepository, TRADE_COLUMNS
from app.services.market_data.repositories.funding_repo import FundingRateRepository, FUNDING_COLUMNS
from app.services.market_data.repositories.open_interest_repo import OpenInterestRepository, OI_COLUMNS
from app.services.market_data.repositories.long_short_repo import LongShortRatioRepository, LSR_COLUMNS

logger = logging.getLogger(__name__)

BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_API = "https://api.binance.com"
REQUEST_TIMEOUT = 15
RATE_LIMIT_PER_SEC = 8
MAX_CONCURRENT = 3


class BackfillService:
    """
    Background service that processes backfill jobs from qd_backfill_jobs table.
    Uses aiohttp for concurrent REST API calls.
    """

    def __init__(self):
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = False
        self._job_repo = BackfillJobRepository()
        self._kline_repo = KlineRepository()
        self._trade_repo = TradeRepository()
        self._funding_repo = FundingRateRepository()
        self._oi_repo = OpenInterestRepository()
        self._lsr_repo = LongShortRatioRepository()

        self._concurrency = int(os.environ.get("BACKFILL_CONCURRENCY", "3"))
        self._rate_limit = int(os.environ.get("BACKFILL_RATE_LIMIT_PER_SEC", "8"))
        self._default_days = int(os.environ.get("BACKFILL_DEFAULT_HISTORY_DAYS", "30"))

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="backfill-worker"
        )
        self._thread.start()
        logger.info("BackfillService started")

    def stop(self) -> None:
        self._running = False
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("BackfillService stopped")

    def _run_loop(self) -> None:
        """Daemon thread target: create asyncio loop and run jobs."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._job_worker())
        except Exception as e:
            logger.error(f"BackfillService loop error: {e}")
        finally:
            self._loop.close()

    async def _job_worker(self) -> None:
        """Main worker: poll for pending jobs and process them."""
        sem = asyncio.Semaphore(self._concurrency)
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        ) as session:
            while self._running:
                try:
                    jobs = self._job_repo.get_pending_jobs(limit=self._concurrency)
                    if not jobs:
                        await asyncio.sleep(10)
                        continue

                    tasks = []
                    for job in jobs:
                        if self._job_repo.claim_job(job["id"]):
                            tasks.append(
                                self._process_job(session, sem, job)
                            )

                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)
                except Exception as e:
                    logger.error(f"BackfillService poll error: {e}")
                    await asyncio.sleep(5)

    async def _process_job(
        self, session: aiohttp.ClientSession, sem: asyncio.Semaphore, job: dict
    ) -> None:
        """Process a single backfill job."""
        job_id = job["id"]
        data_type = job["data_type"]
        symbol = job["symbol"]
        timeframe = job.get("timeframe", "1m")
        current_cursor = job.get("current_cursor")
        cursor = current_cursor if current_cursor is not None else job["start_time"]
        end_time = job["end_time"]
        total_records = job.get("records_fetched", 0)

        logger.info(
            f"Backfill job {job_id}: {symbol} {data_type} "
            f"from {cursor} to {end_time}"
        )

        try:
            async with sem:
                while cursor < end_time and self._running:
                    batch, next_cursor = await self._fetch_batch(
                        session, data_type, symbol, timeframe, cursor, end_time
                    )

                    if not batch:
                        break

                    # Persist
                    self._write_batch(data_type, batch)

                    total_records += len(batch)
                    cursor = next_cursor

                    # Update progress
                    self._job_repo.update_progress(job_id, cursor, total_records)

                    # Rate limit
                    await asyncio.sleep(1.0 / self._rate_limit)

            self._job_repo.complete_job(job_id, total_records)
            logger.info(
                f"Backfill job {job_id}: completed, {total_records} records"
            )
        except Exception as e:
            logger.error(f"Backfill job {job_id}: failed: {e}")
            self._job_repo.fail_job(job_id, str(e))

    async def _fetch_batch(
        self,
        session: aiohttp.ClientSession,
        data_type: str,
        symbol: str,
        timeframe: str,
        cursor: int,
        end_time: int,
    ) -> tuple:
        """Fetch one batch from Binance API. Returns (rows, next_cursor)."""
        try:
            if data_type == "kline":
                return await self._fetch_klines(
                    session, symbol, timeframe, cursor, end_time
                )
            elif data_type == "trade":
                return await self._fetch_trades(session, symbol, cursor, end_time)
            elif data_type == "funding":
                return await self._fetch_funding(session, symbol, cursor, end_time)
            elif data_type == "oi":
                return await self._fetch_oi(session, symbol, cursor, end_time)
            elif data_type == "lsr":
                return await self._fetch_lsr(session, symbol, cursor, end_time)
            else:
                logger.error(
                    f"Backfill job: unknown data_type={data_type!r}, "
                    f"expected one of: kline, trade, funding, oi, lsr"
                )
                raise ValueError(f"Unsupported backfill data_type: {data_type!r}")
        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                retry_after = int(e.headers.get("Retry-After", "60"))
                logger.warning(f"Rate limited, waiting {retry_after}s")
                await asyncio.sleep(retry_after)
            raise

    async def _fetch_klines(
        self, session, symbol, timeframe, cursor, end_time
    ) -> tuple:
        params = {
            "symbol": symbol.upper(),
            "interval": timeframe,
            "startTime": cursor,
            "endTime": end_time,
            "limit": 1500,
        }
        async with session.get(
            f"{BINANCE_FAPI}/fapi/v1/klines", params=params
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

        rows = []
        next_cursor = cursor
        for item in data:
            rows.append((
                symbol.upper(), timeframe, item[0],  # open_time
                float(item[1]), float(item[2]), float(item[3]), float(item[4]),
                float(item[5]), float(item[7]),  # volume, quote_volume
                int(item[8]), True, "backfill",  # trades, is_final, source
            ))
            next_cursor = item[0] + 1

        return rows, next_cursor

    async def _fetch_trades(self, session, symbol, cursor, end_time) -> tuple:
        params = {
            "symbol": symbol.upper(),
            "startTime": cursor,
            "endTime": end_time,
            "limit": 1000,
        }
        async with session.get(
            f"{BINANCE_FAPI}/fapi/v1/aggTrades", params=params
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

        rows = []
        next_cursor = cursor
        for item in data:
            rows.append((
                symbol.upper(), item["a"],  # trade_id (aggTradeId)
                float(item["p"]), float(item["q"]),
                float(item["p"]) * float(item["q"]),
                item.get("m", False), item["T"],
            ))
            next_cursor = item["T"] + 1

        return rows, next_cursor

    async def _fetch_funding(self, session, symbol, cursor, end_time) -> tuple:
        params = {
            "symbol": symbol.upper(),
            "startTime": cursor,
            "endTime": end_time,
            "limit": 1000,
        }
        async with session.get(
            f"{BINANCE_FAPI}/fapi/v1/fundingRate", params=params
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

        rows = []
        next_cursor = cursor
        for item in data:
            rows.append((
                symbol.upper(), item["fundingTime"],
                float(item["fundingRate"]),
                0, 0, "backfill",  # mark_price, index_price, source
            ))
            next_cursor = item["fundingTime"] + 1

        return rows, next_cursor

    async def _fetch_oi(self, session, symbol, cursor, end_time) -> tuple:
        params = {
            "symbol": symbol.upper(),
            "period": "5m",
            "startTime": cursor,
            "endTime": end_time,
            "limit": 30,
        }
        async with session.get(
            f"{BINANCE_FAPI}/futures/data/openInterestHist", params=params
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

        rows = []
        next_cursor = cursor
        for item in data:
            rows.append((
                symbol.upper(), item["timestamp"],
                float(item.get("sumOpenInterest", 0)),
                float(item.get("sumOpenInterestValue", 0)),
                "backfill",
            ))
            next_cursor = item["timestamp"] + 1

        return rows, next_cursor

    async def _fetch_lsr(self, session, symbol, cursor, end_time) -> tuple:
        params = {
            "symbol": symbol.upper(),
            "period": "5m",
            "startTime": cursor,
            "endTime": end_time,
            "limit": 30,
        }
        async with session.get(
            f"{BINANCE_FAPI}/futures/data/globalLongShortAccountRatio",
            params=params,
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

        rows = []
        next_cursor = cursor
        for item in data:
            ratio = float(item.get("longShortRatio", 1.0))
            long_pct = float(item.get("longAccount", 0.5))
            short_pct = float(item.get("shortAccount", 0.5))
            rows.append((
                symbol.upper(), item["timestamp"],
                ratio, long_pct, short_pct, "account", "backfill",
            ))
            next_cursor = item["timestamp"] + 1

        return rows, next_cursor

    def _write_batch(self, data_type: str, rows: list) -> None:
        """Write a batch of rows to the appropriate repository."""
        if not rows:
            return
        try:
            if data_type == "kline":
                self._kline_repo.upsert_klines(rows)
            elif data_type == "trade":
                self._trade_repo.insert_trades(rows)
            elif data_type == "funding":
                self._funding_repo.upsert_rates(rows)
            elif data_type == "oi":
                self._oi_repo.insert_records(rows)
            elif data_type == "lsr":
                self._lsr_repo.upsert_ratios(rows)
        except Exception as e:
            logger.error(f"_write_batch ({data_type}) error: {e}")
            raise
