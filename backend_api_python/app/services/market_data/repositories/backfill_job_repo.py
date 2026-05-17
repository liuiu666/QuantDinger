"""Backfill job repository for tracking and managing data backfill tasks."""

import logging
from typing import List, Dict, Any, Optional

from app.utils.db_postgres import execute_sql

logger = logging.getLogger(__name__)


class BackfillJobRepository:
    """CRUD for qd_backfill_jobs table."""

    def create_job(
        self,
        symbol: str,
        data_type: str,
        start_time: int,
        end_time: int,
        timeframe: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Create a new backfill job. Returns the created job."""
        try:
            from app.utils.db_postgres import get_pg_connection
            from psycopg2.extras import RealDictCursor
            with get_pg_connection() as pg_conn:
                raw_conn = pg_conn._conn
                raw_cursor = raw_conn.cursor(cursor_factory=RealDictCursor)
                raw_cursor.execute(
                    "INSERT INTO qd_backfill_jobs "
                    "(symbol, data_type, timeframe, start_time, end_time, current_cursor, status) "
                    "VALUES (%s, %s, %s, %s, %s, %s, 'pending') "
                    "RETURNING *",
                    (symbol, data_type, timeframe, start_time, end_time, start_time),
                )
                rows = raw_cursor.fetchall()
                raw_conn.commit()
                raw_cursor.close()
                return dict(rows[0]) if rows else None
        except Exception as e:
            logger.error(f"create_job error: {e}")
            return None

    def get_pending_jobs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get pending jobs ordered by creation time."""
        try:
            return execute_sql(
                "SELECT * FROM qd_backfill_jobs "
                "WHERE status = 'pending' "
                "ORDER BY created_at ASC LIMIT %s",
                (limit,),
            )
        except Exception as e:
            logger.error(f"get_pending_jobs error: {e}")
            return []

    def get_running_jobs(self) -> List[Dict[str, Any]]:
        try:
            return execute_sql(
                "SELECT * FROM qd_backfill_jobs WHERE status = 'running'"
            )
        except Exception as e:
            logger.error(f"get_running_jobs error: {e}")
            return []

    def claim_job(self, job_id: int) -> bool:
        """Atomically claim a pending job (set to running).

        Uses raw psycopg2 to avoid PostgresCursor's auto-fetchone()
        consuming RETURNING results.
        """
        try:
            from app.utils.db_postgres import get_pg_connection
            with get_pg_connection() as pg_conn:
                raw_conn = pg_conn._conn
                raw_cursor = raw_conn.cursor()
                raw_cursor.execute(
                    "UPDATE qd_backfill_jobs SET status = 'running', updated_at = NOW() "
                    "WHERE id = %s AND status = 'pending'",
                    (job_id,),
                )
                affected = raw_cursor.rowcount
                raw_conn.commit()
                raw_cursor.close()
                return affected > 0
        except Exception as e:
            logger.error(f"claim_job error: {e}")
            return False

    def update_progress(
        self, job_id: int, cursor: int, records_fetched: int
    ) -> None:
        try:
            execute_sql(
                "UPDATE qd_backfill_jobs "
                "SET current_cursor = %s, records_fetched = %s, updated_at = NOW() "
                "WHERE id = %s",
                (cursor, records_fetched, job_id),
            )
        except Exception as e:
            logger.error(f"update_progress error: {e}")

    def complete_job(self, job_id: int, records_fetched: int) -> None:
        try:
            execute_sql(
                "UPDATE qd_backfill_jobs "
                "SET status = 'completed', records_fetched = %s, updated_at = NOW() "
                "WHERE id = %s",
                (records_fetched, job_id),
            )
        except Exception as e:
            logger.error(f"complete_job error: {e}")

    def fail_job(self, job_id: int, error_message: str) -> None:
        try:
            execute_sql(
                "UPDATE qd_backfill_jobs "
                "SET status = 'failed', error_message = %s, updated_at = NOW() "
                "WHERE id = %s",
                (error_message[:2000], job_id),
            )
        except Exception as e:
            logger.error(f"fail_job error: {e}")

    def get_all_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        try:
            return execute_sql(
                "SELECT * FROM qd_backfill_jobs ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
        except Exception as e:
            logger.error(f"get_all_jobs error: {e}")
            return []
