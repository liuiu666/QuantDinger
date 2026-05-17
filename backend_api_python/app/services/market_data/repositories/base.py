"""Base repository with common CRUD operations for market data tables."""

import logging
from typing import List, Dict, Any, Optional, Tuple

from app.utils.db_postgres import get_pg_connection

logger = logging.getLogger(__name__)


class BaseRepository:
    """Base class for market data repositories with bulk upsert and range queries."""

    # Symbol is always the first column in all market-data tables.
    SYMBOL_INDEX = 0

    def __init__(self, table_name: str, time_column: str = "open_time"):
        self.table_name = table_name
        self.time_column = time_column

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        """Normalize symbol to DB storage format: uppercase, no slash.

        Frontend/CCXT may send ``AIA/USDT`` or ``btc/usdt``, but DB always
        stores ``AIAUSDT`` (uppercase, no separator).
        """
        return symbol.upper().replace("/", "")

    @classmethod
    def _normalize_rows(cls, rows: List[Tuple], symbol_index: int = 0) -> List[Tuple]:
        """Normalize the symbol column in every row tuple (in-place copy)."""
        if not rows:
            return rows
        normalized = []
        for row in rows:
            row_list = list(row)
            row_list[symbol_index] = cls._normalize_symbol(str(row_list[symbol_index]))
            normalized.append(tuple(row_list))
        return normalized

    def bulk_upsert(
        self,
        columns: List[str],
        rows: List[Tuple],
        conflict_cols: List[str],
        update_cols: Optional[List[str]] = None,
    ) -> int:
        """
        Batch INSERT ... ON CONFLICT ... using execute_values.
        Returns number of rows processed.
        """
        if not rows:
            return 0

        import psycopg2.extras

        col_str = ", ".join(columns)
        conflict_str = ", ".join(conflict_cols)

        # execute_values expects a single %s placeholder in the VALUES clause;
        # it expands this into multi-row VALUES automatically from the tuples.
        if update_cols:
            update_str = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in update_cols]
            )
            sql = (
                f"INSERT INTO {self.table_name} ({col_str}) "
                f"VALUES %s "
                f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"
            )
        else:
            sql = (
                f"INSERT INTO {self.table_name} ({col_str}) "
                f"VALUES %s "
                f"ON CONFLICT ({conflict_str}) DO NOTHING"
            )

        rows_processed = 0
        batch_size = 1000
        try:
            with get_pg_connection() as pg_conn:
                # Use the raw psycopg2 connection for execute_values
                raw_conn = pg_conn._conn
                raw_cursor = raw_conn.cursor()
                try:
                    for i in range(0, len(rows), batch_size):
                        batch = rows[i : i + batch_size]
                        psycopg2.extras.execute_values(
                            raw_cursor, sql, batch, page_size=batch_size
                        )
                        rows_processed += len(batch)
                    raw_conn.commit()
                except Exception:
                    raw_conn.rollback()
                    raise
                finally:
                    raw_cursor.close()
        except Exception as e:
            logger.error(
                f"bulk_upsert {self.table_name} error: {e}, rows={len(rows)}"
            )
            raise
        return rows_processed

    def query_range(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
        order: str = "ASC",
        extra_where: str = "",
        extra_params: tuple = (),
    ) -> List[Dict[str, Any]]:
        """Query rows by symbol and time range.
        
        Symbol is normalized to uppercase/no-slash before querying,
        matching the DB storage format.
        """
        conditions = ["symbol = %s"]
        params: list = [self._normalize_symbol(symbol)]

        if start_time is not None:
            conditions.append(f"{self.time_column} >= %s")
            params.append(start_time)
        if end_time is not None:
            conditions.append(f"{self.time_column} <= %s")
            params.append(end_time)
        if extra_where:
            conditions.append(extra_where)
            params.extend(extra_params)

        where_str = " AND ".join(conditions)
        sql = (
            f"SELECT * FROM {self.table_name} "
            f"WHERE {where_str} "
            f"ORDER BY {self.time_column} {order} "
            f"LIMIT %s"
        )
        params.append(limit)

        from app.utils.db_postgres import execute_sql

        try:
            return execute_sql(sql, tuple(params))
        except Exception as e:
            logger.error(f"query_range {self.table_name} error: {e}")
            return []

    def query_latest(
        self,
        symbol: str,
        limit: int = 1,
        extra_where: str = "",
        extra_params: tuple = (),
    ) -> List[Dict[str, Any]]:
        """Query latest N rows for a symbol."""
        conditions = ["symbol = %s"]
        params: list = [self._normalize_symbol(symbol)]
        if extra_where:
            conditions.append(extra_where)
            params.extend(extra_params)

        where_str = " AND ".join(conditions)
        sql = (
            f"SELECT * FROM {self.table_name} "
            f"WHERE {where_str} "
            f"ORDER BY {self.time_column} DESC "
            f"LIMIT %s"
        )
        params.append(limit)

        from app.utils.db_postgres import execute_sql

        try:
            return execute_sql(sql, tuple(params))
        except Exception as e:
            logger.error(f"query_latest {self.table_name} error: {e}")
            return []

    def count_range(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> int:
        """Count rows in a time range."""
        conditions = ["symbol = %s"]
        params: list = [self._normalize_symbol(symbol)]
        if start_time is not None:
            conditions.append(f"{self.time_column} >= %s")
            params.append(start_time)
        if end_time is not None:
            conditions.append(f"{self.time_column} <= %s")
            params.append(end_time)

        where_str = " AND ".join(conditions)
        sql = f"SELECT COUNT(*) as cnt FROM {self.table_name} WHERE {where_str}"

        from app.utils.db_postgres import execute_sql

        try:
            result = execute_sql(sql, tuple(params))
            return result[0]["cnt"] if result else 0
        except Exception as e:
            logger.error(f"count_range {self.table_name} error: {e}")
            return 0

    def get_time_range(
        self, symbol: str, extra_where: str = "", extra_params: tuple = ()
    ) -> Optional[Dict[str, int]]:
        """Get min/max time for a symbol."""
        conditions = ["symbol = %s"]
        params: list = [self._normalize_symbol(symbol)]
        if extra_where:
            conditions.append(extra_where)
            params.extend(extra_params)

        where_str = " AND ".join(conditions)
        sql = (
            f"SELECT MIN({self.time_column}) as min_time, "
            f"MAX({self.time_column}) as max_time "
            f"FROM {self.table_name} WHERE {where_str}"
        )

        from app.utils.db_postgres import execute_sql

        try:
            result = execute_sql(sql, tuple(params))
            if result and result[0]["min_time"] is not None:
                return result[0]
            return None
        except Exception as e:
            logger.error(f"get_time_range {self.table_name} error: {e}")
            return None

    def delete_old(self, before_time: int, symbol: Optional[str] = None) -> int:
        """Delete rows older than given timestamp."""
        conditions = [f"{self.time_column} < %s"]
        params: list = [before_time]
        if symbol:
            conditions.append("symbol = %s")
            params.append(self._normalize_symbol(symbol))

        where_str = " AND ".join(conditions)
        sql = f"DELETE FROM {self.table_name} WHERE {where_str}"

        from app.utils.db_postgres import execute_sql

        try:
            execute_sql(sql, tuple(params))
            return 0
        except Exception as e:
            logger.error(f"delete_old {self.table_name} error: {e}")
            return 0
