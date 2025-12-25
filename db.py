from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Callable, TypeVar

import pymysql
from pymysql.cursors import DictCursor

from config_loader import DBConfig
from resilience import ResilientExecutor, ResilienceConfig, TransientError

from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DBManager:
    """
    Simple MariaDB helper with context-managed connections and basic helpers.

    All public methods are wrapped with a ResilientExecutor so that transient
    DB failures (connection drops, interface errors) are retried up to the
    configured outage window (max_outage_minutes). When that window is
    exceeded, OutageExceededError is raised by the executor and bubbles up
    to main().
    """

    def __init__(self, config: DBConfig, max_outage_minutes: int = 0) -> None:
        self.config = config
        self._executor = ResilientExecutor(
            ResilienceConfig(max_outage_minutes=max_outage_minutes),
            logger=logger,
        )

    def get_connection(self):
        return pymysql.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            cursorclass=DictCursor,
            autocommit=False,
            init_command="SET time_zone = '+00:00'",
        )

    @contextmanager
    def cursor(self):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("DB transaction failed, rolled back")
            raise
        finally:
            conn.close()

    # -------------------------------------------------------------------------
    # Internal resilience wrapper
    # -------------------------------------------------------------------------

    def _call_with_resilience(self, op_desc: str, fn: Callable[[], T]) -> T:
        """
        Execute a DB operation with resilience.

        We treat common connection-level errors as transient; all other errors
        propagate without retry so we still see schema/SQL bugs immediately.
        """

        def inner() -> T:
            try:
                return fn()
            except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
                # Typical connection drops / "MySQL server has gone away" / network issues
                logger.warning("Transient DB error in %s: %s", op_desc, e)
                raise TransientError(str(e)) from e

        return self._executor.call(inner)

    # -------------------------------------------------------------------------
    # Schema & DML methods
    # -------------------------------------------------------------------------

    def initialize_schema(self) -> None:
        """
        Create tables if they don't exist.
        Simplified; adjust decimal sizes to taste.
        """

        def _impl() -> None:
            logger.info("Initializing DB schema (if not exists)")
            with self.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS symbols (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        exchange_symbol VARCHAR(64) NOT NULL,
                        base_asset VARCHAR(32) NOT NULL,
                        quote_asset VARCHAR(32) NOT NULL,
                        market_type VARCHAR(16) NOT NULL,
                        is_active TINYINT(1) NOT NULL DEFAULT 1,
                        UNIQUE KEY uniq_symbol_market (exchange_symbol, market_type)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS klines_raw (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        symbol_id INT NOT NULL,
                        timeframe VARCHAR(16) NOT NULL,
                        open_time DATETIME(3) NOT NULL,
                        close_time DATETIME(3) NULL,
                        open DECIMAL(32, 16) NOT NULL,
                        high DECIMAL(32, 16) NOT NULL,
                        low DECIMAL(32, 16) NOT NULL,
                        close DECIMAL(32, 16) NOT NULL,
                        volume DECIMAL(32, 16) NOT NULL,
                        quote_volume DECIMAL(32, 16) NULL,
                        trade_count INT NULL,
                        extra_json JSON NULL,
                        UNIQUE KEY uniq_kline (symbol_id, timeframe, open_time),
                        KEY idx_kline_sym_int_time (symbol_id, timeframe, open_time),
                        CONSTRAINT fk_klines_symbols FOREIGN KEY (symbol_id) REFERENCES symbols(id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS trades_raw (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        symbol_id INT NOT NULL,
                        exchange_trade_id VARCHAR(128) NOT NULL,
                        ts_exchange DATETIME(3) NOT NULL,
                        ts_inserted TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                        price DECIMAL(32, 16) NOT NULL,
                        quantity DECIMAL(32, 16) NOT NULL,
                        side ENUM('buy','sell') NOT NULL,
                        extra_json JSON NULL,
                        UNIQUE KEY uniq_trade (symbol_id, exchange_trade_id),
                        KEY idx_trades_sym_time (symbol_id, ts_exchange),
                        CONSTRAINT fk_trades_symbols FOREIGN KEY (symbol_id) REFERENCES symbols(id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS orderbook_raw (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        symbol_id INT NOT NULL,
                        ts_exchange DATETIME(3) NOT NULL,
                        ts_inserted TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                        bids_json JSON NOT NULL,
                        asks_json JSON NOT NULL,
                        extra_json JSON NULL,
                        KEY idx_ob_sym_time (symbol_id, ts_exchange),
                        CONSTRAINT fk_ob_symbols FOREIGN KEY (symbol_id) REFERENCES symbols(id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                )

                # NEW: flattened 5-level orderbook for fast spread/bot access
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS orderbook_top_levels (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        symbol_id INT NOT NULL,
                        ts_exchange DATETIME(3) NOT NULL,
                        ts_inserted TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

                        bid_px_1  DECIMAL(32, 16) NULL,
                        bid_qty_1 DECIMAL(32, 16) NULL,
                        ask_px_1  DECIMAL(32, 16) NULL,
                        ask_qty_1 DECIMAL(32, 16) NULL,

                        bid_px_2  DECIMAL(32, 16) NULL,
                        bid_qty_2 DECIMAL(32, 16) NULL,
                        ask_px_2  DECIMAL(32, 16) NULL,
                        ask_qty_2 DECIMAL(32, 16) NULL,

                        bid_px_3  DECIMAL(32, 16) NULL,
                        bid_qty_3 DECIMAL(32, 16) NULL,
                        ask_px_3  DECIMAL(32, 16) NULL,
                        ask_qty_3 DECIMAL(32, 16) NULL,

                        bid_px_4  DECIMAL(32, 16) NULL,
                        bid_qty_4 DECIMAL(32, 16) NULL,
                        ask_px_4  DECIMAL(32, 16) NULL,
                        ask_qty_4 DECIMAL(32, 16) NULL,

                        bid_px_5  DECIMAL(32, 16) NULL,
                        bid_qty_5 DECIMAL(32, 16) NULL,
                        ask_px_5  DECIMAL(32, 16) NULL,
                        ask_qty_5 DECIMAL(32, 16) NULL,

                        UNIQUE KEY uniq_ob_top (symbol_id, ts_exchange),
                        KEY idx_ob_top_sym_time (symbol_id, ts_exchange),
                        CONSTRAINT fk_ob_top_symbols FOREIGN KEY (symbol_id) REFERENCES symbols(id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS acquisition_state (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        symbol_id INT NOT NULL,
                        timeframe VARCHAR(16) NOT NULL,
                        last_kline_open_time DATETIME(3) NULL,
                        last_trade_id VARCHAR(128) NULL,
                        last_updated TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
                        UNIQUE KEY uniq_state (symbol_id, timeframe),
                        CONSTRAINT fk_state_symbols FOREIGN KEY (symbol_id) REFERENCES symbols(id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pipeline_status (
                        symbol_id INT PRIMARY KEY,
                        exchange_symbol VARCHAR(64) NOT NULL,
                        version VARCHAR(32) NOT NULL,
                        sync_status ENUM('pending','running','ok','error') NOT NULL DEFAULT 'pending',
                        request_status ENUM('idle','backfill','gap_fill','live') NOT NULL DEFAULT 'idle',
                        kline_start_time DATETIME(3) NULL,
                        kline_last_time DATETIME(3) NULL,
                        trade_start_time DATETIME(3) NULL,
                        trade_last_time DATETIME(3) NULL,
                        last_updated TIMESTAMP(3) NOT NULL
                            DEFAULT CURRENT_TIMESTAMP(3)
                            ON UPDATE CURRENT_TIMESTAMP(3),
                        CONSTRAINT fk_pipeline_status_symbol
                            FOREIGN KEY (symbol_id) REFERENCES symbols(id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                )

        self._call_with_resilience("initialize_schema", _impl)

    def upsert_symbol(
        self,
        exchange_symbol: str,
        base_asset: str,
        quote_asset: str,
        market_type: str,
    ) -> int:
        """
        Insert symbol if not exists; return id.
        """

        def _impl() -> int:
            with self.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO symbols (exchange_symbol, base_asset, quote_asset, market_type)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        base_asset = VALUES(base_asset),
                        quote_asset = VALUES(quote_asset),
                        market_type = VALUES(market_type),
                        is_active = 1;
                    """,
                    (exchange_symbol, base_asset, quote_asset, market_type),
                )
                cur.execute(
                    "SELECT id FROM symbols WHERE exchange_symbol = %s AND market_type = %s",
                    (exchange_symbol, market_type),
                )
                row = cur.fetchone()
                return int(row["id"])

        return self._call_with_resilience("upsert_symbol", _impl)

    def bulk_upsert_klines(self, rows: Iterable[Dict[str, Any]]) -> None:
        """
        Expect dicts with keys matching klines_raw columns except id.
        """
        rows = list(rows)
        if not rows:
            return

        def _impl() -> None:
            with self.cursor() as cur:
                sql = """
                    INSERT INTO klines_raw (
                        symbol_id, timeframe, open_time, close_time, open, high, low, close,
                        volume, quote_volume, trade_count, extra_json
                    ) VALUES (
                        %(symbol_id)s, %(timeframe)s, %(open_time)s, %(close_time)s,
                        %(open)s, %(high)s, %(low)s, %(close)s,
                        %(volume)s, %(quote_volume)s, %(trade_count)s, %(extra_json)s
                    )
                    ON DUPLICATE KEY UPDATE
                        close_time = VALUES(close_time),
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        volume = VALUES(volume),
                        quote_volume = VALUES(quote_volume),
                        trade_count = VALUES(trade_count),
                        extra_json = VALUES(extra_json);
                """
                cur.executemany(sql, rows)

        self._call_with_resilience("bulk_upsert_klines", _impl)

    def bulk_upsert_trades(self, rows: Iterable[Dict[str, Any]]) -> None:
        rows = list(rows)
        if not rows:
            return

        def _impl() -> None:
            with self.cursor() as cur:
                sql = """
                    INSERT INTO trades_raw (
                        symbol_id, exchange_trade_id, ts_exchange, price, quantity, side, extra_json
                    ) VALUES (
                        %(symbol_id)s, %(exchange_trade_id)s, %(ts_exchange)s,
                        %(price)s, %(quantity)s, %(side)s, %(extra_json)s
                    )
                    ON DUPLICATE KEY UPDATE
                        ts_exchange = VALUES(ts_exchange),
                        price = VALUES(price),
                        quantity = VALUES(quantity),
                        side = VALUES(side),
                        extra_json = VALUES(extra_json);
                """
                cur.executemany(sql, rows)

        self._call_with_resilience("bulk_upsert_trades", _impl)

    def insert_orderbook_snapshots(self, rows: Iterable[Dict[str, Any]]) -> None:
        rows = list(rows)
        if not rows:
            return

        def _impl() -> None:
            with self.cursor() as cur:
                sql = """
                    INSERT INTO orderbook_raw (
                        symbol_id, ts_exchange, bids_json, asks_json, extra_json
                    ) VALUES (
                        %(symbol_id)s, %(ts_exchange)s, %(bids_json)s,
                        %(asks_json)s, %(extra_json)s
                    );
                """
                cur.executemany(sql, rows)

        self._call_with_resilience("insert_orderbook_snapshots", _impl)

    def insert_orderbook_top_levels(self, rows: Iterable[Dict[str, Any]]) -> None:
        """
        Insert flattened 5-level orderbook rows.

        Expected keys per row:
          symbol_id, ts_exchange,
          bid_px_1, bid_qty_1, ask_px_1, ask_qty_1,
          ...
          bid_px_5, bid_qty_5, ask_px_5, ask_qty_5
        """
        rows = list(rows)
        if not rows:
            return

        def _impl() -> None:
            with self.cursor() as cur:
                sql = """
                    INSERT INTO orderbook_top_levels (
                        symbol_id, ts_exchange,
                        bid_px_1, bid_qty_1, ask_px_1, ask_qty_1,
                        bid_px_2, bid_qty_2, ask_px_2, ask_qty_2,
                        bid_px_3, bid_qty_3, ask_px_3, ask_qty_3,
                        bid_px_4, bid_qty_4, ask_px_4, ask_qty_4,
                        bid_px_5, bid_qty_5, ask_px_5, ask_qty_5
                    ) VALUES (
                        %(symbol_id)s, %(ts_exchange)s,
                        %(bid_px_1)s, %(bid_qty_1)s, %(ask_px_1)s, %(ask_qty_1)s,
                        %(bid_px_2)s, %(bid_qty_2)s, %(ask_px_2)s, %(ask_qty_2)s,
                        %(bid_px_3)s, %(bid_qty_3)s, %(ask_px_3)s, %(ask_qty_3)s,
                        %(bid_px_4)s, %(bid_qty_4)s, %(ask_px_4)s, %(ask_qty_4)s,
                        %(bid_px_5)s, %(bid_qty_5)s, %(ask_px_5)s, %(ask_qty_5)s
                    )
                    ON DUPLICATE KEY UPDATE
                        bid_px_1  = VALUES(bid_px_1),
                        bid_qty_1 = VALUES(bid_qty_1),
                        ask_px_1  = VALUES(ask_px_1),
                        ask_qty_1 = VALUES(ask_qty_1),

                        bid_px_2  = VALUES(bid_px_2),
                        bid_qty_2 = VALUES(bid_qty_2),
                        ask_px_2  = VALUES(ask_px_2),
                        ask_qty_2 = VALUES(ask_qty_2),

                        bid_px_3  = VALUES(bid_px_3),
                        bid_qty_3 = VALUES(bid_qty_3),
                        ask_px_3  = VALUES(ask_px_3),
                        ask_qty_3 = VALUES(ask_qty_3),

                        bid_px_4  = VALUES(bid_px_4),
                        bid_qty_4 = VALUES(bid_qty_4),
                        ask_px_4  = VALUES(ask_px_4),
                        ask_qty_4 = VALUES(ask_qty_4),

                        bid_px_5  = VALUES(bid_px_5),
                        bid_qty_5 = VALUES(bid_qty_5),
                        ask_px_5  = VALUES(ask_px_5),
                        ask_qty_5 = VALUES(ask_qty_5);
                """
                cur.executemany(sql, rows)

        self._call_with_resilience("insert_orderbook_top_levels", _impl)

    def get_last_kline_open_time(
        self, symbol_id: int, timeframe: str
    ) -> Optional[datetime]:
        def _impl() -> Optional[datetime]:
            with self.cursor() as cur:
                cur.execute(
                    """
                    SELECT open_time
                    FROM klines_raw
                    WHERE symbol_id = %s AND timeframe = %s
                    ORDER BY open_time DESC
                    LIMIT 1;
                    """,
                    (symbol_id, timeframe),
                )
                row = cur.fetchone()
                if not row:
                    return None
                dt = row["open_time"]
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt

        return self._call_with_resilience("get_last_kline_open_time", _impl)

    def get_last_trade_id(self, symbol_id: int) -> Optional[str]:
        def _impl() -> Optional[str]:
            with self.cursor() as cur:
                cur.execute(
                    """
                    SELECT exchange_trade_id
                    FROM trades_raw
                    WHERE symbol_id = %s
                    ORDER BY ts_exchange DESC
                    LIMIT 1;
                    """,
                    (symbol_id,),
                )
                row = cur.fetchone()
                return row["exchange_trade_id"] if row else None

        return self._call_with_resilience("get_last_trade_id", _impl)

    def get_last_trade_time(self, symbol_id: int) -> Optional[datetime]:
        def _impl() -> Optional[datetime]:
            with self.cursor() as cur:
                cur.execute(
                    """
                    SELECT ts_exchange
                    FROM trades_raw
                    WHERE symbol_id = %s
                    ORDER BY ts_exchange DESC
                    LIMIT 1;
                    """,
                    (symbol_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                dt = row["ts_exchange"]
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt

        return self._call_with_resilience("get_last_trade_time", _impl)

    def ensure_pipeline_status_row(
        self,
        symbol_id: int,
        exchange_symbol: str,
        version: str,
    ) -> None:
        """
        Ensure a status row exists for this symbol/version.
        """

        def _impl() -> None:
            with self.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_status (
                        symbol_id, exchange_symbol, version
                    ) VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        exchange_symbol = VALUES(exchange_symbol),
                        version = VALUES(version);
                    """,
                    (symbol_id, exchange_symbol, version),
                )

        self._call_with_resilience("ensure_pipeline_status_row", _impl)

    def update_pipeline_status(
        self,
        symbol_id: int,
        *,
        version: Optional[str] = None,
        sync_status: Optional[str] = None,
        request_status: Optional[str] = None,
        kline_start_time: Optional[datetime] = None,
        kline_last_time: Optional[datetime] = None,
        trade_start_time: Optional[datetime] = None,
        trade_last_time: Optional[datetime] = None,
    ) -> None:
        """
        Generic partial update helper; only fields provided are updated.
        """
        fields = []
        params: list[Any] = []

        if version is not None:
            fields.append("version = %s")
            params.append(version)
        if sync_status is not None:
            fields.append("sync_status = %s")
            params.append(sync_status)
        if request_status is not None:
            fields.append("request_status = %s")
            params.append(request_status)
        if kline_start_time is not None:
            fields.append("kline_start_time = %s")
            params.append(kline_start_time)
        if kline_last_time is not None:
            fields.append("kline_last_time = %s")
            params.append(kline_last_time)
        if trade_start_time is not None:
            fields.append("trade_start_time = %s")
            params.append(trade_start_time)
        if trade_last_time is not None:
            fields.append("trade_last_time = %s")
            params.append(trade_last_time)

        if not fields:
            return  # nothing to update

        params.append(symbol_id)
        sql = f"UPDATE pipeline_status SET {', '.join(fields)} WHERE symbol_id = %s"

        def _impl() -> None:
            with self.cursor() as cur:
                cur.execute(sql, params)

        self._call_with_resilience("update_pipeline_status", _impl)
