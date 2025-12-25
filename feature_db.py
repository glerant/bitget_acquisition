# feature_db.py

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
from datetime import datetime

import pymysql
from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)


@dataclass
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class FeatureConfig:
    version: str
    db: DBConfig
    timeframes: List[str]
    live_poll_interval_seconds: int
    max_indicator_lookback: int
    backfill_chunk_symbols: int


def load_feature_config(path: str = "feature_config.json") -> FeatureConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    db_cfg = DBConfig(
        host=raw["db"]["host"],
        port=int(raw["db"]["port"]),
        user=raw["db"]["user"],
        password=raw["db"]["password"],
        database=raw["db"]["database"],
    )

    feat = raw["feature"]
    return FeatureConfig(
        version=raw["version"],
        db=db_cfg,
        timeframes=list(feat["timeframes"]),
        live_poll_interval_seconds=int(feat["live_poll_interval_seconds"]),
        max_indicator_lookback=int(feat["max_indicator_lookback"]),
        backfill_chunk_symbols=int(feat["backfill_chunk_symbols"]),
    )


class DBManager:
    def __init__(self, cfg: DBConfig):
        self.cfg = cfg
        self._conn: Optional[pymysql.Connection] = None

    # ---------- connection ----------

    def connect(self) -> None:
        if self._conn is None or not self._conn.open:
            self._conn = pymysql.connect(
                host=self.cfg.host,
                port=self.cfg.port,
                user=self.cfg.user,
                password=self.cfg.password,
                database=self.cfg.database,
                autocommit=False,
                cursorclass=DictCursor,
                init_command="SET time_zone = '+00:00'",
            )

    def cursor(self) -> DictCursor:
        self.connect()
        assert self._conn is not None
        return self._conn.cursor()

    def commit(self) -> None:
        if self._conn:
            self._conn.commit()

    def rollback(self) -> None:
        if self._conn:
            self._conn.rollback()

    # ---------- generic helpers ----------

    def query(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> List[Dict[str, Any]]:
        cur = self.cursor()
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cur.close()
        return rows

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> None:
        cur = self.cursor()
        cur.execute(sql, params or ())
        cur.close()

    def executemany(self, sql: str, params_seq: Sequence[Sequence[Any]]) -> None:
        if not params_seq:
            return
        cur = self.cursor()
        cur.executemany(sql, params_seq)
        cur.close()

    # ---------- schema for features ----------

    def ensure_feature_schema(self, timeframes: Sequence[str]) -> None:
        """
        Create feature_status and per-timeframe kline_features_<tf> tables if they do not exist.
        """
        logger.info("Ensuring feature_status table exists.")
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS feature_status (
                symbol_id INT NOT NULL,
                exchange_symbol VARCHAR(64) NOT NULL,
                version VARCHAR(32) NOT NULL,
                sync_status ENUM('pending','running','ok','error') NOT NULL DEFAULT 'pending',
                request_status ENUM('idle','backfill','gap_fill','live') NOT NULL DEFAULT 'idle',
                kline_feature_last_time DATETIME(3) NULL,
                trade_feature_last_time DATETIME(3) NULL,             
                last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol_id),
                UNIQUE KEY uq_feat_status_symbol (symbol_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )

        for tf in timeframes:
            table = f"kline_features_{tf}"
            logger.info("Ensuring %s table exists.", table)
            self.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    symbol_id INT NOT NULL,
                    open_time DATETIME(3) NOT NULL,

                    -- core return & volatility features
                    ret_1 DOUBLE NULL,
                    ret_5 DOUBLE NULL,
                    ret_15 DOUBLE NULL,
                    ret_60 DOUBLE NULL,
                    logret_1 DOUBLE NULL,
                    vol_5 DOUBLE NULL,
                    vol_15 DOUBLE NULL,
                    vol_30 DOUBLE NULL,
                    vol_60 DOUBLE NULL,
                    atr_14 DOUBLE NULL,
                    atr_norm_14 DOUBLE NULL,
                    ret_ac_1 DOUBLE NULL,
                    ret_ac_2 DOUBLE NULL,
                    ret_ac_5 DOUBLE NULL,
                    vol_ac_1 DOUBLE NULL,
                    vol_ac_2 DOUBLE NULL,
                    vol_ac_5 DOUBLE NULL,

                    -- trend / momentum
                    sma_5 DOUBLE NULL,
                    sma_10 DOUBLE NULL,
                    sma_20 DOUBLE NULL,
                    sma_50 DOUBLE NULL,
                    ema_5 DOUBLE NULL,
                    ema_10 DOUBLE NULL,
                    ema_20 DOUBLE NULL,
                    ema_50 DOUBLE NULL,
                    macd_12_26 DOUBLE NULL,
                    macd_signal_9 DOUBLE NULL,
                    rsi_14 DOUBLE NULL,

                    -- range / location (50-bar window)
                    rng_min_50 DOUBLE NULL,
                    rng_max_50 DOUBLE NULL,
                    rng_pos_50 DOUBLE NULL,
                    rng_z_50 DOUBLE NULL,

                    -- volume features
                    vol DOUBLE NULL,
                    vol_sma_20 DOUBLE NULL,
                    vol_z_20 DOUBLE NULL,

                    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    UNIQUE KEY uq_{table}_symbol_time (symbol_id, open_time),
                    KEY idx_{table}_symbol_time (symbol_id, open_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )

        self.commit()

    # ---------- symbol / raw kline access ----------

    def get_all_symbols(self) -> List[Dict[str, Any]]:
        """
        Uses existing symbols table from acquisition DB.
        Returns all markets (spot + futures/perps).
        """
        return self.query("SELECT id, exchange_symbol FROM symbols ORDER BY id")

    def get_klines(self, symbol_id: int, timeframe: str) -> List[Dict[str, Any]]:
        """
        Read all klines for a symbol/timeframe from existing acquisition tables.
        Uses klines_raw with columns:
          symbol_id, timeframe, open_time, open, high, low, close, volume
        """
        table = "klines_raw"
        sql = f"""
            SELECT symbol_id, open_time, open, high, low, close, volume
            FROM {table}
            WHERE symbol_id = %s AND timeframe = %s
            ORDER BY open_time ASC
        """
        return self.query(sql, (symbol_id, timeframe))

    def get_recent_klines(
        self, symbol_id: int, timeframe: str, limit: int
    ) -> List[Dict[str, Any]]:
        table = "klines_raw"
        sql = f"""
            SELECT symbol_id, open_time, open, high, low, close, volume
            FROM {table}
            WHERE symbol_id = %s AND timeframe = %s
            ORDER BY open_time DESC
            LIMIT %s
        """
        rows = self.query(sql, (symbol_id, timeframe, limit))

        # Ensure we have a mutable list even if query() returns a tuple or other sequence
        rows_list = list(rows)
        rows_list.reverse()  # now oldest → newest, which the indicator maths expects
        return rows_list

    # ---------- feature table access ----------

    def get_max_feature_time(
        self, symbol_id: int, timeframe: str
    ) -> Optional[datetime]:
        table = f"kline_features_{timeframe}"
        rows = self.query(
            f"SELECT MAX(open_time) AS max_time FROM {table} WHERE symbol_id = %s",
            (symbol_id,),
        )
        val = rows[0]["max_time"] if rows and rows[0]["max_time"] is not None else None
        return val  # PyMySQL returns a datetime for DATETIME columns

    def insert_kline_features(
        self,
        timeframe: str,
        rows: Sequence[Tuple[Any, ...]],
    ) -> None:
        """
        rows: sequence of tuples
        (symbol_id, open_time,
         ret_1, ret_5, ret_15, ret_60,
         logret_1,
         vol_5, vol_15, vol_30, vol_60,
         atr_14, atr_norm_14,
         ret_ac_1, ret_ac_2, ret_ac_5,
         vol_ac_1, vol_ac_2, vol_ac_5,
         sma_5, sma_10, sma_20, sma_50,
         ema_5, ema_10, ema_20, ema_50,
         macd_12_26, macd_signal_9,
         rsi_14,
         rng_min_50, rng_max_50, rng_pos_50, rng_z_50,
         vol, vol_sma_20, vol_z_20)
        """
        if not rows:
            return

        table = f"kline_features_{timeframe}"
        sql = f"""
            INSERT INTO {table} (
                symbol_id, open_time,
                ret_1, ret_5, ret_15, ret_60,
                logret_1,
                vol_5, vol_15, vol_30, vol_60,
                atr_14, atr_norm_14,
                ret_ac_1, ret_ac_2, ret_ac_5,
                vol_ac_1, vol_ac_2, vol_ac_5,
                sma_5, sma_10, sma_20, sma_50,
                ema_5, ema_10, ema_20, ema_50,
                macd_12_26, macd_signal_9,
                rsi_14,
                rng_min_50, rng_max_50, rng_pos_50, rng_z_50,
                vol, vol_sma_20, vol_z_20
            )
            VALUES (
                %s, %s,
                %s, %s, %s, %s,
                %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s,
                %s, %s, %s, %s,
                %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                ret_1 = VALUES(ret_1),
                ret_5 = VALUES(ret_5),
                ret_15 = VALUES(ret_15),
                ret_60 = VALUES(ret_60),
                logret_1 = VALUES(logret_1),
                vol_5 = VALUES(vol_5),
                vol_15 = VALUES(vol_15),
                vol_30 = VALUES(vol_30),
                vol_60 = VALUES(vol_60),
                atr_14 = VALUES(atr_14),
                atr_norm_14 = VALUES(atr_norm_14),
                ret_ac_1 = VALUES(ret_ac_1),
                ret_ac_2 = VALUES(ret_ac_2),
                ret_ac_5 = VALUES(ret_ac_5),
                vol_ac_1 = VALUES(vol_ac_1),
                vol_ac_2 = VALUES(vol_ac_2),
                vol_ac_5 = VALUES(vol_ac_5),
                sma_5 = VALUES(sma_5),
                sma_10 = VALUES(sma_10),
                sma_20 = VALUES(sma_20),
                sma_50 = VALUES(sma_50),
                ema_5 = VALUES(ema_5),
                ema_10 = VALUES(ema_10),
                ema_20 = VALUES(ema_20),
                ema_50 = VALUES(ema_50),
                macd_12_26 = VALUES(macd_12_26),
                macd_signal_9 = VALUES(macd_signal_9),
                rsi_14 = VALUES(rsi_14),
                rng_min_50 = VALUES(rng_min_50),
                rng_max_50 = VALUES(rng_max_50),
                rng_pos_50 = VALUES(rng_pos_50),
                rng_z_50 = VALUES(rng_z_50),
                vol = VALUES(vol),
                vol_sma_20 = VALUES(vol_sma_20),
                vol_z_20 = VALUES(vol_z_20)
        """
        self.executemany(sql, rows)
        self.commit()

    # ---------- feature_status helpers ----------

    def ensure_feature_status_row(
        self, symbol_id: int, symbol: str, version: str
    ) -> None:
        sql = """
            INSERT INTO feature_status (symbol_id, exchange_symbol, version)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                exchange_symbol = VALUES(exchange_symbol),
                version = VALUES(version)
        """
        self.execute(sql, (symbol_id, symbol, version))
        self.commit()

    def update_feature_status(
        self,
        symbol_id: int,
        *,
        sync_status: Optional[str] = None,
        request_status: Optional[str] = None,
        kline_feature_last_time: Optional[datetime] = None,
        trade_feature_last_time: Optional[datetime] = None,
    ) -> None:
        fields: List[str] = []
        params: List[Any] = []

        if sync_status is not None:
            fields.append("sync_status = %s")
            params.append(sync_status)
        if request_status is not None:
            fields.append("request_status = %s")
            params.append(request_status)
        if kline_feature_last_time is not None:
            fields.append("kline_feature_last_time = %s")
            params.append(kline_feature_last_time)
        if trade_feature_last_time is not None:
            fields.append("trade_feature_last_time = %s")
            params.append(trade_feature_last_time)

        if not fields:
            return

        sql = f"""
            UPDATE feature_status
            SET {", ".join(fields)}
            WHERE symbol_id = %s
        """
        params.append(symbol_id)
        self.execute(sql, params)
        self.commit()
