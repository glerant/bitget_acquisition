# feature_service.py

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

from feature_db import DBManager, FeatureConfig
import feature_indicators as ind


logger = logging.getLogger(__name__)


class FeatureService:
    def __init__(self, db: DBManager, cfg: FeatureConfig):
        self.db = db
        self.cfg = cfg

    # ---------- orchestration ----------

    def run_backfill(self) -> None:
        """
        Full backfill: for each symbol & timeframe, compute missing feature rows.
        """
        symbols = self.db.get_all_symbols()
        logger.info("Starting feature backfill for %d symbols.", len(symbols))

        for sym in symbols:
            symbol_id = sym["id"]
            symbol_name = sym["exchange_symbol"]
            logger.info("Backfilling features for %s (%s).", symbol_name, symbol_id)

            self.db.ensure_feature_status_row(symbol_id, symbol_name, self.cfg.version)
            self.db.update_feature_status(
                symbol_id,
                sync_status="running",
                request_status="backfill",
            )

            for tf in self.cfg.timeframes:
                self._backfill_symbol_timeframe(symbol_id, symbol_name, tf)

            # after all timeframes
            self.db.update_feature_status(
                symbol_id,
                sync_status="ok",
                request_status="idle",
            )

        logger.info("Feature backfill complete.")

    def run_live_loop(self) -> None:
        """
        Live loop: periodically recompute latest features for each symbol & timeframe.
        """
        poll = self.cfg.live_poll_interval_seconds
        logger.info("Starting feature live loop, poll interval=%s seconds.", poll)
        while True:
            start_ts = time.time()
            symbols = self.db.get_all_symbols()
            for sym in symbols:
                symbol_id = sym["id"]
                symbol_name = sym["exchange_symbol"]
                self.db.ensure_feature_status_row(
                    symbol_id, symbol_name, self.cfg.version
                )
                self.db.update_feature_status(
                    symbol_id,
                    sync_status="ok",
                    request_status="live",
                )
                for tf in self.cfg.timeframes:
                    self._update_latest_bar(symbol_id, tf)
            elapsed = time.time() - start_ts
            sleep_for = max(0.0, poll - elapsed)
            if sleep_for > 0:
                time.sleep(sleep_for)

    # ---------- core logic ----------

    def _backfill_symbol_timeframe(
        self, symbol_id: int, symbol_name: str, tf: str
    ) -> None:
        """
        For one symbol & timeframe:
        - get all klines
        - compute all features
        - insert only those rows with open_time > existing max_feature_time
        """
        logger.info("Backfill %s %s", symbol_name, tf)
        klines = self.db.get_klines(symbol_id, tf)
        if not klines:
            logger.info("No klines for %s %s", symbol_name, tf)
            return

        max_feat_time = self.db.get_max_feature_time(symbol_id, tf)
        logger.info("Existing max_feature_time=%s", max_feat_time)

        rows_to_insert = self._compute_feature_rows(symbol_id, klines, max_feat_time)
        logger.info(
            "Inserting %d new feature rows for %s %s",
            len(rows_to_insert),
            symbol_name,
            tf,
        )
        self.db.insert_kline_features(tf, rows_to_insert)

        if rows_to_insert:
            last_open_time = rows_to_insert[-1][1]
            self.db.update_feature_status(
                symbol_id,
                kline_feature_last_time=last_open_time,
            )

    def _update_latest_bar(self, symbol_id: int, tf: str) -> None:
        """
        In live mode: fetch recent klines, recompute features, and insert the latest row only.
        """
        lookback = max(self.cfg.max_indicator_lookback, 200)
        klines = self.db.get_recent_klines(symbol_id, tf, limit=lookback)
        if not klines:
            return

        # compute features for these klines, then just keep last row
        rows_to_insert = self._compute_feature_rows(
            symbol_id, klines, min_open_time=None
        )
        if not rows_to_insert:
            return

        latest_row = rows_to_insert[-1:]
        self.db.insert_kline_features(tf, latest_row)

        last_open_time = latest_row[0][1]
        self.db.update_feature_status(
            symbol_id,
            kline_feature_last_time=last_open_time,
        )

    # ---------- feature calculations ----------

    def _compute_feature_rows(
        self,
        symbol_id: int,
        klines: List[Dict[str, Any]],
        min_open_time: datetime | None,
    ) -> List[Tuple[Any, ...]]:
        """
        Given ordered klines for a symbol, compute indicator features for each,
        and return tuples ready for insert. If min_open_time is not None,
        only keep rows with open_time > min_open_time.
        """

        open_time: list[datetime] = []
        for k in klines:
            ot = k["open_time"]
            if isinstance(ot, datetime):
                open_time.append(ot)
            else:
                # If raw ever returns int, interpret as ms since epoch (legacy safety)
                open_time.append(datetime.utcfromtimestamp(int(ot) / 1000.0))

        high = [float(k["high"]) for k in klines]
        low = [float(k["low"]) for k in klines]
        close = [float(k["close"]) for k in klines]
        volume = [float(k["volume"]) for k in klines]

        # returns
        ret_1 = ind.simple_returns(close, 1)
        ret_5 = ind.simple_returns(close, 5)
        ret_15 = ind.simple_returns(close, 15)
        ret_60 = ind.simple_returns(close, 60)
        logret_1 = ind.log_returns(close, 1)

        # volatility
        vol_5 = ind.rolling_std(logret_1, 5)
        vol_15 = ind.rolling_std(logret_1, 15)
        vol_30 = ind.rolling_std(logret_1, 30)
        vol_60 = ind.rolling_std(logret_1, 60)

        atr_14 = ind.atr(high, low, close, 14)
        atr_norm_14 = []
        for a, c in zip(atr_14, close):
            if a is None:
                atr_norm_14.append(None)
            else:
                atr_norm_14.append(a / c if c != 0 else None)

        # autocorrelation
        # use log returns for ret_ac; absolute log returns for vol_ac
        abs_logret = [abs(v) if v is not None else None for v in logret_1]
        ret_ac_1 = ind.autocorr(logret_1, lag=1)
        ret_ac_2 = ind.autocorr(logret_1, lag=2)
        ret_ac_5 = ind.autocorr(logret_1, lag=5)
        vol_ac_1 = ind.autocorr(abs_logret, lag=1)
        vol_ac_2 = ind.autocorr(abs_logret, lag=2)
        vol_ac_5 = ind.autocorr(abs_logret, lag=5)

        # trend / momentum
        sma_5 = ind.sma(close, 5)
        sma_10 = ind.sma(close, 10)
        sma_20 = ind.sma(close, 20)
        sma_50 = ind.sma(close, 50)
        ema_5 = ind.ema(close, 5)
        ema_10 = ind.ema(close, 10)
        ema_20 = ind.ema(close, 20)
        ema_50 = ind.ema(close, 50)
        macd_12_26, macd_signal_9 = ind.macd(close, 12, 26, 9)
        rsi_14 = ind.rsi(close, 14)

        # range/location (50 bars)
        rng_min_50, rng_max_50, rng_pos_50, rng_z_50 = ind.range_features(close, 50)

        # volume features
        vol_sma_20, vol_z_20 = ind.volume_features(volume, 20)

        out_rows: List[Tuple[Any, ...]] = []

        for i in range(len(klines)):
            ot = open_time[i]
            if min_open_time is not None and ot <= min_open_time:
                continue

            row = (
                symbol_id,
                ot,
                ret_1[i],
                ret_5[i],
                ret_15[i],
                ret_60[i],
                logret_1[i],
                vol_5[i],
                vol_15[i],
                vol_30[i],
                vol_60[i],
                atr_14[i],
                atr_norm_14[i],
                ret_ac_1[i],
                ret_ac_2[i],
                ret_ac_5[i],
                vol_ac_1[i],
                vol_ac_2[i],
                vol_ac_5[i],
                sma_5[i],
                sma_10[i],
                sma_20[i],
                sma_50[i],
                ema_5[i],
                ema_10[i],
                ema_20[i],
                ema_50[i],
                macd_12_26[i],
                macd_signal_9[i],
                rsi_14[i],
                rng_min_50[i],
                rng_max_50[i],
                rng_pos_50[i],
                rng_z_50[i],
                volume[i],
                vol_sma_20[i],
                vol_z_20[i],
            )
            out_rows.append(row)

        return out_rows
