from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from config_loader import AcquisitionConfig, AppConfig, MarketConfig
from bitget_client import BitgetClient
from db import DBManager
from resilience import OutageExceededError


logger = logging.getLogger(__name__)

# Map our internal interval string to milliseconds
INTERVAL_MS: Dict[str, int] = {
    "1m": 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "6h": 6 * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
    "3d": 3 * 24 * 60 * 60_000,
    "1w": 7 * 24 * 60 * 60_000,
    "1M": 30 * 24 * 60 * 60_000,  # rough, good enough for backfill window
}


def iso_to_datetime(iso_str: str) -> datetime:
    """
    Convert an ISO8601 string from config (optionally ending with 'Z')
    to a timezone-aware UTC datetime.
    """
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )


class AcquisitionService:
    """
    Coordinates backfill and live polling for a single market-type:
      - klines
      - trades (optional)
      - orderbooks (optional)

    This instance is bound to the market_type of the given BitgetClient:
      - client.market_type == "spot"    -> uses cfg.acquisition.markets.spot
      - client.market_type == "futures" -> uses cfg.acquisition.markets.perpetual
    """

    def __init__(
        self, app_config: AppConfig, db: DBManager, client: BitgetClient
    ) -> None:
        self.cfg = app_config
        self.db = db
        self.client = client
        self.version = app_config.version

        # symbol string -> symbol_id mapping in DB (for THIS market only)
        self.symbol_ids: Dict[str, int] = {}

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _current_market_cfg(self) -> MarketConfig:
        """
        Resolve the MarketConfig (spot or perpetual) corresponding to this
        service's BitgetClient.
        """
        acq: AcquisitionConfig = self.cfg.acquisition
        if self.client.market_type == "spot":
            return acq.markets.spot
        elif self.client.market_type == "futures":
            return acq.markets.perpetual
        else:
            raise ValueError(
                f"Unsupported client.market_type={self.client.market_type!r}"
            )

    # -------------------------------------------------------------------------
    # Setup
    # -------------------------------------------------------------------------

    def register_symbols(self) -> None:
        """
        Make sure all configured symbols for this market exist in the DB and
        build a mapping.

        For spot:
          - symbols like "BTCUSDT" -> base=BTC, quote=USDT

        For futures/perps:
          - symbols like "BTCUSDT_UMCBL" -> base=BTC, quote=USDT, stored
            with market_type="futures" (client.market_type).
        """
        acq_market = self._current_market_cfg()
        market_type = self.client.market_type  # "spot" or "futures"

        if not acq_market.enabled:
            logger.info("Market %s is disabled; no symbols to register", market_type)
            return

        for symbol in acq_market.symbols:
            # Handle futures/perps symbols like "BTCUSDT_UMCBL"
            sym_core = symbol.split("_", 1)[0]

            if sym_core.endswith("USDT"):
                base = sym_core[:-4]
                quote = "USDT"
            else:
                # Fallback: store full sym_core as base, quote placeholder
                base = sym_core
                quote = "N/A"

            symbol_id = self.db.upsert_symbol(
                exchange_symbol=symbol,
                base_asset=base,
                quote_asset=quote,
                market_type=market_type,
            )
            self.symbol_ids[symbol] = symbol_id
            logger.info(
                "Registered %s symbol %s -> id %d (base=%s, quote=%s)",
                market_type,
                symbol,
                symbol_id,
                base,
                quote,
            )

            # Ensure pipeline_status row for this symbol/version
            self.db.ensure_pipeline_status_row(symbol_id, symbol, self.version)

    # -------------------------------------------------------------------------
    # Backfill
    # -------------------------------------------------------------------------

    def backfill_all(self) -> None:
        """
        Entry point for one-off backfill of all enabled data types.
        """
        acq = self.cfg.acquisition

        if acq.enable_klines:
            self.backfill_klines_all_symbols()

        if acq.enable_trades:
            self.backfill_trades_all_symbols()

        # Orderbook has no historical REST, so we skip backfill for it.

    def backfill_klines_all_symbols(self) -> None:
        """
        Backfill historical klines for all registered symbols using Bitget's
        kline history.
        """
        acq = self.cfg.acquisition
        interval = acq.kline_interval
        start_dt = iso_to_datetime(acq.history_start)

        if acq.history_end:
            end_dt = iso_to_datetime(acq.history_end)
        else:
            # A small safety margin to avoid hitting an in-progress bar
            end_dt = datetime.now(timezone.utc) - timedelta(minutes=1)

        # Enforce max_history_days window if configured
        max_days = acq.max_history_days
        if (end_dt - start_dt).days > max_days:
            start_dt = end_dt - timedelta(days=max_days)
            logger.warning(
                "history_start truncated to %s based on max_history_days=%d",
                start_dt.isoformat(),
                max_days,
            )

        for symbol, symbol_id in self.symbol_ids.items():
            logger.info(
                "Backfilling klines for %s (%s) from %s to %s",
                symbol,
                interval,
                start_dt,
                end_dt,
            )

            # Mark this symbol as running a kline backfill
            self.db.update_pipeline_status(
                symbol_id,
                version=self.version,
                sync_status="running",
                request_status="backfill",
                kline_start_time=start_dt,
            )

            self._backfill_klines_for_symbol(
                symbol, symbol_id, interval, start_dt, end_dt
            )

            # After backfill, record the latest kline open_time from DB
            last_kline = self.db.get_last_kline_open_time(symbol_id, interval)
            self.db.update_pipeline_status(
                symbol_id,
                version=self.version,
                sync_status="ok",
                request_status="idle",
                kline_last_time=last_kline,
            )

    def _backfill_klines_for_symbol(
        self,
        symbol: str,
        symbol_id: int,
        interval: str,
        start_dt: datetime,
        end_dt: datetime,
    ) -> None:
        """
        Walk through the given time range, requesting klines in batches via
        BitgetClient.fetch_klines, and inserting them into the DB.

        For futures/perps in particular, Bitget requires that the time window
        [startTime, endTime] per request is not too large, so we bound each
        request to roughly 'batch_size' intervals.
        """
        acq = self.cfg.acquisition
        batch_size = acq.kline_batch_size

        step_ms = INTERVAL_MS.get(interval)
        if step_ms is None:
            # Fall back to "old" behaviour if interval is unknown
            logger.warning(
                "Unknown interval %r, using unbounded time window in backfill (may hit API limits)",
                interval,
            )
            cur_start = self.client.datetime_to_ms(start_dt)
            end_ms = self.client.datetime_to_ms(end_dt)
        else:
            cur_start = self.client.datetime_to_ms(start_dt)
            end_ms = self.client.datetime_to_ms(end_dt)

        while cur_start < end_ms:
            if step_ms is not None:
                # Limit each request to at most batch_size * interval duration
                window_ms = batch_size * step_ms
                batch_end = min(cur_start + window_ms - 1, end_ms)
            else:
                # Unknown interval: request full configured range
                batch_end = end_ms

            klines_raw = self.client.fetch_klines(
                symbol=symbol,
                interval=interval,
                start=cur_start,
                end=batch_end,
                limit=batch_size,
            )
            if not klines_raw:
                logger.info(
                    "No more klines returned for %s (cur_start=%d, batch_end=%d); stopping backfill",
                    symbol,
                    cur_start,
                    batch_end,
                )
                break

            rows_for_db: List[Dict[str, Any]] = []
            max_time_ms = cur_start

            for raw in klines_raw:
                parsed = self._parse_kline_raw(symbol_id, interval, raw)
                if parsed is None:
                    # Malformed kline; already logged
                    continue

                rows_for_db.append(parsed)
                ts_ms = int(raw[0])
                if ts_ms > max_time_ms:
                    max_time_ms = ts_ms

            if rows_for_db:
                self.db.bulk_upsert_klines(rows_for_db)
                logger.info("Inserted %d klines for %s", len(rows_for_db), symbol)

            if max_time_ms <= cur_start:
                # Prevent infinite loop if something weird happens
                logger.warning(
                    "max_time_ms <= cur_start for %s; advancing by one interval to avoid loop",
                    symbol,
                )
                if step_ms is not None:
                    cur_start += step_ms
                else:
                    cur_start += 60_000  # fallback: 1 minute
            else:
                cur_start = max_time_ms + 1

            time.sleep(0.2)  # Small pause to be nice to the API

    def backfill_recent_kline_gaps(self) -> None:
        """
        On startup, check the latest kline per symbol and backfill any
        recent gap between that point and 'now', if the gap exceeds
        auto_gap_backfill_minutes.
        """
        acq = self.cfg.acquisition
        if not acq.enable_klines:
            return

        if acq.auto_gap_backfill_minutes <= 0:
            logger.info("auto_gap_backfill_minutes <= 0; skipping gap backfill check")
            return

        interval = acq.kline_interval
        now_utc = datetime.now(timezone.utc)
        gap_threshold = timedelta(minutes=acq.auto_gap_backfill_minutes)

        for symbol, symbol_id in self.symbol_ids.items():
            last_open_time = self.db.get_last_kline_open_time(symbol_id, interval)

            if last_open_time is None:
                logger.info(
                    "No existing klines for %s; skipping gap backfill (use full backfill instead)",
                    symbol,
                )
                continue

            gap = now_utc - last_open_time
            if gap <= gap_threshold:
                logger.info(
                    "Kline gap for %s is %s (<= %s); no gap backfill needed",
                    symbol,
                    gap,
                    gap_threshold,
                )
                continue

            # Backfill from last_open_time up to now; UPSERT is safe
            start_dt = last_open_time
            end_dt = now_utc - timedelta(minutes=1)

            logger.info(
                "Detected kline gap for %s: last=%s, now=%s, gap=%s > %s; backfilling...",
                symbol,
                last_open_time.isoformat(),
                now_utc.isoformat(),
                gap,
                gap_threshold,
            )

            self.db.update_pipeline_status(
                symbol_id,
                version=self.version,
                sync_status="running",
                request_status="gap_fill",
                kline_start_time=start_dt,
            )

            self._backfill_klines_for_symbol(
                symbol, symbol_id, interval, start_dt, end_dt
            )

            last_kline = self.db.get_last_kline_open_time(symbol_id, interval)
            self.db.update_pipeline_status(
                symbol_id,
                version=self.version,
                sync_status="ok",
                request_status="idle",
                kline_last_time=last_kline,
            )

    def backfill_trades_all_symbols(self) -> None:
        """
        Optional: backfill trades up to Bitget's allowed history.
        This implementation is deliberately conservative and just pulls
        a limited number of recent batches per symbol.
        """
        acq = self.cfg.acquisition
        for symbol, symbol_id in self.symbol_ids.items():
            logger.info("Backfilling trades for %s", symbol)
            self._backfill_trades_for_symbol(symbol, symbol_id, acq.trade_batch_size)

    def _backfill_trades_for_symbol(
        self,
        symbol: str,
        symbol_id: int,
        batch_size: int,
    ) -> None:
        """
        Backfill recent trades only, capped by cfg.acquisition.trade_history_days.

        This prevents multi-year crawl times and keeps only execution-relevant data
        (e.g. last 30–180 days of microstructure).
        """
        acq = self.cfg.acquisition

        now_utc = datetime.now(timezone.utc)
        cutoff_dt = now_utc - timedelta(days=acq.trade_history_days)

        logger.info(
            "Backfilling trades for %s (last %d days, cutoff=%s)",
            symbol,
            acq.trade_history_days,
            cutoff_dt.isoformat(),
        )

        # Mark this symbol as running trade backfill
        self.db.update_pipeline_status(
            symbol_id,
            version=self.version,
            sync_status="running",
            request_status="backfill",
            trade_start_time=cutoff_dt,
        )

        max_batches = 10_000  # safety upper bound so we never loop forever
        batches_done = 0

        while batches_done < max_batches:
            trades_raw = self.client.fetch_recent_trades(symbol, limit=batch_size)
            if not trades_raw:
                logger.info("No more trades returned for %s; stopping backfill", symbol)
                break

            rows_for_db: List[Dict[str, Any]] = []
            reached_cutoff = False

            for raw in trades_raw:
                parsed = self._parse_trade_raw(symbol_id, raw)
                if parsed is None:
                    continue

                ts_ex: datetime = parsed["ts_exchange"]

                # HARD STOP: once we cross the cutoff, we stop going further back
                if ts_ex < cutoff_dt:
                    reached_cutoff = True
                    break

                rows_for_db.append(parsed)

            if rows_for_db:
                self.db.bulk_upsert_trades(rows_for_db)
                logger.info(
                    "Inserted %d trades for %s (batch %d)",
                    len(rows_for_db),
                    symbol,
                    batches_done + 1,
                )

            if reached_cutoff:
                logger.info(
                    "Reached trade history cutoff for %s (%s); stopping backfill",
                    symbol,
                    cutoff_dt.isoformat(),
                )
                break

            batches_done += 1
            time.sleep(0.2)

        if batches_done >= max_batches:
            logger.warning(
                "Trade backfill hit max_batches=%d for %s; stopping as safety measure",
                max_batches,
                symbol,
            )

        # Mark trade backfill as finished (best-effort last time = now)
        self.db.update_pipeline_status(
            symbol_id,
            version=self.version,
            sync_status="ok",
            request_status="idle",
            trade_last_time=datetime.now(timezone.utc),
        )

    # -------------------------------------------------------------------------
    # Live Loop
    # -------------------------------------------------------------------------

    def run_live_loop(self) -> None:
        """
        Simple synchronous live loop:
          - Polls klines, trades, and orderbook at configured intervals.

        For more sophistication, we can replace this with an asyncio-based
        loop or a scheduler. But this is perfectly fine for 10-ish pairs.
        """
        acq = self.cfg.acquisition
        intervals = acq.live_poll_intervals

        next_kline_poll = time.time()
        next_trade_poll = time.time()
        next_ob_poll = time.time()

        logger.info(
            "Starting live acquisition loop for market_type=%s",
            self.client.market_type,
        )

        while True:
            now = time.time()

            if acq.enable_klines and now >= next_kline_poll:
                self._poll_klines_live()
                next_kline_poll = now + intervals.klines_seconds

            if acq.enable_trades and now >= next_trade_poll:
                self._poll_trades_live()
                next_trade_poll = now + intervals.trades_seconds

            if acq.enable_orderbook and now >= next_ob_poll:
                self._poll_orderbook_live()
                next_ob_poll = now + intervals.orderbook_seconds

            time.sleep(0.5)

    def _poll_klines_live(self) -> None:
        """
        Fetch the latest few klines per symbol and upsert them into the DB.
        """
        acq = self.cfg.acquisition
        interval = acq.kline_interval
        batch_size = acq.kline_batch_size

        for symbol, symbol_id in self.symbol_ids.items():
            try:
                logger.debug("Live kline poll for %s", symbol)
                # Strategy: just fetch last N klines; DB upsert handles duplicates.
                klines_raw = self.client.fetch_klines(
                    symbol=symbol,
                    interval=interval,
                    start=None,
                    end=None,
                    limit=batch_size,
                )
                if not klines_raw:
                    continue

                rows_for_db: List[Dict[str, Any]] = []
                for raw in klines_raw:
                    parsed = self._parse_kline_raw(symbol_id, interval, raw)
                    if parsed is None:
                        continue
                    rows_for_db.append(parsed)

                if rows_for_db:
                    self.db.bulk_upsert_klines(rows_for_db)
                    logger.info(
                        "Live: upserted %d klines for %s",
                        len(rows_for_db),
                        symbol,
                    )

                    # last open_time in this batch
                    last_dt = rows_for_db[-1]["open_time"]
                    self.db.update_pipeline_status(
                        symbol_id,
                        version=self.version,
                        sync_status="ok",
                        request_status="live",
                        kline_last_time=last_dt,
                    )

            except OutageExceededError:
                # Let the top-level main() handle extended outages and restart.
                raise
            except Exception:
                logger.exception("Error during live kline poll for %s", symbol)

    def _poll_trades_live(self) -> None:
        """
        Fetch recent trades and upsert them; rely on DB uniqueness on
        (symbol_id, exchange_trade_id) to avoid duplicates.
        """
        acq = self.cfg.acquisition
        batch_size = acq.trade_batch_size

        for symbol, symbol_id in self.symbol_ids.items():
            try:
                logger.debug("Live trade poll for %s", symbol)
                trades_raw = self.client.fetch_recent_trades(symbol, limit=batch_size)
                if not trades_raw:
                    continue

                rows_for_db: List[Dict[str, Any]] = []
                for raw in trades_raw:
                    parsed = self._parse_trade_raw(symbol_id, raw)
                    if parsed is None:
                        continue
                    rows_for_db.append(parsed)

                if rows_for_db:
                    self.db.bulk_upsert_trades(rows_for_db)
                    logger.info(
                        "Live: upserted %d trades for %s",
                        len(rows_for_db),
                        symbol,
                    )

                    last_dt = rows_for_db[-1]["ts_exchange"]
                    self.db.update_pipeline_status(
                        symbol_id,
                        version=self.version,
                        sync_status="ok",
                        request_status="live",
                        trade_last_time=last_dt,
                    )

            except OutageExceededError:
                raise
            except Exception:
                logger.exception("Error during live trade poll for %s", symbol)

    def _poll_orderbook_live(self) -> None:
        """
        Fetch orderbook snapshots for each symbol and insert:
          - Full JSON snapshot into orderbook_raw
          - Top N levels (up to 5) flattened into orderbook_top_levels
        """
        acq = self.cfg.acquisition
        depth = acq.orderbook_depth
        max_levels = min(acq.orderbook_levels, 5)

        for symbol, symbol_id in self.symbol_ids.items():
            try:
                logger.debug("Live orderbook poll for %s", symbol)
                ob_raw = self.client.fetch_orderbook(symbol, depth=depth)
                if not ob_raw:
                    continue

                # For v2 mix, docs show 'ts' (timestamp ms), 'b' (bids), 'a' (asks).
                # For spot, structure is similar. We treat it generically.
                ts_ms = int(ob_raw.get("ts", int(time.time() * 1000)))
                bids = ob_raw.get("bids") or ob_raw.get("b") or []
                asks = ob_raw.get("asks") or ob_raw.get("a") or []

                ts_ex = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

                # --- Raw snapshot ---
                ob_row = {
                    "symbol_id": symbol_id,
                    "ts_exchange": ts_ex,
                    "bids_json": json.dumps(bids),
                    "asks_json": json.dumps(asks),
                    "extra_json": json.dumps(ob_raw),
                }
                self.db.insert_orderbook_snapshots([ob_row])

                # --- Top N levels (up to 5) ---
                top_row: Dict[str, Any] = {
                    "symbol_id": symbol_id,
                    "ts_exchange": ts_ex,
                }

                for level in range(1, 6):
                    if level <= max_levels and level <= len(bids):
                        bid_px = float(bids[level - 1][0])
                        bid_qty = float(bids[level - 1][1])
                    else:
                        bid_px = None
                        bid_qty = None

                    if level <= max_levels and level <= len(asks):
                        ask_px = float(asks[level - 1][0])
                        ask_qty = float(asks[level - 1][1])
                    else:
                        ask_px = None
                        ask_qty = None

                    top_row[f"bid_px_{level}"] = bid_px
                    top_row[f"bid_qty_{level}"] = bid_qty
                    top_row[f"ask_px_{level}"] = ask_px
                    top_row[f"ask_qty_{level}"] = ask_qty

                self.db.insert_orderbook_top_levels([top_row])

                logger.info(
                    "Live: inserted orderbook snapshot and top-%d levels for %s",
                    max_levels,
                    symbol,
                )

            except OutageExceededError:
                raise
            except Exception:
                logger.exception("Error during live orderbook poll for %s", symbol)

    # -------------------------------------------------------------------------
    # Parsing helpers
    # -------------------------------------------------------------------------

    def _parse_kline_raw(
        self,
        symbol_id: int,
        interval: str,
        raw: Any,
    ) -> Optional[Dict[str, Any]]:
        """
        Parse a single raw kline array from Bitget into the dict expected
        by DBManager.bulk_upsert_klines.
        """
        try:
            # raw is usually a list of strings; be defensive
            ts_ms = int(raw[0])
            open_price = float(raw[1])
            high = float(raw[2])
            low = float(raw[3])
            close = float(raw[4])
            base_vol = float(raw[5])

            quote_vol: Optional[float] = None
            if len(raw) > 7 and raw[7] not in (None, ""):
                quote_vol = float(raw[7])
            elif len(raw) > 6 and raw[6] not in (None, ""):
                quote_vol = float(raw[6])

            open_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

        except (ValueError, TypeError, IndexError) as e:
            logger.warning("Skipping malformed kline: %r (error: %s)", raw, e)
            return None

        return {
            "symbol_id": symbol_id,
            "timeframe": interval,
            "open_time": open_time,
            "close_time": None,  # We can derive close_time from open_time+interval later
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": base_vol,
            "quote_volume": quote_vol,
            "trade_count": None,
            "extra_json": json.dumps(raw),
        }

    def _parse_trade_raw(
        self,
        symbol_id: int,
        raw: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Parse a single raw trade dict from Bitget into the dict expected by
        DBManager.bulk_upsert_trades.
        """
        try:
            trade_id = str(raw["tradeId"])
            ts_ms = int(raw["ts"])
            price = float(raw["price"])
            qty = float(raw["size"])
            side = raw.get("side", "buy")
        except (KeyError, ValueError, TypeError) as e:
            logger.warning("Skipping malformed trade: %r (error: %s)", raw, e)
            return None

        ts_ex = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return {
            "symbol_id": symbol_id,
            "exchange_trade_id": trade_id,
            "ts_exchange": ts_ex,
            "price": price,
            "quantity": qty,
            "side": side,
            "extra_json": json.dumps(raw),
        }
