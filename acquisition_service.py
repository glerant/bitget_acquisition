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

            self.db.ensure_pipeline_status_row(symbol_id, symbol, self.version)

    # -------------------------------------------------------------------------
    # Backfill - Klines
    # -------------------------------------------------------------------------

    def catchup_klines_to_now(self) -> None:
        """
        Deterministic kline catch-up:
          - If no existing klines: bootstrap from cfg.history_start (capped by max_history_days)
          - Else: from (last_open_time + interval) to last fully-formed candle boundary.
        """
        acq = self.cfg.acquisition
        if not acq.enable_klines:
            return

        interval = acq.kline_interval
        step_ms = INTERVAL_MS.get(interval)
        if step_ms is None:
            raise ValueError(f"Unsupported interval for catch-up: {interval!r}")

        now_utc = datetime.now(timezone.utc)

        # End at the LAST FULLY COMPLETED candle (exclude in-progress)
        now_ms = self.client.datetime_to_ms(now_utc)
        end_ms = ((now_ms // step_ms) * step_ms) - step_ms
        end_ms = max(end_ms, 0)
        end_dt = self.client.ms_to_datetime(end_ms)

        cfg_start = iso_to_datetime(acq.history_start)

        # Enforce max_history_days for bootstrap start
        max_days = acq.max_history_days
        min_start = end_dt - timedelta(days=max_days)
        if cfg_start < min_start:
            cfg_start = min_start

        for symbol, symbol_id in self.symbol_ids.items():
            last_open = self.db.get_last_kline_open_time(symbol_id, interval)

            if last_open is None:
                start_dt = cfg_start
                request_status = "backfill"
                mode = "bootstrap"
            else:
                start_dt = last_open + timedelta(milliseconds=step_ms)
                request_status = "gap_fill"
                mode = "catchup"

            if start_dt >= end_dt:
                logger.info("Klines %s: nothing to do for %s", mode, symbol)
                continue

            logger.info(
                "Klines %s for %s (%s): %s -> %s",
                mode,
                symbol,
                interval,
                start_dt.isoformat(),
                end_dt.isoformat(),
            )

            self.db.update_pipeline_status(
                symbol_id,
                version=self.version,
                sync_status="running",
                request_status=request_status,
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
        """
        acq = self.cfg.acquisition
        batch_size = acq.kline_batch_size

        step_ms = INTERVAL_MS.get(interval)
        cur_start = self.client.datetime_to_ms(start_dt)
        end_ms = self.client.datetime_to_ms(end_dt)

        while cur_start < end_ms:
            if step_ms is not None:
                window_ms = batch_size * step_ms
                batch_end = min(cur_start + window_ms - 1, end_ms)
            else:
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
                    continue

                if parsed["open_time"] > end_dt:
                    continue

                rows_for_db.append(parsed)

                ts_ms = int(raw[0])
                if ts_ms > max_time_ms:
                    max_time_ms = ts_ms

            if rows_for_db:
                rows_for_db.sort(key=lambda r: r["open_time"])
                self.db.bulk_upsert_klines(rows_for_db)
                logger.info(
                    "Inserted %d klines for %s (batch %s -> %s)",
                    len(rows_for_db),
                    symbol,
                    rows_for_db[0]["open_time"].isoformat(),
                    rows_for_db[-1]["open_time"].isoformat(),
                )

            if max_time_ms <= cur_start:
                logger.warning(
                    "max_time_ms <= cur_start for %s; advancing by one interval to avoid loop",
                    symbol,
                )
                cur_start += step_ms if step_ms is not None else 60_000
            else:
                cur_start = max_time_ms + 1

            time.sleep(0.2)

    # -------------------------------------------------------------------------
    # Backfill - Trades
    # -------------------------------------------------------------------------

    def backfill_trades_all_symbols(self) -> None:
        """
        Optional: backfill trades up to Bitget's allowed history.
        Conservative implementation pulls limited windows per symbol.
        """
        acq = self.cfg.acquisition
        for symbol, symbol_id in self.symbol_ids.items():
            logger.info("Backfilling trades for %s", symbol)
            self._backfill_trades_for_symbol(symbol, symbol_id, acq.trade_batch_size)

    def _trade_catchup_start(self, symbol_id: int) -> datetime:
        """
        Determine starting timestamp for trade catch-up.
        If we have existing trades, start from the last stored ts_exchange.
        Otherwise start from now - trade_history_days.
        """
        acq = self.cfg.acquisition
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(days=acq.trade_history_days)

        last_ts = self.db.get_last_trade_time(symbol_id)
        if last_ts is None:
            return cutoff
        return max(last_ts, cutoff)

    def _backfill_trades_for_symbol(
        self, symbol: str, symbol_id: int, batch_size: int
    ) -> None:
        """
        Deterministic trade backfill using fills-history:
        - Catch up from DB watermark to now (bounded by trade_history_days)
        - Query in <=7 day windows
        - Page older within each window using idLessThan
        """
        acq = self.cfg.acquisition
        now_utc = datetime.now(timezone.utc)

        start_dt = self._trade_catchup_start(symbol_id)
        end_dt = now_utc - timedelta(seconds=2)

        if start_dt >= end_dt:
            logger.info(
                "Trades catch-up: nothing to do for %s (start=%s end=%s)",
                symbol,
                start_dt,
                end_dt,
            )
            return

        logger.info(
            "Backfilling trades for %s using history (start=%s end=%s, history_days=%d)",
            symbol,
            start_dt.isoformat(),
            end_dt.isoformat(),
            acq.trade_history_days,
        )

        self.db.update_pipeline_status(
            symbol_id,
            version=self.version,
            sync_status="running",
            request_status="backfill",
            trade_start_time=start_dt,
        )

        window = timedelta(days=7)
        cur_start = start_dt
        total_inserted = 0

        while cur_start < end_dt:
            cur_end = min(cur_start + window, end_dt)

            start_ms = self.client.datetime_to_ms(cur_start)
            end_ms = self.client.datetime_to_ms(cur_end)

            id_less_than: Optional[str] = None
            pages = 0

            while True:
                pages += 1
                trades_raw = self.client.fetch_trades_history(
                    symbol=symbol,
                    start=start_ms,
                    end=end_ms,
                    limit=min(batch_size, 1000),
                    id_less_than=id_less_than,
                )
                if not trades_raw:
                    break

                rows_for_db: List[Dict[str, Any]] = []
                min_ts_in_page: Optional[datetime] = None
                last_id_in_page: Optional[str] = None

                for raw in trades_raw:
                    parsed = self._parse_trade_raw(symbol_id, raw)
                    if parsed is None:
                        continue
                    rows_for_db.append(parsed)

                    ts_ex = parsed["ts_exchange"]
                    if min_ts_in_page is None or ts_ex < min_ts_in_page:
                        min_ts_in_page = ts_ex

                    last_id_in_page = (
                        min(last_id_in_page, parsed["exchange_trade_id"])
                        if last_id_in_page
                        else parsed["exchange_trade_id"]
                    )

                if rows_for_db:
                    self.db.bulk_upsert_trades(rows_for_db)
                    total_inserted += len(rows_for_db)

                if last_id_in_page is None:
                    break

                id_less_than = last_id_in_page

                if min_ts_in_page is not None and min_ts_in_page <= cur_start:
                    break

                if pages > 2000:
                    logger.warning(
                        "Trades history paging safety break for %s in window %s-%s",
                        symbol,
                        cur_start,
                        cur_end,
                    )
                    break

                time.sleep(0.1)

            cur_start = cur_end
            time.sleep(0.2)

        self.db.update_pipeline_status(
            symbol_id,
            version=self.version,
            sync_status="ok",
            request_status="idle",
            trade_last_time=self.db.get_last_trade_time(symbol_id)
            or datetime.now(timezone.utc),
        )

        logger.info(
            "Trade history backfill done for %s (inserted/upserted=%d)",
            symbol,
            total_inserted,
        )

    # -------------------------------------------------------------------------
    # Live Loop
    # -------------------------------------------------------------------------

    def run_live_loop(self) -> None:
        """
        Simple synchronous live loop polling klines/trades/orderbook.
        """
        acq = self.cfg.acquisition
        intervals = acq.live_poll_intervals

        next_kline_poll = time.time()
        next_trade_poll = time.time()
        next_ob_poll = time.time()

        logger.info(
            "Starting live acquisition loop for market_type=%s", self.client.market_type
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

        step_ms = INTERVAL_MS.get(interval)

        for symbol, symbol_id in self.symbol_ids.items():
            try:
                logger.debug("Live kline poll for %s", symbol)

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

                if not rows_for_db:
                    continue

                # ✅ Critical fix: enforce deterministic chronological order
                rows_for_db.sort(key=lambda r: r["open_time"])

                logger.info(
                    "Live klines %s: batch %s -> %s (n=%d)",
                    symbol,
                    rows_for_db[0]["open_time"].isoformat(),
                    rows_for_db[-1]["open_time"].isoformat(),
                    len(rows_for_db),
                )

                self.db.bulk_upsert_klines(rows_for_db)
                logger.info("Live: upserted %d klines for %s", len(rows_for_db), symbol)

                last_dt = rows_for_db[-1]["open_time"]
                self.db.update_pipeline_status(
                    symbol_id,
                    version=self.version,
                    sync_status="ok",
                    request_status="live",
                    kline_last_time=last_dt,
                )

                # Optional self-heal gap fill
                if step_ms is not None:
                    now_ms = self.client.datetime_to_ms(datetime.now(timezone.utc))
                    end_ms = max(((now_ms // step_ms) * step_ms) - step_ms, 0)
                    end_dt = self.client.ms_to_datetime(end_ms)

                    db_last = self.db.get_last_kline_open_time(symbol_id, interval)
                    if db_last is not None:
                        behind = end_dt - db_last
                        if behind > timedelta(milliseconds=2 * step_ms):
                            start_dt = db_last + timedelta(milliseconds=step_ms)
                            logger.warning(
                                "Live kline poll detected gap for %s: db_last=%s end_dt=%s behind=%s; catching up...",
                                symbol,
                                db_last.isoformat(),
                                end_dt.isoformat(),
                                behind,
                            )
                            self._backfill_klines_for_symbol(
                                symbol, symbol_id, interval, start_dt, end_dt
                            )

            except OutageExceededError:
                raise
            except Exception:
                logger.exception("Error during live kline poll for %s", symbol)

    def _poll_trades_live(self) -> None:
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
                        "Live: upserted %d trades for %s", len(rows_for_db), symbol
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
        acq = self.cfg.acquisition
        depth = acq.orderbook_depth
        max_levels = min(acq.orderbook_levels, 5)

        for symbol, symbol_id in self.symbol_ids.items():
            try:
                logger.debug("Live orderbook poll for %s", symbol)
                ob_raw = self.client.fetch_orderbook(symbol, depth=depth)
                if not ob_raw:
                    continue

                ts_ms = int(ob_raw.get("ts", int(time.time() * 1000)))
                bids = ob_raw.get("bids") or ob_raw.get("b") or []
                asks = ob_raw.get("asks") or ob_raw.get("a") or []

                ts_ex = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

                ob_row = {
                    "symbol_id": symbol_id,
                    "ts_exchange": ts_ex,
                    "bids_json": json.dumps(bids),
                    "asks_json": json.dumps(asks),
                    "extra_json": json.dumps(ob_raw),
                }
                self.db.insert_orderbook_snapshots([ob_row])

                top_row: Dict[str, Any] = {"symbol_id": symbol_id, "ts_exchange": ts_ex}

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
        self, symbol_id: int, interval: str, raw: Any
    ) -> Optional[Dict[str, Any]]:
        try:
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
            "close_time": None,
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
        self, symbol_id: int, raw: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        try:
            trade_id_val = raw.get("tradeId") or raw.get("trade_id") or raw.get("id")
            ts_val = raw.get("ts") or raw.get("timestamp") or raw.get("time")

            if trade_id_val is None or ts_val is None:
                raise KeyError(f"Missing tradeId/ts (keys={list(raw.keys())})")

            trade_id = str(trade_id_val)

            ts_num = int(ts_val)
            if ts_num < 10_000_000_000:
                ts_num *= 1000

            price = float(raw["price"])
            qty = float(
                raw.get("size") if raw.get("size") is not None else raw["quantity"]
            )

            side = (raw.get("side") or "buy").lower()
            if side not in ("buy", "sell"):
                if side == "bid":
                    side = "buy"
                elif side == "ask":
                    side = "sell"
                else:
                    side = "buy"

        except (KeyError, ValueError, TypeError) as e:
            logger.warning("Skipping malformed trade: %r (error: %s)", raw, e)
            return None

        ts_ex = datetime.fromtimestamp(ts_num / 1000.0, tz=timezone.utc)
        return {
            "symbol_id": symbol_id,
            "exchange_trade_id": trade_id,
            "ts_exchange": ts_ex,
            "price": price,
            "quantity": qty,
            "side": side,
            "extra_json": json.dumps(raw),
        }
