from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


# --- DB / Bitget -------------------------------------------------------------


@dataclass
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class RateLimitConfig:
    max_requests_per_minute: int
    max_requests_per_second: int


@dataclass
class BitgetConfig:
    base_url: str
    api_key: str
    api_secret: str
    passphrase: str
    use_auth: bool
    rate_limits: RateLimitConfig


# --- Acquisition -------------------------------------------------------------


@dataclass
class LivePollIntervals:
    klines_seconds: int
    trades_seconds: int
    orderbook_seconds: int


@dataclass
class MarketConfig:
    enabled: bool
    symbols: List[str]


@dataclass
class MarketsConfig:
    spot: MarketConfig
    perpetual: MarketConfig


@dataclass
class AcquisitionConfig:
    markets: MarketsConfig

    enable_klines: bool
    enable_trades: bool
    enable_orderbook: bool

    orderbook_levels: int
    orderbook_depth: int

    kline_interval: str
    history_start: str
    history_end: Optional[str]

    max_history_days: int
    kline_batch_size: int
    trade_batch_size: int
    trade_history_days: int

    live_poll_intervals: LivePollIntervals
    auto_gap_backfill_minutes: int


# --- Logging / App config ----------------------------------------------------


@dataclass
class LoggingConfig:
    level: str


@dataclass
class AppConfig:
    version: str
    db: DBConfig
    bitget: BitgetConfig
    acquisition: AcquisitionConfig
    logging: LoggingConfig


# --- Loader ------------------------------------------------------------------


def load_config(path: str | Path) -> AppConfig:
    """
    Load application configuration from JSON file at `path`.

    Expects a structure like:

      {
        "version": "0.3.0",
        "db": { ... },
        "bitget": { ... },
        "acquisition": {
          "markets": {
            "spot": { "enabled": true, "symbols": [...] },
            "perpetual": { "enabled": true, "symbols": [...] }
          },
          "enable_klines": true,
          "enable_trades": true,
          "enable_orderbook": true,
          "orderbook_levels": 5,
          "orderbook_depth": 50,
          "kline_interval": "1m",
          "history_start": "2018-01-01T00:00:00Z",
          "history_end": null,
          "live_poll_intervals": {
            "klines_seconds": 15,
            "trades_seconds": 5,
            "orderbook_seconds": 10
          },
          "max_history_days": 3650,
          "kline_batch_size": 200,
          "trade_batch_size": 1000,
          "trade_history_days": 90,
          "auto_gap_backfill_minutes": 60
        },
        "logging": { "level": "INFO" }
      }
    """
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        raw: Dict[str, Any] = json.load(f)

    # --- Version ---
    version = raw.get("version", "dev")

    # --- DB ---
    db_conf = DBConfig(**raw["db"])

    # --- Bitget ---
    rl_conf = RateLimitConfig(**raw["bitget"]["rate_limits"])
    bitget_conf = BitgetConfig(
        base_url=raw["bitget"]["base_url"],
        api_key=raw["bitget"]["api_key"],
        api_secret=raw["bitget"]["api_secret"],
        passphrase=raw["bitget"]["passphrase"],
        use_auth=raw["bitget"].get("use_auth", False),
        rate_limits=rl_conf,
    )

    # --- Acquisition ---
    acq_raw = raw["acquisition"]
    markets_raw = acq_raw["markets"]

    spot_raw = markets_raw["spot"]
    perp_raw = markets_raw["perpetual"]

    markets = MarketsConfig(
        spot=MarketConfig(
            enabled=spot_raw["enabled"],
            symbols=spot_raw["symbols"],
        ),
        perpetual=MarketConfig(
            enabled=perp_raw["enabled"],
            symbols=perp_raw["symbols"],
        ),
    )

    live_poll = LivePollIntervals(**acq_raw["live_poll_intervals"])

    acq = AcquisitionConfig(
        markets=markets,
        enable_klines=acq_raw["enable_klines"],
        enable_trades=acq_raw["enable_trades"],
        enable_orderbook=acq_raw["enable_orderbook"],
        orderbook_levels=acq_raw["orderbook_levels"],
        orderbook_depth=acq_raw["orderbook_depth"],
        kline_interval=acq_raw["kline_interval"],
        history_start=acq_raw["history_start"],
        history_end=acq_raw.get("history_end"),
        max_history_days=acq_raw["max_history_days"],
        kline_batch_size=acq_raw["kline_batch_size"],
        trade_batch_size=acq_raw["trade_batch_size"],
        trade_history_days=acq_raw["trade_history_days"],
        live_poll_intervals=live_poll,
        auto_gap_backfill_minutes=acq_raw["auto_gap_backfill_minutes"],
    )

    # --- Logging ---
    logging_conf = LoggingConfig(**raw["logging"])

    return AppConfig(
        version=version,
        db=db_conf,
        bitget=bitget_conf,
        acquisition=acq,
        logging=logging_conf,
    )
