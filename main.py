from __future__ import annotations

import argparse
import logging
import sys
import time

from bitget_client import BitgetClient
from config_loader import load_config
from db import DBManager
from acquisition_service import AcquisitionService
from resilience import OutageExceededError


def setup_logging(level: str) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Crypto data acquisition pipeline")
    parser.add_argument(
        "-c",
        "--config",
        default="config.json",
        help="Path to JSON config",
    )
    parser.add_argument(
        "--market",
        choices=["spot", "perpetual"],
        default=None,
        help=(
            "Which market to run for this process. "
            "If omitted and only one market is enabled in config, that one is used. "
            "If both are enabled and this is omitted, 'spot' is chosen by default."
        ),
    )
    parser.add_argument(
        "--oneshot",
        action="store_true",
        help="Run startup catch-up (klines + trades) then exit (no live loop).",
    )
    return parser.parse_args(argv)


def resolve_market_type(args, cfg) -> str:
    """
    Decide which market this process should handle, based on CLI and config.

    - If args.market is given, validate it against enabled markets.
    - If not given:
        * If only one market is enabled in cfg.acquisition.markets, use that.
        * If both are enabled, default to 'spot' and log a warning.
    """
    log = logging.getLogger(__name__)
    acq = cfg.acquisition

    enabled_markets = []
    if acq.markets.spot.enabled:
        enabled_markets.append("spot")
    if acq.markets.perpetual.enabled:
        enabled_markets.append("perpetual")

    if args.market is not None:
        if args.market not in enabled_markets:
            raise RuntimeError(
                f"Requested market {args.market!r} but enabled markets are {enabled_markets!r}"
            )
        return args.market

    # No CLI override
    if len(enabled_markets) == 0:
        raise RuntimeError("No markets enabled in config.acquisition.markets")

    if len(enabled_markets) == 1:
        return enabled_markets[0]

    # Both enabled; default to spot but tell the user
    log.warning(
        "Both spot and perpetual markets are enabled in config, "
        "but --market was not specified. Defaulting to 'spot'. "
        "Run a separate process with --market perpetual for perps."
    )
    return "spot"


def run_pipeline(args, cfg, selected_market: str) -> None:
    """
    One full pipeline run for a given config + selected market.

    Behaviour:
      1) Initialize DB schema
      2) Register configured symbols
      3) Deterministic startup catch-up:
           - klines: from DB watermark -> last completed candle boundary
           - trades: from DB watermark/cutoff -> now (bounded by trade_history_days)
      4) Enter live polling loop (unless --oneshot)

    selected_market:
      - "spot"      -> BitgetClient(market_type="spot")
      - "perpetual" -> BitgetClient(market_type="perpetual"), internally normalised to futures

    Any DB/API code that exhausts the allowed outage window should raise
    OutageExceededError; main() can restart the process if desired.
    """
    futures_product_type = getattr(cfg.bitget, "futures_product_type", "USDT-FUTURES")

    # This is the resilience outage window for DB + API retry logic.
    max_outage_minutes = getattr(cfg.acquisition, "max_outage_minutes", 60)

    db = DBManager(cfg.db, max_outage_minutes=max_outage_minutes)
    db.initialize_schema()

    # Map CLI market to BitgetClient market_type
    if selected_market == "spot":
        market_type_for_client = "spot"
    elif selected_market == "perpetual":
        market_type_for_client = "perpetual"
    else:
        raise ValueError(f"Unsupported selected_market={selected_market!r}")

    client = BitgetClient(
        cfg.bitget,
        market_type=market_type_for_client,
        futures_product_type=futures_product_type,
        max_outage_minutes=max_outage_minutes,
    )

    service = AcquisitionService(cfg, db, client)

    # 1) Ensure symbols exist in DB
    service.register_symbols()

    # 2) Always deterministic startup catch-up (idempotent)
    service.catchup_klines_to_now()
    service.backfill_trades_all_symbols()

    # Optional: allow one-shot startup sync and exit
    if getattr(args, "oneshot", False):
        logging.info("Startup catch-up complete (--oneshot). Exiting.")
        return

    # 3) Live loop forever
    service.run_live_loop()


def main(argv=None):
    """
    Top-level entrypoint that:
    - resolves which market this process should handle (spot/perpetual),
    - runs the pipeline (startup catch-up -> live loop),
    - if an OutageExceededError bubbles up, restarts with the same args
      until it succeeds or is interrupted.
    """
    log = logging.getLogger(__name__)

    # Preserve original argv so we can restart with the same CLI args
    argv = list(argv) if argv is not None else None

    while True:
        args = parse_args(argv)
        cfg = load_config(args.config)
        setup_logging(cfg.logging.level)

        selected_market = resolve_market_type(args, cfg)
        log.info("Starting pipeline for market=%s", selected_market)

        try:
            run_pipeline(args, cfg, selected_market)
            return 0  # clean exit (e.g. --oneshot)
        except OutageExceededError:
            # Extended outage detected by DB/API layer.
            max_minutes = getattr(cfg.acquisition, "max_outage_minutes", 60)
            log.warning(
                "Outage window of %d minutes exceeded; restarting pipeline for market=%s",
                max_minutes,
                selected_market,
            )

            # Restart with the same config + market (and preserve --oneshot if it was set)
            argv = [
                "--config",
                args.config,
                "--market",
                selected_market,
            ]
            if getattr(args, "oneshot", False):
                argv.append("--oneshot")

            # Small delay to avoid hammering in a tight loop
            time.sleep(5)
            continue
        except KeyboardInterrupt:
            log.info("Received KeyboardInterrupt, shutting down gracefully.")
            return 0


if __name__ == "__main__":
    sys.exit(main())
