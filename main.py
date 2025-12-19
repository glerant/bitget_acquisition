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
        "--mode",
        choices=["backfill", "live", "both"],
        default="both",
        help="Run backfill, live loop, or both (backfill then live)",
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
    One full pipeline run for a given config + mode + selected market.

    selected_market:
      - "spot"      -> BitgetClient(market_type="spot")
      - "perpetual" -> BitgetClient(market_type="perpetual"), internally normalised to futures

    Any DB/API code that exhausts the allowed outage window should raise
    OutageExceededError; we catch that in main() and restart with --mode both.
    """
    # Optional: if we want to make futures product type configurable,
    # we can add a field under cfg.bitget (e.g. "futures_product_type")
    # and use that instead of the hard-coded "USDT-FUTURES".
    futures_product_type = getattr(cfg.bitget, "futures_product_type", "USDT-FUTURES")

    max_outage_minutes = cfg.acquisition.auto_gap_backfill_minutes

    # Pass outage window down so DB + BitgetClient can apply resilient retries.
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
    service.register_symbols()

    if args.mode == "backfill":
        service.backfill_all()

    elif args.mode == "both":
        service.backfill_all()
        service.run_live_loop()

    elif args.mode == "live":
        # Auto gap-fill before going live
        service.backfill_recent_kline_gaps()
        # we *could* also call backfill_trades_all_symbols() here if we want
        service.run_live_loop()


def main(argv=None):
    """
    Top-level entrypoint that:
    - resolves which market this process should handle (spot/perpetual),
    - runs the pipeline with the user-selected mode,
    - if an OutageExceededError bubbles up, forces a restart in --mode both
      and keeps trying until it succeeds or is interrupted.
    """
    log = logging.getLogger(__name__)
    # Preserve original argv so we can overwrite mode later
    argv = list(argv) if argv is not None else None

    while True:
        args = parse_args(argv)
        cfg = load_config(args.config)
        setup_logging(cfg.logging.level)

        # Decide which market to handle in this process
        selected_market = resolve_market_type(args, cfg)
        log.info("Starting pipeline for market=%s, mode=%s", selected_market, args.mode)

        try:
            run_pipeline(args, cfg, selected_market)
            return 0  # clean exit
        except OutageExceededError:
            # Extended outage detected by DB/API layer
            max_minutes = cfg.acquisition.auto_gap_backfill_minutes
            log.warning(
                "Outage window of %d minutes exceeded; restarting pipeline in 'both' mode "
                "for market=%s.",
                max_minutes,
                selected_market,
            )

            # Force next iteration to use same config but --mode both
            argv = [
                "--config",
                args.config,
                "--mode",
                "both",
                "--market",
                selected_market,
            ]

            # Small delay to avoid hammering the system in a tight loop
            time.sleep(5)
            continue
        except KeyboardInterrupt:
            log.info("Received KeyboardInterrupt, shutting down gracefully.")
            return 0


if __name__ == "__main__":
    sys.exit(main())
