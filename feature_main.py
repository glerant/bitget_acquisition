# feature_main.py

import argparse
import logging
import sys

from feature_db import DBManager, load_feature_config
from feature_service import FeatureService


def setup_logging(level: str) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Feature service for CEX pipeline.")
    parser.add_argument(
        "--mode",
        choices=["backfill", "live", "both"],
        default="both",
        help="Backfill historical features, run live loop, or both.",
    )
    parser.add_argument(
        "--config",
        default="feature_config.json",
        help="Path to feature_config.json",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_feature_config(args.config)
    setup_logging("INFO")

    logger = logging.getLogger(__name__)
    logger.info("Starting feature service version=%s", cfg.version)

    db = DBManager(cfg.db)
    db.ensure_feature_schema(cfg.timeframes)

    svc = FeatureService(db, cfg)

    if args.mode in ("backfill", "both"):
        svc.run_backfill()
    if args.mode in ("live", "both"):
        svc.run_live_loop()


if __name__ == "__main__":
    main()
