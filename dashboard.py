# dashboard_app.py
from __future__ import annotations

import os
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional

from flask import Flask, jsonify, render_template
import pymysql

from config_loader import load_config


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard")

CONFIG_PATH = os.environ.get("ACQ_CONFIG_PATH", "config.json")
cfg = load_config(CONFIG_PATH)


# ---------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------
def get_db_connection():
    return pymysql.connect(
        host=cfg.db.host,
        port=cfg.db.port,
        user=cfg.db.user,
        password=cfg.db.password,
        database=cfg.db.database,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


STATUS_QUERY = """
SELECT
  s.exchange_symbol AS symbol,
  s.market_type      AS market_type,
  p.version,
  p.sync_status,
  p.request_status,
  p.kline_start_time,
  p.kline_last_time,
  p.trade_start_time,
  p.trade_last_time,
  p.last_updated
FROM pipeline_status p
JOIN symbols s ON p.symbol_id = s.id
ORDER BY s.market_type, s.exchange_symbol;
"""


def fetch_pipeline_status() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Returns (rows, error_message). If error_message is not None, rows will be [].
    """
    try:
        conn = get_db_connection()
    except Exception as e:
        logger.exception("Failed to connect to DB")
        return [], f"DB connection failed: {e}"

    try:
        with conn.cursor() as cur:
            cur.execute(STATUS_QUERY)
            rows = cur.fetchall()
    except Exception as e:
        logger.exception("Failed to fetch pipeline status")
        return [], f"Query failed: {e}"
    finally:
        conn.close()

    now = datetime.now(timezone.utc)

    for row in rows:
        # Normalise datetimes as UTC (MariaDB often returns naive datetimes)
        for key in [
            "kline_start_time",
            "kline_last_time",
            "trade_start_time",
            "trade_last_time",
            "last_updated",
        ]:
            if row.get(key) is not None and row[key].tzinfo is None:
                row[key] = row[key].replace(tzinfo=timezone.utc)

        # Compute ages in seconds since last kline/trade
        for field in ("kline_last_time", "trade_last_time"):
            dt = row.get(field)
            age_key = f"{field}_age_sec"
            if dt is None:
                row[age_key] = None
            else:
                row[age_key] = (now - dt).total_seconds()

    return rows, None


# ---------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------

app = Flask(__name__)


def fmt_ts(dt: Optional[datetime]) -> Optional[str]:
    """
    Format a datetime as 'DD-MM-YY HH:MM:SS' in UTC to save space in the UI.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%d-%m-%y %H:%M:%S")


@app.route("/api/pipeline_status")
def api_pipeline_status():
    rows, error = fetch_pipeline_status()

    payload = []
    for r in rows:
        payload.append(
            {
                "symbol": r["symbol"],
                "market_type": r["market_type"],  # "spot" or "futures"
                "version": r["version"],
                "sync_status": r["sync_status"],
                "request_status": r["request_status"],
                # Start times (formatted)
                "kline_start_time": fmt_ts(r["kline_start_time"]),
                "trade_start_time": fmt_ts(r["trade_start_time"]),
                # Last times (formatted, compact)
                "kline_last_time": fmt_ts(r["kline_last_time"]),
                "trade_last_time": fmt_ts(r["trade_last_time"]),
                "last_updated": fmt_ts(r["last_updated"]),
                # Ages for staleness logic
                "kline_last_time_age_sec": r["kline_last_time_age_sec"],
                "trade_last_time_age_sec": r["trade_last_time_age_sec"],
            }
        )

    return jsonify(
        {
            "version": cfg.version,
            "auto_gap_backfill_minutes": cfg.acquisition.auto_gap_backfill_minutes,
            "error": error,
            "symbols": payload,
        }
    )


@app.route("/")
def dashboard():
    # SPA-style: HTML shell, JS calls /api/pipeline_status
    return render_template(
        "dashboard.html",
        app_version=cfg.version,
        auto_gap_backfill_minutes=cfg.acquisition.auto_gap_backfill_minutes,
    )


@app.route("/health")
def health():
    return "OK", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
