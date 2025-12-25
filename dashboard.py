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

# Same mapping concept as acquisition_service.py
INTERVAL_MS = {
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
    "1M": 30 * 24 * 60 * 60_000,
}


def last_completed_kline_open_time(now_utc: datetime, interval_ms: int) -> datetime:
    """
    Returns open_time of the last fully completed candle.
    """
    now_ms = int(now_utc.timestamp() * 1000)
    end_ms = ((now_ms // interval_ms) * interval_ms) - interval_ms
    end_ms = max(end_ms, 0)
    return datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)


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

    kline_interval = cfg.acquisition.kline_interval
    step_ms = INTERVAL_MS.get(kline_interval)
    expected_kline_open = (
        last_completed_kline_open_time(now, step_ms) if step_ms else None
    )

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

            # --- Kline lag vs expected last completed candle ---
            row["kline_expected_open_time"] = expected_kline_open

            if expected_kline_open is None or row.get("kline_last_time") is None:
                row["kline_lag_sec"] = None
                row["kline_lag_candles"] = None
                row["kline_health"] = "unknown"
            else:
                lag_sec = (expected_kline_open - row["kline_last_time"]).total_seconds()
                if lag_sec < 0:
                    lag_sec = 0  # clock skew / exchange oddness protection
                row["kline_lag_sec"] = lag_sec
                row["kline_lag_candles"] = int(lag_sec // (step_ms / 1000))

                # Health thresholds (tune as you like)
                if row["kline_lag_candles"] <= 1:
                    row["kline_health"] = "ok"
                elif row["kline_lag_candles"] <= 3:
                    row["kline_health"] = "warn"
                else:
                    row["kline_health"] = "error"

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
                # --- Kline times ---
                "kline_start_time": fmt_ts(r["kline_start_time"]),
                "kline_last_time": fmt_ts(r["kline_last_time"]),
                "kline_expected_open_time": fmt_ts(r.get("kline_expected_open_time")),
                # --- Trade times ---
                "trade_start_time": fmt_ts(r["trade_start_time"]),
                "trade_last_time": fmt_ts(r["trade_last_time"]),
                # --- Meta ---
                "last_updated": fmt_ts(r["last_updated"]),
                # --- Legacy age (keep for now, optional) ---
                "kline_last_time_age_sec": r["kline_last_time_age_sec"],
                "trade_last_time_age_sec": r["trade_last_time_age_sec"],
                # --- New health / lag fields ---
                "kline_lag_sec": r.get("kline_lag_sec"),
                "kline_lag_candles": r.get("kline_lag_candles"),
                "kline_health": r.get("kline_health"),
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
