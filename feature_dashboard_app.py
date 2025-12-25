from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pymysql
from flask import Flask, jsonify, render_template

from feature_db import load_feature_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("feature_dashboard")

# Load feature config (DB + version)
cfg = load_feature_config("feature_config.json")


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
        init_command="SET time_zone = '+00:00'",
    )


STATUS_QUERY = """
SELECT
    fs.symbol_id,
    fs.exchange_symbol,
    COALESCE(s.market_type, 'unknown') AS market_type,
    fs.version,
    fs.sync_status,
    fs.request_status,
    fs.kline_feature_last_time,
    fs.trade_feature_last_time,
    fs.last_updated
FROM feature_status fs
LEFT JOIN symbols s ON fs.symbol_id = s.id
ORDER BY market_type, fs.exchange_symbol;
"""


def fetch_feature_status() -> Tuple[List[Dict[str, Any]], Optional[str]]:
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
        logger.exception("Failed to fetch feature status")
        return [], f"Query failed: {e}"
    finally:
        conn.close()

    now_utc = datetime.now(timezone.utc)

    for row in rows:
        # last_updated is a TIMESTAMP (often returned naive). Normalize to UTC.
        lu = row.get("last_updated")
        if lu is not None and getattr(lu, "tzinfo", None) is None:
            row["last_updated"] = lu.replace(tzinfo=timezone.utc)

        # kline_feature_last_time / trade_feature_last_time are now DATETIME(3)
        for field in ("kline_feature_last_time", "trade_feature_last_time"):
            val = row.get(field)  # datetime or None
            age_key = f"{field}_age_sec"

            if val is None:
                row[age_key] = None
                continue

            if getattr(val, "tzinfo", None) is None:
                val = val.replace(tzinfo=timezone.utc)

            # Save normalized value + age
            row[field] = val
            row[age_key] = (now_utc - val).total_seconds()

    return rows, None


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def fmt_dt(dt: Optional[datetime]) -> Optional[str]:
    """
    Format a datetime as 'DD-MM-YY HH:MM:SS' in UTC.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%d-%m-%y %H:%M:%S")


# ---------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------
app = Flask(__name__)


@app.route("/api/feature_status")
def api_feature_status():
    rows, error = fetch_feature_status()

    payload = []
    for r in rows:
        payload.append(
            {
                "symbol": r.get("exchange_symbol"),
                "symbol_id": r.get("symbol_id"),
                "market_type": r.get("market_type", "unknown"),
                "version": r.get("version"),
                "sync_status": r.get("sync_status"),
                "request_status": r.get("request_status"),
                # formatted times (compact)
                "kline_feature_last_time": fmt_dt(r.get("kline_feature_last_time")),
                "trade_feature_last_time": fmt_dt(r.get("trade_feature_last_time")),
                "last_updated": fmt_dt(r.get("last_updated")),
                # ages in seconds for staleness logic (robust)
                "kline_feature_last_time_age_sec": r.get(
                    "kline_feature_last_time_age_sec"
                ),
                "trade_feature_last_time_age_sec": r.get(
                    "trade_feature_last_time_age_sec"
                ),
            }
        )

    return jsonify(
        {
            "version": cfg.version,
            "timeframes": cfg.timeframes,
            "error": error,
            "symbols": payload,
        }
    )


@app.route("/")
def index():
    return render_template(
        "feature_dashboard.html",
        feature_version=cfg.version,
        timeframes=", ".join(cfg.timeframes),
    )


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
