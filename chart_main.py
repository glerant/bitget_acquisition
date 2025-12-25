#!/usr/bin/env python3
from decimal import Decimal
from typing import List, Dict, Any, Tuple

import flask
import pymysql

from config_loader import load_config

# ----------------------------------------------------------------------
# Flask app
# ----------------------------------------------------------------------

app = flask.Flask(__name__)

# ----------------------------------------------------------------------
# DB CONFIG – from config.json
# ----------------------------------------------------------------------

_cfg = load_config("config.json")

DB_CONFIG = {
    "host": _cfg.db.host,
    "user": _cfg.db.user,
    "password": _cfg.db.password,
    "database": _cfg.db.database,
    "port": _cfg.db.port,
    "cursorclass": pymysql.cursors.Cursor,  # keep tuples; code unpacks by position
    "charset": "utf8mb4",
    "autocommit": False,
}

ALLOWED_TIMEFRAMES = [
    "1m",
    "5m",
    "15m",
    "30m",
    "1h",
    "4h",
    "1d",
]

DEFAULT_LIMIT = 300

# ----------------------------------------------------------------------
# FEATURE OVERLAYS (from kline_features_1m)
# key is what the frontend sends, value is the column name
# ----------------------------------------------------------------------

FEATURES = [
    ("none", "None"),
    ("sma_5", "SMA 5"),
    ("sma_10", "SMA 10"),
    ("sma_20", "SMA 20"),
    ("ema_5", "EMA 5"),
    ("ema_10", "EMA 10"),
    ("ema_20", "EMA 20"),
    ("ema_50", "EMA 50"),
    ("macd_12_26", "MACD 12-26"),
    ("macd_signal_9", "MACD Signal 9"),
    ("rsi_14", "RSI 14"),
]

FEATURE_COLUMN_MAP = {k: (None if k == "none" else k) for k, _ in FEATURES}
FEATURE_LABEL_MAP = {k: label for k, label in FEATURES}


def get_db_connection():
    """Return a new pymysql connection."""
    return pymysql.connect(**DB_CONFIG)


# ----------------------------------------------------------------------
# MARKET LIST HELPERS
# ----------------------------------------------------------------------


def get_markets_from_db() -> List[Dict[str, Any]]:
    """
    Fetch all active markets (spot + futures) from symbols table.

    Returns list of dicts:
      { "exchange_symbol": "BTCUSDT", "market_type": "spot" }
    """
    conn = pymysql.connect(
        host=_cfg.db.host,
        user=_cfg.db.user,
        password=_cfg.db.password,
        database=_cfg.db.database,
        port=_cfg.db.port,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT exchange_symbol, market_type
                FROM symbols
                WHERE is_active = 1
                ORDER BY market_type, exchange_symbol
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def find_default_market(markets: List[Dict[str, Any]]) -> str:
    """
    Choose a default market value for the dropdown.

    Preference:
      - BTCUSDT spot
      - any BTCUSDT
      - first row
    Value format: "BTCUSDT::spot"
    """

    def make_val(row: Dict[str, Any]) -> str:
        return f"{row['exchange_symbol']}::{row['market_type']}"

    # Prefer BTCUSDT spot
    for r in markets:
        if r["exchange_symbol"] == "BTCUSDT" and r["market_type"] == "spot":
            return make_val(r)

    # Then any BTCUSDT
    for r in markets:
        if r["exchange_symbol"] == "BTCUSDT":
            return make_val(r)

    # Fallback to first available
    if markets:
        return make_val(markets[0])

    # If truly no markets, just return a placeholder
    return "BTCUSDT::spot"


def is_valid_market(symbol: str, market_type: str) -> bool:
    """
    Check that (symbol, market_type) exists in symbols table.
    """
    conn = pymysql.connect(
        host=_cfg.db.host,
        user=_cfg.db.user,
        password=_cfg.db.password,
        database=_cfg.db.database,
        port=_cfg.db.port,
        cursorclass=pymysql.cursors.Cursor,
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM symbols
                WHERE exchange_symbol = %s
                  AND market_type = %s
                  AND is_active = 1
                LIMIT 1
                """,
                (symbol, market_type),
            )
            row = cur.fetchone()
            return row is not None
    finally:
        conn.close()


# ----------------------------------------------------------------------
# ROUTES
# ----------------------------------------------------------------------


@app.route("/")
def index():
    """
    Renders main page with dropdowns and chart container.

    HTML is in templates/chart.html
    """
    markets = get_markets_from_db()

    # Build list suitable for the template
    # value: "BTCUSDT::spot"
    # label: "BTCUSDT (spot)"
    opt_markets: List[Dict[str, str]] = []
    for r in markets:
        symbol = r["exchange_symbol"]
        mtype = r["market_type"]
        # normalise label a bit
        label_type = "perp" if mtype in ("futures", "perp", "perpetual") else mtype
        opt_markets.append(
            {
                "value": f"{symbol}::{mtype}",
                "label": f"{symbol} ({label_type})",
            }
        )

    default_market = find_default_market(markets)

    return flask.render_template(
        "chart.html",
        markets=opt_markets,
        timeframes=ALLOWED_TIMEFRAMES,
        default_market=default_market,
        default_timeframe="1m",
        default_limit=DEFAULT_LIMIT,
        features=FEATURES,
        default_feature="none",
    )


@app.route("/api/candles")
def api_candles():
    """
    JSON API returning OHLCV data (and optional feature overlay) for the chart.
    """
    req = flask.request

    symbol = req.args.get("symbol", "BTCUSDT")
    market_type = req.args.get("market_type", "spot")
    timeframe = req.args.get("timeframe", "1m")
    feature_key = req.args.get("feature", "none")

    try:
        limit = int(req.args.get("limit", DEFAULT_LIMIT))
    except ValueError:
        limit = DEFAULT_LIMIT

    # Simple validation
    if timeframe not in ALLOWED_TIMEFRAMES:
        return flask.jsonify({"error": "Unsupported timeframe"}), 400
    if feature_key not in FEATURE_COLUMN_MAP:
        feature_key = "none"

    # Validate market against DB
    if not is_valid_market(symbol, market_type):
        return flask.jsonify({"error": "Unsupported market"}), 400

    feature_col = FEATURE_COLUMN_MAP[feature_key]
    feature_label = FEATURE_LABEL_MAP.get(feature_key, "None")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # If timeframe is 1m and a feature is selected, join to kline_features_1m
            if timeframe == "1m" and feature_col is not None:
                # Assumes kline_features_1m.open_time is epoch *milliseconds*.
                query = f"""
                    SELECT
                        kr.open_time,
                        kr.open,
                        kr.high,
                        kr.low,
                        kr.close,
                        kr.volume,
                        kf.{feature_col} AS feature_value
                    FROM klines_raw AS kr
                    JOIN symbols AS s ON s.id = kr.symbol_id
                    LEFT JOIN kline_features_1m AS kf
                        ON kf.symbol_id = s.id
                        AND kf.open_time = UNIX_TIMESTAMP(kr.open_time) * 1000
                    WHERE
                        s.exchange_symbol = %s
                        AND s.market_type = %s
                        AND kr.timeframe = %s
                    ORDER BY kr.open_time DESC
                    LIMIT %s
                """
                cur.execute(query, (symbol, market_type, timeframe, limit))
            else:
                # No feature join (either different TF or "none" selected)
                query = """
                    SELECT
                        kr.open_time,
                        kr.open,
                        kr.high,
                        kr.low,
                        kr.close,
                        kr.volume
                    FROM klines_raw AS kr
                    JOIN symbols AS s ON s.id = kr.symbol_id
                    WHERE
                        s.exchange_symbol = %s
                        AND s.market_type = %s
                        AND kr.timeframe = %s
                    ORDER BY kr.open_time DESC
                    LIMIT %s
                """
                cur.execute(query, (symbol, market_type, timeframe, limit))

            rows = cur.fetchall()
    finally:
        conn.close()

    rows = list(rows)[::-1]  # oldest -> newest

    def _to_float(x):
        if isinstance(x, Decimal):
            return float(x)
        return x

    times = []
    opens = []
    highs = []
    lows = []
    closes = []
    volumes = []
    feature_values = []

    for row in rows:
        if len(row) == 7:
            open_time, o, h, l, c, v, fval = row
        else:
            open_time, o, h, l, c, v = row
            fval = None

        times.append(open_time.isoformat())
        opens.append(_to_float(o))
        highs.append(_to_float(h))
        lows.append(_to_float(l))
        closes.append(_to_float(c))
        volumes.append(_to_float(v))

        if fval is not None:
            feature_values.append(_to_float(fval))
        else:
            feature_values.append(None)

    # If no feature_col or timeframe != 1m, we effectively return all Nones.
    if feature_col is None or timeframe != "1m":
        feature_values = []

    # Normalised label for UI (perp instead of futures, etc.)
    label_type = (
        "perp" if market_type in ("futures", "perp", "perpetual") else market_type
    )
    symbol_display = f"{symbol} ({label_type})"

    return flask.jsonify(
        {
            "symbol": symbol,
            "market_type": market_type,
            "symbol_display": symbol_display,
            "timeframe": timeframe,
            "times": times,
            "opens": opens,
            "highs": highs,
            "lows": lows,
            "closes": closes,
            "volumes": volumes,
            "feature_key": feature_key,
            "feature_label": feature_label,
            "feature_values": feature_values,
        }
    )


# ----------------------------------------------------------------------
# ENTRY POINT
# ----------------------------------------------------------------------

if __name__ == "__main__":
    # Dev server; put behind gunicorn/uwsgi for production.
    app.run(host="0.0.0.0", port=5002, debug=True)
