from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable

# These are from the Bitget v3 python SDK we vendored into this project.
# Path: bitget/v2/spot/market_api.py, bitget/v2/mix/market_api.py
from bitget.v2.spot.market_api import MarketApi as SpotMarketApi
from bitget.v2.mix.market_api import MarketApi as FuturesMarketApi

from config_loader import BitgetConfig
from resilience import (
    ResilientExecutor,
    ResilienceConfig,
    TransientError,
)

logger = logging.getLogger(__name__)


# Map our internal intervals for spot (e.g. "1m") to Bitget v2 granularity strings.
GRANULARITY_MAP: Dict[str, str] = {
    "1m": "1min",
    "3m": "3min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "12h": "12h",
    "1d": "1day",
    "3d": "3day",
    "1w": "1week",
    "1M": "1M",  # monthly
}

# Futures/mix (perpetual) use a different granularity format
FUTURES_GRANULARITY_MAP: Dict[str, str] = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "2h": "2H",
    "4h": "4H",
    "6h": "6H",
    "12h": "12H",
    "1d": "1D",
    "3d": "3D",
    "1w": "1W",
    "1M": "1M",
}


class BitgetClient:
    """
    Wrapper over the official Bitget v3 Python SDK (v2 REST APIs) for
    spot or perpetual futures ("mix") market data.

    This client exposes a simple, exchange-agnostic interface used by the
    acquisition service:

      - fetch_klines(symbol, interval, start, end, limit)
      - fetch_recent_trades(symbol, limit, from_id)
      - fetch_orderbook(symbol, depth)
      - fetch_orderbook_top_levels(symbol, depth, levels)

    It internally uses:
      - bitget.v2.spot.market_api.MarketApi for SPOT, or
      - bitget.v2.mix.market_api.MarketApi for FUTURES/PERP (USDT/COIN-margined)

    All HTTP calls are wrapped in a ResilientExecutor which:
      - Retries transient failures (network/SDK exceptions) up to max_outage_minutes.
      - Raises OutageExceededError once that window is exceeded, which is
        handled by main() to restart the pipeline in --mode both.
    """

    def __init__(
        self,
        cfg: BitgetConfig,
        market_type: str,
        futures_product_type: str = "USDT-FUTURES",
        max_outage_minutes: int = 0,
    ) -> None:
        """
        :param cfg:       BitgetConfig from config_loader (api_key, secret, etc.)
        :param market_type:
            "spot" for spot markets, or any of:
              "futures", "mix", "perp", "perpetual"
            for Bitget mix/perpetual futures markets.
        :param futures_product_type:
            For futures/perps, Bitget productType, e.g.
              "USDT-FUTURES", "COIN-FUTURES", "USDC-FUTURES"
            (ignored for spot).
        :param max_outage_minutes:
            Consecutive outage window for transient errors before giving up.
        """
        self.cfg = cfg
        self.market_type = market_type.lower()
        self.futures_product_type = futures_product_type

        # Wrap the v2 REST Market API from the SDK.
        if self.market_type == "spot":
            self.market = SpotMarketApi(
                cfg.api_key,
                cfg.api_secret,
                cfg.passphrase,
                use_server_time=False,
                first=False,
            )
        elif self.market_type in {"futures", "mix", "perp", "perpetual"}:
            self.market = FuturesMarketApi(
                cfg.api_key,
                cfg.api_secret,
                cfg.passphrase,
                use_server_time=False,
                first=False,
            )
            # Normalise internal representation
            self.market_type = "futures"
        else:
            raise ValueError(f"Unsupported market_type: {market_type!r}")

        # simple internal counters for optional soft rate limiting (over the SDK)
        self._last_request_ts: float = 0.0
        self._requests_in_current_minute: int = 0
        self._current_minute: int = int(time.time() // 60)

        # Resilience layer
        self._executor = ResilientExecutor(
            ResilienceConfig(max_outage_minutes=max_outage_minutes),
            logger=logger,
        )

    # -------------------------------------------------------------------------
    # Internal rate-limiting overlay (SDK itself does basic request handling;
    # this is just to respect our own config caps).
    # -------------------------------------------------------------------------

    def _rate_limit_sleep(self) -> None:
        now = time.time()
        current_minute = int(now // 60)

        if current_minute != self._current_minute:
            self._current_minute = current_minute
            self._requests_in_current_minute = 0

        # Respect per-second config limit
        max_rps = max(self.cfg.rate_limits.max_requests_per_second, 1)
        min_interval = 1.0 / max_rps
        elapsed = now - self._last_request_ts

        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
            now = time.time()

        # Rough per-minute cap
        if (
            self._requests_in_current_minute
            >= self.cfg.rate_limits.max_requests_per_minute
        ):
            seconds_to_next_minute = 60 - (now % 60)
            logger.warning(
                "BitgetClient: per-minute request cap reached, sleeping %.2fs",
                seconds_to_next_minute,
            )
            time.sleep(seconds_to_next_minute)
            now = time.time()
            self._current_minute = int(now // 60)
            self._requests_in_current_minute = 0

        self._last_request_ts = time.time()
        self._requests_in_current_minute += 1

    # -------------------------------------------------------------------------
    # Internal helper: execute a low-level SDK call with resilience
    # -------------------------------------------------------------------------

    def _call_with_resilience(
        self,
        op_desc: str,
        call: Callable[[], Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Wrap a Bitget SDK call with:
          - rate limiting
          - transient error handling & retries via ResilientExecutor
        """

        def inner() -> Dict[str, Any]:
            self._rate_limit_sleep()
            try:
                return call()
            except (ValueError, TypeError) as e:
                # Programming/usage errors – don't retry, propagate directly.
                logger.error("Non-transient error in %s: %s", op_desc, e)
                raise
            except Exception as e:
                # Treat all other SDK / network errors as transient.
                logger.warning("Transient error in %s: %s", op_desc, e)
                raise TransientError(str(e)) from e

        return self._executor.call(inner)

    # -------------------------------------------------------------------------
    # Public methods used by the acquisition service
    # -------------------------------------------------------------------------

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
        limit: int = 200,
    ) -> List[Any]:
        """
        Fetch candle data for a symbol.

        :param symbol:
            For spot: "BTCUSDT".
            For futures/perps: e.g. "BTCUSDT_UMCBL".
        :param interval:
            Our internal string ("1m", "5m", "1h", etc.).
        """
        if self.market_type == "futures":
            granularity = FUTURES_GRANULARITY_MAP.get(interval)
        else:
            granularity = GRANULARITY_MAP.get(interval)

        if granularity is None:
            raise ValueError(
                f"Unsupported interval={interval!r} for Bitget granularity "
                f"(market_type={self.market_type})"
            )

        # Build params according to business line.
        params: Dict[str, Any] = {
            "symbol": symbol,
            "granularity": granularity,
            "limit": str(limit),
        }

        # mix futures use productType; spot does not.
        if self.market_type == "futures":
            params["productType"] = self.futures_product_type

        if start is not None:
            params["startTime"] = str(start)
        if end is not None:
            params["endTime"] = str(end)

        rsp = self._call_with_resilience(
            f"candles {symbol} {interval}",
            lambda: self.market.candles(params),
        )

        code = rsp.get("code")
        if code != "00000":
            logger.error("Bitget candles error for %s %s: %s", symbol, interval, rsp)
            # Treat Bitget business errors as non-transient (no retry here).
            raise RuntimeError(f"Bitget candles error: {rsp}")

        data = rsp.get("data", [])
        return data

    def fetch_recent_trades(
        self,
        symbol: str,
        limit: int = 100,
        from_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch recent trades.

        Note: v2 trade history is cursor-based (time and idLessThan) according to
        the generic docs, but the SDK exposes a simple market.fills(params)
        call that returns recent trades. We'll keep it simple and let the DB
        dedupe using exchange_trade_id.

        :param symbol:
            For spot: "BTCUSDT";
            For futures/perps: "BTCUSDT_UMCBL".
        :param limit:
            Max number of trades to fetch.
        :param from_id:
            Not directly supported by the v2 SDK; kept for
            signature compatibility with our acquisition code.
        :return:
            List of trade dicts as returned by Bitget.
        """
        params: Dict[str, Any] = {
            "symbol": symbol,
            "limit": str(limit),
        }

        if self.market_type == "futures":
            params["productType"] = self.futures_product_type

        rsp = self._call_with_resilience(
            f"fills {symbol}",
            lambda: self.market.fills(params),
        )

        code = rsp.get("code")
        if code != "00000":
            logger.error("Bitget fills error for %s: %s", symbol, rsp)
            raise RuntimeError(f"Bitget fills error: {rsp}")

        data = rsp.get("data", [])
        return data

    def fetch_trades_history(
        self,
        symbol: str,
        start: int,
        end: int,
        limit: int = 1000,
        id_less_than: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical public trades within a time window, with pagination.

        Uses:
        - spot: /api/v2/spot/market/fills-history
        - futures: /api/v2/mix/market/fills-history

        Note: Bitget limits the time window to <= 7 days per request.
        Pagination uses idLessThan to go older.
        """
        params: Dict[str, Any] = {
            "symbol": symbol,
            "limit": str(limit),
            "startTime": str(start),
            "endTime": str(end),
        }
        if id_less_than is not None:
            params["idLessThan"] = str(id_less_than)

        if self.market_type == "futures":
            params["productType"] = self.futures_product_type

        rsp = self._call_with_resilience(
            f"fills-history {symbol}",
            lambda: self.market.fills_history(params),
        )

        code = rsp.get("code")
        if code != "00000":
            logger.error("Bitget fills-history error for %s: %s", symbol, rsp)
            raise RuntimeError(f"Bitget fills-history error: {rsp}")

        data = rsp.get("data", [])
        return data

    def fetch_orderbook(
        self,
        symbol: str,
        depth: int = 50,
    ) -> Dict[str, Any]:
        """
        Fetch orderbook snapshot.

        For v2 mix (futures/perps), SDK uses:
          - market.orderbook(params) -> GET /api/v2/mix/market/orderbook

        For v2 spot, there is a similar market.orderbook in the v2 spot MarketApi.

        :param symbol:
            For spot: "BTCUSDT";
            For futures/perps: "BTCUSDT_UMCBL".
        :param depth:
            Depth of book. Bitget v2 docs mention tiers (1,5,15,50,max),
            but the SDK wraps this into parameters; we use 'limit' or
            'type' depending on business line.
        :return:
            A dict with at least bids/asks arrays; exact structure is what
            Bitget returns (we can JSON-dump it into DB).
        """
        params: Dict[str, Any] = {
            "symbol": symbol,
        }

        if self.market_type == "futures":
            params["productType"] = self.futures_product_type
            params["limit"] = str(depth)
        else:
            params["limit"] = str(depth)

        rsp = self._call_with_resilience(
            f"orderbook {symbol}",
            lambda: self.market.orderbook(params),
        )

        code = rsp.get("code")
        if code != "00000":
            logger.error("Bitget orderbook error for %s: %s", symbol, rsp)
            raise RuntimeError(f"Bitget orderbook error: {rsp}")

        data = rsp.get("data", {})
        return data

    def fetch_orderbook_top_levels(
        self,
        symbol: str,
        depth: int,
        levels: int,
    ) -> Dict[str, List[List[Any]]]:
        """
        Convenience wrapper for "N layers of spread" use cases.

        Fetches the full orderbook up to `depth`, then returns only the
        top `levels` bids and asks.

        :param symbol:  Spot or futures symbol, as for fetch_orderbook().
        :param depth:   Requested depth from the exchange (e.g. 50).
        :param levels:  How many top levels to keep (e.g. 5).
        :return:
            {
              "bids": [ [price, size, ...], ... up to N ],
              "asks": [ [price, size, ...], ... up to N ],
            }
        """
        if levels <= 0:
            raise ValueError("levels must be >= 1")

        ob = self.fetch_orderbook(symbol, depth)
        bids = ob.get("bids", []) or []
        asks = ob.get("asks", []) or []

        n = min(levels, len(bids))
        m = min(levels, len(asks))

        return {
            "bids": bids[:n],
            "asks": asks[:m],
        }

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def datetime_to_ms(dt: datetime) -> int:
        """
        Convert timezone-aware datetime to Unix ms.
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    @staticmethod
    def ms_to_datetime(ms: int) -> datetime:
        """
        Convert Unix ms to timezone-aware UTC datetime.
        """
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
