# indicators.py

from __future__ import annotations

import math
from typing import Iterable, List, Optional, Sequence


FloatOrNone = Optional[float]


def _safe_div(a: float, b: float) -> Optional[float]:
    if b == 0:
        return None
    return a / b


def simple_returns(close: Sequence[float], horizon: int = 1) -> List[FloatOrNone]:
    out: List[FloatOrNone] = []
    for i in range(len(close)):
        if i < horizon:
            out.append(None)
        else:
            prev = close[i - horizon]
            out.append(_safe_div(close[i] - prev, prev))
    return out


def log_returns(close: Sequence[float], horizon: int = 1) -> List[FloatOrNone]:
    out: List[FloatOrNone] = []
    for i in range(len(close)):
        if i < horizon:
            out.append(None)
        else:
            prev = close[i - horizon]
            if prev <= 0 or close[i] <= 0:
                out.append(None)
            else:
                out.append(math.log(close[i] / prev))
    return out


def rolling_std(values: Sequence[Optional[float]], window: int) -> List[FloatOrNone]:
    out: List[FloatOrNone] = []
    buf: List[float] = []
    for v in values:
        if v is None:
            buf.append(0.0)
        else:
            buf.append(v)
        if len(buf) > window:
            buf.pop(0)
        if len(buf) < window:
            out.append(None)
        else:
            mean = sum(buf) / window
            var = sum((x - mean) ** 2 for x in buf) / window
            out.append(math.sqrt(var))
    return out


def sma(values: Sequence[float], window: int) -> List[FloatOrNone]:
    out: List[FloatOrNone] = []
    s = 0.0
    for i, v in enumerate(values):
        s += v
        if i >= window:
            s -= values[i - window]
        if i >= window - 1:
            out.append(s / window)
        else:
            out.append(None)
    return out


def ema(values: Sequence[float], window: int) -> List[FloatOrNone]:
    out: List[FloatOrNone] = []
    if not values:
        return out
    alpha = 2.0 / (window + 1.0)
    ema_val = values[0]
    out.append(ema_val)
    for i in range(1, len(values)):
        ema_val = alpha * values[i] + (1 - alpha) * ema_val
        out.append(ema_val)
    return out


def rsi(close: Sequence[float], window: int = 14) -> List[FloatOrNone]:
    gains: List[float] = []
    losses: List[float] = []
    prev: Optional[float] = None
    for c in close:
        if prev is None:
            gains.append(0.0)
            losses.append(0.0)
        else:
            diff = c - prev
            gains.append(max(diff, 0.0))
            losses.append(max(-diff, 0.0))
        prev = c

    out: List[FloatOrNone] = []
    avg_gain = 0.0
    avg_loss = 0.0
    for i, (g, l) in enumerate(zip(gains, losses)):
        if i < window:
            avg_gain += g
            avg_loss += l
            out.append(None)
            if i == window - 1:
                avg_gain /= window
                avg_loss /= window
        else:
            avg_gain = (avg_gain * (window - 1) + g) / window
            avg_loss = (avg_loss * (window - 1) + l) / window
            if avg_loss == 0:
                out.append(100.0)
            else:
                rs = avg_gain / avg_loss
                out.append(100.0 - (100.0 / (1.0 + rs)))
    return out


def atr(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    window: int = 14,
) -> List[FloatOrNone]:
    tr: List[float] = []
    prev_close: Optional[float] = None
    for h, l, c in zip(high, low, close):
        if prev_close is None:
            tr.append(h - l)
        else:
            tr.append(max(h - l, abs(h - prev_close), abs(l - prev_close)))
        prev_close = c
    # smoothing with EMA
    atr_vals = ema(tr, window)
    return atr_vals


def autocorr(values: Sequence[Optional[float]], lag: int) -> List[FloatOrNone]:
    """
    Rolling lag-k autocorrelation; naive but fine.
    """
    out: List[FloatOrNone] = []
    n = len(values)
    for i in range(n):
        if i < lag + 10:  # need a minimum history
            out.append(None)
            continue
        window_vals = [v for v in values[i - lag - 10 : i + 1] if v is not None]
        if len(window_vals) < lag + 5:
            out.append(None)
            continue
        # compute correlation between x_{t}, x_{t-lag}
        x = window_vals[lag:]
        y = window_vals[:-lag]
        if len(x) != len(y) or len(x) < 5:
            out.append(None)
            continue
        mean_x = sum(x) / len(x)
        mean_y = sum(y) / len(y)
        num = sum((a - mean_x) * (b - mean_y) for a, b in zip(x, y))
        den_x = math.sqrt(sum((a - mean_x) ** 2 for a in x))
        den_y = math.sqrt(sum((b - mean_y) ** 2 for b in y))
        if den_x == 0 or den_y == 0:
            out.append(None)
        else:
            out.append(num / (den_x * den_y))
    return out


def macd(
    close: Sequence[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[List[FloatOrNone], List[FloatOrNone]]:
    ema_fast = ema(close, fast)
    ema_slow = ema(close, slow)
    macd_line: List[FloatOrNone] = []
    for f, s in zip(ema_fast, ema_slow):
        if f is None or s is None:
            macd_line.append(None)
        else:
            macd_line.append(f - s)
    # For simplicity, treat None as 0 in EMA
    macd_clean = [0.0 if v is None else v for v in macd_line]
    macd_signal = ema(macd_clean, signal)
    return macd_line, macd_signal


def range_features(
    close: Sequence[float],
    window: int = 50,
) -> tuple[List[FloatOrNone], List[FloatOrNone], List[FloatOrNone], List[FloatOrNone]]:
    """
    Returns rng_min, rng_max, rng_pos (0..1), rng_z.
    """
    rng_min: List[FloatOrNone] = []
    rng_max: List[FloatOrNone] = []
    rng_pos: List[FloatOrNone] = []
    rng_z: List[FloatOrNone] = []

    buf: List[float] = []
    for c in close:
        buf.append(c)
        if len(buf) > window:
            buf.pop(0)
        mn = min(buf)
        mx = max(buf)
        rng_min.append(mn)
        rng_max.append(mx)
        if mx == mn:
            pos = 0.5
        else:
            pos = (c - mn) / (mx - mn)
        rng_pos.append(pos)

        mean = sum(buf) / len(buf)
        var = sum((x - mean) ** 2 for x in buf) / len(buf)
        sd = math.sqrt(var)
        if sd == 0:
            rng_z.append(None)
        else:
            rng_z.append((c - mean) / sd)

    return rng_min, rng_max, rng_pos, rng_z


def volume_features(
    volume: Sequence[float],
    window: int = 20,
) -> tuple[List[FloatOrNone], List[FloatOrNone]]:
    """
    volume_sma, volume_z
    """
    vol_sma = sma(volume, window)
    vol_z: List[FloatOrNone] = []
    buf: List[float] = []
    for v in volume:
        buf.append(v)
        if len(buf) > window:
            buf.pop(0)
        if len(buf) < window:
            vol_z.append(None)
        else:
            mean = sum(buf) / window
            var = sum((x - mean) ** 2 for x in buf) / window
            sd = math.sqrt(var)
            if sd == 0:
                vol_z.append(None)
            else:
                vol_z.append((v - mean) / sd)
    return vol_sma, vol_z
