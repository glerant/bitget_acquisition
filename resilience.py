from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional, TypeVar, Generic

T = TypeVar("T")


class OutageExceededError(RuntimeError):
    """Raised when transient errors have persisted beyond the allowed outage window."""


class TransientError(RuntimeError):
    """
    Wraps low-level transient failures (network glitches, DB connection drops, etc.).

    DBManager and BitgetClient should catch their driver-specific exceptions
    (e.g. connection resets, timeouts, 5xx API responses) and re-raise TransientError.
    """


@dataclass
class ResilienceConfig:
    max_outage_minutes: int
    base_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 30.0


class ResilientExecutor(Generic[T]):
    """
    Encapsulates retry logic for transient failures over an outage window.

    - On success: outage window is reset.
    - On TransientError: retry with exponential backoff, until total elapsed
      outage time reaches max_outage_minutes, then raise OutageExceededError.
    """

    def __init__(
        self,
        config: ResilienceConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._cfg = config
        self._log = logger or logging.getLogger(__name__)
        self._outage_started_at: Optional[float] = None
        self._outage_window_secs = max(0, self._cfg.max_outage_minutes * 60)

    def _reset_outage(self) -> None:
        self._outage_started_at = None

    def call(self, fn: Callable[[], T]) -> T:
        """
        Execute fn() with resilience.

        fn must either:
        - succeed (return T), or
        - raise TransientError for retry-able failures, or
        - raise some other exception for non-retryable, which will propagate.
        """
        # If resilience disabled (0 minutes), just call fn normally.
        if self._outage_window_secs == 0:
            return fn()

        backoff = self._cfg.base_backoff_seconds

        while True:
            try:
                result = fn()
                # Success resets outage window.
                self._reset_outage()
                return result

            except TransientError as e:
                now = time.monotonic()
                if self._outage_started_at is None:
                    self._outage_started_at = now

                elapsed = now - self._outage_started_at
                if elapsed >= self._outage_window_secs:
                    self._log.error(
                        "Outage window exceeded (elapsed %.1fs ≥ %.1fs); "
                        "raising OutageExceededError.",
                        elapsed,
                        self._outage_window_secs,
                    )
                    raise OutageExceededError(
                        f"Transient outage exceeded "
                        f"{self._cfg.max_outage_minutes} minutes"
                    ) from e

                # Sleep with exponential backoff, but don't sleep past window.
                remaining = self._outage_window_secs - elapsed
                sleep_for = min(backoff, self._cfg.max_backoff_seconds, remaining)
                self._log.warning(
                    "Transient error: %s. Retrying in %.1fs "
                    "(elapsed %.1fs / %.1fs).",
                    e,
                    sleep_for,
                    elapsed,
                    self._outage_window_secs,
                )
                time.sleep(sleep_for)
                backoff = min(backoff * 2, self._cfg.max_backoff_seconds)
