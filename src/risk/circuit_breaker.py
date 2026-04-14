from __future__ import annotations

import logging
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class CircuitBreaker:

    def __init__(
        self,
        max_consecutive_losses: int = 5,
        max_hourly_losses: int = 8,
        cooldown_seconds: float = 1800.0,
        max_drawdown_velocity_pct: float = 0.03,
    ):
        self.max_consecutive_losses = max_consecutive_losses
        self.max_hourly_losses = max_hourly_losses
        self.cooldown_seconds = cooldown_seconds
        self.max_drawdown_velocity = max_drawdown_velocity_pct

        self._consecutive_losses: int = 0
        self._hourly_losses: int = 0
        self._hourly_reset_ts: float = time.time()
        self._tripped: bool = False
        self._tripped_at: float = 0.0
        self._trip_count: int = 0
        self._trip_reason: str = ""

    @property
    def is_tripped(self) -> bool:
        if self._tripped:
            elapsed = time.time() - self._tripped_at
            if elapsed >= self.cooldown_seconds:
                self._reset()
                return False
        return self._tripped

    @property
    def trip_reason(self) -> str:
        return self._trip_reason if self._tripped else ""

    @property
    def remaining_cooldown_s(self) -> float:
        if not self._tripped:
            return 0.0
        elapsed = time.time() - self._tripped_at
        return max(0.0, self.cooldown_seconds - elapsed)

    def on_trade_result(self, outcome: str, pnl_pct: float = 0.0):
        now = time.time()
        if now - self._hourly_reset_ts > 3600:
            self._hourly_losses = 0
            self._hourly_reset_ts = now

        if outcome == "WIN":
            self._consecutive_losses = 0
        elif outcome == "LOSS":
            self._consecutive_losses += 1
            self._hourly_losses += 1

            if self._consecutive_losses >= self.max_consecutive_losses:
                self._trip(f"CONSECUTIVE_LOSSES ({self._consecutive_losses})")

            if self._hourly_losses >= self.max_hourly_losses:
                self._trip(f"HOURLY_LOSSES ({self._hourly_losses})")

    def check_drawdown_velocity(self, drawdown_pct: float):
        if drawdown_pct >= self.max_drawdown_velocity:
            self._trip(f"DRAWDOWN_VELOCITY ({drawdown_pct:.1%})")

    def _trip(self, reason: str):
        if not self._tripped:
            self._tripped = True
            self._tripped_at = time.time()
            self._trip_count += 1
            self._trip_reason = reason
            logger.warning("CIRCUIT BREAKER TRIPPED | reason=%s | cooldown=%.0fs | total_trips=%d",
                          reason, self.cooldown_seconds, self._trip_count)

    def _reset(self):
        logger.info("CIRCUIT BREAKER RESET | was tripped for %.0fs | reason was: %s",
                    time.time() - self._tripped_at, self._trip_reason)
        self._tripped = False
        self._tripped_at = 0.0
        self._trip_reason = ""
        self._consecutive_losses = 0
        self._hourly_losses = 0

    def force_reset(self):
        self._reset()

    def force_trip(self, reason: str = "MANUAL"):
        self._trip(reason)

    def stats(self) -> Dict:
        return {
            "tripped": self._tripped,
            "trip_reason": self._trip_reason,
            "remaining_cooldown_s": self.remaining_cooldown_s,
            "consecutive_losses": self._consecutive_losses,
            "hourly_losses": self._hourly_losses,
            "total_trips": self._trip_count,
        }
