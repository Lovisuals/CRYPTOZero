from __future__ import annotations

import logging
import math
import time
from collections import deque
from typing import Dict

logger = logging.getLogger(__name__)

class VPINDetector:

    def __init__(
        self,
        bucket_volume: float = 1.0,
        n_buckets: int = 50,
        toxicity_threshold: float = 0.7,
        critical_threshold: float = 0.85,
    ):
        self._bucket_volume = bucket_volume
        self._n_buckets = n_buckets
        self._toxicity_threshold = toxicity_threshold
        self._critical_threshold = critical_threshold

        self._current_bucket_buy: float = 0.0
        self._current_bucket_sell: float = 0.0
        self._current_bucket_total: float = 0.0
        self._buckets: deque = deque(maxlen=n_buckets)
        self._vpin: float = 0.0
        self._last_update: float = 0.0
        self._total_trades_processed: int = 0

    def on_trade(self, qty: float, is_buy: bool):
        self._total_trades_processed += 1
        remaining = qty

        while remaining > 0:
            space = self._bucket_volume - self._current_bucket_total
            fill = min(remaining, space)

            if is_buy:
                self._current_bucket_buy += fill
            else:
                self._current_bucket_sell += fill
            self._current_bucket_total += fill
            remaining -= fill

            if self._current_bucket_total >= self._bucket_volume:
                self._finalize_bucket()

    def _finalize_bucket(self):
        if self._current_bucket_total <= 0:
            return
        order_imbalance = abs(self._current_bucket_buy - self._current_bucket_sell)
        self._buckets.append({
            "buy_vol": self._current_bucket_buy,
            "sell_vol": self._current_bucket_sell,
            "imbalance": order_imbalance,
            "total": self._current_bucket_total,
            "ts": time.time(),
        })
        self._current_bucket_buy = 0.0
        self._current_bucket_sell = 0.0
        self._current_bucket_total = 0.0
        self._recalculate()

    def _recalculate(self):
        if len(self._buckets) < 5:
            self._vpin = 0.0
            return
        total_imbalance = sum(b["imbalance"] for b in self._buckets)
        total_volume = sum(b["total"] for b in self._buckets)
        self._vpin = total_imbalance / total_volume if total_volume > 0 else 0.0
        self._last_update = time.time()

    @property
    def vpin(self) -> float:
        return self._vpin

    @property
    def is_toxic(self) -> bool:
        return self._vpin >= self._toxicity_threshold

    @property
    def is_critical(self) -> bool:
        return self._vpin >= self._critical_threshold

    def should_allow_execution(self) -> bool:
        return not self.is_toxic

    def get_toxicity_multiplier(self) -> float:
        if self._vpin < 0.3:
            return 1.0
        elif self._vpin < self._toxicity_threshold:
            return 1.0 - (self._vpin - 0.3) / (self._toxicity_threshold - 0.3) * 0.5
        return 0.0

    def calibrate_bucket_volume(self, avg_trade_volume: float, trades_per_bucket: int = 50):
        new_bucket = avg_trade_volume * trades_per_bucket
        if new_bucket > 0:
            self._bucket_volume = new_bucket
            logger.info("VPIN bucket volume calibrated to %.4f", new_bucket)

    def status(self) -> Dict:
        return {
            "vpin": round(self._vpin, 4),
            "is_toxic": self.is_toxic,
            "is_critical": self.is_critical,
            "buckets_filled": len(self._buckets),
            "bucket_target": self._n_buckets,
            "bucket_volume": self._bucket_volume,
            "trades_processed": self._total_trades_processed,
        }
