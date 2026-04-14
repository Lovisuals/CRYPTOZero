from __future__ import annotations

import logging
import time
from collections import deque
from typing import Dict, Optional

import numpy as np

logger = logging.getLogger(__name__)

class RegimeFilter:

    REGIME_TRENDING = "TRENDING"
    REGIME_RANGING = "RANGING"
    REGIME_VOLATILE = "VOLATILE"
    REGIME_DEAD = "DEAD"

    def __init__(
        self,
        volatility_lookback: int = 100,
        volume_lookback: int = 50,
        dead_vol_percentile: float = 15.0,
        volatile_percentile: float = 90.0,
        trend_threshold: float = 0.002,
        min_samples: int = 30,
    ):
        self._vol_lookback = volatility_lookback
        self._volume_lookback = volume_lookback
        self._dead_pct = dead_vol_percentile
        self._volatile_pct = volatile_percentile
        self._trend_thresh = trend_threshold
        self._min_samples = min_samples

        self._price_history: deque = deque(maxlen=volatility_lookback * 2)
        self._volume_history: deque = deque(maxlen=volume_lookback * 2)
        self._spread_history: deque = deque(maxlen=100)
        self._regime: str = self.REGIME_RANGING
        self._regime_confidence: int = 50
        self._last_update: float = 0.0

    def update(self, price: float, volume: float, spread_bps: int = 0):
        self._price_history.append(price)
        self._volume_history.append(volume)
        if spread_bps > 0:
            self._spread_history.append(spread_bps)
        self._classify()

    def _classify(self):
        prices = list(self._price_history)
        volumes = list(self._volume_history)

        if len(prices) < self._min_samples:
            self._regime = self.REGIME_RANGING
            self._regime_confidence = 30
            return

        returns = np.diff(np.log(np.array(prices[-self._vol_lookback:])))
        if len(returns) < 10:
            return

        current_vol = float(np.std(returns[-20:]))
        historical_vol_dist = [float(np.std(returns[i:i+20])) for i in range(0, len(returns)-20, 5)]
        if not historical_vol_dist:
            return

        vol_percentile = float(np.percentile(historical_vol_dist, [self._dead_pct, 50, self._volatile_pct]).tolist()[1])
        low_thresh = float(np.percentile(historical_vol_dist, self._dead_pct))
        high_thresh = float(np.percentile(historical_vol_dist, self._volatile_pct))

        recent_prices = np.array(prices[-30:])
        x = np.arange(len(recent_prices))
        slope = float(np.polyfit(x, recent_prices, 1)[0])
        trend_strength = abs(slope / (recent_prices[-1] + 1e-9))

        if current_vol <= low_thresh:
            self._regime = self.REGIME_DEAD
            self._regime_confidence = 80
        elif current_vol >= high_thresh:
            self._regime = self.REGIME_VOLATILE
            self._regime_confidence = 85
        elif trend_strength > self._trend_thresh:
            self._regime = self.REGIME_TRENDING
            self._regime_confidence = 75
        else:
            self._regime = self.REGIME_RANGING
            self._regime_confidence = 70

        self._last_update = time.time()

    @property
    def regime(self) -> str:
        return self._regime

    @property
    def confidence(self) -> int:
        return self._regime_confidence

    def should_trade(self, signal_type: str = "") -> bool:
        if self._regime == self.REGIME_DEAD:
            return False
        return True

    def get_regime_multiplier(self) -> float:
        if self._regime == self.REGIME_TRENDING:
            return 1.2
        elif self._regime == self.REGIME_RANGING:
            return 0.8
        elif self._regime == self.REGIME_VOLATILE:
            return 0.5
        return 0.0

    def status(self) -> Dict:
        return {
            "regime": self._regime,
            "confidence": self._regime_confidence,
            "samples": len(self._price_history),
            "last_update": self._last_update,
        }
