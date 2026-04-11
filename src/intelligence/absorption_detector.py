from collections import deque
from sortedcontainers import SortedDict
from typing import Dict, List, Optional
import logging
import time

logger = logging.getLogger(__name__)

class AbsorptionDetector:
    def __init__(self, volume_multiplier: int=50, persistence_periods: int=6, price_threshold: float=0.001):
        self.volume_multiplier = volume_multiplier
        self.persistence_periods = persistence_periods
        self.price_threshold = price_threshold
        self._volumes: deque = deque(maxlen=60)
        self._price_changes: deque = deque(maxlen=60)
        self._cvd_changes: deque = deque(maxlen=60)
        self._in_absorption = False
        self._start_cvd: float = 0.0
        self._start_time: float = 0.0
        self.absorption_count: int = 0

    def update(self, volume_1m: float, price_change_pct: float, current_cvd: float) -> dict | None:
        self._volumes.append(volume_1m)
        self._price_changes.append(abs(price_change_pct))
        if self._cvd_changes:
            self._cvd_changes.append(abs(current_cvd - list(self._cvd_changes)[-1]))
        else:
            self._cvd_changes.append(0.0)
        if len(self._volumes) < self.persistence_periods:
            return None
        prior = list(self._volumes)[:-1]
        avg_volume = sum(prior) / len(prior)
        if volume_1m < self.volume_multiplier * avg_volume:
            self._reset()
            return None
        recent_moves = list(self._price_changes)[-self.persistence_periods:]
        if not all((abs(m) < self.price_threshold for m in recent_moves)):
            self._reset()
            return None
        if not self._in_absorption:
            self._in_absorption = True
            self._start_cvd = current_cvd
            self._start_time = time.time()
        cvd_change = abs(current_cvd - self._start_cvd)
        cvd_ratio = cvd_change / (volume_1m + 1e-09)
        if cvd_ratio >= 0.05:
            return None
        direction = 'BULLISH' if current_cvd > 0 else 'BEARISH'
        self.absorption_count += 1
        return {'pattern': 'ABSORPTION', 'direction': direction, 'confidence': 82, 'volume_multiplier': volume_1m / avg_volume, 'duration_seconds': time.time() - self._start_time, 'cvd_stability': 1.0 - cvd_ratio, 'timestamp': int(time.time() * 1000)}

    def _reset(self):
        self._in_absorption = False
        self._start_cvd = 0.0
        self._start_time = 0.0