"""
Core module for IcebergDetector
"""
from collections import deque
from sortedcontainers import SortedDict
from typing import Callable, Dict, List, Optional
from typing import Dict, List
from typing import Dict, List, Optional
import aiohttp
import asyncio
import logging
import numpy as np
import pandas as pd
import time

import logging
logger = logging.getLogger(__name__)

class IcebergDetector:

    class _Tracker:
        __slots__ = ('cum_vol', 'first_seen', 'last_seen', 'visible_qty', 'side', 'alive')

        def __init__(self, side: str, qty: float):
            self.cum_vol: float = 0.0
            self.first_seen: float = time.time()
            self.last_seen: float = time.time()
            self.visible_qty: float = qty
            self.side: str = side
            self.alive: bool = True

    def __init__(self, executed_ratio: float=5.0, min_persist_seconds: float=30.0, window_seconds: float=60.0, min_notional: float=10000.0):
        self.executed_ratio = executed_ratio
        self.min_persist_seconds = min_persist_seconds
        self.window_seconds = window_seconds
        self.min_notional = min_notional
        self._trackers: Dict[float, IcebergDetector._Tracker] = {}
        self.confirmed: List[Dict] = []
        self.total_detected: int = 0

    def on_trade(self, price: float, qty: float, side: str, bids: SortedDict, asks: SortedDict):
        book = bids if side == 'SELL' else asks
        if price not in book:
            if price in self._trackers:
                self._trackers[price].alive = False
            return
        visible = book[price]
        if price * visible < self.min_notional:
            return
        self._expire(time.time())
        if price not in self._trackers:
            self._trackers[price] = IcebergDetector._Tracker(side=side.lower(), qty=visible)
        t = self._trackers[price]
        t.cum_vol += qty
        t.visible_qty = visible
        t.last_seen = time.time()
        t.alive = True

    def on_book_update(self, bids: SortedDict, asks: SortedDict) -> List[Dict]:
        now = time.time()
        results: List[Dict] = []
        for price, t in list(self._trackers.items()):
            book = bids if t.side == 'sell' else asks
            if price not in book:
                t.alive = False
                continue
            t.visible_qty = book[price]
            ratio = t.cum_vol / (t.visible_qty + 1e-09)
            duration = now - t.first_seen
            if ratio >= self.executed_ratio and duration >= self.min_persist_seconds and t.alive:
                record = self._build(price, t, ratio, duration)
                results.append(record)
                self.total_detected += 1
                logger.info('Iceberg confirmed: %s side @ %.2f | ratio=%.1fx | duration=%.0fs', t.side.upper(), price, ratio, duration)
        self.confirmed = results
        return results

    def _expire(self, now: float):
        expired = [p for p, t in self._trackers.items() if not t.alive or now - t.last_seen > self.window_seconds]
        for p in expired:
            del self._trackers[p]

    @staticmethod
    def _build(price: float, t, ratio: float, duration: float) -> Dict:
        if t.side == 'sell':
            signal, interpretation = ('SELL', 'Distribution zone — sell on rally to this level')
        else:
            signal, interpretation = ('BUY', 'Accumulation zone — buy on dip to this level')
        return {'type': 'ICEBERG', 'side': t.side.upper(), 'price': price, 'visible_qty': t.visible_qty, 'cum_executed_vol': t.cum_vol, 'volume_ratio': ratio, 'duration_seconds': duration, 'signal': signal, 'interpretation': interpretation, 'confidence': 72, 'timestamp': int(time.time() * 1000)}