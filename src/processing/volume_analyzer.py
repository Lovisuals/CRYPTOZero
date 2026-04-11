"""
Core module for VolumeAnalyzer
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

class VolumeAnalyzer:

    def __init__(self, lookback: int=1000):
        self.lookback = lookback
        self.cvd: float = 0.0
        self.total_buy_volume: float = 0.0
        self.total_sell_volume: float = 0.0
        self.trade_count: int = 0
        self.trade_history: deque = deque(maxlen=10000)
        self.cvd_history: deque = deque(maxlen=lookback)
        self.volume_profile: dict = {}

    def process_agg_trade(self, trade: dict):
        price = float(trade['p'])
        qty = float(trade['q'])
        is_buyer_maker: bool = trade['m']
        if is_buyer_maker:
            self.cvd -= qty
            self.total_sell_volume += qty
            side = 'SELL'
        else:
            self.cvd += qty
            self.total_buy_volume += qty
            side = 'BUY'
        self.trade_history.append({'price': price, 'qty': qty, 'side': side, 'timestamp': trade['E']})
        self.cvd_history.append(self.cvd)
        self._update_profile(price, qty, side)
        self.trade_count += 1

    def _update_profile(self, price: float, qty: float, side: str):
        if price > 10000:
            bucket_size = 10.0
        elif price > 1000:
            bucket_size = 1.0
        else:
            bucket_size = 0.1
        bucket = round(price / bucket_size) * bucket_size
        if bucket not in self.volume_profile:
            self.volume_profile[bucket] = {'buy_volume': 0.0, 'sell_volume': 0.0, 'total_volume': 0.0, 'trade_count': 0}
        p = self.volume_profile[bucket]
        if side == 'BUY':
            p['buy_volume'] += qty
        else:
            p['sell_volume'] += qty
        p['total_volume'] += qty
        p['trade_count'] += 1

    def get_cvd_divergence(self, price_series: pd.Series, periods: int=20) -> dict:
        neutral = {'signal': 'NEUTRAL', 'strength': 0.0, 'cvd_slope': 0.0, 'price_slope': 0.0}
        if len(self.cvd_history) < periods or len(price_series) < periods:
            return neutral
        x = np.arange(periods)
        cvd_arr = np.array(list(self.cvd_history)[-periods:])
        price_arr = price_series.iloc[-periods:].values
        cvd_slope = np.polyfit(x, cvd_arr, 1)[0]
        price_slope = np.polyfit(x, price_arr, 1)[0]
        cvd_norm = cvd_slope / (abs(self.cvd) + 1e-09)
        price_norm = price_slope / (price_arr[-1] + 1e-09)
        signal = 'NEUTRAL'
        strength = 0.0
        if cvd_norm > 0.001 and price_norm < -0.0005:
            signal = 'BULLISH_DIVERGENCE'
            strength = abs(cvd_norm - price_norm)
        elif cvd_norm < -0.001 and price_norm > 0.0005:
            signal = 'BEARISH_DIVERGENCE'
            strength = abs(cvd_norm - price_norm)
        return {'signal': signal, 'strength': strength, 'cvd_slope': float(cvd_slope), 'price_slope': float(price_slope)}

    def get_point_of_control(self) -> float:
        if not self.volume_profile:
            return 0.0
        return max(self.volume_profile, key=lambda p: self.volume_profile[p]['total_volume'])

    def get_value_area(self, pct: float=0.7) -> tuple:
        if not self.volume_profile:
            return (0.0, 0.0)
        sorted_levels = sorted(self.volume_profile.items())
        total = sum((v['total_volume'] for _, v in sorted_levels))
        target = total * pct
        poc = self.get_point_of_control()
        poc_idx = next((i for i, (p, _) in enumerate(sorted_levels) if p == poc))
        accumulated = sorted_levels[poc_idx][1]['total_volume']
        lo, hi = (poc_idx, poc_idx)
        while accumulated < target:
            can_lo = lo > 0
            can_hi = hi < len(sorted_levels) - 1
            if not can_lo and (not can_hi):
                break
            lo_vol = sorted_levels[lo - 1][1]['total_volume'] if can_lo else 0
            hi_vol = sorted_levels[hi + 1][1]['total_volume'] if can_hi else 0
            if can_lo and (not can_hi or lo_vol >= hi_vol):
                lo -= 1
                accumulated += lo_vol
            else:
                hi += 1
                accumulated += hi_vol
        return (sorted_levels[lo][0], sorted_levels[hi][0])

    def get_buy_sell_ratio(self, periods: int=100) -> float:
        if len(self.trade_history) < periods:
            return 1.0
        recent = list(self.trade_history)[-periods:]
        buys = sum((t['qty'] for t in recent if t['side'] == 'BUY'))
        sells = sum((t['qty'] for t in recent if t['side'] == 'SELL'))
        return buys / sells if sells > 0 else float('inf')