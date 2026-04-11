"""
Core module for LiquidityAnalyzer
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

class LiquidityAnalyzer:

    def __init__(self, vacuum_threshold: float=0.1):
        self.vacuum_threshold = vacuum_threshold
        self.avg_bid_liquidity: float = 100000.0
        self.avg_ask_liquidity: float = 100000.0
        self.vacuums: List[Dict] = []
        self.walls: List[Dict] = []

    def update_avg_liquidity(self, bids: SortedDict, asks: SortedDict):
        if not bids or not asks:
            return
        alpha = 0.1
        bid_sample = sum((p * q for p, q in list(bids.items())[-20:]))
        ask_sample = sum((p * q for p, q in list(asks.items())[:20]))
        self.avg_bid_liquidity = alpha * bid_sample + (1 - alpha) * self.avg_bid_liquidity
        self.avg_ask_liquidity = alpha * ask_sample + (1 - alpha) * self.avg_ask_liquidity

    def detect_stacked_vacuums(self, bids: SortedDict, asks: SortedDict, scan_range_pct: float=0.02) -> List[Dict]:
        if not bids or not asks:
            return []
        mid = (list(bids.keys())[-1] + list(asks.keys())[0]) / 2
        vacuums: List[Dict] = []
        ask_levels = sorted(asks.keys())
        max_scan = mid * (1 + scan_range_pct)
        i = 0
        while i < len(ask_levels) and ask_levels[i] <= max_scan:
            start = ask_levels[i]
            count = 0
            total_liq = 0.0
            j = i
            while j < min(i + 10, len(ask_levels)):
                if ask_levels[j] > max_scan:
                    break
                window_liq = sum((asks.get(ask_levels[k], 0) * ask_levels[k] for k in range(j, min(j + 3, len(ask_levels)))))
                if window_liq < self.vacuum_threshold * self.avg_ask_liquidity:
                    count += 1
                    total_liq += window_liq
                else:
                    break
                j += 1
            if count >= 2:
                end = ask_levels[min(j - 1, len(ask_levels) - 1)]
                vacuums.append({'zone': 'ASK_VACUUM', 'direction': 'UPWARD', 'start_price': start, 'end_price': end, 'levels': count, 'total_liquidity': total_liq, 'distance_from_price_pct': (start - mid) / mid, 'probability': 0.89, 'expected_speed': 'FAST' if count >= 3 else 'MODERATE'})
                i = j
            else:
                i += 1
        bid_levels = sorted(bids.keys(), reverse=True)
        min_scan = mid * (1 - scan_range_pct)
        i = 0
        while i < len(bid_levels) and bid_levels[i] >= min_scan:
            start = bid_levels[i]
            count = 0
            total_liq = 0.0
            j = i
            while j < min(i + 10, len(bid_levels)):
                if bid_levels[j] < min_scan:
                    break
                window_liq = sum((bids.get(bid_levels[k], 0) * bid_levels[k] for k in range(j, min(j + 3, len(bid_levels)))))
                if window_liq < self.vacuum_threshold * self.avg_bid_liquidity:
                    count += 1
                    total_liq += window_liq
                else:
                    break
                j += 1
            if count >= 2:
                end = bid_levels[min(j - 1, len(bid_levels) - 1)]
                vacuums.append({'zone': 'BID_VACUUM', 'direction': 'DOWNWARD', 'start_price': start, 'end_price': end, 'levels': count, 'total_liquidity': total_liq, 'distance_from_price_pct': (mid - start) / mid, 'probability': 0.89, 'expected_speed': 'FAST' if count >= 3 else 'MODERATE'})
                i = j
            else:
                i += 1
        self.vacuums = vacuums
        return vacuums

    def detect_walls(self, bids: SortedDict, asks: SortedDict, wall_multiplier: float=10.0, min_notional: float=50000.0) -> List[Dict]:
        if not bids or not asks:
            return []
        avg_bid = sum(bids.values()) / len(bids)
        avg_ask = sum(asks.values()) / len(asks)
        walls: List[Dict] = []
        for price, qty in bids.items():
            notional = price * qty
            if qty > wall_multiplier * avg_bid and notional > min_notional:
                walls.append({'type': 'BID_WALL', 'price': price, 'quantity': qty, 'notional': notional, 'multiplier': qty / avg_bid})
        for price, qty in asks.items():
            notional = price * qty
            if qty > wall_multiplier * avg_ask and notional > min_notional:
                walls.append({'type': 'ASK_WALL', 'price': price, 'quantity': qty, 'notional': notional, 'multiplier': qty / avg_ask})
        self.walls = walls
        return walls