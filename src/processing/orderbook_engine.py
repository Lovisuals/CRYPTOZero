"""
Core module for OrderBookEngine
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

class OrderBookEngine:

    def __init__(self, symbol: str, depth: int=500):
        self.symbol = symbol.upper()
        self.depth = depth
        self.bids = SortedDict()
        self.asks = SortedDict()
        self.last_update_id = 0
        self.timestamp = 0
        self._buffer = []
        self._synced = False
        self._session = None
        self.gap_count = 0
        self.update_count = 0
        self.last_rest_check = 0

    async def ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

    async def fetch_snapshot(self) -> bool:
        await self.ensure_session()
        url = 'https://api1.binance.com/api/v3/depth'
        params = {'symbol': self.symbol, 'limit': min(self.depth, 1000)}
        for attempt in range(3):
            try:
                timeout = aiohttp.ClientTimeout(total=5)
                async with self._session.get(url, params=params, timeout=timeout) as resp:
                    if resp.status != 200:
                        logger.error(f'[{self.symbol}] Snapshot fetch failed (HTTP {resp.status})')
                        await asyncio.sleep(2 ** attempt)
                        continue
                    data = await resp.json()
                    self.bids.clear()
                    self.asks.clear()
                    for price_str, qty_str in data['bids']:
                        price, qty = (float(price_str), float(qty_str))
                        if qty > 0:
                            self.bids[price] = qty
                    for price_str, qty_str in data['asks']:
                        price, qty = (float(price_str), float(qty_str))
                        if qty > 0:
                            self.asks[price] = qty
                    self.last_update_id = data['lastUpdateId']
                    self.timestamp = int(time.time() * 1000)
                    self._synced = True
                    logger.info(f'[{self.symbol}] Snapshot loaded: bids={len(self.bids)}, asks={len(self.asks)}, last_update_id={self.last_update_id}')
                    await self.apply_buffered_updates()
                    return True
            except Exception as e:
                logger.error(f'[{self.symbol}] Snapshot fetch error (attempt {attempt + 1}): {e}')
                await asyncio.sleep(2 ** attempt)
        return False

    async def apply_buffered_updates(self):
        if not self._buffer:
            return
        self._buffer.sort(key=lambda x: x['U'])
        first_valid_found = False
        for update in self._buffer:
            U = update['U']
            u = update['u']
            if u <= self.last_update_id:
                continue
            if not first_valid_found:
                if U <= self.last_update_id + 1 <= u:
                    first_valid_found = True
                    logger.debug(f'[{self.symbol}] First valid buffered event: U={U}, u={u}')
                else:
                    logger.warning(f'[{self.symbol}] Buffered event out of sequence after snapshot (U={U}, u={u}, snapshot_id={self.last_update_id}) — skipping')
                    continue
            self.apply_update(update)
        self._buffer.clear()
        logger.info(f'[{self.symbol}] Buffered update application complete')

    def apply_update(self, update: dict) -> bool:
        U = update['U']
        u = update['u']
        if not self._synced:
            self._buffer.append(update)
            return False
        if u <= self.last_update_id:
            return False
        if U > self.last_update_id + 1:
            logger.warning(f'[{self.symbol}] ORDERBOOK GAP: U={U}, local={self.last_update_id}, gap={U - self.last_update_id - 1} updates missed')
            self.gap_count += 1
            asyncio.create_task(self.recover_from_gap())
            return False
        for price_str, qty_str in update.get('b', []):
            price, qty = (float(price_str), float(qty_str))
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for price_str, qty_str in update.get('a', []):
            price, qty = (float(price_str), float(qty_str))
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty
        self.last_update_id = u
        self.timestamp = update.get('E', int(time.time() * 1000))
        self.update_count += 1
        if self.update_count % 100 == 0:
            self._prune()
        if time.time() - self.last_rest_check > 60:
            asyncio.create_task(self.validate_against_rest())
        return True

    async def recover_from_gap(self):
        logger.info(f'[{self.symbol}] Starting gap recovery...')
        self._synced = False
        success = await self.fetch_snapshot()
        if success:
            logger.info(f'[{self.symbol}] Gap recovery complete. gap_count={self.gap_count}')
        else:
            logger.error(f'[{self.symbol}] Gap recovery FAILED — will retry on next update')

    async def validate_against_rest(self):
        self.last_rest_check = time.time()
        await self.ensure_session()
        url = 'https://api1.binance.com/api/v3/depth'
        params = {'symbol': self.symbol, 'limit': 10}
        try:
            timeout = aiohttp.ClientTimeout(total=3)
            async with self._session.get(url, params=params, timeout=timeout) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
                if not self.bids or not self.asks:
                    return
                rest_top_bid = float(data['bids'][0][0]) if data['bids'] else 0
                rest_top_ask = float(data['asks'][0][0]) if data['asks'] else 0
                local_top_bid = self.bids.peekitem(-1)[0]
                local_top_ask = self.asks.peekitem(0)[0]
                bid_diff = abs(local_top_bid - rest_top_bid) / (rest_top_bid + 1e-09)
                ask_diff = abs(local_top_ask - rest_top_ask) / (rest_top_ask + 1e-09)
                DRIFT_THRESHOLD = 0.0001
                if bid_diff > DRIFT_THRESHOLD or ask_diff > DRIFT_THRESHOLD:
                    logger.warning(f'[{self.symbol}] Drift detected: bid_diff={bid_diff:.5%}, ask_diff={ask_diff:.5%}. Resyncing...')
                    await self.recover_from_gap()
        except Exception as e:
            logger.debug(f'[{self.symbol}] REST validation error: {e}')

    def _prune(self):
        while len(self.bids) > self.depth:
            self.bids.popitem(index=0)
        while len(self.asks) > self.depth:
            self.asks.popitem(index=-1)
        if not self.bids or not self.asks:
            return
        min_notional = 1000.0
        stale_bids = [p for p, q in self.bids.items() if p * q < min_notional]
        for p in stale_bids:
            self.bids.pop(p, None)
        stale_asks = [p for p, q in self.asks.items() if p * q < min_notional]
        for p in stale_asks:
            self.asks.pop(p, None)

    def get_top_liquidity(self, levels: int=10) -> dict:
        bid_liquidity = sum((p * q for p, q in list(self.bids.items())[-levels:])) if self.bids else 0.0
        ask_liquidity = sum((p * q for p, q in list(self.asks.items())[:levels])) if self.asks else 0.0
        return {'bid_liquidity': bid_liquidity, 'ask_liquidity': ask_liquidity, 'imbalance_ratio': bid_liquidity / ask_liquidity if ask_liquidity > 0 else 0.0}

    def get_spread_bps(self) -> int:
        if not self.bids or not self.asks:
            return 0
        best_bid = self.bids.peekitem(-1)[0]
        best_ask = self.asks.peekitem(0)[0]
        mid = (best_bid + best_ask) / 2
        return int((best_ask - best_bid) / mid * 10000)

    def get_mid_price(self) -> float:
        if not self.bids or not self.asks:
            return 0.0
        return (self.bids.peekitem(-1)[0] + self.asks.peekitem(0)[0]) / 2

    async def close(self):
        if self._session and (not self._session.closed):
            await self._session.close()