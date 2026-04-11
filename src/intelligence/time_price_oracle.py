from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Deque, Dict, List, Optional

import aiohttp
import numpy as np

logger = logging.getLogger(__name__)


class TimePriceDeliveryOracle:
    """
    Multi-timeframe Wyckoff/fractal analyzer.
    """

    BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        timeframes: Optional[List[str]] = None,
        history_limit: int = 500,
    ):
        self._external_session = session
        self._session: Optional[aiohttp.ClientSession] = session
        self._session_lock = asyncio.Lock()
        self.timeframes = timeframes or ["1m", "5m", "15m", "1h", "4h", "1d"]
        self.history_limit = history_limit
        self.price_history: Dict[str, Deque[float]] = {
            tf: deque(maxlen=history_limit) for tf in self.timeframes
        }
        self.volume_history: Dict[str, Deque[float]] = {
            tf: deque(maxlen=history_limit) for tf in self.timeframes
        }
        self.last_fetch_ts: Dict[str, float] = {}

    async def close(self):
        if self._session and (not self._session.closed) and self._external_session is None:
            await self._session.close()

    async def _ensure_session(self):
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()

    async def fetch_multi_timeframe_data(self, symbol: str):
        await self._ensure_session()
        tasks = [self._fetch_klines(symbol, tf, limit=self.history_limit) for tf in self.timeframes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for tf, result in zip(self.timeframes, results):
            if isinstance(result, Exception) or result is None:
                continue
            self.price_history[tf].clear()
            self.volume_history[tf].clear()
            self.price_history[tf].extend(result["close"])
            self.volume_history[tf].extend(result["volume"])
            self.last_fetch_ts[tf] = time.time()

    async def _fetch_klines(self, symbol: str, timeframe: str, limit: int = 500) -> Optional[Dict]:
        assert self._session is not None
        normalized = symbol.upper().replace("/", "")
        params = {"symbol": normalized, "interval": timeframe, "limit": min(limit, 1000)}
        timeout = aiohttp.ClientTimeout(total=5)
        try:
            async with self._session.get(self.BINANCE_KLINES_URL, params=params, timeout=timeout) as resp:
                if resp.status != 200:
                    return None
                rows = await resp.json()
        except Exception as exc:
            logger.debug("Kline fetch failed for %s@%s: %s", symbol, timeframe, exc)
            return None

        closes = [float(r[4]) for r in rows]
        volumes = [float(r[5]) for r in rows]
        return {"close": closes, "volume": volumes}

    def detect_wyckoff_phase(self, timeframe: str) -> Dict:
        prices = list(self.price_history.get(timeframe, []))
        volumes = list(self.volume_history.get(timeframe, []))
        if len(prices) < 50 or len(volumes) < 50:
            return {"phase": "UNKNOWN", "confidence": 0, "timeframe": timeframe}

        volatility = float(np.std(prices[-20:]) / (np.mean(prices[-20:]) + 1e-9))
        volume_trend = float(np.polyfit(range(20), volumes[-20:], 1)[0])
        price_trend = float(np.polyfit(range(20), prices[-20:], 1)[0])
        swing_state = self._analyze_swing_structure(prices)

        if volatility < 0.01 and volume_trend < 0:
            phase, confidence, action = "ACCUMULATION", 75, "PREPARE_FOR_MARKUP"
        elif price_trend > 0 and volume_trend > 0 and swing_state == "HH_HL":
            phase, confidence, action = "MARKUP", 85, "RIDE_THE_TREND"
        elif volatility > 0.03 and volume_trend > 0:
            phase, confidence, action = "DISTRIBUTION", 80, "PREPARE_FOR_MARKDOWN"
        elif price_trend < 0 and volume_trend > 0 and swing_state == "LH_LL":
            phase, confidence, action = "MARKDOWN", 85, "RIDE_THE_TREND"
        else:
            phase, confidence, action = "TRANSITION", 50, "WAIT"

        return {
            "phase": phase,
            "timeframe": timeframe,
            "confidence": confidence,
            "action": action,
            "swing_structure": swing_state,
            "volatility_pct": volatility * 100.0,
            "volume_trend": "RISING" if volume_trend > 0 else "FALLING",
            "price_trend": price_trend,
        }

    def _analyze_swing_structure(self, prices: List[float]) -> str:
        highs: List[float] = []
        lows: List[float] = []
        if len(prices) < 20:
            return "RANGING"

        for i in range(5, len(prices) - 5):
            if all(prices[i] > prices[i - j] for j in range(1, 6)) and all(
                prices[i] > prices[i + j] for j in range(1, 6)
            ):
                highs.append(prices[i])
            if all(prices[i] < prices[i - j] for j in range(1, 6)) and all(
                prices[i] < prices[i + j] for j in range(1, 6)
            ):
                lows.append(prices[i])

        if len(highs) < 3 or len(lows) < 3:
            return "RANGING"

        recent_highs = highs[-3:]
        recent_lows = lows[-3:]
        highs_increasing = all(recent_highs[i] < recent_highs[i + 1] for i in range(2))
        lows_increasing = all(recent_lows[i] < recent_lows[i + 1] for i in range(2))

        if highs_increasing and lows_increasing:
            return "HH_HL"
        if (not highs_increasing) and (not lows_increasing):
            return "LH_LL"
        return "RANGING"

    def get_multi_timeframe_confluence(self) -> Dict:
        phases = {tf: self.detect_wyckoff_phase(tf) for tf in self.timeframes}

        bullish_count = sum(
            1 for p in phases.values() if p.get("phase") in {"ACCUMULATION", "MARKUP"}
        )
        bearish_count = sum(
            1 for p in phases.values() if p.get("phase") in {"DISTRIBUTION", "MARKDOWN"}
        )

        if bullish_count >= 4:
            signal = "STRONG_BUY"
            confidence = min(85 + (bullish_count - 4) * 5, 95)
            direction = "BUY"
        elif bearish_count >= 4:
            signal = "STRONG_SELL"
            confidence = min(85 + (bearish_count - 4) * 5, 95)
            direction = "SELL"
        else:
            signal = "NEUTRAL"
            confidence = 50
            direction = "NEUTRAL"

        return {
            "signal_type": "TIME_DELIVERY",
            "signal": signal,
            "direction": direction,
            "confidence": int(confidence),
            "timeframe_breakdown": phases,
            "bullish_count": bullish_count,
            "bearish_count": bearish_count,
        }
