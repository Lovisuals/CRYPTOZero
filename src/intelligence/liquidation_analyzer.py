from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from typing import Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class LiquidationHeatMapAnalyzer:
    """
    Estimates liquidation cluster zones using a hybrid of:
    - Binance futures mark/open-interest data
    - Optional Coinglass liquidation heat map API
    """

    BINANCE_PREMIUM_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
    BINANCE_OI_URL = "https://fapi.binance.com/fapi/v1/openInterest"
    COINGLASS_URL = "https://open-api.coinglass.com/public/v2/liquidation_map_chart"

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        coinglass_api_key: Optional[str] = None,
        min_cluster_usd: float = 10_000_000.0,
    ):
        self._external_session = session
        self._session: Optional[aiohttp.ClientSession] = session
        self._lock = asyncio.Lock()
        self.coinglass_api_key = coinglass_api_key
        self.min_cluster_usd = min_cluster_usd
        self.liquidation_clusters: Dict[str, List[Dict]] = {}
        self.funding_rate_history = deque(maxlen=200)
        self.open_interest_history = deque(maxlen=200)

    async def close(self):
        if self._session and (not self._session.closed) and self._external_session is None:
            await self._session.close()

    async def _ensure_session(self):
        async with self._lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()

    async def fetch_liquidation_data(self, symbol: str, current_price: Optional[float] = None) -> List[Dict]:
        await self._ensure_session()
        mark_data = await self._fetch_binance_mark_open_interest(symbol)
        if not mark_data:
            return []

        mark_price = current_price or mark_data["mark_price"]
        self.funding_rate_history.append(mark_data["funding_rate"])
        self.open_interest_history.append(mark_data["open_interest_usd"])

        clusters = self._estimate_clusters(mark_data, mark_price)
        cg_clusters = await self._fetch_coinglass_clusters(symbol, mark_price)
        if cg_clusters:
            clusters.extend(cg_clusters)

        deduped = self._merge_close_clusters(clusters, tolerance_pct=0.0015)
        prioritized = sorted(deduped, key=lambda x: x["liquidity_usd"], reverse=True)
        self.liquidation_clusters[symbol] = prioritized
        return prioritized

    async def _fetch_binance_mark_open_interest(self, symbol: str) -> Optional[Dict]:
        assert self._session is not None
        normalized_symbol = symbol.upper().replace("/", "")
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with self._session.get(
                self.BINANCE_PREMIUM_URL,
                params={"symbol": normalized_symbol},
                timeout=timeout,
            ) as premium_resp:
                if premium_resp.status != 200:
                    return None
                premium = await premium_resp.json()

            async with self._session.get(
                self.BINANCE_OI_URL,
                params={"symbol": normalized_symbol},
                timeout=timeout,
            ) as oi_resp:
                if oi_resp.status != 200:
                    return None
                oi = await oi_resp.json()

            mark_price = float(premium.get("markPrice", 0.0))
            funding_rate = float(premium.get("lastFundingRate", 0.0))
            oi_qty = float(oi.get("openInterest", 0.0))
            open_interest_usd = mark_price * oi_qty
            if mark_price <= 0:
                return None

            return {
                "mark_price": mark_price,
                "funding_rate": funding_rate,
                "open_interest_qty": oi_qty,
                "open_interest_usd": open_interest_usd,
            }
        except Exception as exc:
            logger.debug("Liquidation data fetch failed for %s: %s", symbol, exc)
            return None

    def _estimate_clusters(self, mark_data: Dict, current_price: float) -> List[Dict]:
        """
        Heuristic clusters when heat-map vendors are unavailable:
        we project stop pools around nearby leverage sweep distances.
        """
        open_interest_usd = mark_data["open_interest_usd"]
        funding_rate = mark_data["funding_rate"]
        if open_interest_usd <= 0 or current_price <= 0:
            return []

        distance_levels = [0.003, 0.005, 0.008, 0.012]
        distance_weights = [0.36, 0.32, 0.20, 0.12]

        funding_pressure = min(abs(funding_rate) / 0.0015, 1.0)
        volatility_boost = 1.0 + min(self._recent_oi_volatility(), 0.08)
        size_scalar = max(0.0015, min(0.004, 0.0015 + funding_pressure * 0.002))

        base_liq_budget = open_interest_usd * size_scalar * volatility_boost
        clusters: List[Dict] = []
        for dist, w in zip(distance_levels, distance_weights):
            long_price = current_price * (1.0 - dist)
            short_price = current_price * (1.0 + dist)
            long_liq = base_liq_budget * w
            short_liq = base_liq_budget * w

            if long_liq >= self.min_cluster_usd:
                data = {
                    "funding_rate": funding_rate,
                    "distance_pct": dist,
                    "atr_pct": 0.008 + self._recent_oi_volatility(),
                }
                clusters.append(
                    {
                        "type": "LONG_LIQUIDATION_ZONE",
                        "price": long_price,
                        "liquidity_usd": long_liq,
                        "distance_pct": dist,
                        "probability": self._calculate_hit_probability(long_liq, data),
                        "action": "EXPECT_WICK_DOWN_THEN_REVERSAL",
                        "confidence": 82,
                        "source": "BINANCE_HEURISTIC",
                    }
                )

            if short_liq >= self.min_cluster_usd:
                data = {
                    "funding_rate": funding_rate,
                    "distance_pct": dist,
                    "atr_pct": 0.008 + self._recent_oi_volatility(),
                }
                clusters.append(
                    {
                        "type": "SHORT_LIQUIDATION_ZONE",
                        "price": short_price,
                        "liquidity_usd": short_liq,
                        "distance_pct": dist,
                        "probability": self._calculate_hit_probability(short_liq, data),
                        "action": "EXPECT_WICK_UP_THEN_REVERSAL",
                        "confidence": 82,
                        "source": "BINANCE_HEURISTIC",
                    }
                )

        return clusters

    async def _fetch_coinglass_clusters(self, symbol: str, current_price: float) -> List[Dict]:
        if not self.coinglass_api_key:
            return []

        assert self._session is not None
        normalized_symbol = symbol.upper().replace("USDT", "")
        headers = {"coinglassSecret": self.coinglass_api_key}
        params = {"symbol": normalized_symbol, "exchange": "Binance", "range": "24h"}

        try:
            timeout = aiohttp.ClientTimeout(total=6)
            async with self._session.get(
                self.COINGLASS_URL, params=params, headers=headers, timeout=timeout
            ) as resp:
                if resp.status != 200:
                    return []
                payload = await resp.json()
                data = payload.get("data", {})
        except Exception as exc:
            logger.debug("Coinglass fetch failed for %s: %s", symbol, exc)
            return []

        long_map = data.get("long", {})
        short_map = data.get("short", {})
        clusters: List[Dict] = []

        for price_str, amount in long_map.items():
            price = float(price_str)
            liq_usd = float(amount)
            if liq_usd < self.min_cluster_usd:
                continue
            dist = abs(current_price - price) / current_price
            clusters.append(
                {
                    "type": "LONG_LIQUIDATION_ZONE",
                    "price": price,
                    "liquidity_usd": liq_usd,
                    "distance_pct": dist,
                    "probability": self._calculate_hit_probability(liq_usd, {"distance_pct": dist}),
                    "action": "EXPECT_WICK_DOWN_THEN_REVERSAL",
                    "confidence": 86,
                    "source": "COINGLASS",
                }
            )

        for price_str, amount in short_map.items():
            price = float(price_str)
            liq_usd = float(amount)
            if liq_usd < self.min_cluster_usd:
                continue
            dist = abs(price - current_price) / current_price
            clusters.append(
                {
                    "type": "SHORT_LIQUIDATION_ZONE",
                    "price": price,
                    "liquidity_usd": liq_usd,
                    "distance_pct": dist,
                    "probability": self._calculate_hit_probability(liq_usd, {"distance_pct": dist}),
                    "action": "EXPECT_WICK_UP_THEN_REVERSAL",
                    "confidence": 86,
                    "source": "COINGLASS",
                }
            )

        return clusters

    def _merge_close_clusters(self, clusters: List[Dict], tolerance_pct: float = 0.001) -> List[Dict]:
        if not clusters:
            return []
        buckets: List[List[Dict]] = []
        for cluster in sorted(clusters, key=lambda c: c["price"]):
            inserted = False
            for bucket in buckets:
                anchor = bucket[0]["price"]
                if abs(cluster["price"] - anchor) / max(anchor, 1e-9) <= tolerance_pct:
                    bucket.append(cluster)
                    inserted = True
                    break
            if not inserted:
                buckets.append([cluster])

        merged: List[Dict] = []
        for bucket in buckets:
            dominant = max(bucket, key=lambda c: c["liquidity_usd"]).copy()
            dominant["liquidity_usd"] = sum(c["liquidity_usd"] for c in bucket)
            dominant["confidence"] = int(
                min(95, max(c.get("confidence", 80) for c in bucket) + math.log10(len(bucket) + 1) * 4)
            )
            dominant["probability"] = min(0.95, max(c.get("probability", 0.6) for c in bucket))
            merged.append(dominant)

        return merged

    def _recent_oi_volatility(self) -> float:
        values = list(self.open_interest_history)
        if len(values) < 10:
            return 0.01
        mean = sum(values) / len(values)
        if mean <= 0:
            return 0.01
        variance = sum((v - mean) ** 2 for v in values[-20:]) / min(len(values), 20)
        return math.sqrt(variance) / mean

    def _calculate_hit_probability(self, liq_usd: float, data: Dict) -> float:
        if liq_usd > 50_000_000:
            base_prob = 0.85
        elif liq_usd > 20_000_000:
            base_prob = 0.70
        else:
            base_prob = 0.55

        funding_rate = float(data.get("funding_rate", 0.0))
        if abs(funding_rate) > 0.001:
            base_prob += 0.10

        atr_pct = float(data.get("atr_pct", 0.02))
        distance_pct = abs(float(data.get("distance_pct", 0.0)))
        if distance_pct < atr_pct * 2:
            base_prob += 0.05

        return min(base_prob, 0.95)

    def generate_liquidation_signal(
        self, clusters: List[Dict], current_price: float, trigger_distance_pct: float = 0.005
    ) -> Optional[Dict]:
        if not clusters or current_price <= 0:
            return None

        nearest_cluster = min(clusters, key=lambda x: abs(x["price"] - current_price))
        distance = abs(nearest_cluster["price"] - current_price) / current_price
        if distance > trigger_distance_pct or nearest_cluster["probability"] <= 0.70:
            return None

        if nearest_cluster["type"] == "LONG_LIQUIDATION_ZONE":
            return {
                "signal_type": "LIQUIDATION",
                "direction": "BUY",
                "entry_price": nearest_cluster["price"] * 0.999,
                "stop_loss": nearest_cluster["price"] * 0.995,
                "take_profit": current_price * 1.005,
                "confidence": nearest_cluster["confidence"],
                "rationale": (
                    f"${nearest_cluster['liquidity_usd'] / 1e6:.1f}M long liquidations "
                    f"near ${nearest_cluster['price']:.2f}. Sweep-reversal setup."
                ),
                "leverage_multiplier": 3,
                "liquidity_usd": nearest_cluster["liquidity_usd"],
                "cluster_price": nearest_cluster["price"],
                "source": nearest_cluster.get("source", "UNKNOWN"),
                "timestamp": int(time.time() * 1000),
            }

        return {
            "signal_type": "LIQUIDATION",
            "direction": "SELL",
            "entry_price": nearest_cluster["price"] * 1.001,
            "stop_loss": nearest_cluster["price"] * 1.005,
            "take_profit": current_price * 0.995,
            "confidence": nearest_cluster["confidence"],
            "rationale": (
                f"${nearest_cluster['liquidity_usd'] / 1e6:.1f}M short liquidations "
                f"near ${nearest_cluster['price']:.2f}. Sweep-reversal setup."
            ),
            "leverage_multiplier": 3,
            "liquidity_usd": nearest_cluster["liquidity_usd"],
            "cluster_price": nearest_cluster["price"],
            "source": nearest_cluster.get("source", "UNKNOWN"),
            "timestamp": int(time.time() * 1000),
        }
