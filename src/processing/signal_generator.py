from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Callable, Dict, List, Optional

import pandas as pd

from src.intelligence.absorption_detector import AbsorptionDetector
from src.intelligence.apex_fusion import ApexSignalFusion
from src.intelligence.iceberg_detector import IcebergDetector
from src.intelligence.liquidation_analyzer import LiquidationHeatMapAnalyzer
from src.intelligence.liquidity_analyzer import LiquidityAnalyzer
from src.intelligence.social_intelligence import SocialIntelligenceEngine
from src.intelligence.time_price_oracle import TimePriceDeliveryOracle
from src.processing.orderbook_engine import OrderBookEngine
from src.processing.volume_analyzer import VolumeAnalyzer

logger = logging.getLogger(__name__)


class SignalGenerator:
    COOLDOWN = 300

    def __init__(
        self,
        symbols: List[str],
        on_signal: Optional[Callable] = None,
        orderbook_depth: int = 500,
        imbalance_buy: float = 3.0,
        imbalance_sell: float = 0.33,
        absorption_multiplier: int = 50,
        vacuum_min_levels: int = 3,
        iceberg_ratio: float = 5.0,
        iceberg_persist_s: float = 30.0,
        executor=None,
        auto_trade_enabled: bool = False,
        apex_enabled: bool = False,
        apex_poll_seconds: int = 30,
        apex_min_confidence: int = 85,
        coinglass_api_key: Optional[str] = None,
        twitter_bearer_token: Optional[str] = None,
        reddit_client_id: Optional[str] = None,
        reddit_client_secret: Optional[str] = None,
        whale_alert_api_key: Optional[str] = None,
    ):
        self.executor = executor
        self.auto_trade_enabled = auto_trade_enabled
        self.symbols = [s.upper() for s in symbols]
        self.on_signal = on_signal
        self.orderbooks: Dict[str, OrderBookEngine] = {
            s: OrderBookEngine(s, depth=orderbook_depth) for s in self.symbols
        }
        self.volume_analyzers: Dict[str, VolumeAnalyzer] = {s: VolumeAnalyzer(lookback=1000) for s in self.symbols}
        self.absorption_detectors: Dict[str, AbsorptionDetector] = {
            s: AbsorptionDetector(
                volume_multiplier=absorption_multiplier,
                persistence_periods=6,
                price_threshold=0.001,
            )
            for s in self.symbols
        }
        self.liquidity_analyzers: Dict[str, LiquidityAnalyzer] = {
            s: LiquidityAnalyzer(vacuum_threshold=0.1) for s in self.symbols
        }
        self.iceberg_detectors: Dict[str, IcebergDetector] = {
            s: IcebergDetector(
                executed_ratio=iceberg_ratio,
                min_persist_seconds=iceberg_persist_s,
                window_seconds=60.0,
            )
            for s in self.symbols
        }

        self._imbalance_buy = imbalance_buy
        self._imbalance_sell = imbalance_sell
        self._vacuum_min = vacuum_min_levels
        self._cooldowns: Dict[tuple, float] = {}
        self._price_series: Dict[str, list] = {s: [] for s in self.symbols}
        self._liq_calibrated: Dict[str, float] = {s: 0.0 for s in self.symbols}
        self._trade_bufs: Dict[str, list] = {s: [] for s in self.symbols}
        self._recent_signals: Dict[str, Dict[str, Dict]] = {s: {} for s in self.symbols}
        self.start_time: float = time.time()
        self.signal_count: int = 0

        self.apex_enabled = apex_enabled
        self._apex_poll_seconds = max(10, int(apex_poll_seconds))
        self._apex_min_confidence = apex_min_confidence
        self._apex_recent_ttl = 120
        self._apex_task: Optional[asyncio.Task] = None
        self._apex_busy = False
        self._coinglass_api_key = coinglass_api_key or os.getenv("COINGLASS_API_KEY")

        self.liquidation_analyzers: Dict[str, LiquidationHeatMapAnalyzer] = {}
        self.time_oracle: Optional[TimePriceDeliveryOracle] = None
        self.social_engine: Optional[SocialIntelligenceEngine] = None
        self.apex_fusion: Optional[ApexSignalFusion] = None
        if self.apex_enabled:
            self._init_apex_layers(
                twitter_bearer_token=twitter_bearer_token,
                reddit_client_id=reddit_client_id,
                reddit_client_secret=reddit_client_secret,
                whale_alert_api_key=whale_alert_api_key,
            )

    def _init_apex_layers(
        self,
        twitter_bearer_token: Optional[str],
        reddit_client_id: Optional[str],
        reddit_client_secret: Optional[str],
        whale_alert_api_key: Optional[str],
    ):
        self.liquidation_analyzers = {
            s: LiquidationHeatMapAnalyzer(coinglass_api_key=self._coinglass_api_key) for s in self.symbols
        }
        self.time_oracle = TimePriceDeliveryOracle()
        self.social_engine = SocialIntelligenceEngine(
            twitter_bearer_token=twitter_bearer_token,
            reddit_client_id=reddit_client_id,
            reddit_client_secret=reddit_client_secret,
            whale_alert_api_key=whale_alert_api_key,
        )
        self.apex_fusion = ApexSignalFusion()

    async def initialize(self):
        if os.getenv("REPLAY_MODE", "false").lower() == "true":
            logger.info("Replay mode: Skipping order book snapshot fetching")
        else:
            logger.info("Fetching order book snapshots for: %s", self.symbols)
            results = await asyncio.gather(
                *[self.orderbooks[s].fetch_snapshot() for s in self.symbols],
                return_exceptions=True,
            )
            for symbol, result in zip(self.symbols, results):
                if isinstance(result, Exception) or not result:
                    logger.warning("Snapshot failed for %s: %s", symbol, result)
                else:
                    logger.info("%s order book ready", symbol)

        if self.apex_enabled and self._apex_task is None:
            self._apex_task = asyncio.create_task(self._apex_loop())
            logger.info("APEX loop started (%ss cadence)", self._apex_poll_seconds)

    async def close(self):
        if self._apex_task:
            self._apex_task.cancel()
            try:
                await self._apex_task
            except asyncio.CancelledError:
                pass

        for analyzer in self.liquidation_analyzers.values():
            await analyzer.close()
        if self.time_oracle:
            await self.time_oracle.close()
        if self.social_engine:
            await self.social_engine.close()

        for ob in self.orderbooks.values():
            await ob.close()

    async def on_depth_update(self, symbol: str, data: dict):
        ob = self.orderbooks.get(symbol)
        if ob is None or not ob.apply_update(data):
            return

        now = time.time()
        la = self.liquidity_analyzers[symbol]
        if now - self._liq_calibrated[symbol] > 60:
            la.update_avg_liquidity(ob.bids, ob.asks)
            self._liq_calibrated[symbol] = now

        liq = ob.get_top_liquidity(10)
        ratio = liq["imbalance_ratio"]
        if ratio > self._imbalance_buy:
            await self._emit(
                symbol,
                "IMBALANCE",
                "BUY",
                70,
                {
                    "imbalance_ratio": ratio,
                    "bid_liquidity": liq["bid_liquidity"],
                    "ask_liquidity": liq["ask_liquidity"],
                    "spread_bps": ob.get_spread_bps(),
                    "mid_price": ob.get_mid_price(),
                },
            )
        elif 0 < ratio < self._imbalance_sell:
            await self._emit(
                symbol,
                "IMBALANCE",
                "SELL",
                70,
                {
                    "imbalance_ratio": ratio,
                    "bid_liquidity": liq["bid_liquidity"],
                    "ask_liquidity": liq["ask_liquidity"],
                    "spread_bps": ob.get_spread_bps(),
                    "mid_price": ob.get_mid_price(),
                },
            )

        for vac in la.detect_stacked_vacuums(ob.bids, ob.asks):
            if vac["levels"] >= self._vacuum_min:
                direction = "BUY" if vac["direction"] == "UPWARD" else "SELL"
                await self._emit(
                    symbol,
                    "VACUUM",
                    direction,
                    86,
                    {
                        "zone_start": vac["start_price"],
                        "zone_end": vac["end_price"],
                        "levels": vac["levels"],
                        "speed": vac["expected_speed"],
                        "distance_pct": vac["distance_from_price_pct"],
                    },
                )

        for ic in self.iceberg_detectors[symbol].on_book_update(ob.bids, ob.asks):
            await self._emit(
                symbol,
                "ICEBERG",
                ic["signal"],
                ic["confidence"],
                {
                    "price": ic["price"],
                    "volume_ratio": ic["volume_ratio"],
                    "duration_s": ic["duration_seconds"],
                    "interpretation": ic["interpretation"],
                },
            )

    async def on_trade_update(self, symbol: str, data: dict):
        va = self.volume_analyzers.get(symbol)
        ob = self.orderbooks.get(symbol)
        if va is None or ob is None:
            return

        price = float(data["p"])
        qty = float(data["q"])
        side = "SELL" if data.get("m") else "BUY"
        va.process_agg_trade(data)

        buf = self._trade_bufs[symbol]
        buf.append({"price": price, "qty": qty, "ts": data.get("E", 0)})
        if len(buf) > 10000:
            self._trade_bufs[symbol] = buf[-10000:]

        self._price_series[symbol].append(price)
        if len(self._price_series[symbol]) > 1000:
            self._price_series[symbol] = self._price_series[symbol][-1000:]

        self.iceberg_detectors[symbol].on_trade(price, qty, side, ob.bids, ob.asks)
        series = self._price_series[symbol]

        if len(series) >= 60:
            price_60s_ago = series[-60]
            change_pct = abs(price - price_60s_ago) / (price_60s_ago + 1e-9)
            now_ts = data.get("E", 0)
            cutoff = now_ts - 60000
            recent_vol = sum((t["qty"] for t in buf if t["ts"] >= cutoff))
            result = self.absorption_detectors[symbol].update(
                volume_1m=recent_vol,
                price_change_pct=change_pct,
                current_cvd=va.cvd,
            )
            if result:
                direction = "BUY" if result["direction"] == "BULLISH" else "SELL"
                await self._emit(
                    symbol,
                    "ABSORPTION",
                    direction,
                    result["confidence"],
                    {
                        "volume_multiplier": result["volume_multiplier"],
                        "duration_s": result["duration_seconds"],
                        "cvd_stability": result["cvd_stability"],
                    },
                )

        if len(series) >= 20:
            div = va.get_cvd_divergence(pd.Series(series[-100:]), periods=20)
            if div["signal"] == "BULLISH_DIVERGENCE":
                await self._emit(
                    symbol,
                    "CVD",
                    "BUY",
                    78,
                    {
                        "cvd": va.cvd,
                        "strength": div["strength"],
                        "cvd_slope": div["cvd_slope"],
                        "price_slope": div["price_slope"],
                    },
                )
            elif div["signal"] == "BEARISH_DIVERGENCE":
                await self._emit(
                    symbol,
                    "CVD",
                    "SELL",
                    78,
                    {
                        "cvd": va.cvd,
                        "strength": div["strength"],
                        "cvd_slope": div["cvd_slope"],
                        "price_slope": div["price_slope"],
                    },
                )

    async def _apex_loop(self):
        while True:
            cycle_start = time.time()
            if self._apex_busy:
                await asyncio.sleep(1)
                continue
            self._apex_busy = True
            try:
                for symbol in self.symbols:
                    await self._run_apex_for_symbol(symbol)
            except Exception as exc:
                logger.error("APEX loop failure: %s", exc, exc_info=True)
            finally:
                self._apex_busy = False

            elapsed = time.time() - cycle_start
            sleep_s = max(1.0, self._apex_poll_seconds - elapsed)
            await asyncio.sleep(sleep_s)

    async def _run_apex_for_symbol(self, symbol: str):
        if not self.apex_fusion:
            return
        ob = self.orderbooks[symbol]
        current_price = ob.get_mid_price()
        if current_price <= 0:
            return

        all_signals = self._collect_recent_signals(symbol)

        liquidation_signal = await self._build_liquidation_signal(symbol, current_price)
        if liquidation_signal:
            all_signals["LIQUIDATION"] = liquidation_signal
            await self._emit(
                symbol,
                "LIQUIDATION",
                liquidation_signal["direction"],
                liquidation_signal["confidence"],
                {
                    "liquidity_usd": liquidation_signal.get("liquidity_usd"),
                    "cluster_price": liquidation_signal.get("cluster_price"),
                    "entry_price": liquidation_signal.get("entry_price"),
                    "stop_loss": liquidation_signal.get("stop_loss"),
                    "take_profit": liquidation_signal.get("take_profit"),
                    "rationale": liquidation_signal.get("rationale", ""),
                    "source": liquidation_signal.get("source", "UNKNOWN"),
                },
            )

        time_signal = await self._build_time_signal(symbol)
        if time_signal:
            all_signals["TIME_DELIVERY"] = time_signal
            await self._emit(
                symbol,
                "TIME_DELIVERY",
                time_signal["direction"],
                time_signal["confidence"],
                {
                    "bullish_count": time_signal.get("bullish_count"),
                    "bearish_count": time_signal.get("bearish_count"),
                    "signal": time_signal.get("signal"),
                },
            )

        social_signal = await self._build_social_signal(symbol)
        if social_signal:
            all_signals["SOCIAL"] = social_signal
            await self._emit(
                symbol,
                "SOCIAL",
                social_signal["direction"],
                social_signal["confidence"],
                {
                    "sentiment_score": social_signal.get("sentiment_score"),
                    "buzz_multiplier": social_signal.get("buzz_multiplier"),
                    "rationale": social_signal.get("rationale", ""),
                },
            )

        apex_signal = self.apex_fusion.fuse_signals(all_signals, min_agreeing=3)
        if apex_signal and apex_signal["confidence"] >= self._apex_min_confidence:
            await self._emit(
                symbol,
                "APEX_FUSION",
                apex_signal["direction"],
                apex_signal["confidence"],
                {
                    "supporting_signals": apex_signal.get("supporting_signals", []),
                    "signal_count": apex_signal.get("signal_count", 0),
                    "position_multiplier": apex_signal.get("position_multiplier", 0.01),
                    "leverage_recommended": apex_signal.get("leverage_recommended", 1),
                    "rationale": apex_signal.get("rationale", ""),
                },
            )

    def _collect_recent_signals(self, symbol: str) -> Dict[str, Dict]:
        now = time.time()
        usable: Dict[str, Dict] = {}
        for signal_type, signal in self._recent_signals.get(symbol, {}).items():
            age = now - signal.get("_stored_at", now)
            if age > self._apex_recent_ttl:
                continue
            if signal_type not in {"IMBALANCE", "CVD", "ABSORPTION", "ICEBERG", "VACUUM"}:
                continue
            usable[signal_type] = {
                "signal_type": signal_type,
                "direction": signal.get("direction"),
                "confidence": signal.get("confidence", 0),
            }
        return usable

    async def _build_liquidation_signal(self, symbol: str, current_price: float) -> Optional[Dict]:
        analyzer = self.liquidation_analyzers.get(symbol)
        if not analyzer:
            return None
        clusters = await analyzer.fetch_liquidation_data(symbol, current_price=current_price)
        return analyzer.generate_liquidation_signal(clusters, current_price)

    async def _build_time_signal(self, symbol: str) -> Optional[Dict]:
        if not self.time_oracle:
            return None
        await self.time_oracle.fetch_multi_timeframe_data(symbol)
        confluence = self.time_oracle.get_multi_timeframe_confluence()
        if confluence.get("signal") == "NEUTRAL" or confluence.get("confidence", 0) < 75:
            return None
        return confluence

    async def _build_social_signal(self, symbol: str) -> Optional[Dict]:
        if not self.social_engine:
            return None
        base_asset = self._base_asset(symbol)
        twitter_task = self.social_engine.fetch_twitter_sentiment(base_asset)
        reddit_task = self.social_engine.fetch_reddit_sentiment(base_asset)
        whale_task = self.social_engine.track_whale_wallets(base_asset)
        twitter_data, reddit_data, whale_data = await asyncio.gather(
            twitter_task, reddit_task, whale_task, return_exceptions=False
        )
        return self.social_engine.generate_social_signal(twitter_data, reddit_data, whale_data)

    @staticmethod
    def _base_asset(symbol: str) -> str:
        normalized = symbol.upper().replace("/", "")
        for suffix in ("USDT", "FDUSD", "BUSD", "USDC", "USD"):
            if normalized.endswith(suffix):
                return normalized[: -len(suffix)]
        return normalized

    async def _emit(self, symbol: str, signal_type: str, direction: str, confidence: float, meta: dict):
        key = (symbol, signal_type)
        now = time.time()
        if now - self._cooldowns.get(key, 0) < self.COOLDOWN:
            return
        self._cooldowns[key] = now
        self.signal_count += 1

        payload = {
            "symbol": symbol,
            "type": signal_type,
            "signal_type": signal_type,
            "direction": direction,
            "confidence": confidence,
            "timestamp": int(now * 1000),
            **meta,
        }
        self._recent_signals[symbol][signal_type] = {**payload, "_stored_at": now}

        logger.info("SIGNAL | %s | %s | %s | conf=%.1f%%", symbol, signal_type, direction, confidence)
        if self.on_signal:
            try:
                await self.on_signal(payload)
            except Exception as exc:
                logger.error("Signal callback error: %s", exc, exc_info=True)

        if getattr(self, "auto_trade_enabled", False) and self.executor:
            if signal_type == "APEX_FUSION" and hasattr(self.executor, "execute_apex_signal"):
                asyncio.create_task(self.executor.execute_apex_signal(payload))
            elif (not self.apex_enabled) and confidence >= 70 and hasattr(self.executor, "execute_signal"):
                asyncio.create_task(self.executor.execute_signal(payload))

    def health(self) -> dict:
        return {
            s: {
                "synced": self.orderbooks[s]._synced,
                "gap_count": self.orderbooks[s].gap_count,
                "update_count": self.orderbooks[s].update_count,
                "bid_levels": len(self.orderbooks[s].bids),
                "ask_levels": len(self.orderbooks[s].asks),
                "spread_bps": self.orderbooks[s].get_spread_bps(),
                "cvd": self.volume_analyzers[s].cvd,
                "iceberg_active": len(self.iceberg_detectors[s].confirmed),
            }
            for s in self.symbols
        }

    def uptime_seconds(self) -> float:
        return time.time() - self.start_time
