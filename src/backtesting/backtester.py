import asyncio
import json
import logging
import math
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncIterator, Optional
import numpy as np
from src.processing.orderbook_engine import OrderBookEngine
from src.processing.volume_analyzer import VolumeAnalyzer
from src.intelligence.absorption_detector import AbsorptionDetector
from src.intelligence.liquidity_analyzer import LiquidityAnalyzer
from src.intelligence.iceberg_detector import IcebergDetector
from src.strategy_rules import StrategyRules

logger = logging.getLogger(__name__)

_SHARPE_TAG = 'AUTORESEARCH_SHARPE='
_WINRATE_TAG = 'AUTORESEARCH_WINRATE='
_PF_TAG = 'AUTORESEARCH_PROFIT_FACTOR='
_SIGNALS_TAG = 'AUTORESEARCH_TOTAL_SIGNALS='

@dataclass
class SignalRecord:
    signal_type: str
    direction: str
    confidence: float
    entry_price: float
    signal_ts: float
    delivery_ts: float
    exit_price: Optional[float] = None
    outcome: Optional[str] = None
    pnl_pct: Optional[float] = None

@dataclass
class BacktestResults:
    total_signals: int = 0
    wins: int = 0
    losses: int = 0
    flats: int = 0
    gross_pnl_pct: float = 0.0
    signals: list = field(default_factory=list)
    type_stats: dict = field(default_factory=dict)

    @property
    def win_rate(self) -> float:
        denom = self.wins + self.losses
        return self.wins / denom if denom > 0 else 0.0

    @property
    def profit_factor(self) -> float:
        total_win = sum((s.pnl_pct for s in self.signals if s.pnl_pct and s.pnl_pct > 0))
        total_loss = sum((abs(s.pnl_pct) for s in self.signals if s.pnl_pct and s.pnl_pct < 0))
        return total_win / total_loss if total_loss > 0 else float('inf')

    @property
    def sharpe_ratio(self) -> float:
        returns = [s.pnl_pct for s in self.signals if s.pnl_pct is not None]
        if len(returns) < 2:
            return 0.0
        arr = np.array(returns)
        mean_ret = np.mean(arr)
        std_ret = np.std(arr, ddof=1)
        if std_ret < 1e-12:
            return 0.0
        raw_sharpe = mean_ret / std_ret
        annualization = math.sqrt(3650)
        return float(raw_sharpe * annualization)

    @property
    def max_drawdown_pct(self) -> float:
        if not self.signals:
            return 0.0
        cumulative = 0.0
        peak = 0.0
        max_dd = 0.0
        for s in self.signals:
            if s.pnl_pct is not None:
                cumulative += s.pnl_pct
                peak = max(peak, cumulative)
                dd = peak - cumulative
                max_dd = max(max_dd, dd)
        return max_dd

    @property
    def sortino_ratio(self) -> float:
        returns = [s.pnl_pct for s in self.signals if s.pnl_pct is not None]
        if len(returns) < 2:
            return 0.0
        arr = np.array(returns)
        mean_ret = np.mean(arr)
        downside = arr[arr < 0]
        if len(downside) < 1:
            return float('inf') if mean_ret > 0 else 0.0
        downside_std = np.std(downside, ddof=1)
        if downside_std < 1e-12:
            return 0.0
        raw = mean_ret / downside_std
        return float(raw * math.sqrt(3650))

class Backtester:
    def __init__(self, data_path: str, symbol: str='BTCUSDT'):
        R = StrategyRules
        self.data_path = Path(data_path)
        self.symbol = symbol.upper()
        self.forward_window_ms = R.FORWARD_WINDOW_MS
        self.min_latency_ms = R.MIN_LATENCY_MS
        self.max_latency_ms = R.MAX_LATENCY_MS
        self.fee_pct = R.FEE_PCT
        self.slippage_pct = R.SLIPPAGE_PCT
        self.gap_drop_prob = R.GAP_DROP_PROB
        self._imbalance_buy = R.IMBALANCE_BUY_THRESHOLD
        self._imbalance_sell = R.IMBALANCE_SELL_THRESHOLD
        self._imbalance_levels = R.IMBALANCE_LEVELS
        self._vacuum_min = R.VACUUM_MIN_LEVELS
        self._cvd_periods = R.CVD_DIVERGENCE_PERIODS
        self._cvd_norm_thresh = R.CVD_NORM_THRESHOLD
        self._cvd_price_thresh = R.CVD_PRICE_NORM_THRESHOLD
        self._require_cvd_for_imbalance = R.REQUIRE_CVD_CONFLUENCE_FOR_IMBALANCE
        self._cvd_confluence_slope = R.CVD_CONFLUENCE_MIN_SLOPE
        self._require_imbalance_for_vacuum = R.REQUIRE_IMBALANCE_CONFLUENCE_FOR_VACUUM
        self._vacuum_imbalance_ratio = R.VACUUM_IMBALANCE_MIN_RATIO
        self._weights = {'IMBALANCE': R.WEIGHT_IMBALANCE, 'CVD': R.WEIGHT_CVD, 'ABSORPTION': R.WEIGHT_ABSORPTION, 'VACUUM': R.WEIGHT_VACUUM, 'ICEBERG': R.WEIGHT_ICEBERG}
        self._cooldown_s = R.SIGNAL_COOLDOWN_SECONDS
        self._cooldowns: dict[tuple, float] = {}
        self._orderbook = OrderBookEngine(symbol)
        self._volume = VolumeAnalyzer(lookback=1000)
        self._absorption = AbsorptionDetector(volume_multiplier=R.ABSORPTION_VOLUME_MULTIPLIER, persistence_periods=R.ABSORPTION_PERSISTENCE_PERIODS, price_threshold=R.ABSORPTION_PRICE_THRESHOLD)
        self._liquidity = LiquidityAnalyzer(vacuum_threshold=R.VACUUM_THRESHOLD)
        self._iceberg = IcebergDetector(executed_ratio=R.ICEBERG_EXECUTED_RATIO, min_persist_seconds=R.ICEBERG_MIN_PERSIST_SECONDS, window_seconds=R.ICEBERG_WINDOW_SECONDS, min_notional=R.ICEBERG_MIN_NOTIONAL)
        self._price_log: list[tuple[int, float]] = []
        self._price_series: list[float] = []
        self._pending: list[SignalRecord] = []
        self._trade_buf: list[dict] = []
        self._sim_clock: int = 0

    async def run(self) -> BacktestResults:
        results = BacktestResults()
        logger.info(f'[Backtester] Starting replay: {self.data_path} | latency={self.min_latency_ms}-{self.max_latency_ms}ms | gap_drop_prob={self.gap_drop_prob:.1%}')
        self._orderbook._synced = True
        self._orderbook.last_update_id = 0
        async for event in self._replay_stream():
            await self._process_event(event, results)
            self._settle_pending(results)
        self._force_settle_all(results)
        logger.info(f'[Backtester] Complete — signals={results.total_signals}, win_rate={results.win_rate:.1%}, sharpe={results.sharpe_ratio:.2f}, profit_factor={results.profit_factor:.2f}')
        return results

    async def _process_event(self, event: dict, results: BacktestResults):
        evt_type = event.get('type')
        ts = event.get('ts', event.get('E', 0))
        self._sim_clock = ts
        if evt_type == 'depth':
            await self._handle_depth(event, results)
        elif evt_type == 'trade':
            self._handle_trade(event, results)

    async def _handle_depth(self, event: dict, results: BacktestResults):
        if self.gap_drop_prob > 0 and random.random() < self.gap_drop_prob:
            logger.debug(f"[Backtester] Simulated packet drop at U={event.get('U')}")
            return
        self._orderbook.apply_update(event)
        latency_ms = random.randint(self.min_latency_ms, self.max_latency_ms)
        effective_ts = self._sim_clock + latency_ms
        if not self._orderbook._synced or not self._orderbook.bids or (not self._orderbook.asks):
            return
        mid = self._orderbook.get_mid_price()
        liquidity = self._orderbook.get_top_liquidity(self._imbalance_levels)
        ratio = liquidity['imbalance_ratio']
        cvd_ok_buy = True
        cvd_ok_sell = True
        if self._require_cvd_for_imbalance and len(self._volume.cvd_history) >= 5:
            recent_cvd_slope = list(self._volume.cvd_history)[-1] - list(self._volume.cvd_history)[-5]
            cvd_ok_buy = recent_cvd_slope > self._cvd_confluence_slope
            cvd_ok_sell = recent_cvd_slope < -self._cvd_confluence_slope
        if ratio > self._imbalance_buy and cvd_ok_buy:
            self._emit_signal(results, 'IMBALANCE', 'BUY', 70, mid, effective_ts)
        elif 0 < ratio < self._imbalance_sell and cvd_ok_sell:
            self._emit_signal(results, 'IMBALANCE', 'SELL', 70, mid, effective_ts)
        self._liquidity.update_avg_liquidity(self._orderbook.bids, self._orderbook.asks)
        vacuums = self._liquidity.detect_stacked_vacuums(self._orderbook.bids, self._orderbook.asks, scan_range_pct=StrategyRules.VACUUM_SCAN_RANGE_PCT)
        for v in vacuums:
            if v['levels'] >= self._vacuum_min:
                direction = 'BUY' if v['direction'] == 'UPWARD' else 'SELL'
                if self._require_imbalance_for_vacuum:
                    if direction == 'BUY' and ratio < self._vacuum_imbalance_ratio:
                        continue
                    if direction == 'SELL' and ratio > 1 / self._vacuum_imbalance_ratio:
                        continue
                self._emit_signal(results, 'VACUUM', direction, 86, mid, effective_ts)
        iceberg_signals = self._iceberg.on_book_update(self._orderbook.bids, self._orderbook.asks)
        for ic in iceberg_signals:
            self._emit_signal(results, 'ICEBERG', ic['signal'], ic['confidence'], mid, effective_ts)

    def _handle_trade(self, event: dict, results: BacktestResults):
        price = float(event['p'])
        qty = float(event['q'])
        side = 'SELL' if event.get('m') else 'BUY'
        self._price_log.append((self._sim_clock, price))
        self._price_series.append(price)
        if len(self._price_series) > 1000:
            self._price_series = self._price_series[-1000:]
        self._trade_buf.append({'price': price, 'qty': qty, 'ts': self._sim_clock})
        if len(self._trade_buf) > 10000:
            self._trade_buf = self._trade_buf[-10000:]
        self._volume.process_agg_trade(event)
        self._iceberg.on_trade(price, qty, side, self._orderbook.bids, self._orderbook.asks)
        latency_ms = random.randint(self.min_latency_ms, self.max_latency_ms)
        effective_ts = self._sim_clock + latency_ms
        if self._orderbook.bids and self._orderbook.asks:
            import pandas as pd
            if len(self._price_series) >= self._cvd_periods:
                prices = pd.Series(self._price_series[-100:])
                divergence = self._volume.get_cvd_divergence(prices, periods=self._cvd_periods)
                if divergence['signal'] == 'BULLISH_DIVERGENCE':
                    self._emit_signal(results, 'CVD', 'BUY', 78, price, effective_ts)
                elif divergence['signal'] == 'BEARISH_DIVERGENCE':
                    self._emit_signal(results, 'CVD', 'SELL', 78, price, effective_ts)
            if len(self._price_series) >= 60:
                price_60_ago = self._price_series[-60]
                change_pct = abs(price - price_60_ago) / (price_60_ago + 1e-09)
                cutoff = self._sim_clock - 60000
                recent_vol = sum((t['qty'] for t in self._trade_buf if t['ts'] >= cutoff))
                result = self._absorption.update(volume_1m=recent_vol, price_change_pct=change_pct, current_cvd=self._volume.cvd)
                if result:
                    direction = 'BUY' if result['direction'] == 'BULLISH' else 'SELL'
                    self._emit_signal(results, 'ABSORPTION', direction, result['confidence'], price, effective_ts)

    def _emit_signal(self, results: BacktestResults, sig_type: str, direction: str, confidence: float, entry_price: float, delivery_ts: float):
        key = (self.symbol, sig_type)
        if self._sim_clock - self._cooldowns.get(key, 0) < self._cooldown_s * 1000:
            return
        self._cooldowns[key] = self._sim_clock
        weight = self._weights.get(sig_type, 1.0)
        weighted_confidence = confidence * weight
        record = SignalRecord(signal_type=sig_type, direction=direction, confidence=weighted_confidence, entry_price=entry_price, signal_ts=self._sim_clock, delivery_ts=delivery_ts)
        self._pending.append(record)
        results.total_signals += 1

    def _settle_pending(self, results: BacktestResults):
        still_pending = []
        for record in self._pending:
            exit_ts = record.delivery_ts + self.forward_window_ms
            if self._sim_clock >= exit_ts:
                exit_price = self._lookup_price_at(int(exit_ts))
                if exit_price is None:
                    still_pending.append(record)
                    continue
                raw_pct = (exit_price - record.entry_price) / record.entry_price
                cost = 2 * (self.fee_pct + self.slippage_pct)
                net_pct = raw_pct - cost if record.direction == 'BUY' else -raw_pct - cost
                record.exit_price = exit_price
                record.pnl_pct = net_pct
                if net_pct > 0:
                    record.outcome = 'WIN'
                    results.wins += 1
                elif net_pct < -0.0005:
                    record.outcome = 'LOSS'
                    results.losses += 1
                else:
                    record.outcome = 'FLAT'
                    results.flats += 1
                results.gross_pnl_pct += net_pct
                results.signals.append(record)
                stats = results.type_stats.setdefault(record.signal_type, {'wins': 0, 'losses': 0, 'flats': 0})
                stats[record.outcome.lower()] += 1
            else:
                still_pending.append(record)
        self._pending = still_pending

    def _force_settle_all(self, results: BacktestResults):
        if not self._price_log:
            return
        last_price = self._price_log[-1][1]
        for record in self._pending:
            record.exit_price = last_price
            record.pnl_pct = 0.0
            record.outcome = 'FLAT'
            results.flats += 1
            results.signals.append(record)
        self._pending.clear()

    def _lookup_price_at(self, ts_ms: int) -> Optional[float]:
        for log_ts, price in self._price_log:
            if log_ts >= ts_ms:
                return price
        return None

    async def _replay_stream(self) -> AsyncIterator[dict]:
        if not self.data_path.exists():
            logger.error(f'[Backtester] Data file not found: {self.data_path}')
            return
        with open(self.data_path, 'r') as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as e:
                    logger.warning(f'[Backtester] Bad JSON line: {e}')
                await asyncio.sleep(0)

    @staticmethod
    def print_summary(results: BacktestResults):
        sep = '─' * 55
        print(f'\n{sep}')
        print('  BACKTEST RESULTS (with latency injection)')
        print(sep)
        print(f'  Total signals    : {results.total_signals}')
        print(f'  Wins             : {results.wins}')
        print(f'  Losses           : {results.losses}')
        print(f'  Flats            : {results.flats}')
        print(f'  Win rate         : {results.win_rate:.1%}')
        print(f'  Profit factor    : {results.profit_factor:.2f}')
        print(f'  Sharpe ratio     : {results.sharpe_ratio:.2f}')
        print(f'  Sortino ratio    : {results.sortino_ratio:.2f}')
        print(f'  Max drawdown     : {results.max_drawdown_pct:.3%}')
        print(f'  Gross PnL        : {results.gross_pnl_pct:.2%}')
        print(f'\n  Per-type breakdown:')
        for sig_type, stats in results.type_stats.items():
            w, l, f = (stats['wins'], stats['losses'], stats['flats'])
            total = w + l + f
            wr = w / (w + l) if w + l > 0 else 0
            print(f'    {sig_type:<12} | signals={total:>4} | win_rate={wr:.0%}')
        print(sep)
        print(f'\n{_SHARPE_TAG}{results.sharpe_ratio:.6f}')
        print(f'{_WINRATE_TAG}{results.win_rate:.6f}')
        print(f'{_PF_TAG}{results.profit_factor:.6f}')
        print(f'{_SIGNALS_TAG}{results.total_signals}')
        print()

if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s')
    data_file = sys.argv[1] if len(sys.argv) > 1 else 'data/btcusdt_ticks.jsonl'
    symbol = sys.argv[2] if len(sys.argv) > 2 else 'BTCUSDT'
    bt = Backtester(data_path=data_file, symbol=symbol)
    res = asyncio.run(bt.run())
    bt.print_summary(res)
