from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

class PositionManager:

    def __init__(
        self,
        trade_journal,
        executor,
        on_position_closed: Optional[Callable] = None,
        trailing_stop_activation_pct: float = 0.01,
        trailing_stop_callback_pct: float = 0.005,
        max_holding_time_s: float = 3600.0,
        check_interval_s: float = 2.0,
    ):
        self._journal = trade_journal
        self._executor = executor
        self._on_closed = on_position_closed
        self._trailing_activation = trailing_stop_activation_pct
        self._trailing_callback = trailing_stop_callback_pct
        self._max_holding_time = max_holding_time_s
        self._check_interval = check_interval_s
        self._price_cache: Dict[str, float] = {}
        self._trailing_peaks: Dict[str, float] = {}
        self._monitor_task: Optional[asyncio.Task] = None
        self._running = False

    def update_price(self, symbol: str, price: float):
        self._price_cache[symbol] = price

    def start(self):
        if not self._running:
            self._running = True
            self._monitor_task = asyncio.create_task(self._monitor_loop())
            logger.info("PositionManager monitor started (interval=%.1fs)", self._check_interval)

    async def stop(self):
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

    async def _monitor_loop(self):
        while self._running:
            try:
                await self._check_all_positions()
            except Exception as exc:
                logger.error("Position monitor error: %s", exc, exc_info=True)
            await asyncio.sleep(self._check_interval)

    async def _check_all_positions(self):
        for trade in list(self._journal.open_trades):
            current_price = self._price_cache.get(trade.symbol)
            if current_price is None or current_price <= 0:
                continue

            exit_reason = self._evaluate_exit(trade, current_price)
            if exit_reason:
                await self._execute_close(trade, current_price, exit_reason)

    def _evaluate_exit(self, trade, current_price: float) -> Optional[str]:
        if trade.direction == "BUY":
            unrealized_pct = (current_price - trade.entry_price) / trade.entry_price
        else:
            unrealized_pct = (trade.entry_price - current_price) / trade.entry_price

        if trade.stop_loss > 0:
            if trade.direction == "BUY" and current_price <= trade.stop_loss:
                return "STOP_LOSS_HIT"
            elif trade.direction == "SELL" and current_price >= trade.stop_loss:
                return "STOP_LOSS_HIT"

        if trade.take_profit > 0:
            if trade.direction == "BUY" and current_price >= trade.take_profit:
                return "TAKE_PROFIT_HIT"
            elif trade.direction == "SELL" and current_price <= trade.take_profit:
                return "TAKE_PROFIT_HIT"

        if unrealized_pct >= self._trailing_activation:
            peak_key = trade.trade_id
            if peak_key not in self._trailing_peaks:
                self._trailing_peaks[peak_key] = unrealized_pct
                logger.info("Trailing stop ACTIVATED for %s at %.2f%%", trade.trade_id, unrealized_pct * 100)
            else:
                self._trailing_peaks[peak_key] = max(self._trailing_peaks[peak_key], unrealized_pct)

            peak = self._trailing_peaks[peak_key]
            if peak - unrealized_pct >= self._trailing_callback:
                return f"TRAILING_STOP (peak={peak:.2%}, current={unrealized_pct:.2%})"

        if trade.holding_time_s > self._max_holding_time:
            return f"MAX_HOLDING_TIME ({trade.holding_time_s:.0f}s > {self._max_holding_time:.0f}s)"

        return None

    async def _execute_close(self, trade, current_price: float, exit_reason: str):
        logger.info("CLOSING POSITION | %s | reason=%s | price=%.2f", trade.trade_id, exit_reason, current_price)

        fees = 0.0
        if self._executor and hasattr(self._executor, "close_position"):
            try:
                result = await self._executor.close_position(trade.symbol, trade.direction, trade.quantity)
                fees = float(result.get("fees", 0)) if result else 0.0
            except Exception as exc:
                logger.error("Close execution failed for %s: %s", trade.trade_id, exc)

        self._trailing_peaks.pop(trade.trade_id, None)
        closed = self._journal.close_trade(trade.trade_id, current_price, exit_reason, fees)

        if closed and self._on_closed:
            try:
                await self._on_closed(closed)
            except Exception as exc:
                logger.error("Position closed callback error: %s", exc)

    def open_positions_count(self) -> int:
        return len(self._journal.open_trades)

    def open_positions_for_symbol(self, symbol: str) -> int:
        return len(self._journal.get_open_for_symbol(symbol))

    def unrealized_pnl(self) -> Dict[str, float]:
        result = {}
        for trade in self._journal.open_trades:
            price = self._price_cache.get(trade.symbol, 0)
            if price <= 0:
                continue
            if trade.direction == "BUY":
                pnl = (price - trade.entry_price) / trade.entry_price
            else:
                pnl = (trade.entry_price - price) / trade.entry_price
            result[trade.trade_id] = pnl
        return result
