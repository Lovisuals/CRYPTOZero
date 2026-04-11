from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Optional

import ccxt.async_support as ccxt

logger = logging.getLogger(__name__)


class ApexTradeExecutor:
    """
    Futures-capable executor with leverage and Kelly-based sizing.
    """

    def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
        self.testnet = testnet
        self.exchange = ccxt.binance(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                "options": {"defaultType": "future"},
            }
        )
        if testnet:
            self.exchange.set_sandbox_mode(True)
            logger.info("ApexTradeExecutor initialized -> TESTNET futures mode")
        else:
            logger.warning("ApexTradeExecutor initialized -> LIVE futures mode (HIGH RISK)")

        self._lock = asyncio.Lock()

    async def _get_futures_balance_unlocked(self) -> float:
        try:
            balance = await self.exchange.fetch_balance()
            total = balance.get("total", {}) if isinstance(balance, dict) else {}
            return float(total.get("USDT", 0.0))
        except Exception as exc:
            logger.error("Futures balance fetch failed: %s", exc)
            return 0.0

    async def execute_apex_signal(self, signal: Dict) -> Dict:
        symbol = signal["symbol"]
        direction = signal["direction"]
        confidence = int(signal.get("confidence", 0))
        leverage = int(signal.get("leverage_recommended", 1))
        leverage = min(max(leverage, 1), 10)
        position_multiplier = float(signal.get("position_multiplier", 0.02))
        position_multiplier = min(max(position_multiplier, 0.01), 0.10)

        async with self._lock:
            try:
                balance = await self._get_futures_balance_unlocked()
                if balance <= 0:
                    return {"status": "FAILED", "error": "No USDT balance"}

                base_position = balance * position_multiplier
                leveraged_position = base_position * leverage

                ticker = await self.exchange.fetch_ticker(symbol)
                current_price = float(ticker["last"])
                if current_price <= 0:
                    return {"status": "FAILED", "error": "Invalid market price"}

                quantity = leveraged_position / current_price
                quantity = float(Decimal(str(quantity)).quantize(Decimal("0.00001"), rounding=ROUND_DOWN))
                if quantity <= 0:
                    return {"status": "FAILED", "error": "Computed quantity too small"}

                if direction == "BUY":
                    stop_loss = float(signal.get("stop_loss", current_price * 0.98))
                    take_profit = float(signal.get("take_profit", current_price * 1.04))
                    entry_side = "buy"
                    reduce_side = "sell"
                else:
                    stop_loss = float(signal.get("stop_loss", current_price * 1.02))
                    take_profit = float(signal.get("take_profit", current_price * 0.96))
                    entry_side = "sell"
                    reduce_side = "buy"

                await self.exchange.set_leverage(leverage, symbol)
                order = await self.exchange.create_market_order(symbol=symbol, side=entry_side, amount=quantity)

                await self.exchange.create_order(
                    symbol=symbol,
                    type="stop_market",
                    side=reduce_side,
                    amount=quantity,
                    params={"stopPrice": stop_loss, "reduceOnly": True},
                )
                await self.exchange.create_order(
                    symbol=symbol,
                    type="take_profit_market",
                    side=reduce_side,
                    amount=quantity,
                    params={"stopPrice": take_profit, "reduceOnly": True},
                )

                rr_denominator = abs(current_price - stop_loss)
                rr = abs((take_profit - current_price) / rr_denominator) if rr_denominator > 0 else 0.0
                return {
                    "status": "SUCCESS",
                    "order_id": order.get("id"),
                    "symbol": symbol,
                    "direction": direction,
                    "quantity": quantity,
                    "entry_price": current_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "leverage": leverage,
                    "confidence": confidence,
                    "risk_reward": rr,
                    "timestamp": int(time.time() * 1000),
                }
            except Exception as exc:
                logger.error("APEX execution failed: %s", exc, exc_info=True)
                return {"status": "FAILED", "error": str(exc)}

    async def close(self):
        try:
            await self.exchange.close()
        except Exception:
            pass
