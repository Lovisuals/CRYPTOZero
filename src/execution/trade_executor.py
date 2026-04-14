from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Optional

import ccxt.async_support as ccxt

logger = logging.getLogger(__name__)

class TradeExecutor:

    def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
        self.testnet = testnet
        self.exchange = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        })
        if testnet:
            self.exchange.set_sandbox_mode(True)
            logger.info("TradeExecutor initialized -> TESTNET spot mode")
        else:
            logger.warning("TradeExecutor initialized -> LIVE spot mode")

        self._lock = asyncio.Lock()

    async def get_usdt_balance(self) -> float:
        async with self._lock:
            return await self._get_usdt_balance_unlocked()

    async def _get_usdt_balance_unlocked(self) -> float:
        try:
            balance = await self.exchange.fetch_balance()
            return float(balance.get("total", {}).get("USDT", 0.0))
        except Exception as e:
            logger.error("Balance fetch failed: %s", e)
            return 0.0

    async def get_current_price(self, symbol: str) -> float:
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            return float(ticker["last"])
        except Exception as e:
            logger.error("Price fetch failed for %s: %s", symbol, e)
            return 0.0

    async def execute_signal(self, signal: dict, quantity: Optional[float] = None) -> Optional[Dict]:
        if signal.get("confidence", 0) < 70:
            return None

        symbol = signal["symbol"]
        direction = signal.get("direction", "").upper()
        side = "buy" if "BUY" in direction else "sell"

        async with self._lock:
            try:
                balance_usdt = await self._get_usdt_balance_unlocked()
                if balance_usdt < 15.0:
                    logger.warning("Insufficient USDT for execution")
                    return None

                price = (signal.get("mid_price") or
                         signal.get("price") or
                         signal.get("zone_start") or
                         signal.get("entry_price"))

                if not price:
                    ticker = await self.exchange.fetch_ticker(symbol)
                    price = ticker["last"]

                if quantity is None:
                    risk_amount = balance_usdt * 0.02
                    stop_distance_pct = signal.get("stop_distance_pct", 0.015)
                    quantity = risk_amount / (float(price) * stop_distance_pct)

                quantity = float(Decimal(str(quantity)).quantize(Decimal("0.00001"), rounding=ROUND_DOWN))
                if quantity < 0.0002:
                    logger.warning("Amount too small for %s", symbol)
                    return None

                order = await self.exchange.create_market_order(symbol, side, quantity)

                fill_price = float(order.get("average", price))
                logger.info("EXECUTED: %s %s %.6f %s @ %.2f | Order: %s",
                           "TESTNET" if self.testnet else "LIVE",
                           side.upper(), quantity, symbol, fill_price, order.get("id"))

                return {
                    "status": "SUCCESS",
                    "order_id": order.get("id"),
                    "symbol": symbol,
                    "side": side,
                    "direction": direction,
                    "quantity": quantity,
                    "entry_price": fill_price,
                    "fees": float(order.get("fee", {}).get("cost", 0) or 0),
                    "timestamp": int(time.time() * 1000),
                }

            except Exception as e:
                logger.error("Execution error on %s: %s", symbol, e)
                return None

    async def close_position(self, symbol: str, direction: str, quantity: float) -> Optional[Dict]:
        close_side = "sell" if direction == "BUY" else "buy"

        async with self._lock:
            try:
                quantity = float(Decimal(str(quantity)).quantize(Decimal("0.00001"), rounding=ROUND_DOWN))
                if quantity < 0.0002:
                    return None

                order = await self.exchange.create_market_order(symbol, close_side, quantity)
                fill_price = float(order.get("average", 0))
                fees = float(order.get("fee", {}).get("cost", 0) or 0)

                logger.info("POSITION CLOSED: %s %s %.6f %s @ %.2f",
                           "TESTNET" if self.testnet else "LIVE",
                           close_side.upper(), quantity, symbol, fill_price)

                return {
                    "status": "CLOSED",
                    "order_id": order.get("id"),
                    "close_price": fill_price,
                    "fees": fees,
                }
            except Exception as e:
                logger.error("Position close error on %s: %s", symbol, e)
                return None

    async def close(self):
        try:
            await self.exchange.close()
        except Exception:
            pass
