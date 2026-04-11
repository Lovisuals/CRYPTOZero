import ccxt.async_support as ccxt
import logging
from decimal import Decimal, ROUND_DOWN
import asyncio

logger = logging.getLogger(__name__)

class TradeExecutor:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
        self.testnet = testnet
        self.exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'},
        })
        if testnet:
            self.exchange.set_sandbox_mode(True)
            logger.info("TradeExecutor initialized → TESTNET mode")
        else:
            logger.warning("TradeExecutor initialized → LIVE mode (HIGH RISK)")

        self.max_risk_pct = 0.02
        self._lock = asyncio.Lock()

    async def get_usdt_balance(self) -> float:
        async with self._lock:
            try:
                balance = await self.exchange.fetch_balance()
                return float(balance.get('total', {}).get('USDT', 0.0))
            except Exception as e:
                logger.error(f"Balance fetch failed: {e}")
                return 0.0

    async def execute_signal(self, signal: dict) -> dict | None:
        if signal.get('confidence', 0) < 70:
            return None

        symbol = signal['symbol']
        direction = signal.get('direction', '').upper()
        side = 'buy' if 'BUY' in direction else 'sell'

        async with self._lock:
            try:
                balance_usdt = await self.get_usdt_balance()
                if balance_usdt < 15.0:
                    logger.warning("Insufficient USDT for safe execution")
                    return None

                price = (signal.get('mid_price') or 
                        signal.get('price') or 
                        signal.get('zone_start') or 
                        signal.get('entry_price'))

                if not price:
                    ticker = await self.exchange.fetch_ticker(symbol)
                    price = ticker['last']

                risk_amount = balance_usdt * self.max_risk_pct
                stop_distance_pct = signal.get('stop_distance_pct', 0.015)

                amount = risk_amount / (float(price) * stop_distance_pct)
                amount = float(Decimal(str(amount)).quantize(Decimal('0.00001'), rounding=ROUND_DOWN))

                if amount < 0.0002:
                    logger.warning(f"Amount too small for {symbol}")
                    return None

                order = await self.exchange.create_market_order(symbol, side, amount)

                logger.info(f"TESTNET EXECUTED: {side.upper()} {amount:.6f} {symbol} | Order: {order.get('id')}")
                return order

            except Exception as e:
                logger.error(f"Execution error on {symbol}: {e}")
                return None

    async def close(self):
        try:
            await self.exchange.close()
        except Exception:
            pass
