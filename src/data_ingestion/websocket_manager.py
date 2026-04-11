from typing import Callable, List, Optional
import asyncio
import json
import logging
import websockets

logger = logging.getLogger(__name__)

class CombinedStreamManager:
    def __init__(self, symbols: List[str], streams: List[str]=None, orderbook_callback: Optional[Callable]=None, trade_callback: Optional[Callable]=None):
        self.symbols = [s.upper() for s in symbols]
        self.streams = streams or ['depth@100ms', 'aggTrade']
        self.orderbook_callback = orderbook_callback
        self.trade_callback = trade_callback
        self._ws = None
        self._running = False
        self._reconnect_count = 0
        self.messages_received = 0
        self.last_message_time: float = 0.0

    def _build_url(self) -> str:
        subs = [f'{symbol.lower()}@{stream}' for symbol in self.symbols for stream in self.streams]
        return f"wss://stream.binance.com:9443/stream?streams={'/'.join(subs)}"

    async def start(self):
        self._running = True
        url = self._build_url()
        logger.info('Connecting to combined stream: %d sub-streams', len(self.symbols) * len(self.streams))
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    self._ws = ws
                    self._reconnect_count = 0
                    logger.info('Combined stream connected')
                    async for raw in ws:
                        await self._dispatch(raw)
            except websockets.exceptions.ConnectionClosed as exc:
                logger.warning('Stream closed: %s', exc)
                await self._backoff()
            except Exception as exc:
                logger.error('Stream error: %s', exc, exc_info=True)
                await self._backoff()

    async def stop(self):
        self._running = False
        if self._ws and (not self._ws.closed):
            await self._ws.close()
        logger.info('Combined stream stopped')

    async def _dispatch(self, raw: str):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug('Unparseable message dropped')
            return
        stream = msg.get('stream', '')
        data = msg.get('data')
        if not stream or data is None:
            return
        self.messages_received += 1
        self.last_message_time = asyncio.get_event_loop().time()
        symbol = stream.split('@')[0].upper()
        if '@depth' in stream and self.orderbook_callback:
            await self.orderbook_callback(symbol, data)
        elif '@aggTrade' in stream and self.trade_callback:
            await self.trade_callback(symbol, data)

    async def _backoff(self):
        if not self._running:
            return
        self._reconnect_count += 1
        delay = min(2 ** self._reconnect_count, 60)
        logger.info('Reconnecting in %ds (attempt %d)', delay, self._reconnect_count)
        await asyncio.sleep(delay)

    def stats(self) -> dict:
        return {'messages_received': self.messages_received, 'reconnect_count': self._reconnect_count, 'connected': self._ws is not None and (not self._ws.closed)}