import asyncio
import csv
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class HistoricalReplaySimulator:
    def __init__(self, signal_gen, telegram_bot):
        self.signal_gen = signal_gen
        self.telegram_bot = telegram_bot
        self.replay_speed = 5.0   # 5x real-time (adjust as needed)

    async def replay(self, trades_csv: str = "data/historical/trades.csv",
                     depth_csv: str = "data/historical/depth_updates.csv"):
        logger.info("Starting historical replay simulation...")

        # Load depth updates
        with open(depth_csv, newline='', encoding='utf-8') as f:
            depth_rows = list(csv.DictReader(f))

        # Load trades
        with open(trades_csv, newline='', encoding='utf-8') as f:
            trade_rows = list(csv.DictReader(f))

        # Combine and sort events by timestamp
        events = []
        for row in depth_rows:
            events.append({
                'type': 'depth',
                'ts': int(row['timestamp']),
                'data': {
                    'E': int(row['timestamp']),
                    'U': int(row['U']),
                    'u': int(row['u']),
                    'b': json.loads(row['bids']),
                    'a': json.loads(row['asks']),
                    's': row['symbol']
                }
            })

        for row in trade_rows:
            events.append({
                'type': 'trade',
                'ts': int(row['timestamp']),
                'data': {
                    'E': int(row['timestamp']),
                    'p': float(row['price']),
                    'q': float(row['quantity']),
                    'm': row['is_buyer_maker'].lower() == 'true',
                    's': row['symbol']
                }
            })

        events.sort(key=lambda x: x['ts'])

        start_real = asyncio.get_event_loop().time()
        start_data = events[0]['ts'] / 1000.0

        for event in events:
            # Simulate timing
            data_time = event['ts'] / 1000.0
            delay = (data_time - start_data) / self.replay_speed
            target = start_real + delay

            while asyncio.get_event_loop().time() < target:
                await asyncio.sleep(0.005)

            symbol = event['data'].get('s', 'BTCUSDT')

            if event['type'] == 'depth':
                ob = self.signal_gen.orderbooks.get(symbol)
                if ob and not ob._synced:
                    ob._synced = True
                    ob.last_update_id = event['data']['U'] - 1
                await self.signal_gen.on_depth_update(symbol, event['data'])
            else:
                await self.signal_gen.on_trade_update(symbol, event['data'])

            logger.debug(f"Replayed {event['type']} @ {datetime.fromtimestamp(data_time)}")

        logger.info("Historical replay finished.")
