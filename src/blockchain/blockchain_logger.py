"""
Core module for BlockchainLogger
"""
from datetime import datetime, timezone
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes
from typing import Callable, List, Optional
from typing import Optional
from typing import Optional, Dict, Any
import aiohttp
import asyncio
import hashlib
import json
import logging
import os
import time
import websockets

import logging
logger = logging.getLogger(__name__)

class BlockchainLogger:
    BSC_MAINNET_RPC = 'https://bsc-dataseed.binance.org/'
    MIN_CONFIDENCE = 80

    def __init__(self, private_key: Optional[str]=None, rpc_url: Optional[str]=None, min_confidence: int=MIN_CONFIDENCE):
        self._private_key = private_key
        self._rpc_url = rpc_url or self.BSC_MAINNET_RPC
        self._min_confidence = min_confidence
        self._w3 = None
        self._account = None
        self._enabled = False
        if private_key:
            self._setup()

    def _setup(self):
        try:
            from web3 import Web3
            self._w3 = Web3(Web3.HTTPProvider(self._rpc_url))
            if not self._w3.is_connected():
                logger.warning('BlockchainLogger: BSC node unreachable — logging disabled')
                return
            self._account = self._w3.eth.account.from_key(self._private_key)
            self._enabled = True
            logger.info('BlockchainLogger: active | address=%s | rpc=%s', self._account.address, self._rpc_url)
        except ImportError:
            logger.warning('BlockchainLogger: web3 not installed — run `pip install web3`')
        except Exception as exc:
            logger.error('BlockchainLogger setup failed: %s', exc)

    async def log_signal(self, signal: dict) -> Optional[str]:
        if not self._enabled:
            return None
        if signal.get('confidence', 0) < self._min_confidence:
            return None
        payload = {'symbol': signal['symbol'], 'type': signal['type'], 'direction': signal['direction'], 'confidence': signal['confidence'], 'timestamp': signal['timestamp']}
        raw = json.dumps(payload, separators=(',', ':'), sort_keys=True)
        digest = hashlib.sha256(raw.encode()).hexdigest()
        try:
            tx_hash = await self._broadcast(digest)
            logger.info('Signal logged on-chain: %s | tx=%s', signal['type'], tx_hash)
            return tx_hash
        except Exception as exc:
            logger.error('On-chain logging failed: %s', exc)
            return None

    async def _broadcast(self, data_hex: str) -> str:
        from web3 import Web3
        nonce = self._w3.eth.get_transaction_count(self._account.address)
        gas_price = self._w3.eth.gas_price
        tx = {'to': self._account.address, 'value': 0, 'gas': 30000, 'gasPrice': gas_price, 'nonce': nonce, 'data': Web3.to_bytes(hexstr=data_hex), 'chainId': 56}
        signed = self._account.sign_transaction(tx)
        tx_hash = self._w3.eth.send_raw_transaction(signed.rawTransaction)
        return tx_hash.hex()