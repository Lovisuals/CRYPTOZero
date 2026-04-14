from datetime import datetime, timezone
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes
from typing import Callable, List, Optional, Dict, Any
import asyncio
import logging
import os
import time

logger = logging.getLogger(__name__)

def _fmt_time(ts_seconds: float) -> str:
    dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
    return dt.strftime('%Y-%m-%d %H:%M:%S UTC')

def _fmt_uptime(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int(seconds % 3600 // 60)
    s = int(seconds % 60)
    return f'{h}h {m}m {s}s'

def _fmt_signal(sig: dict) -> str:
    lines = [f'SIGNAL DETECTED', f'', f"Symbol    : {sig['symbol']}", f"Type      : {sig['type']}", f"Direction : {sig['direction']}", f"Confidence: {sig.get('confidence', 0)}%", f'']
    if sig.get('regime'):
        lines.append(f"Regime    : {sig.get('regime')}")
    if sig.get('vpin') is not None:
        vpin_val = sig.get('vpin', 0)
        toxic_flag = " [TOXIC]" if sig.get('flow_toxic') else ""
        lines.append(f"VPIN      : {vpin_val:.3f}{toxic_flag}")
    lines.append('')
    if sig['type'] == 'IMBALANCE':
        lines += [f"Bid Liq   : ${sig.get('bid_liquidity', 0):,.0f}", f"Ask Liq   : ${sig.get('ask_liquidity', 0):,.0f}", f"Ratio     : {sig.get('imbalance_ratio', 0):.2f}:1", f"Mid Price : ${sig.get('mid_price', 0):,.2f}", f"Spread    : {sig.get('spread_bps', 0)} bps"]
    elif sig['type'] == 'CVD':
        cvd = sig.get('cvd', 0)
        lines += [f'CVD       : {cvd:+.4f}', f"Strength  : {sig.get('strength', 0):.5f}", f"CVD Slope : {sig.get('cvd_slope', 0):+.6f}", f"Px Slope  : {sig.get('price_slope', 0):+.6f}"]
    elif sig['type'] == 'ABSORPTION':
        lines += [f"Vol Mult  : {sig.get('volume_multiplier', 0):.1f}x avg", f"Duration  : {sig.get('duration_s', 0):.0f}s", f"CVD Stab  : {sig.get('cvd_stability', 0):.2f}"]
    elif sig['type'] == 'VACUUM':
        lines += [f"Zone      : ${sig.get('zone_start', 0):,.2f} — ${sig.get('zone_end', 0):,.2f}", f"Levels    : {sig.get('levels', 0)}", f"Speed     : {sig.get('speed', 'N/A')}", f"Distance  : {sig.get('distance_pct', 0):.3%}"]
    elif sig['type'] == 'ICEBERG':
        lines += [f"Price     : ${sig.get('price', 0):,.2f}", f"Vol Ratio : {sig.get('volume_ratio', 0):.1f}x", f"Persisted : {sig.get('duration_s', 0):.0f}s", f"Context   : {sig.get('interpretation', '')}"]
    elif sig['type'] == 'LIQUIDATION':
        lines += [
            f"Cluster   : ${sig.get('cluster_price', 0):,.2f}",
            f"Liq USD   : ${sig.get('liquidity_usd', 0):,.0f}",
            f"Entry     : ${sig.get('entry_price', 0):,.2f}",
            f"Stop/TP   : ${sig.get('stop_loss', 0):,.2f} / ${sig.get('take_profit', 0):,.2f}",
            f"Source    : {sig.get('source', 'N/A')}",
            f"Rationale : {sig.get('rationale', '')}",
        ]
    elif sig['type'] == 'TIME_DELIVERY':
        lines += [
            f"Confluence: {sig.get('signal', 'N/A')}",
            f"Bull/Bear : {sig.get('bullish_count', 0)} / {sig.get('bearish_count', 0)}",
        ]
    elif sig['type'] == 'SOCIAL':
        lines += [
            f"Sentiment : {sig.get('sentiment_score', 0):+.2f}",
            f"Buzz      : {sig.get('buzz_multiplier', 1.0):.2f}x",
            f"Rationale : {sig.get('rationale', '')}",
        ]
    elif sig['type'] == 'APEX_FUSION':
        support = sig.get('supporting_signals', [])
        if isinstance(support, list):
            support_text = ', '.join(support)
        else:
            support_text = str(support)
        lines += [
            f"Signals   : {sig.get('signal_count', 0)}",
            f"Support   : {support_text}",
            f"Kelly Size: {sig.get('position_multiplier', 0):.2%}",
            f"Leverage  : {sig.get('leverage_recommended', 1)}x",
            f"Rationale : {sig.get('rationale', '')}",
        ]
    ts = sig.get('timestamp', int(time.time() * 1000)) / 1000
    lines += ['', f'Issued    : {_fmt_time(ts)}']
    return '\n'.join(lines)

def _fmt_health(health: dict, uptime: float, stream_stats: dict) -> str:
    lines = ['SYSTEM HEALTH', f'', f'Uptime    : {_fmt_uptime(uptime)}', f"Msg/recv  : {stream_stats.get('messages_received', 0):,}", f"Reconnects: {stream_stats.get('reconnect_count', 0)}", f'', 'Per-Symbol Status:']
    for symbol, h in health.items():
        sync_flag = 'OK' if h['synced'] else 'DESYNCED'
        lines += [f'', f'  {symbol}', f'    Sync      : {sync_flag}', f"    Gaps      : {h['gap_count']}", f"    Updates   : {h['update_count']:,}", f"    Bids/Asks : {h['bid_levels']} / {h['ask_levels']}", f"    Spread    : {h['spread_bps']} bps", f"    CVD       : {h['cvd']:+.4f}", f"    Icebergs  : {h['iceberg_active']} active"]
        if 'regime' in h:
            lines.append(f"    Regime    : {h['regime']}")
        if 'vpin' in h:
            lines.append(f"    VPIN      : {h['vpin']}")
    return '\n'.join(lines)

def _fmt_orderbook(symbol: str, liq: dict, spread_bps: int, mid: float) -> str:
    ratio = liq['imbalance_ratio']
    lines = [f'ORDER BOOK — {symbol}', f'', f'Mid Price : ${mid:,.2f}', f'Spread    : {spread_bps} bps', f'', f"Bid Liq (10L) : ${liq['bid_liquidity']:,.0f}", f"Ask Liq (10L) : ${liq['ask_liquidity']:,.0f}", f'Imbalance     : {ratio:.2f}:1', f'', 'Bias      : ' + ('BULLISH' if ratio > 1.5 else 'BEARISH' if ratio < 0.67 else 'NEUTRAL')]
    return '\n'.join(lines)

def _main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('Health', callback_data='health'), InlineKeyboardButton('Signals', callback_data='signals')],
        [InlineKeyboardButton('Book BTC', callback_data='ob_BTCUSDT'), InlineKeyboardButton('Market', callback_data='market')],
        [InlineKeyboardButton('P&L', callback_data='pnl'), InlineKeyboardButton('Risk', callback_data='risk_status')],
        [InlineKeyboardButton('Auto ON', callback_data='auto_on'), InlineKeyboardButton('Auto OFF', callback_data='auto_off')],
    ])

class WeaponBot:
    def __init__(self, token: str, chat_id: int, signal_generator, stream_manager, allowed_symbols: list, coinranking=None, bitcoin_uuid: str=None, trade_journal=None, risk_engine=None, circuit_breaker=None, position_manager=None):
        self._token = token
        self._chat_id = chat_id
        self._sg = signal_generator
        self._stream = stream_manager
        self._symbols = [s.upper() for s in allowed_symbols]
        self._coinranking = coinranking
        self._bitcoin_uuid = bitcoin_uuid
        self._trade_journal = trade_journal
        self._risk_engine = risk_engine
        self._circuit_breaker = circuit_breaker
        self._position_manager = position_manager
        self._auto_trade: bool = False
        self._last_signals: list = []
        self._app: Optional[Application] = None
        self._send_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
        self._sender_task: Optional[asyncio.Task] = None

    def build(self) -> Application:
        app = Application.builder().token(self._token).build()
        app.add_handler(CommandHandler('start', self._cmd_start))
        app.add_handler(CommandHandler('status', self._cmd_status))
        app.add_handler(CommandHandler('health', self._cmd_health))
        app.add_handler(CommandHandler('signals', self._cmd_signals))
        app.add_handler(CommandHandler('orderbook', self._cmd_orderbook))
        app.add_handler(CommandHandler('market', self._cmd_market))
        app.add_handler(CommandHandler('coin', self._cmd_coin))
        app.add_handler(CommandHandler('pnl', self._cmd_pnl))
        app.add_handler(CommandHandler('risk', self._cmd_risk))
        app.add_handler(CommandHandler('positions', self._cmd_positions))
        app.add_handler(CommandHandler('circuit', self._cmd_circuit))
        app.add_handler(CallbackQueryHandler(self._on_button))
        app.add_handler(CommandHandler('toggle_auto', self._cmd_toggle_auto))
        self._app = app
        return app

    async def push_signal(self, signal: dict):
        self._last_signals.append(signal)
        if len(self._last_signals) > 50:
            self._last_signals = self._last_signals[-50:]
        if not self._app:
            return
        self._ensure_sender_task()
        text = _fmt_signal(signal)
        payload = {'chat_id': self._chat_id, 'text': text}
        try:
            self._send_queue.put_nowait(payload)
        except asyncio.QueueFull:
            _ = self._send_queue.get_nowait()
            self._send_queue.task_done()
            self._send_queue.put_nowait(payload)
            logger.warning('Signal queue full; oldest Telegram message dropped')

    async def push_trade_update(self, trade_record):
        if not self._app:
            return
        lines = [
            "TRADE CLOSED",
            "",
            f"ID        : {trade_record.trade_id}",
            f"Symbol    : {trade_record.symbol}",
            f"Direction : {trade_record.direction}",
            f"Signal    : {trade_record.signal_type}",
            "",
            f"Entry     : ${trade_record.entry_price:,.2f}",
            f"Exit      : ${trade_record.exit_price:,.2f}",
            f"P&L       : {(trade_record.pnl_pct or 0) * 100:+.3f}% (${trade_record.pnl_usd or 0:+.2f})",
            f"Outcome   : {trade_record.outcome}",
            f"Reason    : {trade_record.exit_reason}",
            f"Duration  : {trade_record.holding_time_s:.0f}s",
        ]
        self._ensure_sender_task()
        payload = {'chat_id': self._chat_id, 'text': '\n'.join(lines)}
        try:
            self._send_queue.put_nowait(payload)
        except asyncio.QueueFull:
            pass

    def _ensure_sender_task(self):
        if self._sender_task is None or self._sender_task.done():
            self._sender_task = asyncio.create_task(self._sender_loop())

    async def _sender_loop(self):
        while True:
            msg = await self._send_queue.get()
            try:
                await self._send_with_retry(msg)
            finally:
                self._send_queue.task_done()

    async def _send_with_retry(self, payload: dict, retries: int = 3):
        if not self._app:
            return
        for attempt in range(1, retries + 1):
            try:
                await self._app.bot.send_message(chat_id=payload['chat_id'], text=payload['text'])
                return
            except Exception as exc:
                if attempt == retries:
                    logger.error('Telegram send failed after retries: %s', exc)
                    return
                await asyncio.sleep(min(0.25 * (2 ** (attempt - 1)), 2.0))

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.info(f"Incoming /start from {update.effective_user.id}")
        await update.message.reply_text(text='OrderBook Weapon Machine v5.0\n\nAutonomous order flow intelligence + execution.\nUse the buttons below or type a command.', reply_markup=_main_keyboard())

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.info(f"Incoming /status from {update.effective_user.id}")
        await self._send_health(update.message.reply_text)

    async def _cmd_health(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._send_health(update.message.reply_text)

    async def _cmd_signals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._last_signals:
            await update.message.reply_text('No signals recorded yet.')
            return
        recent = self._last_signals[-5:]
        lines = [f'Recent Signals ({len(self._last_signals)} total):', '']
        for s in reversed(recent):
            ts = s.get('timestamp', 0) / 1000
            lines.append(f"{_fmt_time(ts)} | {s['symbol']} | {s['type']} | {s['direction']} | {s.get('confidence', 0)}%")
        await update.message.reply_text(chr(10).join(lines))

    async def _cmd_orderbook(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        args = context.args
        symbol = args[0].upper() if args else self._symbols[0]
        await self._send_orderbook(update.message.reply_text, symbol)

    async def _cmd_pnl(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._send_pnl(update.message.reply_text)

    async def _cmd_risk(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._send_risk_status(update.message.reply_text)

    async def _cmd_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._send_positions(update.message.reply_text)

    async def _cmd_circuit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._circuit_breaker:
            await update.message.reply_text("Circuit breaker not configured.")
            return
        args = context.args
        if args and args[0].lower() == "reset":
            self._circuit_breaker.force_reset()
            await update.message.reply_text("Circuit breaker manually RESET.")
        elif args and args[0].lower() == "trip":
            self._circuit_breaker.force_trip("MANUAL_TELEGRAM")
            await update.message.reply_text("Circuit breaker manually TRIPPED.")
        else:
            cb = self._circuit_breaker.stats()
            lines = [
                "CIRCUIT BREAKER",
                "",
                f"Status     : {'TRIPPED' if cb['tripped'] else 'ARMED'}",
                f"Reason     : {cb['trip_reason'] or 'N/A'}",
                f"Cooldown   : {cb['remaining_cooldown_s']:.0f}s remaining",
                f"Consec L   : {cb['consecutive_losses']}",
                f"Hourly L   : {cb['hourly_losses']}",
                f"Total Trips: {cb['total_trips']}",
            ]
            await update.message.reply_text('\n'.join(lines))

    async def _on_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        if data == 'health':
            await self._send_health(query.message.reply_text)
        elif data == 'signals':
            await self._cmd_signals.__wrapped__(self, update, context) if hasattr(self._cmd_signals, '__wrapped__') else await query.message.reply_text('Use /signals command')
        elif data.startswith('ob_'):
            symbol = data[3:]
            await self._send_orderbook(query.message.reply_text, symbol)
        elif data == 'auto_on':
            if self._sg:
                self._sg.auto_trade_enabled = True
            self._auto_trade = True
            await query.message.reply_text('Auto-trade: ENABLED')
        elif data == 'auto_off':
            if self._sg:
                self._sg.auto_trade_enabled = False
            self._auto_trade = False
            await query.message.reply_text('Auto-trade: DISABLED')
        elif data == 'market':
            await self._send_market(query.message.reply_text)
        elif data == 'pnl':
            await self._send_pnl(query.message.reply_text)
        elif data == 'risk_status':
            await self._send_risk_status(query.message.reply_text)

    async def _cmd_toggle_auto(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if self._sg:
            self._sg.auto_trade_enabled = not self._sg.auto_trade_enabled
            status = 'ENABLED (TESTNET)' if self._sg.auto_trade_enabled else 'DISABLED'
            await update.message.reply_text(f"Auto-trade: {status}")
        else:
            await update.message.reply_text("Auto-trade cannot be toggled at this time.")

    async def _send_health(self, reply_fn):
        h = self._sg.health()
        uptime = self._sg.uptime_seconds()
        stats = self._stream.stats()
        text = _fmt_health(h, uptime, stats)
        await reply_fn(text)

    async def _send_orderbook(self, reply_fn, symbol: str):
        ob = self._sg.orderbooks.get(symbol)
        if ob is None or not ob.bids or (not ob.asks):
            await reply_fn(f'No data for {symbol}.')
            return
        liq = ob.get_top_liquidity(10)
        spread = ob.get_spread_bps()
        mid = ob.get_mid_price()
        text = _fmt_orderbook(symbol, liq, spread, mid)
        await reply_fn(text)

    async def _send_pnl(self, reply_fn):
        if not self._trade_journal:
            await reply_fn("Trade journal not configured.")
            return
        stats = self._trade_journal.stats_summary()
        lines = [
            "P&L DASHBOARD",
            "",
            f"Total Trades : {stats.get('total_trades', 0)}",
            f"Open Trades  : {stats.get('open_trades', 0)}",
            f"Wins         : {stats.get('wins', 0)}",
            f"Losses       : {stats.get('losses', 0)}",
            f"Win Rate     : {stats.get('win_rate', 0):.1%}",
            "",
            f"Total P&L    : ${stats.get('total_pnl_usd', 0):+,.2f}",
            f"Profit Factor: {stats.get('profit_factor', 0):.2f}",
            f"Max Drawdown : ${stats.get('max_drawdown_usd', 0):,.2f}",
            "",
            f"Avg Win      : ${stats.get('avg_win_usd', 0):+,.2f}",
            f"Avg Loss     : ${stats.get('avg_loss_usd', 0):+,.2f}",
            f"Rolling WR50 : {stats.get('rolling_win_rate_50', 0):.1%}",
        ]
        await reply_fn('\n'.join(lines))

    async def _send_risk_status(self, reply_fn):
        lines = ["RISK STATUS", ""]
        if self._risk_engine:
            rs = self._risk_engine.stats()
            lines += [
                f"Peak Equity  : ${rs.get('peak_equity', 0):,.2f}",
                f"Current Eq   : ${rs.get('current_equity', 0):,.2f}",
                f"Drawdown     : {rs.get('drawdown_pct', 0):.2%}",
                f"Daily P&L    : ${rs.get('daily_pnl', 0):+,.2f}",
                f"Approvals    : {rs.get('approvals', 0)}",
                f"Rejections   : {rs.get('rejections', 0)}",
                f"Approval Rate: {rs.get('approval_rate', 0):.1%}",
            ]
        if self._circuit_breaker:
            cb = self._circuit_breaker.stats()
            lines += [
                "",
                f"Circuit      : {'TRIPPED' if cb['tripped'] else 'ARMED'}",
                f"Consec Losses: {cb['consecutive_losses']}",
                f"Total Trips  : {cb['total_trips']}",
            ]
        if self._position_manager:
            unr = self._position_manager.unrealized_pnl()
            total_unr = sum(unr.values())
            lines += [
                "",
                f"Open Pos     : {len(unr)}",
                f"Unrealized   : {total_unr:+.3%}",
            ]
        await reply_fn('\n'.join(lines))

    async def _send_positions(self, reply_fn):
        if not self._trade_journal:
            await reply_fn("Trade journal not configured.")
            return
        open_trades = self._trade_journal.open_trades
        if not open_trades:
            await reply_fn("No open positions.")
            return
        lines = [f"OPEN POSITIONS ({len(open_trades)})", ""]
        for t in open_trades:
            unr = self._position_manager.unrealized_pnl().get(t.trade_id, 0) if self._position_manager else 0
            lines.append(f"  {t.symbol} | {t.direction} | entry=${t.entry_price:,.2f} | unr={unr:+.2%} | {t.holding_time_s:.0f}s")
        await reply_fn('\n'.join(lines))

    async def _cmd_market(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._send_market(update.message.reply_text)

    async def _cmd_coin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._coinranking:
            await update.message.reply_text('Coinranking API disabled.')
            return
        args = context.args
        uuid = args[0] if args else self._bitcoin_uuid
        if not uuid:
            await update.message.reply_text('Please provide a coin UUID.')
            return
        coin = await self._coinranking.fetch_coin_details(uuid)
        if not coin:
            await update.message.reply_text('Failed to fetch coin details.')
            return
        text = [f"COIN DETAILS: {coin['name']} ({coin['symbol']})", f'', f"Price: ${float(coin['price']):,.2f}", f"Market Cap: ${float(coin['marketCap']):,.0f}", f"24h Volume: ${float(coin['24hVolume']):,.0f}", f"Change: {coin['change']}%", f"Rank: {coin['rank']}"]
        await update.message.reply_text(chr(10).join(text))

    async def _send_market(self, reply_fn):
        if not self._coinranking:
            await reply_fn('Coinranking API disabled.')
            return
        stats = await self._coinranking.fetch_global_stats()
        if not stats:
            await reply_fn('Failed to fetch global market stats.')
            return
        text = ['GLOBAL MARKET STATS', f'', f"Total Coins: {stats['totalCoins']:,}", f"Total Markets: {stats['totalMarkets']:,}", f"Total Market Cap: ${float(stats['totalMarketCap']):,.0f}", f"Total 24h Vol: ${float(stats['total24hVolume']):,.0f}", f"BTC Dominance: {stats['btcDominance']:.2f}%"]
        await reply_fn(chr(10).join(text))
