import asyncio
import logging
import os
import signal

import yaml
from dotenv import load_dotenv

from src.blockchain.blockchain_logger import BlockchainLogger
from src.data_ingestion.websocket_manager import CombinedStreamManager
from src.execution.apex_executor import ApexTradeExecutor
from src.execution.trade_executor import TradeExecutor
from src.notifications.telegram_bot import WeaponBot
from src.processing.signal_generator import SignalGenerator
from src.replay_simulator import HistoricalReplaySimulator
from src.risk.circuit_breaker import CircuitBreaker
from src.risk.position_manager import PositionManager
from src.risk.risk_engine import RiskEngine
from src.risk.trade_journal import TradeJournal

load_dotenv(override=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

def _load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)

async def main():
    cfg = _load_config()
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = int(os.environ["TELEGRAM_CHAT_ID"])

    symbols: list = cfg["trading_pairs"]
    thresholds = cfg["signal_thresholds"]
    system_cfg = cfg.get("system", {})
    apex_cfg = cfg.get("apex", {})
    external_cfg = cfg.get("external_apis", {})
    risk_cfg = cfg.get("risk", {})

    blockchain = BlockchainLogger(
        private_key=os.environ.get("BSC_PRIVATE_KEY"),
        rpc_url=os.environ.get("BSC_RPC_URL"),
        min_confidence=cfg.get("blockchain", {}).get("min_confidence", 80),
    )

    testnet_mode = system_cfg.get("testnet_mode", True)
    apex_enabled = bool(apex_cfg.get("enabled", False))
    use_futures_executor = bool(apex_cfg.get("execution", {}).get("use_futures_executor", False))

    if apex_enabled and use_futures_executor:
        executor = ApexTradeExecutor(
            api_key=os.getenv("BINANCE_API_KEY", ""),
            api_secret=os.getenv("BINANCE_API_SECRET", ""),
            testnet=testnet_mode,
        )
        logger.info("Using ApexTradeExecutor (futures)")
    else:
        executor = TradeExecutor(
            api_key=os.getenv("BINANCE_API_KEY", ""),
            api_secret=os.getenv("BINANCE_API_SECRET", ""),
            testnet=testnet_mode,
        )
        logger.info("Using TradeExecutor (spot)")

    trade_journal = TradeJournal(journal_path=risk_cfg.get("journal_path", "data/trade_journal.jsonl"))
    logger.info("Trade journal loaded: %d closed trades, %d open trades",
                len(trade_journal.closed_trades), len(trade_journal.open_trades))

    risk_engine = RiskEngine(
        max_portfolio_drawdown_pct=risk_cfg.get("max_portfolio_drawdown_pct", 0.10),
        max_daily_loss_pct=risk_cfg.get("max_daily_loss_pct", 0.05),
        max_open_positions=risk_cfg.get("max_open_positions", 5),
        max_risk_per_trade_pct=risk_cfg.get("max_risk_per_trade_pct", 0.02),
        min_balance_usd=risk_cfg.get("min_balance_usd", 50.0),
    )

    circuit_breaker = CircuitBreaker(
        max_consecutive_losses=risk_cfg.get("max_consecutive_losses", 5),
        max_hourly_losses=risk_cfg.get("max_hourly_losses", 8),
        cooldown_seconds=risk_cfg.get("circuit_cooldown_seconds", 1800.0),
    )

    bot = None

    async def on_position_closed(trade_record):
        if circuit_breaker:
            circuit_breaker.on_trade_result(trade_record.outcome or "FLAT", trade_record.pnl_pct or 0)
        if risk_engine and trade_record.pnl_usd:
            risk_engine.record_daily_pnl(trade_record.pnl_usd)
        if bot:
            await bot.push_trade_update(trade_record)
        logger.info("POSITION LIFECYCLE COMPLETE | %s | %s | pnl=%.4f%%",
                    trade_record.trade_id, trade_record.outcome, (trade_record.pnl_pct or 0) * 100)

    position_manager = PositionManager(
        trade_journal=trade_journal,
        executor=executor,
        on_position_closed=on_position_closed,
        trailing_stop_activation_pct=risk_cfg.get("trailing_stop_activation_pct", 0.01),
        trailing_stop_callback_pct=risk_cfg.get("trailing_stop_callback_pct", 0.005),
        max_holding_time_s=risk_cfg.get("max_holding_time_s", 3600.0),
        check_interval_s=risk_cfg.get("position_check_interval_s", 2.0),
    )

    async def on_signal(sig: dict):
        if bot:
            await bot.push_signal(sig)
        await blockchain.log_signal(sig)

    sg = SignalGenerator(
        symbols=symbols,
        on_signal=on_signal,
        orderbook_depth=system_cfg.get("orderbook_depth", 500),
        imbalance_buy=thresholds["imbalance_ratio_buy"],
        imbalance_sell=thresholds["imbalance_ratio_sell"],
        absorption_multiplier=thresholds["absorption_volume_multiplier"],
        vacuum_min_levels=thresholds["vacuum_min_levels"],
        iceberg_ratio=thresholds["iceberg_volume_multiplier"],
        iceberg_persist_s=thresholds["iceberg_persist_seconds"],
        executor=executor,
        auto_trade_enabled=system_cfg.get("auto_trade_enabled", False),
        apex_enabled=apex_enabled,
        apex_poll_seconds=apex_cfg.get("poll_seconds", 30),
        apex_min_confidence=apex_cfg.get("min_confidence", 85),
        coinglass_api_key=external_cfg.get("coinglass_api_key"),
        twitter_bearer_token=external_cfg.get("twitter_bearer_token"),
        reddit_client_id=external_cfg.get("reddit_client_id"),
        reddit_client_secret=external_cfg.get("reddit_client_secret"),
        whale_alert_api_key=external_cfg.get("whale_alert_api_key"),
        risk_engine=risk_engine,
        trade_journal=trade_journal,
        position_manager=position_manager,
        circuit_breaker=circuit_breaker,
    )

    stream = CombinedStreamManager(
        symbols=symbols,
        streams=["depth@100ms", "aggTrade"],
        orderbook_callback=sg.on_depth_update,
        trade_callback=sg.on_trade_update,
    )
    bot = WeaponBot(
        token=token,
        chat_id=chat_id,
        signal_generator=sg,
        stream_manager=stream,
        allowed_symbols=symbols,
        trade_journal=trade_journal,
        risk_engine=risk_engine,
        circuit_breaker=circuit_breaker,
        position_manager=position_manager,
    )
    app = bot.build()

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    logger.info("Telegram interface online")

    init_task = asyncio.create_task(sg.initialize())
    if os.getenv("REPLAY_MODE", "false").lower() == "true":
        logger.info("REPLAY MODE ENABLED - Starting historical simulation")
        simulator = HistoricalReplaySimulator(sg, bot)
        stream_task = asyncio.create_task(simulator.replay())
    else:
        stream_task = asyncio.create_task(stream.start())
        logger.info("Live data synchronization started in background")

    position_manager.start()
    logger.info("Position manager active")

    logger.info("="*60)
    logger.info("CRYPTZero v5.0 ONLINE")
    logger.info("  Symbols       : %s", ", ".join(symbols))
    logger.info("  Mode          : %s", "TESTNET" if testnet_mode else "LIVE")
    logger.info("  APEX          : %s", "ENABLED" if apex_enabled else "DISABLED")
    logger.info("  Auto-trade    : %s", "ENABLED" if system_cfg.get("auto_trade_enabled") else "DISABLED")
    logger.info("  Risk engine   : ACTIVE (max_dd=%.0f%%, daily_cap=%.0f%%, max_pos=%d)",
                risk_engine.max_portfolio_drawdown_pct * 100,
                risk_engine.max_daily_loss_pct * 100,
                risk_engine.max_open_positions)
    logger.info("  Circuit break : ARMED (max_consec=%d, hourly=%d, cooldown=%.0fs)",
                circuit_breaker.max_consecutive_losses,
                circuit_breaker.max_hourly_losses,
                circuit_breaker.cooldown_seconds)
    logger.info("  Journal       : %s", trade_journal._path)
    logger.info("="*60)

    stop = asyncio.Event()

    def _shutdown(sig_num, frame):
        logger.info("Shutdown signal received (%s)", sig_num)
        stop.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    await stop.wait()
    logger.info("Shutting down...")

    await position_manager.stop()

    stream_task.cancel()
    try:
        await stream_task
    except asyncio.CancelledError:
        pass

    init_task.cancel()
    try:
        await init_task
    except asyncio.CancelledError:
        pass

    await executor.close()
    await sg.close()

    await app.updater.stop()
    await app.stop()
    await app.shutdown()

    journal_stats = trade_journal.stats_summary()
    logger.info("SHUTDOWN STATS: %s", journal_stats)
    logger.info("Clean shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
