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

    bot = None

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

    logger.info("System online - monitoring %d symbols", len(symbols))
    stop = asyncio.Event()

    def _shutdown(sig_num, frame):
        logger.info("Shutdown signal received (%s)", sig_num)
        stop.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    await stop.wait()
    logger.info("Shutting down...")

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
    logger.info("Clean shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
