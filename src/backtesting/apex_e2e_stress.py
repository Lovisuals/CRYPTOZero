import argparse
import asyncio
import random
import statistics
import time
from collections import Counter
from types import SimpleNamespace
from typing import Dict, List

from src.notifications.telegram_bot import WeaponBot
from src.processing.signal_generator import SignalGenerator


def _depth_update(seq: int, price: float, bullish: bool, ts_ms: int) -> dict:
    bid_base = 6.0 if bullish else 1.8
    ask_base = 1.8 if bullish else 6.0
    bids = [[f"{price - i * 0.5:.2f}", f"{bid_base + i * 0.4:.5f}"] for i in range(1, 11)]
    asks = [[f"{price + i * 0.5:.2f}", f"{ask_base + i * 0.4:.5f}"] for i in range(1, 11)]
    return {"U": seq, "u": seq, "b": bids, "a": asks, "E": ts_ms}


def _trade_update(price: float, qty: float, is_buyer_maker: bool, ts_ms: int) -> dict:
    return {"E": ts_ms, "p": f"{price:.2f}", "q": f"{qty:.6f}", "m": is_buyer_maker}


def _validate_signal(sig: dict) -> List[str]:
    problems: List[str] = []
    required = {"symbol", "type", "direction", "confidence", "timestamp"}
    missing = [k for k in required if k not in sig]
    if missing:
        problems.append(f"missing_keys:{','.join(missing)}")
    if sig.get("direction") not in {"BUY", "SELL"}:
        problems.append("invalid_direction")
    try:
        c = float(sig.get("confidence", -1))
        if not (0 <= c <= 100):
            problems.append("invalid_confidence_range")
    except Exception:
        problems.append("invalid_confidence_type")
    return problems


class SignalProbe:
    def __init__(self):
        self.total = 0
        self.by_type: Counter = Counter()
        self.latencies_ms: List[float] = []
        self.validation_errors: List[str] = []

    async def on_signal(self, sig: dict):
        self.total += 1
        self.by_type[sig.get("type", "UNKNOWN")] += 1
        now_ms = time.time() * 1000.0
        self.latencies_ms.append(max(0.0, now_ms - float(sig.get("timestamp", now_ms))))
        self.validation_errors.extend(_validate_signal(sig))


class _DummyTelegramClient:
    def __init__(self):
        self.sent = 0

    async def send_message(self, chat_id: int, text: str):
        _ = (chat_id, text)
        self.sent += 1


async def core_pipeline_stress(symbol: str, loops: int) -> Dict:
    probe = SignalProbe()
    sg = SignalGenerator(
        symbols=[symbol],
        on_signal=probe.on_signal,
        auto_trade_enabled=False,
        apex_enabled=False,
    )
    sg.COOLDOWN = 0
    ob = sg.orderbooks[symbol]
    ob._synced = True
    ob.last_update_id = 0

    seq = 1
    ts0 = int(time.time() * 1000)
    px = 65000.0

    started = time.perf_counter()
    for i in range(loops):
        px += random.uniform(-3.5, 3.5)
        bullish = (i % 4) in (0, 1)
        depth = _depth_update(seq=seq, price=px, bullish=bullish, ts_ms=ts0 + i)
        await sg.on_depth_update(symbol, depth)
        trade = _trade_update(
            price=px + random.uniform(-0.8, 0.8),
            qty=random.uniform(0.01, 0.8),
            is_buyer_maker=(i % 2 == 0),
            ts_ms=ts0 + i,
        )
        await sg.on_trade_update(symbol, trade)
        seq += 1
    elapsed = time.perf_counter() - started

    latencies = probe.latencies_ms or [0.0]
    summary = {
        "loops": loops,
        "events_processed": loops * 2,
        "signals_emitted": probe.total,
        "signals_by_type": dict(probe.by_type),
        "throughput_events_per_sec": round((loops * 2) / max(elapsed, 1e-9), 2),
        "callback_latency_ms_p50": round(statistics.median(latencies), 3),
        "callback_latency_ms_p95": round(sorted(latencies)[int(0.95 * (len(latencies) - 1))], 3),
        "validation_error_count": len(probe.validation_errors),
    }
    await sg.close()
    return summary


async def apex_fusion_stress(symbol: str, iterations: int) -> Dict:
    probe = SignalProbe()
    sg = SignalGenerator(
        symbols=[symbol],
        on_signal=probe.on_signal,
        auto_trade_enabled=False,
        apex_enabled=True,
        apex_poll_seconds=99999,
    )
    sg.COOLDOWN = 0
    ob = sg.orderbooks[symbol]
    ob._synced = True
    ob.last_update_id = 0
    ob.apply_update(_depth_update(seq=1, price=65000.0, bullish=True, ts_ms=int(time.time() * 1000)))

    now = time.time()
    sg._recent_signals[symbol] = {
        "IMBALANCE": {"direction": "BUY", "confidence": 70, "_stored_at": now},
        "CVD": {"direction": "BUY", "confidence": 78, "_stored_at": now},
        "ABSORPTION": {"direction": "BUY", "confidence": 82, "_stored_at": now},
    }

    async def _liq(_symbol: str, current_price: float):
        return {
            "signal_type": "LIQUIDATION",
            "direction": "BUY",
            "confidence": 84,
            "liquidity_usd": 25_000_000,
            "cluster_price": current_price * 0.997,
            "entry_price": current_price * 0.996,
            "stop_loss": current_price * 0.992,
            "take_profit": current_price * 1.006,
        }

    async def _time(_symbol: str):
        return {
            "signal_type": "TIME_DELIVERY",
            "direction": "BUY",
            "confidence": 86,
            "signal": "STRONG_BUY",
            "bullish_count": 5,
            "bearish_count": 1,
        }

    async def _social(_symbol: str):
        return {
            "signal_type": "SOCIAL",
            "direction": "BUY",
            "confidence": 80,
            "sentiment_score": 0.71,
            "buzz_multiplier": 2.3,
        }

    sg._build_liquidation_signal = _liq  # type: ignore[attr-defined]
    sg._build_time_signal = _time  # type: ignore[attr-defined]
    sg._build_social_signal = _social  # type: ignore[attr-defined]

    started = time.perf_counter()
    for _ in range(iterations):
        await sg._run_apex_for_symbol(symbol)
    elapsed = time.perf_counter() - started
    latencies = probe.latencies_ms or [0.0]

    summary = {
        "iterations": iterations,
        "signals_emitted": probe.total,
        "apex_fusion_signals": probe.by_type.get("APEX_FUSION", 0),
        "throughput_iterations_per_sec": round(iterations / max(elapsed, 1e-9), 2),
        "callback_latency_ms_p50": round(statistics.median(latencies), 3),
        "callback_latency_ms_p95": round(sorted(latencies)[int(0.95 * (len(latencies) - 1))], 3),
        "validation_error_count": len(probe.validation_errors),
    }
    await sg.close()
    return summary


async def telegram_queue_stress(samples: int) -> Dict:
    bot = WeaponBot(
        token="dummy",
        chat_id=1,
        signal_generator=None,
        stream_manager=SimpleNamespace(stats=lambda: {}),
        allowed_symbols=["BTCUSDT"],
    )
    fake_client = _DummyTelegramClient()
    bot._app = SimpleNamespace(bot=fake_client)

    started = time.perf_counter()
    for i in range(samples):
        sig = {
            "symbol": "BTCUSDT",
            "type": "APEX_FUSION" if i % 3 == 0 else "IMBALANCE",
            "direction": "BUY" if i % 2 == 0 else "SELL",
            "confidence": 88 if i % 3 == 0 else 70,
            "timestamp": int(time.time() * 1000),
            "supporting_signals": ["LIQUIDATION", "TIME_DELIVERY", "SOCIAL"],
            "signal_count": 3,
            "position_multiplier": 0.03,
            "leverage_recommended": 3,
            "rationale": "Deterministic queue stress probe.",
        }
        await bot.push_signal(sig)
    await asyncio.wait_for(bot._send_queue.join(), timeout=30)
    elapsed = time.perf_counter() - started
    delivered = fake_client.sent

    if bot._sender_task:
        bot._sender_task.cancel()
        try:
            await bot._sender_task
        except asyncio.CancelledError:
            pass

    return {
        "samples": samples,
        "delivered": delivered,
        "delivery_success_ratio": round(delivered / max(samples, 1), 4),
        "throughput_msgs_per_sec": round(samples / max(elapsed, 1e-9), 2),
    }


async def run_suite(core_loops: int, apex_iterations: int, telegram_samples: int):
    core = await core_pipeline_stress("BTCUSDT", core_loops)
    apex = await apex_fusion_stress("BTCUSDT", apex_iterations)
    tg = await telegram_queue_stress(telegram_samples)

    smoke_guns = []
    if core["validation_error_count"] > 0:
        smoke_guns.append("core_signal_schema_validation_failed")
    if apex["validation_error_count"] > 0:
        smoke_guns.append("apex_signal_schema_validation_failed")
    if tg["delivery_success_ratio"] < 1.0:
        smoke_guns.append("telegram_delivery_loss_detected")
    if core["signals_emitted"] == 0:
        smoke_guns.append("core_pipeline_no_signals")
    if apex["apex_fusion_signals"] == 0:
        smoke_guns.append("apex_pipeline_no_fusion_signal")

    verdict = "PASS" if not smoke_guns else "FAIL"
    print("APEX_E2E_STRESS_VERDICT=", verdict)
    print("CORE_SUMMARY=", core)
    print("APEX_SUMMARY=", apex)
    print("TELEGRAM_SUMMARY=", tg)
    print("SMOKING_GUNS=", smoke_guns)


def parse_args():
    parser = argparse.ArgumentParser(description="Massive end-to-end stress harness for CRYPTZero APEX.")
    parser.add_argument("--core-loops", type=int, default=40000)
    parser.add_argument("--apex-iterations", type=int, default=4000)
    parser.add_argument("--telegram-samples", type=int, default=3000)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run_suite(args.core_loops, args.apex_iterations, args.telegram_samples))
