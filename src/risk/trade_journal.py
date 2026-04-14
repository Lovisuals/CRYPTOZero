from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

@dataclass
class TradeRecord:
    trade_id: str
    symbol: str
    signal_type: str
    direction: str
    confidence: float
    entry_price: float
    entry_ts: float
    quantity: float
    leverage: int = 1
    stop_loss: float = 0.0
    take_profit: float = 0.0
    exit_price: Optional[float] = None
    exit_ts: Optional[float] = None
    pnl_usd: Optional[float] = None
    pnl_pct: Optional[float] = None
    outcome: Optional[str] = None
    fees_usd: float = 0.0
    slippage_pct: float = 0.0
    exit_reason: Optional[str] = None
    order_id: Optional[str] = None
    metadata: Dict = field(default_factory=dict)

    @property
    def is_open(self) -> bool:
        return self.exit_price is None

    @property
    def holding_time_s(self) -> float:
        end = self.exit_ts or time.time()
        return end - self.entry_ts

    def close(self, exit_price: float, exit_reason: str, fees_usd: float = 0.0):
        self.exit_price = exit_price
        self.exit_ts = time.time()
        self.exit_reason = exit_reason
        self.fees_usd += fees_usd
        raw = (exit_price - self.entry_price) / self.entry_price
        if self.direction == "SELL":
            raw = -raw
        raw *= self.leverage
        self.pnl_pct = raw - (self.fees_usd / (self.entry_price * self.quantity + 1e-9))
        self.pnl_usd = self.pnl_pct * self.entry_price * self.quantity
        self.outcome = "WIN" if self.pnl_pct > 0 else ("LOSS" if self.pnl_pct < -0.0005 else "FLAT")

class TradeJournal:
    def __init__(self, journal_path: str = "data/trade_journal.jsonl"):
        self._path = Path(journal_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._open_trades: Dict[str, TradeRecord] = {}
        self._closed_trades: List[TradeRecord] = []
        self._trade_counter = 0
        self._load_existing()

    def _load_existing(self):
        if not self._path.exists():
            return
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    data = json.loads(line)
                    rec = TradeRecord(**{k: v for k, v in data.items() if k in TradeRecord.__dataclass_fields__})
                    if rec.is_open:
                        self._open_trades[rec.trade_id] = rec
                    else:
                        self._closed_trades.append(rec)
                    self._trade_counter += 1
        except Exception as exc:
            logger.error("Journal load error: %s", exc)

    def open_trade(
        self,
        symbol: str,
        signal_type: str,
        direction: str,
        confidence: float,
        entry_price: float,
        quantity: float,
        leverage: int = 1,
        stop_loss: float = 0.0,
        take_profit: float = 0.0,
        order_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> TradeRecord:
        self._trade_counter += 1
        trade_id = f"{symbol}_{signal_type}_{int(time.time()*1000)}_{self._trade_counter}"
        rec = TradeRecord(
            trade_id=trade_id,
            symbol=symbol,
            signal_type=signal_type,
            direction=direction,
            confidence=confidence,
            entry_price=entry_price,
            entry_ts=time.time(),
            quantity=quantity,
            leverage=leverage,
            stop_loss=stop_loss,
            take_profit=take_profit,
            order_id=order_id,
            metadata=metadata or {},
        )
        self._open_trades[trade_id] = rec
        self._persist(rec)
        logger.info("TRADE OPENED | %s | %s %s @ %.2f | qty=%.6f | SL=%.2f TP=%.2f",
                     trade_id, direction, symbol, entry_price, quantity, stop_loss, take_profit)
        return rec

    def close_trade(self, trade_id: str, exit_price: float, exit_reason: str, fees_usd: float = 0.0) -> Optional[TradeRecord]:
        rec = self._open_trades.pop(trade_id, None)
        if rec is None:
            logger.warning("Cannot close unknown trade: %s", trade_id)
            return None
        rec.close(exit_price, exit_reason, fees_usd)
        self._closed_trades.append(rec)
        self._persist(rec)
        logger.info("TRADE CLOSED | %s | %s | pnl=%.4f%% ($%.2f) | reason=%s",
                     trade_id, rec.outcome, (rec.pnl_pct or 0) * 100, rec.pnl_usd or 0, exit_reason)
        return rec

    def _persist(self, rec: TradeRecord):
        try:
            with open(self._path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(asdict(rec), separators=(",", ":"), default=str) + "\n")
        except Exception as exc:
            logger.error("Journal write error: %s", exc)

    @property
    def open_trades(self) -> List[TradeRecord]:
        return list(self._open_trades.values())

    @property
    def closed_trades(self) -> List[TradeRecord]:
        return list(self._closed_trades)

    def get_open_for_symbol(self, symbol: str) -> List[TradeRecord]:
        return [t for t in self._open_trades.values() if t.symbol == symbol]

    def rolling_win_rate(self, window: int = 50) -> float:
        recent = self._closed_trades[-window:]
        if not recent:
            return 0.5
        wins = sum(1 for t in recent if t.outcome == "WIN")
        decided = sum(1 for t in recent if t.outcome in ("WIN", "LOSS"))
        return wins / decided if decided > 0 else 0.5

    def rolling_profit_factor(self, window: int = 50) -> float:
        recent = self._closed_trades[-window:]
        gains = sum(t.pnl_usd for t in recent if t.pnl_usd and t.pnl_usd > 0)
        losses = sum(abs(t.pnl_usd) for t in recent if t.pnl_usd and t.pnl_usd < 0)
        return gains / losses if losses > 0 else float("inf")

    def total_pnl_usd(self) -> float:
        return sum(t.pnl_usd for t in self._closed_trades if t.pnl_usd)

    def equity_curve(self) -> List[float]:
        curve = [0.0]
        for t in self._closed_trades:
            if t.pnl_usd is not None:
                curve.append(curve[-1] + t.pnl_usd)
        return curve

    def max_drawdown_usd(self) -> float:
        curve = self.equity_curve()
        peak = 0.0
        max_dd = 0.0
        for val in curve:
            peak = max(peak, val)
            dd = peak - val
            max_dd = max(max_dd, dd)
        return max_dd

    def stats_summary(self) -> Dict:
        closed = self._closed_trades
        if not closed:
            return {"total_trades": 0, "open_trades": len(self._open_trades)}
        wins = [t for t in closed if t.outcome == "WIN"]
        losses = [t for t in closed if t.outcome == "LOSS"]
        decided = len(wins) + len(losses)
        return {
            "total_trades": len(closed),
            "open_trades": len(self._open_trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": len(wins) / decided if decided > 0 else 0.0,
            "total_pnl_usd": self.total_pnl_usd(),
            "profit_factor": self.rolling_profit_factor(len(closed)),
            "max_drawdown_usd": self.max_drawdown_usd(),
            "avg_win_usd": sum(t.pnl_usd for t in wins if t.pnl_usd) / len(wins) if wins else 0.0,
            "avg_loss_usd": sum(t.pnl_usd for t in losses if t.pnl_usd) / len(losses) if losses else 0.0,
            "rolling_win_rate_50": self.rolling_win_rate(50),
        }
