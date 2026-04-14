from __future__ import annotations

import logging
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class RiskEngine:

    def __init__(
        self,
        max_portfolio_drawdown_pct: float = 0.10,
        max_daily_loss_pct: float = 0.05,
        max_open_positions: int = 5,
        max_risk_per_trade_pct: float = 0.02,
        max_correlated_exposure_pct: float = 0.06,
        min_balance_usd: float = 50.0,
    ):
        self.max_portfolio_drawdown_pct = max_portfolio_drawdown_pct
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_open_positions = max_open_positions
        self.max_risk_per_trade_pct = max_risk_per_trade_pct
        self.max_correlated_exposure_pct = max_correlated_exposure_pct
        self.min_balance_usd = min_balance_usd

        self._peak_equity: float = 0.0
        self._current_equity: float = 0.0
        self._daily_start_equity: float = 0.0
        self._daily_pnl: float = 0.0
        self._day_start_ts: float = 0.0
        self._rejections: int = 0
        self._approvals: int = 0

    def update_equity(self, balance_usd: float):
        self._current_equity = balance_usd
        self._peak_equity = max(self._peak_equity, balance_usd)
        if self._daily_start_equity == 0:
            self._daily_start_equity = balance_usd
            self._day_start_ts = time.time()

    def record_daily_pnl(self, pnl_usd: float):
        self._daily_pnl += pnl_usd
        now = time.time()
        if now - self._day_start_ts > 86400:
            self._daily_pnl = 0.0
            self._daily_start_equity = self._current_equity
            self._day_start_ts = now

    def check_risk(
        self,
        signal: Dict,
        open_position_count: int,
        balance_usd: float,
        circuit_breaker_tripped: bool = False,
    ) -> Dict:
        self.update_equity(balance_usd)

        if circuit_breaker_tripped:
            self._rejections += 1
            return {"approved": False, "reason": "CIRCUIT_BREAKER_ACTIVE", "position_size_pct": 0.0}

        if balance_usd < self.min_balance_usd:
            self._rejections += 1
            return {"approved": False, "reason": f"BALANCE_BELOW_MINIMUM (${balance_usd:.2f} < ${self.min_balance_usd:.2f})", "position_size_pct": 0.0}

        if open_position_count >= self.max_open_positions:
            self._rejections += 1
            return {"approved": False, "reason": f"MAX_POSITIONS_REACHED ({open_position_count}/{self.max_open_positions})", "position_size_pct": 0.0}

        if self._peak_equity > 0:
            drawdown = (self._peak_equity - self._current_equity) / self._peak_equity
            if drawdown >= self.max_portfolio_drawdown_pct:
                self._rejections += 1
                return {"approved": False, "reason": f"PORTFOLIO_DRAWDOWN_LIMIT ({drawdown:.1%} >= {self.max_portfolio_drawdown_pct:.1%})", "position_size_pct": 0.0}

        if self._daily_start_equity > 0:
            daily_loss_pct = abs(min(0, self._daily_pnl)) / self._daily_start_equity
            if daily_loss_pct >= self.max_daily_loss_pct:
                self._rejections += 1
                return {"approved": False, "reason": f"DAILY_LOSS_LIMIT ({daily_loss_pct:.1%} >= {self.max_daily_loss_pct:.1%})", "position_size_pct": 0.0}

        confidence = float(signal.get("confidence", 0))
        base_risk = self.max_risk_per_trade_pct
        if confidence >= 85:
            risk_pct = base_risk * 1.5
        elif confidence >= 75:
            risk_pct = base_risk * 1.0
        else:
            risk_pct = base_risk * 0.5

        risk_pct = min(risk_pct, self.max_risk_per_trade_pct * 2)

        self._approvals += 1
        logger.info("RISK APPROVED | conf=%.0f%% | risk=%.2f%% | open=%d/%d | equity=$%.2f",
                     confidence, risk_pct * 100, open_position_count, self.max_open_positions, balance_usd)

        return {"approved": True, "reason": "APPROVED", "position_size_pct": risk_pct}

    def calculate_position_size(
        self,
        balance_usd: float,
        risk_pct: float,
        entry_price: float,
        stop_loss_price: float,
    ) -> float:
        risk_amount = balance_usd * risk_pct
        stop_distance = abs(entry_price - stop_loss_price)
        if stop_distance <= 0:
            stop_distance = entry_price * 0.015
        quantity = risk_amount / stop_distance
        return quantity

    def stats(self) -> Dict:
        return {
            "peak_equity": self._peak_equity,
            "current_equity": self._current_equity,
            "drawdown_pct": (self._peak_equity - self._current_equity) / self._peak_equity if self._peak_equity > 0 else 0.0,
            "daily_pnl": self._daily_pnl,
            "approvals": self._approvals,
            "rejections": self._rejections,
            "approval_rate": self._approvals / (self._approvals + self._rejections) if (self._approvals + self._rejections) > 0 else 1.0,
        }
