from __future__ import annotations

import time
from typing import Dict, List, Optional

class ApexSignalFusion:

    def __init__(self):
        self.signal_weights = {
            "IMBALANCE": 0.12,
            "CVD": 0.15,
            "ABSORPTION": 0.18,
            "ICEBERG": 0.10,
            "VACUUM": 0.12,
            "LIQUIDATION": 0.15,
            "TIME_DELIVERY": 0.10,
            "SOCIAL": 0.08,
        }
        self.dynamic_weights = self.signal_weights.copy()

    def update_weights(self, updates: Dict[str, float]):
        for key, value in updates.items():
            if key in self.dynamic_weights and value > 0:
                self.dynamic_weights[key] = float(value)

    def fuse_signals(self, signals: Dict[str, Dict], min_agreeing: int = 3) -> Optional[Dict]:
        if not signals:
            return None

        normalized = {}
        for k, sig in signals.items():
            signal_type = sig.get("signal_type") or sig.get("type") or k
            normalized[k] = {**sig, "signal_type": signal_type}

        buy_signals = [s for s in normalized.values() if s.get("direction") == "BUY"]
        sell_signals = [s for s in normalized.values() if s.get("direction") == "SELL"]
        if len(buy_signals) < min_agreeing and len(sell_signals) < min_agreeing:
            return None

        if len(buy_signals) >= len(sell_signals):
            direction = "BUY"
            supporting_signals = buy_signals
        else:
            direction = "SELL"
            supporting_signals = sell_signals

        weighted_sum = 0.0
        weight_sum = 0.0
        for signal in supporting_signals:
            signal_type = signal["signal_type"]
            w = self.dynamic_weights.get(signal_type, 0.1)
            weighted_sum += float(signal.get("confidence", 0)) * w
            weight_sum += w
        if weight_sum <= 0:
            return None

        composite_confidence = weighted_sum / weight_sum
        if composite_confidence < 75:
            return None

        position_multiplier = self._calculate_kelly_size(
            win_prob=composite_confidence / 100.0,
            win_loss_ratio=2.0,
        )

        return {
            "signal_type": "APEX_FUSION",
            "direction": direction,
            "confidence": int(composite_confidence),
            "supporting_signals": [s["signal_type"] for s in supporting_signals],
            "signal_count": len(supporting_signals),
            "position_multiplier": position_multiplier,
            "leverage_recommended": min(max(int(position_multiplier * 50), 1), 10),
            "rationale": self._generate_rationale(supporting_signals),
            "timestamp": int(time.time() * 1000),
        }

    def _calculate_kelly_size(self, win_prob: float, win_loss_ratio: float) -> float:
        p = win_prob
        q = 1 - p
        b = win_loss_ratio
        kelly_fraction = (b * p - q) / b
        safe_fraction = kelly_fraction * 0.25
        return max(0.01, min(safe_fraction, 0.10))

    @staticmethod
    def _generate_rationale(signals: List[Dict]) -> str:
        parts = []
        for s in signals:
            parts.append(f"{s.get('signal_type', 'UNKNOWN')} ({int(s.get('confidence', 0))}%)")
        return f"{len(signals)} signals aligned: " + ", ".join(parts)
