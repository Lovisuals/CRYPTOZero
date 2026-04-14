from __future__ import annotations

import json
import logging
import time
from typing import Dict, List, Optional

import openai

logger = logging.getLogger(__name__)

class MetaCognitiveOptimizer:

    def __init__(self, openai_api_key: str, model: str = "gpt-4o-mini"):
        self.client = openai.AsyncOpenAI(api_key=openai_api_key)
        self.model = model
        self.performance_log: List[Dict] = []

    @staticmethod
    def _format_trades_for_llm(trades: List[Dict]) -> str:
        compact = []
        for t in trades[-100:]:
            compact.append(
                {
                    "signal_type": t.get("signal_type") or t.get("type"),
                    "direction": t.get("direction"),
                    "entry_price": t.get("entry_price"),
                    "exit_price": t.get("exit_price"),
                    "pnl_pct": t.get("pnl_pct"),
                    "timeframe": t.get("timeframe"),
                    "result": t.get("result"),
                    "confidence": t.get("confidence"),
                    "timestamp": t.get("timestamp"),
                }
            )
        return json.dumps(compact, separators=(",", ":"))

    async def analyze_performance(self, trades: List[Dict]) -> Dict:
        if not trades:
            return {
                "win_rate_by_signal": {},
                "best_timeframes": [],
                "high_win_confluences": [],
                "parameter_recommendations": {},
                "new_ideas": [],
            }

        trade_summary = self._format_trades_for_llm(trades)
        prompt = f"""
You are an expert quantitative trading analyst.
Analyze the following 100 trades from a multi-signal crypto system:
{trade_summary}

Provide:
1. Win rate by signal type
2. Best performing timeframes
3. Confluence patterns with >70% win rate
4. Recommended parameter adjustments
5. New signal combination ideas

Return ONLY valid JSON:
{{
  "win_rate_by_signal": {{}},
  "best_timeframes": [],
  "high_win_confluences": [],
  "parameter_recommendations": {{}},
  "new_ideas": []
}}
"""
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a quantitative trading analyst specializing in crypto markets.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content or "{}"
            return json.loads(content)
        except Exception as exc:
            logger.error("Meta-optimizer analysis failed: %s", exc)
            return {
                "win_rate_by_signal": {},
                "best_timeframes": [],
                "high_win_confluences": [],
                "parameter_recommendations": {},
                "new_ideas": [],
                "error": str(exc),
            }

    async def optimize_strategy(self, analysis: Dict) -> Dict:
        applied = 0
        for signal_type, win_rate in analysis.get("win_rate_by_signal", {}).items():
            if isinstance(win_rate, (float, int)) and win_rate > 0.75:
                logger.info("Meta optimization: boost %s (win_rate=%.2f)", signal_type, win_rate)
                applied += 1

        confluences = analysis.get("high_win_confluences", []) or []
        for confluence in confluences:
            logger.info("Meta optimization: new confluence candidate %s", confluence)

        return {
            "optimizations_applied": applied,
            "new_signals_created": len(analysis.get("new_ideas", []) or []),
            "timestamp": time.time(),
        }
