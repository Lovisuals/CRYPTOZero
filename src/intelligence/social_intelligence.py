from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections import deque
from typing import Dict, List, Optional

import aiohttp
import numpy as np

logger = logging.getLogger(__name__)


POSITIVE_LEXICON = {
    "bullish",
    "breakout",
    "moon",
    "long",
    "buy",
    "pump",
    "accumulation",
    "adoption",
    "uptrend",
    "strong",
}
NEGATIVE_LEXICON = {
    "bearish",
    "dump",
    "short",
    "sell",
    "crash",
    "rug",
    "fear",
    "hack",
    "downtrend",
    "rejection",
}


class SocialIntelligenceEngine:
    """
    Multi-source social layer with graceful degradation.
    """

    TWITTER_SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"
    REDDIT_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
    REDDIT_SEARCH_URL = "https://oauth.reddit.com/r/{subreddit}/search"
    WHALE_ALERT_URL = "https://api.whale-alert.io/v1/transactions"

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        twitter_bearer_token: Optional[str] = None,
        reddit_client_id: Optional[str] = None,
        reddit_client_secret: Optional[str] = None,
        whale_alert_api_key: Optional[str] = None,
        subreddits: Optional[List[str]] = None,
    ):
        self._external_session = session
        self._session: Optional[aiohttp.ClientSession] = session
        self._session_lock = asyncio.Lock()

        self.twitter_bearer_token = twitter_bearer_token or os.getenv("TWITTER_BEARER_TOKEN")
        self.reddit_client_id = reddit_client_id or os.getenv("REDDIT_CLIENT_ID")
        self.reddit_client_secret = reddit_client_secret or os.getenv("REDDIT_CLIENT_SECRET")
        self.whale_alert_api_key = whale_alert_api_key or os.getenv("WHALE_ALERT_API_KEY")

        self.subreddits = subreddits or ["cryptocurrency", "bitcoin", "ethtrader", "wallstreetbets"]
        self.sentiment_history = deque(maxlen=2000)
        self._reddit_token: Optional[str] = None
        self._reddit_token_expiry: float = 0.0
        self._buzz_baseline: Dict[str, float] = {}

    async def close(self):
        if self._session and (not self._session.closed) and self._external_session is None:
            await self._session.close()

    async def _ensure_session(self):
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()

    def _analyze_sentiment(self, text: str) -> float:
        tokens = re.findall(r"[a-zA-Z]+", text.lower())
        if not tokens:
            return 0.0
        pos = sum(1 for t in tokens if t in POSITIVE_LEXICON)
        neg = sum(1 for t in tokens if t in NEGATIVE_LEXICON)
        raw = (pos - neg) / max(len(tokens), 1)
        return float(max(-1.0, min(1.0, raw * 6)))

    async def fetch_twitter_sentiment(self, symbol: str) -> Dict:
        await self._ensure_session()
        if not self.twitter_bearer_token:
            return self._neutral("TWITTER", symbol)

        assert self._session is not None
        query = f"${symbol} OR #{symbol} -is:retweet lang:en"
        params = {"query": query, "max_results": 100, "tweet.fields": "public_metrics,created_at"}
        headers = {"Authorization": f"Bearer {self.twitter_bearer_token}"}

        try:
            timeout = aiohttp.ClientTimeout(total=6)
            async with self._session.get(
                self.TWITTER_SEARCH_URL, params=params, headers=headers, timeout=timeout
            ) as resp:
                if resp.status != 200:
                    return self._neutral("TWITTER", symbol)
                payload = await resp.json()
        except Exception as exc:
            logger.debug("Twitter sentiment fetch failed: %s", exc)
            return self._neutral("TWITTER", symbol)

        tweets = payload.get("data", [])
        scores = [self._analyze_sentiment(t.get("text", "")) for t in tweets]
        avg_sentiment = float(np.mean(scores)) if scores else 0.0
        buzz = len(tweets)
        self._update_buzz_baseline(f"TWITTER:{symbol}", buzz)

        return {
            "platform": "TWITTER",
            "symbol": symbol,
            "sentiment_score": avg_sentiment,
            "buzz_volume": buzz,
            "buzz_multiplier": self._buzz_multiplier(f"TWITTER:{symbol}", buzz),
            "timestamp": time.time(),
        }

    async def _refresh_reddit_token(self) -> bool:
        if not self.reddit_client_id or not self.reddit_client_secret:
            return False
        if self._reddit_token and time.time() < self._reddit_token_expiry - 30:
            return True
        await self._ensure_session()
        assert self._session is not None

        auth = aiohttp.BasicAuth(self.reddit_client_id, self.reddit_client_secret)
        headers = {"User-Agent": "CRYPTZero-Apex/1.0"}
        data = {"grant_type": "client_credentials"}
        try:
            timeout = aiohttp.ClientTimeout(total=6)
            async with self._session.post(
                self.REDDIT_TOKEN_URL, data=data, headers=headers, auth=auth, timeout=timeout
            ) as resp:
                if resp.status != 200:
                    return False
                payload = await resp.json()
        except Exception as exc:
            logger.debug("Reddit token refresh failed: %s", exc)
            return False

        self._reddit_token = payload.get("access_token")
        expires = int(payload.get("expires_in", 0))
        self._reddit_token_expiry = time.time() + expires
        return bool(self._reddit_token)

    async def fetch_reddit_sentiment(self, symbol: str) -> Dict:
        if not await self._refresh_reddit_token():
            return self._neutral("REDDIT", symbol)

        assert self._session is not None
        headers = {"Authorization": f"bearer {self._reddit_token}", "User-Agent": "CRYPTZero-Apex/1.0"}
        query = f"{symbol} OR ${symbol}"

        tasks = []
        for subreddit in self.subreddits:
            params = {"q": query, "restrict_sr": "on", "sort": "new", "limit": "25"}
            url = self.REDDIT_SEARCH_URL.format(subreddit=subreddit)
            tasks.append(self._session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=6)))

        sentiment_scores: List[float] = []
        total_posts = 0
        for req in tasks:
            try:
                async with req as resp:
                    if resp.status != 200:
                        continue
                    payload = await resp.json()
                    children = payload.get("data", {}).get("children", [])
                    total_posts += len(children)
                    for child in children:
                        d = child.get("data", {})
                        text = f"{d.get('title', '')} {d.get('selftext', '')}"
                        sentiment_scores.append(self._analyze_sentiment(text))
            except Exception:
                continue

        avg_sentiment = float(np.mean(sentiment_scores)) if sentiment_scores else 0.0
        self._update_buzz_baseline(f"REDDIT:{symbol}", total_posts)
        return {
            "platform": "REDDIT",
            "symbol": symbol,
            "sentiment_score": avg_sentiment,
            "buzz_volume": total_posts,
            "buzz_multiplier": self._buzz_multiplier(f"REDDIT:{symbol}", total_posts),
            "timestamp": time.time(),
        }

    async def track_whale_wallets(self, symbol: str) -> Dict:
        await self._ensure_session()
        if not self.whale_alert_api_key:
            return {"signal": "NEUTRAL", "confidence": 50, "net_flow": 0.0, "movements": []}

        assert self._session is not None
        params = {
            "api_key": self.whale_alert_api_key,
            "min_value": 2_000_000,
            "currency": symbol.lower(),
            "start": int(time.time() - 3600),
        }
        try:
            timeout = aiohttp.ClientTimeout(total=6)
            async with self._session.get(self.WHALE_ALERT_URL, params=params, timeout=timeout) as resp:
                if resp.status != 200:
                    return {"signal": "NEUTRAL", "confidence": 50, "net_flow": 0.0, "movements": []}
                payload = await resp.json()
        except Exception:
            return {"signal": "NEUTRAL", "confidence": 50, "net_flow": 0.0, "movements": []}

        movements = payload.get("transactions", [])
        net_flow = 0.0
        parsed = []
        for item in movements:
            amount = float(item.get("amount", 0.0))
            from_owner = (item.get("from", {}) or {}).get("owner_type", "unknown")
            to_owner = (item.get("to", {}) or {}).get("owner_type", "unknown")
            if from_owner == "exchange" and to_owner != "exchange":
                net_flow += amount
                direction = "FROM_EXCHANGE"
            elif to_owner == "exchange" and from_owner != "exchange":
                net_flow -= amount
                direction = "TO_EXCHANGE"
            else:
                direction = "UNKNOWN"
            parsed.append({"amount": amount, "direction": direction, "tx_hash": item.get("hash", "")})

        if net_flow > 1000:
            signal, confidence = "WHALE_ACCUMULATION", 75
        elif net_flow < -1000:
            signal, confidence = "WHALE_DISTRIBUTION", 75
        else:
            signal, confidence = "NEUTRAL", 50

        return {"signal": signal, "confidence": confidence, "net_flow": net_flow, "movements": parsed}

    def generate_social_signal(
        self, twitter_data: Dict, reddit_data: Dict, whale_data: Dict
    ) -> Optional[Dict]:
        social_sentiment = (
            twitter_data.get("sentiment_score", 0.0) * 0.4
            + reddit_data.get("sentiment_score", 0.0) * 0.3
            + (
                1.0
                if whale_data.get("signal") == "WHALE_ACCUMULATION"
                else -1.0 if whale_data.get("signal") == "WHALE_DISTRIBUTION" else 0.0
            )
            * 0.3
        )

        twitter_multiplier = float(twitter_data.get("buzz_multiplier", 1.0))
        reddit_multiplier = float(reddit_data.get("buzz_multiplier", 1.0))
        buzz_multiplier = (twitter_multiplier + reddit_multiplier) / 2.0
        self.sentiment_history.append(social_sentiment)

        if social_sentiment > 0.5 and buzz_multiplier > 1.5:
            return {
                "signal_type": "SOCIAL",
                "direction": "BUY",
                "confidence": min(int(abs(social_sentiment) * 100), 85),
                "rationale": (
                    f"Social sentiment {social_sentiment:.2f}, buzz {buzz_multiplier:.1f}x, "
                    f"whale status {whale_data.get('signal', 'NEUTRAL')}."
                ),
                "leverage_multiplier": 2,
                "sentiment_score": social_sentiment,
                "buzz_multiplier": buzz_multiplier,
                "timestamp": int(time.time() * 1000),
            }

        if social_sentiment < -0.5 and buzz_multiplier > 1.5:
            return {
                "signal_type": "SOCIAL",
                "direction": "SELL",
                "confidence": min(int(abs(social_sentiment) * 100), 85),
                "rationale": (
                    f"Social sentiment {social_sentiment:.2f}, buzz {buzz_multiplier:.1f}x, "
                    f"whale status {whale_data.get('signal', 'NEUTRAL')}."
                ),
                "leverage_multiplier": 2,
                "sentiment_score": social_sentiment,
                "buzz_multiplier": buzz_multiplier,
                "timestamp": int(time.time() * 1000),
            }

        return None

    @staticmethod
    def _neutral(platform: str, symbol: str) -> Dict:
        return {
            "platform": platform,
            "symbol": symbol,
            "sentiment_score": 0.0,
            "buzz_volume": 0,
            "buzz_multiplier": 1.0,
            "timestamp": time.time(),
        }

    def _update_buzz_baseline(self, key: str, sample: float):
        baseline = self._buzz_baseline.get(key)
        if baseline is None:
            self._buzz_baseline[key] = max(1.0, sample)
            return
        self._buzz_baseline[key] = baseline * 0.9 + sample * 0.1

    def _buzz_multiplier(self, key: str, sample: float) -> float:
        baseline = max(1.0, self._buzz_baseline.get(key, 1.0))
        return max(0.1, sample / baseline)
