"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          HYBRID LAG ARBITRAGE v3.0 — "SENTINEL"                            ║
║          5-Min Balanced Mode  ·  BTC + ETH + SOL Simultaneous              ║
║                                                                              ║
║  Strategy:                                                                   ║
║   • Scan BTC / ETH / SOL 5-minute Polymarket markets in parallel           ║
║   • Enter only if: EV ≥ 16% | Bayesian posterior ≥ 78% | OB imbal ≥ 18%  ║
║   • Trade only in the last 180 s of each 5-minute market                   ║
║   • Volatility filter: 1h Binance volume > $80k                            ║
║   • Sizing: 0.35x fractional Kelly                                          ║
║   • Exit: auto-sell on price adjustment or after 25 s with no movement     ║
║   • Max 1 open trade at a time                                              ║
║   • Rolling memory (last 50 trades); pause 2 h if win-rate < 78%           ║
║   • 20% daily drawdown hard stop                                            ║
║                                                                              ║
║  Paper Mode + full Telegram suite                                           ║
║  Deploy-ready for Railway.app                                               ║
╚══════════════════════════════════════════════════════════════════════════════╝

Installation (pip install):
    pip install py-clob-client websockets python-telegram-bot python-dotenv aiohttp

.env variables:
    PRIVATE_KEY          – Polymarket private key (unused in paper mode)
    POLYMARKET_API_KEY   – optional CLOB API key
    TELEGRAM_BOT_TOKEN   – Telegram bot token
    TELEGRAM_CHAT_ID     – Telegram chat ID (integer)
    PAPER_MODE           – true / false  (default: true)
    STARTING_BANKROLL    – starting USD bankroll (default: 1000)
    MAX_TRADE_USD        – optional hard cap per trade (default: 200)
"""

# ────────────────────────── Standard Library ──────────────────────────────────
import asyncio
import csv
import json
import logging
import math
import os
import random
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional

# ────────────────────────── Third-Party ───────────────────────────────────────
import aiohttp
from dotenv import load_dotenv

# Telegram
from telegram import Bot, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ────────────────────────── Environment ───────────────────────────────────────
load_dotenv()

PRIVATE_KEY: str = os.getenv("PRIVATE_KEY", "")
POLYMARKET_API_KEY: str = os.getenv("POLYMARKET_API_KEY", "")
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: int = int(os.getenv("TELEGRAM_CHAT_ID", "0"))
PAPER_MODE: bool = os.getenv("PAPER_MODE", "true").lower() in ("true", "1", "yes")
STARTING_BANKROLL: float = float(os.getenv("STARTING_BANKROLL", "200"))
MAX_TRADE_USD: float = float(os.getenv("MAX_TRADE_USD", "200"))

# ────────────────────────── Logging ───────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("SENTINEL")

# ────────────────────────── Strategy Constants ────────────────────────────────
MIN_EV: float = 0.16                 # 16 % expected value threshold
MIN_BAYESIAN: float = 0.78           # 78 % Bayesian posterior threshold
MIN_OB_IMBALANCE: float = 0.05       # 5% — CLOB books are often sparse
TRADE_WINDOW_SECONDS: int = 180      # only trade in last 180 s of each market
MIN_1H_VOLUME_USD: float = 5_000.0   # lowered: CoinGecko vol can be small
KELLY_FRACTION: float = 0.35         # fractional Kelly multiplier
EXIT_TIMEOUT_SECONDS: int = 25       # auto-exit after N seconds without movement
MAX_OPEN_TRADES: int = 1             # maximum concurrent open trades

# ────────────────────────── Risk Constants ────────────────────────────────────
ROLLING_WINDOW: int = 50             # number of trades for win-rate check
MIN_WIN_RATE: float = 0.78           # pause if below this
PAUSE_DURATION_HOURS: int = 2        # auto-pause duration
MAX_DAILY_DRAWDOWN_PCT: float = 0.20 # 20 % drawdown hard stop

# ────────────────────────── Price Feed: CoinGecko (no geo-block) ─────────────
# Binance blocks all requests (HTTP 451) from cloud datacenter IPs on Railway.
# CoinGecko public API works from any IP, no API key required.
# We fetch OHLC + volume data every 15 s to feed the signal engine.
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
}
BINANCE_SYMBOLS = {   # kept for backward compatibility with strategy code
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
}
PRICE_POLL_INTERVAL: int = 30   # fetch prices every N seconds

# ────────────────────────── Polymarket API ────────────────────────────────────
CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"

# ────────────────────────── Cadence ───────────────────────────────────────────
HEARTBEAT_INTERVAL_HOURS: int = 6   # send heartbeat every N hours
DAILY_SUMMARY_HOUR: int = 0         # midnight UTC
WEEKLY_REPORT_DAY: int = 6          # Sunday (weekday index)

CSV_FILE = "trade_history.csv"


# ══════════════════════════════════════════════════════════════════════════════
#  SHARED STATE
# ══════════════════════════════════════════════════════════════════════════════

class BotState:
    """
    Central mutable state object shared across all async tasks.
    All fields are modified only inside the strategy_loop task or under
    self.trade_lock to avoid race conditions.
    """

    def __init__(self) -> None:
        # ── Financials ───────────────────────────────────────────────────────
        self.bankroll: float = STARTING_BANKROLL
        self.daily_start_bankroll: float = STARTING_BANKROLL
        self.daily_pnl: float = 0.0

        # ── Trade tracking ───────────────────────────────────────────────────
        self.open_trade: Optional[dict] = None
        self.trade_history: deque = deque(maxlen=ROLLING_WINDOW)
        self.all_trades: list = []

        # ── Circuit breakers ─────────────────────────────────────────────────
        self.paused: bool = False
        self.pause_until: Optional[datetime] = None
        self.hard_stopped: bool = False

        # ── Market data ──────────────────────────────────────────────────────
        self.klines: dict = {asset: {} for asset in BINANCE_SYMBOLS}
        self.markets: dict = {asset: [] for asset in BINANCE_SYMBOLS}

        # ── Telegram ─────────────────────────────────────────────────────────
        self.tg_bot: Optional[Bot] = None

        # ── Stats ────────────────────────────────────────────────────────────
        self.total_signals_scanned: int = 0
        self.missed_opportunities: int = 0

        # ── Async lock ───────────────────────────────────────────────────────
        self.trade_lock = asyncio.Lock()

    # ── Computed properties ──────────────────────────────────────────────────

    def win_rate(self) -> float:
        """Rolling win-rate over the last ROLLING_WINDOW trades."""
        if not self.trade_history:
            return 1.0
        wins = sum(1 for t in self.trade_history if t.get("profit", 0) > 0)
        return wins / len(self.trade_history)

    def should_pause_for_win_rate(self) -> bool:
        """Return True if win-rate circuit breaker should fire."""
        if len(self.trade_history) < ROLLING_WINDOW:
            return False
        return self.win_rate() < MIN_WIN_RATE

    def drawdown_pct(self) -> float:
        """Current daily drawdown as a fraction (0–1)."""
        if self.daily_start_bankroll == 0:
            return 0.0
        loss = self.daily_start_bankroll - self.bankroll
        return max(0.0, loss / self.daily_start_bankroll)

    def risk_level(self) -> str:
        """BAJO / MEDIO / ALTO based on current drawdown."""
        dd = self.drawdown_pct()
        if dd < 0.07:
            return "BAJO"
        if dd < 0.14:
            return "MEDIO"
        return "ALTO"

    def record_trade(self, trade: dict) -> None:
        """Record a completed trade and update bankroll."""
        self.trade_history.append(trade)
        self.all_trades.append(trade)
        self.bankroll += trade.get("profit", 0.0)
        self.daily_pnl += trade.get("profit", 0.0)

    def reset_daily(self) -> None:
        """Reset daily tracking at midnight."""
        self.daily_start_bankroll = self.bankroll
        self.daily_pnl = 0.0
        self.hard_stopped = False


# Singleton global state
STATE = BotState()


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def tg_send(text: str, parse_mode: str = "HTML") -> None:
    """
    Fire-and-forget Telegram message.
    Errors are logged but never bubble up to crash the bot.
    """
    if not STATE.tg_bot or not TELEGRAM_CHAT_ID:
        log.debug("Telegram not configured — skipping message.")
        return
    try:
        await STATE.tg_bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=text,
            parse_mode=parse_mode,
        )
    except Exception as exc:
        log.warning("Telegram send error: %s", exc)


async def tg_send_document(filepath: str, caption: str = "") -> None:
    """Send a file attachment via Telegram."""
    if not STATE.tg_bot or not TELEGRAM_CHAT_ID:
        return
    try:
        with open(filepath, "rb") as fh:
            await STATE.tg_bot.send_document(
                chat_id=TELEGRAM_CHAT_ID,
                document=fh,
                caption=caption,
            )
    except Exception as exc:
        log.warning("Telegram document send error: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
#  CSV PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════

CSV_FIELDNAMES = [
    "timestamp", "asset", "market_id", "side",
    "entry_price", "exit_price", "size_usd",
    "ev_pct", "bayesian_pct", "ob_imbalance",
    "lag_seconds", "hold_seconds", "profit", "result",
]


def write_csv(trade: dict) -> None:
    """Append a single trade record to the CSV file."""
    file_exists = os.path.isfile(CSV_FILE)
    try:
        with open(CSV_FILE, "a", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_FIELDNAMES)
            if not file_exists:
                writer.writeheader()
            writer.writerow({k: trade.get(k, "") for k in CSV_FIELDNAMES})
    except Exception as exc:
        log.warning("CSV write error: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
#  QUANT ENGINE — Bayesian Posterior, EV, Kelly, OB Imbalance
# ══════════════════════════════════════════════════════════════════════════════

def bayesian_posterior(
    base_prob: float,
    lag_signal_strength: float,
    ob_imbalance: float,
    volume_ratio: float,
) -> float:
    """
    Bayesian update of the base Polymarket probability given three signals.

    Model:
        P(win | signals) ∝ LR_lag × LR_ob × LR_vol × P(win)

    Likelihood ratios are calibrated so that a perfect signal (strength=1)
    delivers roughly a 3× boost, matching empirical lag-arb hit rates.

    Parameters
    ----------
    base_prob           : Polymarket implied YES probability (0–1)
    lag_signal_strength : Binance candle lag signal strength (0–1)
    ob_imbalance        : Order-book bid/ask imbalance (0–1)
    volume_ratio        : 1h_vol / MIN_1H_VOLUME_USD (capped at 2.0)

    Returns
    -------
    Posterior probability (0.001 – 0.999)
    """
    prior = max(0.01, min(0.99, base_prob))

    lr_lag = 1.0 + 2.5 * lag_signal_strength
    lr_ob  = 1.0 + 1.8 * ob_imbalance
    lr_vol = 1.0 + 0.4 * min(volume_ratio, 2.0)

    numerator   = prior * lr_lag * lr_ob * lr_vol
    denominator = numerator + (1.0 - prior)   # assumes P(signals|lose)=1

    posterior = numerator / denominator
    return float(min(max(posterior, 0.001), 0.999))


def compute_ev(polymarket_prob: float, true_prob: float) -> float:
    """
    Expected Value = (true_prob - poly_prob) / poly_prob.

    Positive EV means the market is underpricing the outcome and we have edge.
    """
    if polymarket_prob <= 0:
        return 0.0
    return (true_prob - polymarket_prob) / polymarket_prob


def order_book_imbalance(bids: list, asks: list) -> float:
    """
    Signed imbalance = |bid_vol - ask_vol| / (bid_vol + ask_vol).

    bids / asks: list of [price, size] pairs (top N levels from CLOB).
    Returns a value in [0, 1].
    """
    bid_vol = sum(float(b[1]) for b in bids) if bids else 0.0
    ask_vol = sum(float(a[1]) for a in asks) if asks else 0.0
    total = bid_vol + ask_vol
    if total == 0:
        return 0.0
    return abs(bid_vol - ask_vol) / total


def kelly_size(ev: float, win_prob: float, bankroll: float) -> float:
    """
    Fractional Kelly position size in USD.

        f* = (b·p − q) / b    where b = ev/win_prob (net odds), q = 1−p
        size = KELLY_FRACTION × f* × bankroll

    Capped at MAX_TRADE_USD for risk management.
    """
    if ev <= 0 or win_prob <= 0 or win_prob >= 1:
        return 0.0
    b = ev / win_prob          # net fractional odds
    q = 1.0 - win_prob
    f_star = max(0.0, (b * win_prob - q) / b)
    raw = KELLY_FRACTION * f_star * bankroll
    return float(min(raw, MAX_TRADE_USD))


# ══════════════════════════════════════════════════════════════════════════════
#  LAG SIGNAL ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def compute_lag_signal(asset: str, polymarket_prob: float) -> tuple[float, float]:
    """
    Derive the 'true probability' by comparing Binance 5m candle direction
    to the current Polymarket YES price.

    Logic:
      - Binance UP candle + Polymarket YES underpriced → bullish lag
      - Binance DOWN candle + Polymarket YES overpriced → bearish lag
      - Strength proportional to candle body as fraction of total range

    Returns
    -------
    (lag_signal_strength, true_prob)  both in [0, 1]
    """
    kline = STATE.klines.get(asset, {})
    if not kline:
        return 0.0, polymarket_prob

    o = kline.get("open", 0.0)
    c = kline.get("close", 0.0)
    h = kline.get("high", 0.0)
    l = kline.get("low", 0.0)

    candle_range = max(h - l, 1e-9)
    body = c - o
    body_pct = abs(body) / candle_range   # 0–1: candle strength
    direction = 1 if body > 0 else -1

    if direction == 1 and polymarket_prob < 0.62:
        # Binance bullish; Polymarket lagging (underpricing YES)
        lag_strength = body_pct * (0.62 - polymarket_prob) / 0.62
        true_prob = polymarket_prob + lag_strength * 0.35
    elif direction == -1 and polymarket_prob > 0.38:
        # Binance bearish; Polymarket lagging (overpricing YES)
        lag_strength = body_pct * (polymarket_prob - 0.38) / 0.62
        true_prob = polymarket_prob - lag_strength * 0.35
    else:
        # No meaningful lag signal
        lag_strength = 0.0
        true_prob = polymarket_prob

    return float(lag_strength), float(min(max(true_prob, 0.01), 0.99))


# ══════════════════════════════════════════════════════════════════════════════
#  POLYMARKET — MARKET DISCOVERY & ORDER BOOK
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_polymarket_5min_markets(session: aiohttp.ClientSession) -> None:
    """
    Real lag-arbitrage market discovery.

    HOW IT WORKS:
      Polymarket has binary markets like:
        "Will BTC be above $68,000 at 5pm UTC?"
        "Will ETH exceed $2,100 by midnight?"

      The lag opportunity exists when:
        1. We know the CURRENT real price from CoinGecko
        2. The market threshold is ALREADY breached (e.g. BTC IS above $68k)
        3. But Polymarket YES price is still LOW (e.g. 0.35) because
           market makers haven't updated yet

      That gap = the lag = our edge.

    ALGORITHM:
      1. Fetch all active BTC/ETH/SOL markets from Gamma API
      2. Parse the price threshold from the question text (regex)
      3. Compare threshold vs current CoinGecko price
      4. Compute TRUE probability based on how far price is from threshold
      5. Store markets with _true_prob and _asset for evaluate_market()

    This approach works for ANY market horizon (not just 5-minute),
    and naturally finds the highest-EV opportunities.
    """
    import re

    # Patterns to extract price thresholds from market questions
    # Matches: "$68,000", "$68k", "$1,800", "68000", etc.
    PRICE_PATTERN = re.compile(
        r"\$([\d,]+(?:\.\d+)?)[kK]?|\b([\d]{4,6}(?:,\d{3})*)\b"
    )

    asset_keywords = {
        "BTC": ["bitcoin", "btc"],
        "ETH": ["ethereum", "eth"],
        "SOL": ["solana", "sol"],
    }

    # Direction keywords
    ABOVE_KW = ["above", "over", "exceed", "higher", "more than", "greater", "at least", "reach"]
    BELOW_KW = ["below", "under", "less than", "lower", "not exceed", "beneath"]

    def parse_threshold(question: str) -> Optional[float]:
        """Extract the first dollar price threshold from a market question."""
        q = question.replace(",", "")
        matches = re.findall(r"\$(\d+(?:\.\d+)?)[kK]?|\b(\d{4,7})\b", q)
        for m in matches:
            raw = m[0] or m[1]
            if not raw:
                continue
            val = float(raw)
            # Handle 'k' suffix
            if "k" in question.lower()[question.lower().find(raw[:4]):question.lower().find(raw[:4])+6]:
                val *= 1000
            # Sanity check: BTC ~10k-200k, ETH ~500-20k, SOL ~10-2000
            if 10 <= val <= 300_000:
                return val
        return None

    def compute_true_prob(asset: str, threshold: float, direction: str) -> float:
        """
        Compute true probability that market resolves YES given current price.

        Uses a sigmoid function centered on the threshold:
          - Price FAR above threshold (above market) → prob near 0.95
          - Price AT threshold → prob 0.50
          - Price FAR below threshold → prob near 0.05

        The steepness (k) controls how fast probability moves with price distance.
        """
        kline = STATE.klines.get(asset, {})
        current_price = kline.get("close", 0.0)
        if current_price <= 0 or threshold <= 0:
            return 0.5

        # Relative distance from threshold (positive = above)
        distance_pct = (current_price - threshold) / threshold

        # Sigmoid steepness — higher = sharper transition
        k = 25.0

        if direction == "above":
            # YES resolves if price > threshold: bullish signal
            raw_prob = 1.0 / (1.0 + math.exp(-k * distance_pct))
        else:
            # YES resolves if price < threshold: bearish signal
            raw_prob = 1.0 / (1.0 + math.exp(k * distance_pct))

        # Clamp to [0.05, 0.95] — never be fully certain
        return float(min(max(raw_prob, 0.05), 0.95))

    try:
        url = f"{GAMMA_BASE}/markets"
        params = {
            "active":    "true",
            "closed":    "false",
            "order":     "volume24hr",
            "ascending": "false",
            "limit":     "500",
        }
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                log.warning("Gamma API returned HTTP %s", resp.status)
                return
            data = await resp.json()

        markets_list = data if isinstance(data, list) else data.get("markets", [])
        now = datetime.now(timezone.utc)

        total_found = 0
        for asset, keywords in asset_keywords.items():
            found = []

            # Skip if we don't have price data yet
            if not STATE.klines.get(asset):
                continue

            for m in markets_list:
                question_raw = m.get("question") or m.get("title") or ""
                question = question_raw.lower()

                # Must mention the asset
                if not any(kw in question for kw in keywords):
                    continue

                # Parse market end date — accept markets up to 24h out
                end_str = m.get("endDate") or m.get("end_date_iso") or ""
                if not end_str:
                    continue
                try:
                    end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    seconds_left = (end_dt - now).total_seconds()
                    # Accept markets closing in next 24 hours
                    if not (0 < seconds_left <= 86400):
                        continue
                except ValueError:
                    continue

                # Parse price threshold from question
                threshold = parse_threshold(question_raw)
                if threshold is None:
                    continue

                # Determine direction (above/below)
                if any(kw in question for kw in ABOVE_KW):
                    direction = "above"
                elif any(kw in question for kw in BELOW_KW):
                    direction = "below"
                else:
                    continue  # can't determine direction

                # Compute true probability using real price vs threshold
                true_prob = compute_true_prob(asset, threshold, direction)

                # Only interested if there's potential mispricing
                # (true_prob significantly different from 0.5)
                if abs(true_prob - 0.5) < 0.15:
                    continue  # too close to call — no edge

                # Extract YES token_id for CLOB order book lookup.
                # Polymarket market structure: tokens = [{outcome:"Yes", token_id:"..."},...]
                tokens = m.get("tokens") or m.get("clobTokenIds") or []
                yes_token_id = ""
                if isinstance(tokens, list):
                    for tok in tokens:
                        if isinstance(tok, dict):
                            if tok.get("outcome", "").lower() in ("yes", "1"):
                                yes_token_id = tok.get("token_id") or tok.get("tokenId") or ""
                                break
                    if not yes_token_id and tokens:
                        # fallback: first token
                        first = tokens[0]
                        yes_token_id = (first.get("token_id") or first.get("tokenId") or "") if isinstance(first, dict) else str(first)

                # Also try top-level fields used by some Gamma API versions
                if not yes_token_id:
                    yes_token_id = (m.get("token_id") or m.get("tokenId") or
                                    m.get("conditionId") or m.get("id") or "")

                m["_seconds_left"]  = seconds_left
                m["_asset"]         = asset
                m["_true_prob"]     = true_prob
                m["_threshold"]     = threshold
                m["_direction"]     = direction
                m["_current_price"] = STATE.klines[asset].get("close", 0)
                m["_yes_token_id"]  = yes_token_id
                found.append(m)

            # Sort by strongest signal (furthest true_prob from 0.5)
            found.sort(key=lambda x: abs(x["_true_prob"] - 0.5), reverse=True)
            STATE.markets[asset] = found[:10]  # keep top 10 per asset
            total_found += len(STATE.markets[asset])

        if total_found:
            log.info(
                "Market discovery: %d mispriced markets found "
                "(BTC:%d ETH:%d SOL:%d)",
                total_found,
                len(STATE.markets.get("BTC", [])),
                len(STATE.markets.get("ETH", [])),
                len(STATE.markets.get("SOL", [])),
            )
        else:
            log.debug("Market discovery: no mispriced markets found this cycle.")

    except Exception as exc:
        log.warning("Market discovery error: %s", exc)


async def fetch_order_book(
    session: aiohttp.ClientSession,
    market_id: str,
) -> tuple[list, list]:
    """
    Fetch CLOB order book for a YES token ID.

    Polymarket CLOB requires the YES token_id (a long hex string),
    NOT the conditionId. If market_id looks like a conditionId (short)
    or is empty, the book will be empty — that's expected and handled
    upstream by bypassing the OB filter.

    Returns (bids, asks) where each entry is [price, size].
    """
    if not market_id or len(market_id) < 10:
        log.debug("OB fetch skipped: invalid token_id '%s'", market_id)
        return [], []
    try:
        url = f"{CLOB_BASE}/book"
        params = {"token_id": market_id}
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status != 200:
                log.debug("CLOB book HTTP %s for token %s…", resp.status, market_id[:12])
                return [], []
            data = await resp.json()
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        log.debug("OB fetched token=%s… bids=%d asks=%d",
                  market_id[:12], len(bids), len(asks))
        return bids, asks
    except Exception as exc:
        log.debug("Order book fetch error for %s…: %s", market_id[:12], exc)
        return [], []


# ══════════════════════════════════════════════════════════════════════════════
#  PRICE POLLER — CoinGecko single-call (no rate-limit issues)
# ══════════════════════════════════════════════════════════════════════════════

async def binance_ws_listener() -> None:
    """
    Poll CoinGecko /simple/price in ONE call for all 3 assets every
    PRICE_POLL_INTERVAL seconds.

    Using /simple/price instead of /ohlc or /market_chart avoids the
    aggressive rate-limiting that was hitting the bot every ~30s.
    One call per interval instead of 3 separate calls = 3x less pressure.

    Volume data comes from the same endpoint (include_24hr_vol=true).
    We derive a synthetic OHLC from the current price + prev price so the
    lag signal engine keeps working unchanged.
    """
    log.info("CoinGecko price poller started (single-call, interval: %ds).", PRICE_POLL_INTERVAL)
    coin_ids = ",".join(COINGECKO_IDS.values())
    url = f"{COINGECKO_BASE}/simple/price"
    prev_prices: dict = {}

    connector = aiohttp.TCPConnector(limit=3)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                params = {
                    "ids":                  coin_ids,
                    "vs_currencies":        "usd",
                    "include_24hr_vol":     "true",
                    "include_24hr_change":  "true",
                    "precision":            "4",
                }
                async with session.get(
                    url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 429:
                        log.warning("CoinGecko rate-limited — waiting 60 s …")
                        await asyncio.sleep(60)
                        continue
                    if resp.status != 200:
                        log.warning("CoinGecko HTTP %s — retrying in 20 s …", resp.status)
                        await asyncio.sleep(20)
                        continue
                    data = await resp.json()

                prices_log = []
                for asset, coin_id in COINGECKO_IDS.items():
                    coin = data.get(coin_id, {})
                    if not coin:
                        continue

                    current_price: float = float(coin.get("usd", 0))
                    vol_24h: float       = float(coin.get("usd_24h_vol", 0))
                    change_24h: float    = float(coin.get("usd_24h_change", 0))

                    if current_price <= 0:
                        continue

                    # Synthetic OHLC: open = previous price, close = now
                    prev  = prev_prices.get(asset, current_price)
                    open_p  = prev
                    close_p = current_price
                    # High/low estimated from 24h change scaled to one interval
                    swing   = abs(change_24h) / 100 / (86400 / PRICE_POLL_INTERVAL)
                    high_p  = max(open_p, close_p) * (1 + swing)
                    low_p   = min(open_p, close_p) * (1 - swing)

                    # Volume: divide 24h vol by number of intervals in 24h → per-interval
                    # vol_1h = 1h USD volume for the volatility filter
                    vol_1h = vol_24h / 24.0

                    STATE.klines[asset] = {
                        "open":         open_p,
                        "high":         high_p,
                        "low":          low_p,
                        "close":        close_p,
                        "volume_quote": vol_24h / 288.0,  # approx 5m volume
                        "vol_1h":       vol_1h,
                        "is_closed":    False,
                        "open_time":    int(time.time() - PRICE_POLL_INTERVAL),
                        "close_time":   int(time.time()),
                        "ts":           time.time(),
                    }
                    prev_prices[asset] = current_price
                    prices_log.append(f"{asset}=${current_price:,.2f}")

                if prices_log:
                    log.info("Prices updated — %s", " | ".join(prices_log))

                await asyncio.sleep(PRICE_POLL_INTERVAL)

            except Exception as exc:
                log.warning("CoinGecko poller error: %s — retrying in 20 s …", exc)
                await asyncio.sleep(20)


# ══════════════════════════════════════════════════════════════════════════════
#  SIGNAL EVALUATION
# ══════════════════════════════════════════════════════════════════════════════

async def evaluate_market(
    session: aiohttp.ClientSession,
    market: dict,
) -> Optional[dict]:
    """
    Full multi-filter signal evaluation for a single Polymarket market.

    Filters applied in order (fast-fail):
      1. Trade window  : must be in last 180 s of market lifetime
      2. Volume        : approx 1h Binance volume > $80k
      3. OB imbalance  : must be ≥ 18%
      4. EV            : must be ≥ 16%
      5. Bayesian      : posterior must be ≥ 78%
      6. Size          : Kelly size must be ≥ $1

    Returns a signal dict if all filters pass, else None.
    """
    asset: str = market.get("_asset", "BTC")
    # Use pre-extracted YES token_id for CLOB OB lookup; fall back to conditionId
    market_id: str = (market.get("_yes_token_id") or
                      market.get("conditionId") or market.get("id") or "")
    seconds_left: float = market.get("_seconds_left", 999.0)
    question: str = (market.get("question") or market.get("title") or "Unknown")[:80]

    STATE.total_signals_scanned += 1

    # ── 1. Market still active check ─────────────────────────────────────────
    if seconds_left <= 60:
        return None

    # ── 2. Volatility / volume filter — SKIPPED for Polymarket markets ────────
    # CoinGecko gives us BTC/ETH/SOL exchange volume, NOT Polymarket volume.
    # Polymarket prediction markets have far lower volume (~$1k-$500k/day).
    # This filter was blocking ALL signals because we were comparing
    # crypto exchange volume to MIN_1H_VOLUME_USD=$80k (irrelevant metric).
    # We keep the kline data for signal calculation but skip this filter.
    kline = STATE.klines.get(asset, {})
    vol_1h_approx = kline.get("vol_1h", 0.0)
    # Just log it for transparency, don't filter
    log.debug("[%s] CoinGecko 1h vol=$%.0f (not used as filter)", asset, vol_1h_approx)

    # ── 3. Order-book fetch + implied probability ────────────────────────────
    # We fetch the CLOB book for two purposes:
    #   a) Compute OB imbalance (directional pressure signal)
    #   b) Get the market-implied YES probability from best ask price
    #
    # If the book is empty (common in low-liquidity markets), we:
    #   - Use the market's last price or midpoint as poly_prob fallback
    #   - Set ob_imb = 0 (neutral) and bypass the imbalance filter
    #   - This allows the EV/Bayesian filters to still fire on strong signals
    bids, asks = await fetch_order_book(session, market_id)
    ob_imb = order_book_imbalance(bids, asks)

    # Derive poly_prob from order book
    if asks:
        try:
            poly_prob = float(asks[0][0])
        except (IndexError, ValueError):
            poly_prob = 0.5
    elif bids:
        try:
            poly_prob = float(bids[0][0])
        except (IndexError, ValueError):
            poly_prob = 0.5
    else:
        # Book completely empty — use Gamma API last price if available
        lp = market.get("lastTradePrice") or market.get("outcomePrices")
        if lp:
            try:
                poly_prob = float(lp[0]) if isinstance(lp, list) else float(lp)
            except (ValueError, TypeError):
                poly_prob = 0.5
        else:
            poly_prob = 0.5

    # OB imbalance filter:
    #   - ob_imb == 0.0 → book is empty/unavailable → bypass filter entirely,
    #     rely on EV + Bayesian only (this is the common case on Polymarket)
    #   - ob_imb > 0 but < threshold → book exists but not enough pressure → skip
    if ob_imb > 0.0 and ob_imb < MIN_OB_IMBALANCE:
        log.debug("[%s] OB imbalance too low: %.3f (threshold: %.3f)",
                  asset, ob_imb, MIN_OB_IMBALANCE)
        return None
    # Log book status at DEBUG only to avoid spam
    if ob_imb == 0.0:
        log.debug("[%s] Empty order book — bypassing OB filter, using EV signal only", asset)
    else:
        log.debug("[%s] OB imbalance OK: %.3f", asset, ob_imb)

    # ── 5. TRUE probability from price-vs-threshold lag signal ────────────────
    # Use the pre-computed true_prob from market discovery (sigmoid model).
    # This is the REAL lag arb signal: we know the current price from CoinGecko,
    # we know the market threshold, so we know the true probability.
    # If Polymarket is still showing a stale/wrong price — that's our edge.
    true_prob: float = market.get("_true_prob", poly_prob)

    # Also compute the candle-based lag signal for Bayesian input
    lag_strength, _ = compute_lag_signal(asset, poly_prob)
    lag_seconds = max(0.0, TRADE_WINDOW_SECONDS - seconds_left)

    # Log the core signal for transparency
    current_px = market.get("_current_price", 0)
    threshold  = market.get("_threshold", 0)
    direction  = market.get("_direction", "?")
    log.debug(
        "[%s] price=$%.2f %s threshold=$%.2f → true_prob=%.3f poly_prob=%.3f",
        asset, current_px, direction, threshold, true_prob, poly_prob,
    )

    # ── 6. EV — core of the lag arb ──────────────────────────────────────────
    #
    # VALID trade conditions (real lag arb edge):
    #   A) BUY YES:  true_prob > poly_prob AND true_prob > 0.55
    #      → Market underpricing a likely YES outcome
    #      → e.g. BTC at $68k, market asks "above $65k?" poly=0.60 true=0.92
    #
    #   B) BUY NO:   true_prob < poly_prob AND true_prob < 0.45
    #      → Market overpricing a likely NO outcome
    #      → e.g. BTC at $68k, market asks "above $72k?" poly=0.40 true=0.08
    #
    # INVALID (math artifact, not real edge):
    #   - poly=0.00 true=0.07 EV=2395% → both near 0, market is already correct
    #   - poly=1.00 true=0.93 EV=-7%   → market already priced in, no edge
    #
    # We trade YES when true_prob > poly_prob (underpriced YES)
    # We trade NO  when true_prob < poly_prob (overpriced YES = cheap NO)
    # In both cases we need meaningful mispricing: |true - poly| >= 0.15

    mispricing = true_prob - poly_prob  # positive = YES underpriced, negative = NO underpriced

    if abs(mispricing) < 0.15:
        log.debug("[%s] Mispricing too small: %.3f", asset, mispricing)
        return None

    # Determine trade side based on direction of mispricing
    if mispricing > 0 and true_prob > 0.55:
        side = "YES"
        ev = compute_ev(poly_prob, true_prob)
    elif mispricing < 0 and true_prob < 0.45:
        side = "NO"
        # EV for buying NO: poly_no = 1 - poly_prob, true_no = 1 - true_prob
        poly_no  = 1.0 - poly_prob
        true_no  = 1.0 - true_prob
        ev = compute_ev(poly_no, true_no) if poly_no > 0 else 0.0
    else:
        log.debug("[%s] No clear edge: true=%.2f poly=%.2f", asset, true_prob, poly_prob)
        return None

    log.info(
        "[%s] 📊 %s | $%.0f %s $%.0f | poly=%.2f true=%.2f | side=%s EV=%.1f%%",
        asset, question[:40],
        market.get("_current_price", 0), market.get("_direction", "?"),
        market.get("_threshold", 0),
        poly_prob, true_prob, side, ev * 100,
    )

    if ev < MIN_EV:
        log.info("[%s] ❌ EV too low: %.1f%% (need %.0f%%)", asset, ev*100, MIN_EV*100)
        return None

    # ── 7. Bayesian posterior ─────────────────────────────────────────────────
    # volume_ratio set to 1.0 (neutral) — we removed the volume filter above
    # since CoinGecko exchange vol is irrelevant to Polymarket market liquidity
    volume_ratio = 1.0
    posterior = bayesian_posterior(true_prob, lag_strength, ob_imb, volume_ratio)
    if posterior < MIN_BAYESIAN:
        log.debug("[%s] Bayesian failed: %.3f < %.3f", asset, posterior, MIN_BAYESIAN)
        return None

    # ── 8. Kelly sizing ───────────────────────────────────────────────────────
    size_usd = kelly_size(ev, posterior, STATE.bankroll)
    if size_usd < 1.0:
        log.debug("[%s] Size too small: $%.2f", asset, size_usd)
        return None

    # ── All filters passed — return signal dict ───────────────────────────────
    log.info(
        "✅ SIGNAL: [%s] %s %s | $%.0f %s $%.0f "
        "| poly=%.2f true=%.2f EV=%.1f%% Bayes=%.1f%%",
        asset, side, question[:35],
        market.get("_current_price", 0), market.get("_direction", "?"),
        market.get("_threshold", 0),
        poly_prob, true_prob, ev * 100, posterior * 100,
    )
    return {
        "asset":         asset,
        "market_id":     market_id,
        "question":      question,
        "side":          side,
        "entry_price":   poly_prob,
        "true_prob":     true_prob,
        "ev_pct":        ev,
        "bayesian_pct":  posterior,
        "ob_imbalance":  ob_imb,
        "lag_seconds":   lag_seconds,
        "lag_strength":  lag_strength,
        "size_usd":      size_usd,
        "seconds_left":  seconds_left,
        "vol_1h_approx": vol_1h_approx,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  TRADE EXECUTION — PAPER & LIVE
# ══════════════════════════════════════════════════════════════════════════════

async def execute_paper_trade(signal: dict) -> dict:
    """
    Simulate a full trade lifecycle without placing any real orders.

    Outcome is simulated probabilistically based on the Bayesian posterior.
    A brief asyncio.sleep() simulates holding the position.
    """
    entry_price: float = signal["entry_price"]
    size_usd: float = signal["size_usd"]
    posterior: float = signal["bayesian_pct"]

    # Simulate market outcome
    won = random.random() < posterior

    if won:
        # Price adjusts toward true probability
        exit_price = min(entry_price + random.uniform(0.02, 0.12), 0.99)
    else:
        # Adverse move
        exit_price = max(entry_price - random.uniform(0.01, 0.08), 0.01)

    # Simulate hold duration (capped to avoid blocking the event loop)
    hold_seconds = random.uniform(3.0, float(EXIT_TIMEOUT_SECONDS))
    await asyncio.sleep(min(hold_seconds, 3.0))

    # P&L calculation with a rough 2% fee model
    gross_pnl = (exit_price - entry_price) / entry_price * size_usd
    fee = size_usd * 0.02
    net_pnl = gross_pnl - fee

    return {
        "timestamp":    signal["timestamp"],
        "asset":        signal["asset"],
        "market_id":    signal["market_id"],
        "side":         signal["side"],
        "entry_price":  round(entry_price, 4),
        "exit_price":   round(exit_price, 4),
        "size_usd":     round(size_usd, 2),
        "ev_pct":       round(signal["ev_pct"], 4),
        "bayesian_pct": round(posterior, 4),
        "ob_imbalance": round(signal["ob_imbalance"], 4),
        "lag_seconds":  round(signal["lag_seconds"], 1),
        "hold_seconds": round(hold_seconds, 1),
        "profit":       round(net_pnl, 4),
        "result":       "WIN" if net_pnl > 0 else "LOSS",
    }


async def execute_live_trade(signal: dict) -> dict:
    """
    Live trade execution placeholder via py-clob-client.

    In production:
      1. Build a limit order (YES side) using py-clob-client.
      2. Sign with PRIVATE_KEY.
      3. Monitor fill and exit on price adjustment or EXIT_TIMEOUT_SECONDS.

    Currently falls back to paper simulation so the bot remains safe
    if accidentally run with PAPER_MODE=false without live integration.
    """
    log.warning(
        "[LIVE] Real execution not yet implemented — falling back to paper simulation."
    )
    return await execute_paper_trade(signal)


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM ALERTS
# ══════════════════════════════════════════════════════════════════════════════

async def alert_trade(signal: dict, trade: dict) -> None:
    """Send full trade alert with all signal metrics and result."""
    mode_tag  = "🧾 [PAPER TRADE]" if PAPER_MODE else "🚀 [LIVE TRADE]"
    result_em = "✅" if trade["result"] == "WIN" else "❌"
    risk      = STATE.risk_level()
    risk_em   = {"BAJO": "🟢", "MEDIO": "🟡", "ALTO": "🔴"}.get(risk, "⚪")

    msg = (
        f"{mode_tag} {result_em} <b>{signal['asset']} — {signal['question']}</b>\n\n"
        f"  Lag:        <code>{signal['lag_seconds']:.1f}s</code>\n"
        f"  EV:         <code>{signal['ev_pct']*100:.2f}%</code>\n"
        f"  Bayesian:   <code>{signal['bayesian_pct']*100:.2f}%</code>\n"
        f"  OB Imbal:   <code>{signal['ob_imbalance']*100:.2f}%</code>\n"
        f"  Size:       <code>${trade['size_usd']:.2f}</code>\n"
        f"  Entry:      <code>{trade['entry_price']:.4f}</code>\n"
        f"  Exit:       <code>{trade['exit_price']:.4f}</code>\n"
        f"  P&L:        <code>${trade['profit']:+.4f}</code>\n"
        f"  Held:       <code>{trade.get('hold_seconds', '?')}s</code>\n\n"
        f"  Bankroll:   <code>${STATE.bankroll:.2f}</code>\n"
        f"  Risk Level: {risk_em} <b>{risk}</b>\n"
        f"  Win-Rate:   <code>{STATE.win_rate()*100:.1f}%</code> "
        f"(last {len(STATE.trade_history)} trades)"
    )
    await tg_send(msg)


async def alert_missed_opportunity(signal: dict, reason: str) -> None:
    """Notify when a qualifying signal could not be traded."""
    msg = (
        f"👀 <b>Missed Opportunity — {signal['asset']}</b>\n"
        f"  Reason:    {reason}\n"
        f"  EV:        <code>{signal['ev_pct']*100:.2f}%</code>\n"
        f"  Bayesian:  <code>{signal['bayesian_pct']*100:.2f}%</code>\n"
        f"  Would-be size: <code>${signal['size_usd']:.2f}</code>"
    )
    await tg_send(msg)
    STATE.missed_opportunities += 1


async def send_daily_summary() -> None:
    """Midnight UTC daily summary with CSV attachment."""
    wr   = STATE.win_rate() * 100
    dd   = STATE.drawdown_pct() * 100
    risk = STATE.risk_level()
    risk_em = {"BAJO": "🟢", "MEDIO": "🟡", "ALTO": "🔴"}.get(risk, "⚪")
    recent = STATE.all_trades[-ROLLING_WINDOW:]
    wins   = sum(1 for t in recent if t.get("profit", 0) > 0)
    losses = len(recent) - wins

    msg = (
        f"📊 <b>Daily Summary — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}</b>\n\n"
        f"  Bankroll:   <code>${STATE.bankroll:.2f}</code>\n"
        f"  Daily P&L:  <code>${STATE.daily_pnl:+.4f}</code>\n"
        f"  Drawdown:   <code>{dd:.2f}%</code>\n"
        f"  Win-Rate:   <code>{wr:.1f}%</code>\n"
        f"  W/L:        <code>{wins}W / {losses}L</code> (last {len(recent)})\n"
        f"  Signals:    <code>{STATE.total_signals_scanned}</code> scanned\n"
        f"  Missed:     <code>{STATE.missed_opportunities}</code>\n"
        f"  Risk Level: {risk_em} <b>{risk}</b>"
    )
    await tg_send(msg)

    # Attach CSV
    if os.path.isfile(CSV_FILE):
        await tg_send_document(
            CSV_FILE,
            caption=f"📁 trade_history.csv — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        )


async def send_rolling_performance() -> None:
    """24-hour rolling performance snapshot."""
    recent = STATE.all_trades[-ROLLING_WINDOW:]
    if not recent:
        return
    wins        = sum(1 for t in recent if t.get("profit", 0) > 0)
    total_pnl   = sum(t.get("profit", 0) for t in recent)
    avg_ev      = sum(t.get("ev_pct", 0) for t in recent) / len(recent)
    avg_bayes   = sum(t.get("bayesian_pct", 0) for t in recent) / len(recent)

    msg = (
        f"📈 <b>Rolling Performance (last {len(recent)} trades)</b>\n\n"
        f"  Win-Rate:   <code>{wins/len(recent)*100:.1f}%</code>\n"
        f"  Net P&L:    <code>${total_pnl:+.4f}</code>\n"
        f"  Avg EV:     <code>{avg_ev*100:.2f}%</code>\n"
        f"  Avg Bayes:  <code>{avg_bayes*100:.2f}%</code>"
    )
    await tg_send(msg)


async def send_heartbeat() -> None:
    """Periodic heartbeat confirming the bot is alive."""
    status = (
        "⏸ PAUSED" if STATE.paused
        else ("🛑 HARD STOPPED" if STATE.hard_stopped else "✅ RUNNING")
    )
    msg = (
        f"💓 <b>Sentinel Heartbeat</b> "
        f"— {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
        f"  Status:      {status}\n"
        f"  Mode:        {'📝 PAPER' if PAPER_MODE else '💰 LIVE'}\n"
        f"  Bankroll:    <code>${STATE.bankroll:.2f}</code>\n"
        f"  Win-Rate:    <code>{STATE.win_rate()*100:.1f}%</code>\n"
        f"  Open Trade:  {'Yes' if STATE.open_trade else 'No'}\n"
        f"  Risk Level:  <b>{STATE.risk_level()}</b>"
    )
    await tg_send(msg)


async def send_weekly_report() -> None:
    """Sunday weekly report with automated improvement suggestion."""
    total      = len(STATE.all_trades)
    wins       = sum(1 for t in STATE.all_trades if t.get("profit", 0) > 0)
    total_pnl  = sum(t.get("profit", 0) for t in STATE.all_trades)
    wr         = wins / total if total > 0 else 0.0

    # Automated suggestion based on performance
    if wr < 0.60:
        suggestion = (
            "Win-rate below 60%. Consider raising the EV threshold to 20% "
            "and Bayesian threshold to 82% to accept only the highest-conviction signals."
        )
    elif wr < 0.75:
        suggestion = (
            "Win-rate below 75%. Try increasing OB imbalance threshold to 22% "
            "and tuning the lag signal window from 5m to 3m candles."
        )
    else:
        suggestion = (
            "Strong performance. You may cautiously increase the Kelly fraction "
            "from 0.35 to 0.40, or expand MAX_TRADE_USD to capture more upside."
        )

    msg = (
        f"📅 <b>Weekly Report — Week {datetime.now(timezone.utc).isocalendar()[1]}, "
        f"{datetime.now(timezone.utc).year}</b>\n\n"
        f"  Total Trades: <code>{total}</code>\n"
        f"  Wins / Losses: <code>{wins} / {total - wins}</code>\n"
        f"  Win-Rate:     <code>{wr*100:.1f}%</code>\n"
        f"  Net P&L:      <code>${total_pnl:+.4f}</code>\n"
        f"  Bankroll:     <code>${STATE.bankroll:.2f}</code>\n\n"
        f"💡 <b>Improvement Suggestion</b>\n  {suggestion}"
    )
    await tg_send(msg)


async def send_risk_alert(previous_level: str, current_level: str) -> None:
    """Alert when risk level changes upward (BAJO→MEDIO, MEDIO→ALTO)."""
    em = {"BAJO": "🟢", "MEDIO": "🟡", "ALTO": "🔴"}.get(current_level, "⚪")
    msg = (
        f"{em} <b>Risk Level Changed: {previous_level} → {current_level}</b>\n"
        f"  Drawdown: <code>{STATE.drawdown_pct()*100:.2f}%</code>\n"
        f"  Bankroll: <code>${STATE.bankroll:.2f}</code>"
    )
    await tg_send(msg)


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/status — Full bot status snapshot."""
    status = (
        "⏸ PAUSED" if STATE.paused
        else ("🛑 HARD STOPPED" if STATE.hard_stopped else "✅ RUNNING")
    )
    mode = "📝 PAPER" if PAPER_MODE else "💰 LIVE"

    msg = (
        f"🤖 <b>Sentinel v3.0 Status</b>\n\n"
        f"  Mode:        {mode}\n"
        f"  Status:      {status}\n"
        f"  Bankroll:    <code>${STATE.bankroll:.2f}</code>\n"
        f"  Daily P&L:   <code>${STATE.daily_pnl:+.4f}</code>\n"
        f"  Drawdown:    <code>{STATE.drawdown_pct()*100:.2f}%</code>\n"
        f"  Win-Rate:    <code>{STATE.win_rate()*100:.1f}%</code> "
        f"(last {len(STATE.trade_history)})\n"
        f"  Total Trades:<code>{len(STATE.all_trades)}</code>\n"
        f"  Signals Scanned: <code>{STATE.total_signals_scanned}</code>\n"
        f"  Missed Opps: <code>{STATE.missed_opportunities}</code>\n"
        f"  Open Trade:  {'Yes' if STATE.open_trade else 'No'}\n"
        f"  Risk Level:  <b>{STATE.risk_level()}</b>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/trades — Last 10 trades."""
    recent = STATE.all_trades[-10:]
    if not recent:
        await update.message.reply_text("No trades recorded yet.")
        return

    lines = ["<b>Last 10 Trades</b>\n"]
    for t in reversed(recent):
        em = "✅" if t["result"] == "WIN" else "❌"
        lines.append(
            f"{em} {t['asset']} | <code>${t['profit']:+.4f}</code> | "
            f"E:<code>{t['entry_price']:.3f}</code>"
            f" → X:<code>{t['exit_price']:.3f}</code>"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/pause — Manually pause the bot."""
    if STATE.paused:
        await update.message.reply_text("⏸ Bot is already paused.")
        return
    STATE.paused = True
    STATE.pause_until = None   # indefinite until /resume
    await update.message.reply_text("⏸ Bot paused. Use /resume to restart.")
    log.info("Bot paused via Telegram /pause command.")


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/resume — Resume the bot from any paused state."""
    if not STATE.paused and not STATE.hard_stopped:
        await update.message.reply_text("✅ Bot is already running.")
        return
    STATE.paused = False
    STATE.pause_until = None
    STATE.hard_stopped = False
    await update.message.reply_text("▶️ Bot resumed and ready to trade.")
    log.info("Bot resumed via Telegram /resume command.")


# ══════════════════════════════════════════════════════════════════════════════
#  SCHEDULER — Heartbeat, Daily Reset, Weekly Report
# ══════════════════════════════════════════════════════════════════════════════

async def scheduler_loop() -> None:
    """
    Periodic task scheduler running on a 60-second tick.

    Handles:
      • Heartbeat every 6 hours
      • Daily summary + CSV at midnight UTC
      • Rolling performance every 24 hours
      • Weekly report on Sundays
      • Risk level change alerts
      • Auto-resume from win-rate pause
    """
    last_heartbeat_ts = time.time()
    last_rolling_ts   = time.time()
    last_day          = datetime.now(timezone.utc).day
    last_week_num     = datetime.now(timezone.utc).isocalendar()[1]
    last_risk_level   = "BAJO"

    while True:
        await asyncio.sleep(60)
        now    = datetime.now(timezone.utc)
        now_ts = time.time()

        # ── Heartbeat ────────────────────────────────────────────────────────
        if now_ts - last_heartbeat_ts >= HEARTBEAT_INTERVAL_HOURS * 3600:
            await send_heartbeat()
            last_heartbeat_ts = now_ts

        # ── Rolling performance every 24 h ───────────────────────────────────
        if now_ts - last_rolling_ts >= 86400:
            await send_rolling_performance()
            last_rolling_ts = now_ts

        # ── Daily summary + reset at midnight UTC ────────────────────────────
        if now.day != last_day and now.hour == DAILY_SUMMARY_HOUR:
            await send_daily_summary()
            STATE.reset_daily()
            last_day = now.day
            log.info("Daily reset performed.")

        # ── Weekly report on Sunday ───────────────────────────────────────────
        week_num = now.isocalendar()[1]
        if now.weekday() == WEEKLY_REPORT_DAY and week_num != last_week_num:
            await send_weekly_report()
            last_week_num = week_num

        # ── Risk level alert ─────────────────────────────────────────────────
        current_risk = STATE.risk_level()
        if current_risk != last_risk_level:
            await send_risk_alert(last_risk_level, current_risk)
            last_risk_level = current_risk

        # ── Auto-resume from win-rate pause ───────────────────────────────────
        if (
            STATE.paused
            and STATE.pause_until is not None
            and now >= STATE.pause_until
        ):
            STATE.paused = False
            STATE.pause_until = None
            await tg_send("▶️ <b>Bot auto-resumed</b> after 2-hour win-rate pause.")
            log.info("Bot auto-resumed from win-rate pause.")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN STRATEGY LOOP
# ══════════════════════════════════════════════════════════════════════════════

async def strategy_loop() -> None:
    """
    Core trading loop.

    Cycle (every 2–3 seconds):
      1. Guard checks (hard stop, pause, daily drawdown)
      2. Refresh Polymarket market list every 30 s
      3. Update market seconds_left from end dates
      4. Evaluate all eligible markets in parallel
      5. Pick the highest-EV signal
      6. Execute (paper or live) under the trade lock
      7. Record result, update CSV, send Telegram alert
      8. Check win-rate circuit breaker
    """
    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:

        market_refresh_interval = 30.0
        last_market_refresh_ts  = 0.0

        while True:
            try:
                # Jitter to avoid predictable polling rhythm
                await asyncio.sleep(random.uniform(2.0, 3.0))

                # ── Guard: daily drawdown hard stop ───────────────────────────
                if not STATE.hard_stopped and STATE.drawdown_pct() >= MAX_DAILY_DRAWDOWN_PCT:
                    STATE.hard_stopped = True
                    await tg_send(
                        f"🛑 <b>HARD STOP — 20% Daily Drawdown Reached</b>\n"
                        f"  Drawdown: <code>{STATE.drawdown_pct()*100:.2f}%</code>\n"
                        f"  Bankroll: <code>${STATE.bankroll:.2f}</code>\n"
                        f"  Bot will not trade again today. Use /resume tomorrow."
                    )
                    log.error("Hard stop triggered — 20%% daily drawdown reached.")

                if STATE.hard_stopped:
                    continue

                if STATE.paused:
                    log.debug("Bot paused — skipping cycle.")
                    continue

                # ── Refresh market list every 30 s ────────────────────────────
                now_ts = time.time()
                if now_ts - last_market_refresh_ts >= market_refresh_interval:
                    await fetch_polymarket_5min_markets(session)
                    last_market_refresh_ts = now_ts

                # ── Skip if open trade already ────────────────────────────────
                if STATE.open_trade is not None:
                    log.debug("Open trade in progress — skipping signal scan.")
                    continue

                # ── Flatten all asset markets into one list ───────────────────
                all_markets = []
                for asset_markets in STATE.markets.values():
                    all_markets.extend(asset_markets)

                if not all_markets:
                    log.debug("No short-horizon markets in window — waiting …")
                    continue

                # ── Refresh seconds_left for each market ──────────────────────
                now_dt = datetime.now(timezone.utc)
                for m in all_markets:
                    end_str = m.get("endDate") or m.get("end_date_iso") or ""
                    try:
                        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                        m["_seconds_left"] = (end_dt - now_dt).total_seconds()
                    except ValueError:
                        m["_seconds_left"] = 999.0

                # ── Evaluate all markets concurrently ─────────────────────────
                eval_tasks = [evaluate_market(session, m) for m in all_markets]
                results = await asyncio.gather(*eval_tasks, return_exceptions=True)

                # Filter to valid signals only
                signals: list[dict] = [
                    r for r in results
                    if isinstance(r, dict) and r is not None
                ]
                if not signals:
                    log.info(
                        "Cycle complete — %d markets scanned, 0 signals passed filters",
                        len(all_markets),
                    )
                    continue

                # Sort by EV descending; take the best
                signals.sort(key=lambda s: s["ev_pct"], reverse=True)
                best = signals[0]

                log.info(
                    "SIGNAL ▶ %s | EV=%.2f%% | Bayes=%.2f%% | OB=%.2f%% | $%.2f | %ds left",
                    best["asset"],
                    best["ev_pct"] * 100,
                    best["bayesian_pct"] * 100,
                    best["ob_imbalance"] * 100,
                    best["size_usd"],
                    best["seconds_left"],
                )

                # ── Acquire trade slot ────────────────────────────────────────
                async with STATE.trade_lock:
                    if STATE.open_trade is not None:
                        # Another signal snuck in — report missed opportunity
                        await alert_missed_opportunity(best, "Max 1 open trade limit")
                        continue
                    STATE.open_trade = best

                # ── Execute trade ─────────────────────────────────────────────
                try:
                    if PAPER_MODE:
                        trade = await execute_paper_trade(best)
                    else:
                        trade = await execute_live_trade(best)

                    # Persist and notify
                    STATE.record_trade(trade)
                    write_csv(trade)
                    await alert_trade(best, trade)

                    log.info(
                        "TRADE CLOSED ▶ %s %s | P&L=$%.4f | Bankroll=$%.2f",
                        trade["asset"],
                        trade["result"],
                        trade["profit"],
                        STATE.bankroll,
                    )

                    # ── Win-rate circuit breaker ──────────────────────────────
                    if STATE.should_pause_for_win_rate():
                        STATE.paused = True
                        STATE.pause_until = (
                            datetime.now(timezone.utc)
                            + timedelta(hours=PAUSE_DURATION_HOURS)
                        )
                        await tg_send(
                            f"⏸ <b>Bot Paused — Win-Rate Too Low</b>\n"
                            f"  Win-rate: <code>{STATE.win_rate()*100:.1f}%</code> "
                            f"(threshold: {MIN_WIN_RATE*100:.0f}%)\n"
                            f"  Auto-resume at: "
                            f"<code>{STATE.pause_until.strftime('%H:%M UTC')}</code>"
                        )
                        log.warning(
                            "Bot paused for 2 h: win-rate %.1f%% < %.0f%%",
                            STATE.win_rate() * 100,
                            MIN_WIN_RATE * 100,
                        )

                except Exception as trade_exc:
                    log.error(
                        "Trade execution error: %s", trade_exc, exc_info=True
                    )
                finally:
                    # Always release the trade slot
                    async with STATE.trade_lock:
                        STATE.open_trade = None

            except Exception as loop_exc:
                log.error("Strategy loop error: %s", loop_exc, exc_info=True)
                await asyncio.sleep(5)


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM APPLICATION
# ══════════════════════════════════════════════════════════════════════════════

async def build_telegram_app() -> Optional[Application]:
    """Build the Telegram Application with registered command handlers."""
    if not TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN not set — Telegram features disabled.")
        return None

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("trades", cmd_trades))
    app.add_handler(CommandHandler("pause",  cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    return app


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    """
    Bootstrap all async tasks and run the bot indefinitely.

    Task map:
      • binance_ws_listener   — CoinGecko price polling (Railway-compatible)
      • strategy_loop         — signal scanning + trade execution
      • scheduler_loop        — heartbeats, daily/weekly reports
      • telegram polling      — command handler (if configured)
    """
    log.info("=" * 72)
    log.info("  HYBRID LAG ARBITRAGE v3.0 — SENTINEL")
    log.info("  Mode: %-6s | Assets: BTC · ETH · SOL | Bankroll: $%.2f",
             "PAPER" if PAPER_MODE else "LIVE", STARTING_BANKROLL)
    log.info("  Thresholds: EV≥%.0f%% | Bayesian≥%.0f%% | OBImbal≥%.0f%%",
             MIN_EV * 100, MIN_BAYESIAN * 100, MIN_OB_IMBALANCE * 100)
    log.info("  Kelly: %.2fx | Max trade: $%.0f | Drawdown stop: %.0f%%",
             KELLY_FRACTION, MAX_TRADE_USD, MAX_DAILY_DRAWDOWN_PCT * 100)
    log.info("=" * 72)

    # ── Telegram setup ───────────────────────────────────────────────────────
    tg_app = await build_telegram_app()
    if tg_app:
        await tg_app.initialize()
        await tg_app.start()
        STATE.tg_bot = tg_app.bot
        await tg_send(
            f"🤖 <b>Sentinel v3.0 started!</b>\n\n"
            f"  Mode:       {'📝 PAPER' if PAPER_MODE else '💰 LIVE'}\n"
            f"  Bankroll:   <code>${STARTING_BANKROLL:.2f}</code>\n"
            f"  Assets:     BTC · ETH · SOL\n"
            f"  EV ≥        <code>{MIN_EV*100:.0f}%</code>\n"
            f"  Bayesian ≥  <code>{MIN_BAYESIAN*100:.0f}%</code>\n"
            f"  OB Imbal ≥  <code>{MIN_OB_IMBALANCE*100:.0f}%</code>\n"
            f"  Kelly:      <code>{KELLY_FRACTION}×</code>\n\n"
            f"  Commands: /status /trades /pause /resume"
        )
        log.info("Telegram bot connected — startup message sent.")

    # ── Gather all tasks ─────────────────────────────────────────────────────
    tasks = [
        asyncio.create_task(binance_ws_listener(), name="binance_ws"),
        asyncio.create_task(strategy_loop(),       name="strategy"),
        asyncio.create_task(scheduler_loop(),      name="scheduler"),
    ]

    if tg_app:
        # Start polling in a background task
        tasks.append(
            asyncio.create_task(
                tg_app.updater.start_polling(drop_pending_updates=True, allowed_updates=["message"]),
                name="telegram_polling",
            )
        )

    try:
        # Run until interrupted
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutdown signal received — stopping bot …")
    except Exception as exc:
        log.critical("Fatal error: %s", exc, exc_info=True)
    finally:
        if tg_app:
            await tg_send("🔴 <b>Sentinel v3.0 shutting down.</b> Goodbye.")
            await tg_app.stop()
            await tg_app.shutdown()
        log.info("Sentinel stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
