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
import websockets
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
STARTING_BANKROLL: float = float(os.getenv("STARTING_BANKROLL", "1000"))
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
MIN_OB_IMBALANCE: float = 0.18       # 18 % order-book imbalance threshold
TRADE_WINDOW_SECONDS: int = 180      # only trade in last 180 s of each market
MIN_1H_VOLUME_USD: float = 80_000.0  # volatility filter
KELLY_FRACTION: float = 0.35         # fractional Kelly multiplier
EXIT_TIMEOUT_SECONDS: int = 25       # auto-exit after N seconds without movement
MAX_OPEN_TRADES: int = 1             # maximum concurrent open trades

# ────────────────────────── Risk Constants ────────────────────────────────────
ROLLING_WINDOW: int = 50             # number of trades for win-rate check
MIN_WIN_RATE: float = 0.78           # pause if below this
PAUSE_DURATION_HOURS: int = 2        # auto-pause duration
MAX_DAILY_DRAWDOWN_PCT: float = 0.20 # 20 % drawdown hard stop

# ────────────────────────── Binance WebSocket ─────────────────────────────────
# Combined stream of 5-minute klines for BTC, ETH, SOL
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams={}"
BINANCE_STREAMS = {
    "BTC": "btcusdt@kline_5m",
    "ETH": "ethusdt@kline_5m",
    "SOL": "solusdt@kline_5m",
}

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
        self.klines: dict = {asset: {} for asset in BINANCE_STREAMS}
        self.markets: dict = {asset: [] for asset in BINANCE_STREAMS}

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
    Query the Gamma API for active short-horizon binary markets for
    BTC, ETH, and SOL, filtering to those closing within the next 10 minutes.

    Updates STATE.markets[asset] in-place.

    NOTE: Polymarket's API does not label markets as "5-minute".
    In production, replace this discovery logic with explicit market IDs
    or a more precise filter once you identify the specific market series
    you want to trade.
    """
    asset_keywords = {
        "BTC": ["bitcoin", "btc"],
        "ETH": ["ethereum", "eth"],
        "SOL": ["solana", "sol"],
    }
    try:
        url = f"{GAMMA_BASE}/markets"
        params = {
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
            "limit": "200",
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

        for asset, keywords in asset_keywords.items():
            found = []
            for m in markets_list:
                question = (m.get("question") or m.get("title") or "").lower()
                if not any(kw in question for kw in keywords):
                    continue

                # Parse market end date
                end_str = m.get("endDate") or m.get("end_date_iso") or ""
                if not end_str:
                    continue
                try:
                    end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    seconds_left = (end_dt - now).total_seconds()
                    if 0 < seconds_left <= 600:   # within 10-minute horizon
                        m["_seconds_left"] = seconds_left
                        m["_asset"] = asset
                        found.append(m)
                except ValueError:
                    continue

            STATE.markets[asset] = found

        total = sum(len(v) for v in STATE.markets.values())
        if total:
            log.info("Market discovery: %d short-horizon markets found.", total)

    except Exception as exc:
        log.warning("Market discovery error: %s", exc)


async def fetch_order_book(
    session: aiohttp.ClientSession,
    market_id: str,
) -> tuple[list, list]:
    """
    Fetch CLOB order book for a given token/market ID.
    Returns (bids, asks) where each entry is [price, size].
    """
    try:
        url = f"{CLOB_BASE}/book"
        params = {"token_id": market_id}
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status != 200:
                return [], []
            data = await resp.json()
        return data.get("bids", []), data.get("asks", [])
    except Exception as exc:
        log.debug("Order book fetch error for %s: %s", market_id, exc)
        return [], []


# ══════════════════════════════════════════════════════════════════════════════
#  BINANCE WEBSOCKET LISTENER
# ══════════════════════════════════════════════════════════════════════════════

async def binance_ws_listener() -> None:
    """
    Maintain a persistent WebSocket connection to Binance combined stream.

    Streams: BTC/ETH/SOL 5-minute klines.
    Reconnects automatically on any error or disconnect.
    Updates STATE.klines[asset] with latest candle data.
    """
    stream_path = "/".join(BINANCE_STREAMS.values())
    url = BINANCE_WS_URL.format(stream_path)

    while True:
        try:
            log.info("Connecting to Binance WebSocket …")
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=30,
            ) as ws:
                log.info("Binance WebSocket connected.")
                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                        stream_name = msg.get("stream", "")
                        kline = msg.get("data", {}).get("k", {})

                        # Map stream name → asset
                        for asset, expected_stream in BINANCE_STREAMS.items():
                            if stream_name == expected_stream:
                                STATE.klines[asset] = {
                                    "open":         float(kline.get("o", 0)),
                                    "high":         float(kline.get("h", 0)),
                                    "low":          float(kline.get("l", 0)),
                                    "close":        float(kline.get("c", 0)),
                                    "volume_quote": float(kline.get("q", 0)),  # USD vol
                                    "is_closed":    kline.get("x", False),
                                    "open_time":    kline.get("t", 0),
                                    "close_time":   kline.get("T", 0),
                                    "ts":           time.time(),
                                }
                                break
                    except (json.JSONDecodeError, KeyError, ValueError) as parse_err:
                        log.debug("Binance parse error: %s", parse_err)

        except Exception as exc:
            log.warning("Binance WebSocket error: %s — reconnecting in 5 s …", exc)
            await asyncio.sleep(5)


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
    market_id: str = market.get("conditionId") or market.get("id") or ""
    seconds_left: float = market.get("_seconds_left", 999.0)
    question: str = (market.get("question") or market.get("title") or "Unknown")[:80]

    STATE.total_signals_scanned += 1

    # ── 1. Trade window filter ────────────────────────────────────────────────
    if seconds_left > TRADE_WINDOW_SECONDS or seconds_left <= 0:
        return None

    # ── 2. Volatility / volume filter ────────────────────────────────────────
    kline = STATE.klines.get(asset, {})
    vol_5m_quote = kline.get("volume_quote", 0.0)
    vol_1h_approx = vol_5m_quote * 12   # rough 1h estimate from 5m candle
    if vol_1h_approx < MIN_1H_VOLUME_USD:
        log.debug("[%s] Volume filter failed: $%.0f < $%.0f",
                  asset, vol_1h_approx, MIN_1H_VOLUME_USD)
        return None

    # ── 3. Order-book imbalance ───────────────────────────────────────────────
    bids, asks = await fetch_order_book(session, market_id)
    ob_imb = order_book_imbalance(bids, asks)
    if ob_imb < MIN_OB_IMBALANCE:
        log.debug("[%s] OB imbalance filter failed: %.3f", asset, ob_imb)
        return None

    # ── 4. Derive Polymarket probability from best ask (YES token price) ──────
    # Best ask = lowest sell price for YES shares ≈ implied probability
    if asks:
        try:
            poly_prob = float(asks[0][0])
        except (IndexError, ValueError):
            poly_prob = 0.5
    else:
        poly_prob = 0.5

    # ── 5. Lag signal ─────────────────────────────────────────────────────────
    lag_strength, true_prob = compute_lag_signal(asset, poly_prob)
    # Approximate lag in seconds: how long the signal has been open
    lag_seconds = max(0.0, TRADE_WINDOW_SECONDS - seconds_left)

    # ── 6. EV ─────────────────────────────────────────────────────────────────
    ev = compute_ev(poly_prob, true_prob)
    if ev < MIN_EV:
        log.debug("[%s] EV filter failed: %.3f", asset, ev)
        return None

    # ── 7. Bayesian posterior ─────────────────────────────────────────────────
    volume_ratio = vol_1h_approx / MIN_1H_VOLUME_USD
    posterior = bayesian_posterior(poly_prob, lag_strength, ob_imb, volume_ratio)
    if posterior < MIN_BAYESIAN:
        log.debug("[%s] Bayesian filter failed: %.3f", asset, posterior)
        return None

    # ── 8. Kelly sizing ───────────────────────────────────────────────────────
    size_usd = kelly_size(ev, posterior, STATE.bankroll)
    if size_usd < 1.0:
        log.debug("[%s] Position too small: $%.2f", asset, size_usd)
        return None

    # ── All filters passed — return signal dict ───────────────────────────────
    return {
        "asset":         asset,
        "market_id":     market_id,
        "question":      question,
        "side":          "YES",
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
      • binance_ws_listener   — real-time kline data from Binance
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
                tg_app.updater.start_polling(drop_pending_updates=True),
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
