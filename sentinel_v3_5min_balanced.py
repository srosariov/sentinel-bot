"""
╔══════════════════════════════════════════════════════════════════════════════╗
║ HYBRID LAG ARBITRAGE v3.0 — "SENTINEL" ║
║ 5-Min Balanced Mode · BTC + ETH + SOL Simultaneous ║
║ ║
║ Strategy: ║
║ • Scan BTC / ETH / SOL 5-minute Polymarket markets in parallel ║
║ • Enter only if: EV ≥ 16% | Bayesian posterior ≥ 78% | OB imbal ≥ 18% ║
║ • Trade only in the last 180 s of each 5-minute market ║
║ • Volatility filter: 1h Binance volume > $80k ║
║ • Sizing: 0.35x fractional Kelly ║
║ • Exit: auto-sell on price adjustment or after 25 s with no movement ║
║ • Max 1 open trade at a time ║
║ • Rolling memory (last 50 trades); pause 2 h if win-rate < 78% ║
║ • 20% daily drawdown hard stop ║
║ ║
║ Paper Mode + full Telegram suite ║
║ Deploy-ready for Railway.app ║
╚══════════════════════════════════════════════════════════════════════════════╝
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
MIN_EV: float = 0.16
MIN_BAYESIAN: float = 0.78
MIN_OB_IMBALANCE: float = 0.18
TRADE_WINDOW_SECONDS: int = 180
MIN_1H_VOLUME_USD: float = 80_000.0
KELLY_FRACTION: float = 0.35
EXIT_TIMEOUT_SECONDS: int = 25
MAX_OPEN_TRADES: int = 1

# ────────────────────────── Risk Constants ────────────────────────────────────
ROLLING_WINDOW: int = 50
MIN_WIN_RATE: float = 0.78
PAUSE_DURATION_HOURS: int = 2
MAX_DAILY_DRAWDOWN_PCT: float = 0.20

# ────────────────────────── Binance WebSocket ─────────────────────────────────
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
HEARTBEAT_INTERVAL_HOURS: int = 6
DAILY_SUMMARY_HOUR: int = 0
WEEKLY_REPORT_DAY: int = 6
CSV_FILE = "trade_history.csv"

# ══════════════════════════════════════════════════════════════════════════════
# SHARED STATE
# ══════════════════════════════════════════════════════════════════════════════
class BotState:
    def __init__(self) -> None:
        self.bankroll: float = STARTING_BANKROLL
        self.daily_start_bankroll: float = STARTING_BANKROLL
        self.daily_pnl: float = 0.0
        self.open_trade: Optional[dict] = None
        self.trade_history: deque = deque(maxlen=ROLLING_WINDOW)
        self.all_trades: list = []
        self.paused: bool = False
        self.pause_until: Optional[datetime] = None
        self.hard_stopped: bool = False
        self.klines: dict = {asset: {} for asset in BINANCE_STREAMS}
        self.markets: dict = {asset: [] for asset in BINANCE_STREAMS}
        self.tg_bot: Optional[Bot] = None
        self.total_signals_scanned: int = 0
        self.missed_opportunities: int = 0
        self.trade_lock = asyncio.Lock()

    def win_rate(self) -> float:
        if not self.trade_history:
            return 1.0
        wins = sum(1 for t in self.trade_history if t.get("profit", 0) > 0)
        return wins / len(self.trade_history)

    def should_pause_for_win_rate(self) -> bool:
        if len(self.trade_history) < ROLLING_WINDOW:
            return False
        return self.win_rate() < MIN_WIN_RATE

    def drawdown_pct(self) -> float:
        if self.daily_start_bankroll == 0:
            return 0.0
        loss = self.daily_start_bankroll - self.bankroll
        return max(0.0, loss / self.daily_start_bankroll)

    def risk_level(self) -> str:
        dd = self.drawdown_pct()
        if dd < 0.07:
            return "BAJO"
        if dd < 0.14:
            return "MEDIO"
        return "ALTO"

    def record_trade(self, trade: dict) -> None:
        self.trade_history.append(trade)
        self.all_trades.append(trade)
        self.bankroll += trade.get("profit", 0.0)
        self.daily_pnl += trade.get("profit", 0.0)

    def reset_daily(self) -> None:
        self.daily_start_bankroll = self.bankroll
        self.daily_pnl = 0.0
        self.hard_stopped = False

STATE = BotState()

# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM HELPERS
# ══════════════════════════════════════════════════════════════════════════════
async def tg_send(text: str, parse_mode: str = "HTML") -> None:
    if not STATE.tg_bot or not TELEGRAM_CHAT_ID:
        return
    try:
        await STATE.tg_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=parse_mode)
    except Exception as exc:
        log.warning("Telegram send error: %s", exc)

async def tg_send_document(filepath: str, caption: str = "") -> None:
    if not STATE.tg_bot or not TELEGRAM_CHAT_ID:
        return
    try:
        with open(filepath, "rb") as fh:
            await STATE.tg_bot.send_document(chat_id=TELEGRAM_CHAT_ID, document=fh, caption=caption)
    except Exception as exc:
        log.warning("Telegram document send error: %s", exc)

# ══════════════════════════════════════════════════════════════════════════════
# CSV PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════
CSV_FIELDNAMES = [
    "timestamp", "asset", "market_id", "side",
    "entry_price", "exit_price", "size_usd",
    "ev_pct", "bayesian_pct", "ob_imbalance",
    "lag_seconds", "hold_seconds", "profit", "result",
]

def write_csv(trade: dict) -> None:
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
# QUANT ENGINE
# ══════════════════════════════════════════════════════════════════════════════
def bayesian_posterior(base_prob: float, lag_signal_strength: float, ob_imbalance: float, volume_ratio: float) -> float:
    prior = max(0.01, min(0.99, base_prob))
    lr_lag = 1.0 + 2.5 * lag_signal_strength
    lr_ob = 1.0 + 1.8 * ob_imbalance
    lr_vol = 1.0 + 0.4 * min(volume_ratio, 2.0)
    numerator = prior * lr_lag * lr_ob * lr_vol
    denominator = numerator + (1.0 - prior)
    posterior = numerator / denominator
    return float(min(max(posterior, 0.001), 0.999))

def compute_ev(polymarket_prob: float, true_prob: float) -> float:
    if polymarket_prob <= 0:
        return 0.0
    return (true_prob - polymarket_prob) / polymarket_prob

def order_book_imbalance(bids: list, asks: list) -> float:
    bid_vol = sum(float(b[1]) for b in bids) if bids else 0.0
    ask_vol = sum(float(a[1]) for a in asks) if asks else 0.0
    total = bid_vol + ask_vol
    if total == 0:
        return 0.0
    return abs(bid_vol - ask_vol) / total

def kelly_size(ev: float, win_prob: float, bankroll: float) -> float:
    if ev <= 0 or win_prob <= 0 or win_prob >= 1:
        return 0.0
    b = ev / win_prob
    q = 1.0 - win_prob
    f_star = max(0.0, (b * win_prob - q) / b)
    raw = KELLY_FRACTION * f_star * bankroll
    return float(min(raw, MAX_TRADE_USD))

# ══════════════════════════════════════════════════════════════════════════════
# LAG SIGNAL ENGINE
# ══════════════════════════════════════════════════════════════════════════════
def compute_lag_signal(asset: str, polymarket_prob: float) -> tuple[float, float]:
    kline = STATE.klines.get(asset, {})
    if not kline:
        return 0.0, polymarket_prob
    o = kline.get("open", 0.0)
    c = kline.get("close", 0.0)
    h = kline.get("high", 0.0)
    l = kline.get("low", 0.0)
    candle_range = max(h - l, 1e-9)
    body = c - o
    body_pct = abs(body) / candle_range
    direction = 1 if body > 0 else -1
    if direction == 1 and polymarket_prob < 0.62:
        lag_strength = body_pct * (0.62 - polymarket_prob) / 0.62
        true_prob = polymarket_prob + lag_strength * 0.35
    elif direction == -1 and polymarket_prob > 0.38:
        lag_strength = body_pct * (polymarket_prob - 0.38) / 0.62
        true_prob = polymarket_prob - lag_strength * 0.35
    else:
        lag_strength = 0.0
        true_prob = polymarket_prob
    return float(lag_strength), float(min(max(true_prob, 0.01), 0.99))

# ══════════════════════════════════════════════════════════════════════════════
# POLYMARKET — MARKET DISCOVERY & ORDER BOOK
# ══════════════════════════════════════════════════════════════════════════════
async def fetch_polymarket_5min_markets(session: aiohttp.ClientSession) -> None:
    # (código original sin cambios)
    asset_keywords = {"BTC": ["bitcoin", "btc"], "ETH": ["ethereum", "eth"], "SOL": ["solana", "sol"]}
    try:
        url = f"{GAMMA_BASE}/markets"
        params = {"active": "true", "closed": "false", "order": "volume24hr", "ascending": "false", "limit": "200"}
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
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
                end_str = m.get("endDate") or m.get("end_date_iso") or ""
                if not end_str:
                    continue
                try:
                    end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    seconds_left = (end_dt - now).total_seconds()
                    if 0 < seconds_left <= 600:
                        m["_seconds_left"] = seconds_left
                        m["_asset"] = asset
                        found.append(m)
                except ValueError:
                    continue
            STATE.markets[asset] = found
    except Exception as exc:
        log.warning("Market discovery error: %s", exc)

async def fetch_order_book(session: aiohttp.ClientSession, market_id: str) -> tuple[list, list]:
    try:
        url = f"{CLOB_BASE}/book"
        params = {"token_id": market_id}
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status != 200:
                return [], []
            data = await resp.json()
        return data.get("bids", []), data.get("asks", [])
    except Exception:
        return [], []

# ══════════════════════════════════════════════════════════════════════════════
# BINANCE WEBSOCKET LISTENER (CORREGIDO CON USER-AGENT)
# ══════════════════════════════════════════════════════════════════════════════
async def binance_ws_listener() -> None:
    stream_path = "/".join(BINANCE_STREAMS.values())
    url = BINANCE_WS_URL.format(stream_path)
    while True:
        try:
            log.info("Connecting to Binance WebSocket …")
            async with websockets.connect(
                url,
                extra_headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
                },
                ping_interval=20,
                ping_timeout=30,
            ) as ws:
                log.info("Binance WebSocket connected.")
                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                        stream_name = msg.get("stream", "")
                        kline = msg.get("data", {}).get("k", {})
                        for asset, expected_stream in BINANCE_STREAMS.items():
                            if stream_name == expected_stream:
                                STATE.klines[asset] = {
                                    "open": float(kline.get("o", 0)),
                                    "high": float(kline.get("h", 0)),
                                    "low": float(kline.get("l", 0)),
                                    "close": float(kline.get("c", 0)),
                                    "volume_quote": float(kline.get("q", 0)),
                                    "is_closed": kline.get("x", False),
                                    "open_time": kline.get("t", 0),
                                    "close_time": kline.get("T", 0),
                                    "ts": time.time(),
                                }
                                break
                    except Exception as parse_err:
                        log.debug("Binance parse error: %s", parse_err)
        except Exception as exc:
            log.warning("Binance WebSocket error: %s — reconnecting in 5 s …", exc)
            await asyncio.sleep(5)

# (El resto del código es idéntico al original – no lo cambio para evitar errores)

# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
async def main() -> None:
    log.info("=" * 72)
    log.info(" HYBRID LAG ARBITRAGE v3.0 — SENTINEL")
    log.info(" Mode: %-6s | Assets: BTC · ETH · SOL | Bankroll: $%.2f",
             "PAPER" if PAPER_MODE else "LIVE", STARTING_BANKROLL)
    log.info("=" * 72)

    tg_app = await build_telegram_app()
    if tg_app:
        await tg_app.initialize()
        await tg_app.start()
        STATE.tg_bot = tg_app.bot
        await tg_send(
            f"🤖 <b>Sentinel v3.0 started!</b>\n\n"
            f" Mode: {'📝 PAPER' if PAPER_MODE else '💰 LIVE'}\n"
            f" Bankroll: <code>${STARTING_BANKROLL:.2f}</code>\n"
            f" Assets: BTC · ETH · SOL\n"
            f" Commands: /status /trades /pause /resume"
        )

    tasks = [
        asyncio.create_task(binance_ws_listener(), name="binance_ws"),
        asyncio.create_task(strategy_loop(), name="strategy"),
        asyncio.create_task(scheduler_loop(), name="scheduler"),
    ]
    if tg_app:
        tasks.append(asyncio.create_task(tg_app.updater.start_polling(drop_pending_updates=True)))

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutdown signal received — stopping bot …")
    finally:
        if tg_app:
            await tg_send("🔴 <b>Sentinel v3.0 shutting down.</b> Goodbye.")
            await tg_app.stop()
            await tg_app.shutdown()
        log.info("Sentinel stopped cleanly.")

if __name__ == "__main__":
    asyncio.run(main())
