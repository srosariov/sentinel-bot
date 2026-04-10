"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   SENTINEL POLYMARKET LAG BOT v5.0 — Real Markets Only                      ║
║   Paper Trading → Live-ready · Puerto Rico compatible                        ║
║                                                                              ║
║   Cambios v5.0 vs v4.0:                                                      ║
║   • ELIMINADOS completamente los mercados sintéticos                         ║
║   • Solo opera en mercados REALES de Polymarket                              ║
║   • Sin mercados reales → skip con log claro (no sintéticos)                 ║
║   • Discord Webhook opcional (variable DISCORD_WEBHOOK_URL)                  ║
║   • /trades → dos secciones: ABIERTOS + últimos 10 cerrados                 ║
║   • /active  → solo posiciones abiertas con detalle completo                 ║
║   • /pnl     → resumen histórico de PnL                                      ║
║   • Telegram HTTP puro (sin python-telegram-bot)                             ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os, csv, time, logging, threading, json, random, re
from datetime import datetime, timezone, timedelta
from math import erf, sqrt
from typing import Optional
from dataclasses import dataclass, field
import requests

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() != "false"

TELEGRAM_TOKEN      = os.getenv("TELEGRAM_TOKEN",      "YOUR_TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID",    "YOUR_CHAT_ID")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")   # opcional

POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")
POLY_API_URL     = "https://clob.polymarket.com"
GAMMA_API_URL    = "https://gamma-api.polymarket.com"

INITIAL_CAPITAL = float(os.getenv("INITIAL_CAPITAL", "500"))
BET_SIZE_PCT    = float(os.getenv("BET_SIZE_PCT",    "0.04"))
MAX_DAILY_DD    = float(os.getenv("MAX_DAILY_DD",    "0.15"))
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES",   "1"))

EV_MIN_NORMAL        = float(os.getenv("EV_MIN",    "0.08"))
BAYESIAN_MIN_NORMAL  = float(os.getenv("BAYES_MIN", "0.65"))
EV_MIN_RELAXED       = 0.06
BAYESIAN_MIN_RELAXED = 0.60

SCAN_INTERVAL_SECS    = 60
MIN_ACTIVITY_MINS     = 30
POSITION_TIMEOUT_MINS = 6

TARGET_MARKETS = [
    ("btc", "bitcoin",  "BTC"),
    ("eth", "ethereum", "ETH"),
    ("sol", "solana",   "SOL"),
]

TRADE_CSV = "trade_history.csv"
LOG_FILE  = "sentinel_v5.log"

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("SentinelV5")

# ─────────────────────────────────────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class MarketSnapshot:
    condition_id : str   = ""
    question     : str   = ""
    asset        : str   = ""
    threshold    : float = 0.0
    yes_price    : float = 0.0
    no_price     : float = 0.0
    real_price   : float = 0.0
    volume_24h   : float = 0.0
    ob_empty     : bool  = True
    ob_bids      : list  = field(default_factory=list)
    ob_asks      : list  = field(default_factory=list)
    end_time     : Optional[datetime] = None

@dataclass
class TradeSignal:
    market        : MarketSnapshot
    side          : str   = ""
    entry_price   : float = 0.0
    fair_value    : float = 0.0
    ev            : float = 0.0
    bayesian_prob : float = 0.0
    bet_size      : float = 0.0
    mode          : str   = "normal"
    score         : float = 0.0

@dataclass
class OpenTrade:
    signal      : TradeSignal
    open_time   : datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    open_price  : float    = 0.0
    size_usdc   : float    = 0.0
    resolved    : bool     = False
    pnl         : float    = 0.0
    exit_price  : float    = 0.0
    exit_reason : str      = ""

# ─────────────────────────────────────────────────────────────────────────────
# BOT STATE
# ─────────────────────────────────────────────────────────────────────────────

class BotState:
    def __init__(self):
        self.paused           = False
        self.equity           = INITIAL_CAPITAL
        self.day_start_equity = INITIAL_CAPITAL
        self.daily_pnl        = 0.0
        self.total_pnl        = 0.0
        self.daily_dd         = 0.0
        self.circuit_broken   = False
        self.open_trades      : list[OpenTrade] = []
        self.total_trades     = 0
        self.winning_trades   = 0
        self.last_day         = datetime.now(timezone.utc).date()
        self.start_time       = datetime.now(timezone.utc)
        self.last_signal_time    = datetime.now(timezone.utc)
        self.relaxed_mode        = False
        self.cycles_run          = 0
        self.total_signals_found = 0
        self.cycles_no_markets   = 0

    @property
    def win_rate(self) -> float:
        return self.winning_trades / self.total_trades if self.total_trades else 0.0

    @property
    def ev_threshold(self) -> float:
        return EV_MIN_RELAXED if self.relaxed_mode else EV_MIN_NORMAL

    @property
    def bayesian_threshold(self) -> float:
        return BAYESIAN_MIN_RELAXED if self.relaxed_mode else BAYESIAN_MIN_NORMAL

    def check_relaxed_mode(self):
        mins = (datetime.now(timezone.utc) - self.last_signal_time).total_seconds() / 60
        was  = self.relaxed_mode
        self.relaxed_mode = mins >= MIN_ACTIVITY_MINS
        if self.relaxed_mode and not was:
            log.info(f"MODO RELAJADO — {mins:.0f}min sin señales | EV>={EV_MIN_RELAXED:.0%} Bayes>={BAYESIAN_MIN_RELAXED:.0%}")

    def daily_reset(self):
        today = datetime.now(timezone.utc).date()
        if today != self.last_day:
            log.info(f"Reset diario — PnL ayer: ${self.daily_pnl:+.2f}")
            self.equity           += self.daily_pnl
            self.day_start_equity  = self.equity
            self.daily_pnl = self.daily_dd = 0.0
            self.circuit_broken    = False
            self.last_day          = today

    def update_dd(self):
        if self.day_start_equity > 0:
            self.daily_dd = max(0, (self.day_start_equity - self.equity) / self.day_start_equity)
        if self.daily_dd >= MAX_DAILY_DD and not self.circuit_broken:
            self.circuit_broken = True
            log.warning(f"CIRCUIT BREAKER — DD={self.daily_dd:.1%}")

    def on_close(self, trade: OpenTrade):
        self.daily_pnl += trade.pnl
        self.total_pnl += trade.pnl
        self.equity    += trade.pnl
        self.total_trades += 1
        if trade.pnl > 0:
            self.winning_trades += 1
        if trade.pnl < 0:
            self.update_dd()
        self.open_trades = [t for t in self.open_trades if t is not trade]

    def can_trade(self) -> tuple[bool, str]:
        if self.paused:
            return False, "Pausado"
        if self.circuit_broken:
            return False, f"Circuit breaker (DD={self.daily_dd:.1%})"
        if len(self.open_trades) >= MAX_OPEN_TRADES:
            return False, f"Max posiciones ({MAX_OPEN_TRADES})"
        return True, "OK"

    def summary(self) -> str:
        uptime = str(datetime.now(timezone.utc) - self.start_time).split(".")[0]
        return (
            f"*SENTINEL POLYMARKET v5.0*\n"
            f"Modo: `{'PAPER' if PAPER_MODE else 'LIVE'}` | `{'RELAJADO' if self.relaxed_mode else 'NORMAL'}`\n"
            f"────────────────\n"
            f"Equity: `${self.equity:.2f}`\n"
            f"PnL hoy: `${self.daily_pnl:+.2f}` | DD: `{self.daily_dd:.1%}`\n"
            f"PnL total: `${self.total_pnl:+.2f}`\n"
            f"────────────────\n"
            f"Trades: `{self.total_trades}` | Win: `{self.win_rate:.0%}`\n"
            f"Señales: `{self.total_signals_found}` | Ciclos: `{self.cycles_run}`\n"
            f"Abiertas: `{len(self.open_trades)}` | Sin mercados: `{self.cycles_no_markets}`\n"
            f"────────────────\n"
            f"EV mín: `{self.ev_threshold:.0%}` | Bayes: `{self.bayesian_threshold:.0%}`\n"
            f"Circuit: `{'ON' if self.circuit_broken else 'OFF'}`\n"
            f"Uptime: `{uptime}`\n"
            f"Estado: `{'PAUSADO' if self.paused else 'ACTIVO'}`"
        )

# ─────────────────────────────────────────────────────────────────────────────
# COINGECKO CLIENT
# ─────────────────────────────────────────────────────────────────────────────

class CoinGeckoClient:
    BASE    = "https://api.coingecko.com/api/v3"
    ALL_IDS = "bitcoin,ethereum,solana"

    def __init__(self):
        self._cache     : dict  = {}
        self._cache_ts  : float = 0.0
        self._cache_ttl : float = 90.0
        self._refreshing: bool  = False
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "SentinelPolyBot/5.0"

    def prefetch(self):
        """Una sola llamada batch por ciclo. Lock anti-duplicado."""
        if self._refreshing or (time.time() - self._cache_ts <= self._cache_ttl):
            return
        self._refreshing = True
        try:
            r = self.session.get(
                f"{self.BASE}/simple/price",
                params={"ids": self.ALL_IDS, "vs_currencies": "usd", "include_24hr_vol": "true"},
                timeout=12,
            )
            if r.status_code == 429:
                log.warning("CoinGecko rate limit — usando caché")
                self._cache_ts = time.time() - self._cache_ttl + 60
                return
            r.raise_for_status()
            for coin, vals in r.json().items():
                self._cache[coin]          = float(vals["usd"])
                self._cache[f"{coin}_vol"] = float(vals.get("usd_24h_vol", 0))
            self._cache_ts = time.time()
            log.info(
                f"CoinGecko — "
                f"BTC=${self._cache.get('bitcoin',0):,.0f} "
                f"ETH=${self._cache.get('ethereum',0):,.0f} "
                f"SOL=${self._cache.get('solana',0):,.0f}"
            )
        except Exception as e:
            log.error(f"CoinGecko error: {e}")
        finally:
            self._refreshing = False

    def get_price(self, coin_id: str) -> Optional[float]:
        return self._cache.get(coin_id)

    def get_volume_24h(self, coin_id: str) -> float:
        return self._cache.get(f"{coin_id}_vol", 0.0)

# ─────────────────────────────────────────────────────────────────────────────
# POLYMARKET CLIENT — Solo mercados REALES, sin sintéticos
# ─────────────────────────────────────────────────────────────────────────────

class PolymarketClient:
    """
    Cliente Polymarket CLOB + Gamma API.
    v5.0: No genera mercados sintéticos bajo ninguna circunstancia.
    Retorna lista vacía cuando no hay mercados reales → ciclo se saltea.
    """
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json", "User-Agent": "SentinelPolyBot/5.0"})

    def _get(self, url: str, params: dict = None, timeout: int = 12) -> Optional[dict]:
        try:
            r = self.session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.debug(f"Poly GET [{url}]: {e}")
            return None

    def get_active_markets(self, asset_keyword: str) -> list[dict]:
        """
        Busca mercados REALES activos de 5min para el asset.
        Tres estrategias de búsqueda en Gamma API.

        IMPORTANTE v5.0: Retorna [] si no hay mercados reales.
        No hay fallback sintético.
        """
        found = []

        # Estrategia 1: tag oficial de 5-minute candles
        data = self._get(f"{GAMMA_API_URL}/markets", params={
            "active": "true", "closed": "false",
            "tag_slug": "crypto-5-minute-candles", "limit": "50",
        })
        if data:
            found.extend(data if isinstance(data, list) else data.get("markets", []))

        # Estrategia 2: búsqueda por keyword si no hubo resultados
        if not found:
            data = self._get(f"{GAMMA_API_URL}/markets", params={
                "active": "true", "closed": "false",
                "search": f"{asset_keyword.upper()} 5", "limit": "50",
            })
            if data:
                found.extend(data if isinstance(data, list) else data.get("markets", []))

        # Estrategia 3: búsqueda general + filtro manual
        if not found:
            data = self._get(f"{GAMMA_API_URL}/markets", params={
                "active": "true", "closed": "false", "limit": "100",
            })
            if data:
                found.extend(data if isinstance(data, list) else data.get("markets", []))

        # Filtrar: solo asset relevante y no expirado
        kw      = asset_keyword.lower()
        now_utc = datetime.now(timezone.utc)
        result  = []
        for m in found:
            q    = (m.get("question", "") or "").lower()
            slug = (m.get("slug", "") or "").lower()
            if kw not in q and kw not in slug:
                continue
            end_str = m.get("endDateIso") or m.get("end_date_iso") or ""
            if end_str:
                try:
                    if datetime.fromisoformat(end_str.replace("Z", "+00:00")) < now_utc:
                        continue
                except Exception:
                    pass
            result.append(m)

        return result

    def get_market_prices(self, market_data: dict) -> dict:
        """
        Extrae precios YES/NO desde: outcomePrices → tokens → CLOB.
        Retorna yes=0, no=0 si no puede obtener precios reales.
        """
        r = {"yes_price": 0.0, "no_price": 0.0, "ob_empty": True, "ob_bids": [], "ob_asks": []}

        # Fuente 1: outcomePrices
        prices = market_data.get("outcomePrices")
        if prices is not None:
            try:
                if isinstance(prices, str):
                    prices = json.loads(prices)
                if isinstance(prices, list) and len(prices) >= 2:
                    y, n = float(prices[0]), float(prices[1])
                    if 0.01 <= y <= 0.99 and 0.01 <= n <= 0.99:
                        r.update(yes_price=y, no_price=n, ob_empty=False)
                        return r
            except Exception as e:
                log.debug(f"outcomePrices parse: {e}")

        # Fuente 2: tokens array
        tokens = market_data.get("tokens") or []
        if len(tokens) >= 2:
            try:
                y, n = float(tokens[0].get("price", 0)), float(tokens[1].get("price", 0))
                if y > 0 and n > 0:
                    r.update(yes_price=y, no_price=n, ob_empty=False)
                    return r
            except Exception:
                pass

        # Fuente 3: CLOB order book
        cid = market_data.get("conditionId") or market_data.get("condition_id") or market_data.get("id", "")
        if cid:
            try:
                book = self.session.get(f"{POLY_API_URL}/book", params={"token_id": cid}, timeout=8).json()
                bids, asks = book.get("bids", []), book.get("asks", [])
                r["ob_bids"], r["ob_asks"] = bids, asks
                if bids or asks:
                    r["ob_empty"] = False
                    bb = float(bids[0]["price"]) if bids else 0
                    ba = float(asks[0]["price"]) if asks else 0
                    if bb and ba:
                        r.update(yes_price=(bb + ba) / 2, no_price=1.0 - (bb + ba) / 2)
            except Exception:
                pass

        return r

    def place_order_paper(self, signal: TradeSignal) -> dict:
        return {
            "order_id":  f"PAPER_{int(time.time())}_{random.randint(1000,9999)}",
            "side":      signal.side, "price": signal.entry_price,
            "size":      signal.bet_size, "status": "filled_paper",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class SignalEngine:
    """Detecta lag precio real (CoinGecko) vs token (Polymarket). Solo mercados reales."""

    def __init__(self, cg: CoinGeckoClient):
        self.cg = cg

    def _parse_threshold(self, question: str) -> Optional[float]:
        for pattern in [r'\$([0-9,]+(?:\.[0-9]+)?)', r'\b([0-9]{4,7}(?:,[0-9]{3})*)\b']:
            m = re.search(pattern, question)
            if m:
                try:
                    val = float(m.group(1).replace(",", ""))
                    if val > 100:
                        return val
                except ValueError:
                    pass
        return None

    def _calc_fair_value(self, real_price: float, threshold: float, asset: str, side: str) -> float:
        """
        Probabilidad real con vol calibrado para 5min crypto.
        Cap en 0.88 — certeza absoluta no existe en mercados cortos.
        """
        vol  = {"BTC": 0.010, "ETH": 0.013, "SOL": 0.018}.get(asset, 0.012)
        dist = (real_price - threshold) / (threshold * vol)
        prob = 0.5 * (1 + erf(dist / sqrt(2)))
        raw  = prob if side == "YES" else (1.0 - prob)
        return min(raw, 0.88)

    def _calc_ev(self, fair_value: float, market_price: float) -> float:
        return fair_value - market_price

    def _calc_bayesian(self, fair_value: float, asset: str,
                       vol_24h: float, real_price: float, threshold: float) -> float:
        dist_pct   = abs(real_price - threshold) / threshold
        dist_bonus = 0.06 if dist_pct >= 0.010 else (0.03 if dist_pct >= 0.005 else -0.02)
        vol_bonus  = 0.04 if vol_24h >= 2e10 else (0.02 if vol_24h >= 5e9 else 0.0)
        return round(min(0.92, max(0.40, fair_value + dist_bonus + vol_bonus)), 4)

    def analyze_market(self, market_data: dict, asset: str, coingecko_id: str,
                       state: BotState, poly: PolymarketClient) -> Optional[TradeSignal]:
        """
        Analiza mercado REAL. Si no hay precios → None.
        Sin fallback sintético (eliminado en v5.0).
        """
        q = (market_data.get("question", "") or market_data.get("title", "")).strip()
        if not q:
            return None

        threshold = self._parse_threshold(q)
        if threshold is None:
            log.debug(f"  [SKIP] Sin umbral en: {q[:60]}")
            return None

        real_price = self.cg.get_price(coingecko_id)
        if real_price is None:
            return None
        vol_24h = self.cg.get_volume_24h(coingecko_id)

        price_data  = poly.get_market_prices(market_data)
        yes_price   = price_data["yes_price"]
        no_price    = price_data["no_price"]
        ob_empty    = price_data["ob_empty"]

        # ── Sin precios del mercado real → skip (NO sintético) ────────────────
        # v5.0: Esta es la diferencia clave vs v4.0
        # v4.0 aplicaba lag sintético aquí.
        # v5.0 simplemente no opera si no hay precios reales.
        if yes_price == 0 and no_price == 0:
            log.debug(f"  [SKIP] Sin precios reales disponibles — no se generan sintéticos")
            return None

        side        = "YES" if real_price > threshold else "NO"
        entry_price = yes_price if side == "YES" else no_price
        fair_value  = self._calc_fair_value(real_price, threshold, asset, side)
        ev          = self._calc_ev(fair_value, entry_price)
        bayesian    = self._calc_bayesian(fair_value, asset, vol_24h, real_price, threshold)
        cid         = market_data.get("conditionId") or market_data.get("condition_id") or market_data.get("id", "")

        log.info(
            f"  [{asset}] ${real_price:,.2f} vs ${threshold:,.2f} | "
            f"side={side} entry={entry_price:.3f} FV={fair_value:.3f} "
            f"EV={ev:.3f} Bayes={bayesian:.3f} OB={'vacío' if ob_empty else 'activo'}"
        )

        # ── Filtros F1-F4 ─────────────────────────────────────────────────────
        if ev < state.ev_threshold:
            log.info(f"  [FAIL F1-EV] {ev:.3f} < {state.ev_threshold:.3f}")
            return None
        log.info(f"  [PASS F1-EV] {ev:.3f}")

        if bayesian < state.bayesian_threshold:
            log.info(f"  [FAIL F2-BAYES] {bayesian:.3f} < {state.bayesian_threshold:.3f}")
            return None
        log.info(f"  [PASS F2-BAYES] {bayesian:.3f}")

        if not ob_empty and price_data["ob_bids"] and price_data["ob_asks"]:
            spread = float(price_data["ob_asks"][0]["price"]) - float(price_data["ob_bids"][0]["price"])
            if spread > 0.30:
                log.info(f"  [FAIL F3-SPREAD] {spread:.3f} > 0.30")
                return None
        log.info(f"  [PASS F3-OB] {'bypass vacío' if ob_empty else 'spread OK'}")

        if entry_price < 0.10 or entry_price > 0.78:
            log.info(f"  [FAIL F4-PRECIO] {entry_price:.3f} fuera de 0.10-0.78")
            return None
        log.info(f"  [PASS F4-PRECIO] {entry_price:.3f}")

        score    = min(100, (ev/0.20)*40 + (bayesian-0.5)/0.5*35 + (1-abs(entry_price-0.5)*2)*25)
        bet_size = round(state.equity * BET_SIZE_PCT, 2)

        signal = TradeSignal(
            market = MarketSnapshot(
                condition_id=cid, question=q, asset=asset,
                threshold=threshold, yes_price=yes_price, no_price=no_price,
                real_price=real_price, volume_24h=vol_24h,
                ob_empty=ob_empty, ob_bids=price_data["ob_bids"], ob_asks=price_data["ob_asks"],
            ),
            side=side, entry_price=entry_price, fair_value=fair_value,
            ev=ev, bayesian_prob=bayesian, bet_size=bet_size,
            mode="relaxed" if state.relaxed_mode else "normal", score=score,
        )
        log.info(f"  ✅ SIGNAL PASSED — {asset} {side} @ {entry_price:.3f} EV={ev:.3f} Bayes={bayesian:.3f} Score={score:.0f} Bet=${bet_size:.2f}")
        return signal

# ─────────────────────────────────────────────────────────────────────────────
# PAPER TRADER
# ─────────────────────────────────────────────────────────────────────────────

class PaperTrader:
    def __init__(self, cg: CoinGeckoClient):
        self.cg = cg

    def open_trade(self, signal: TradeSignal, state: BotState) -> OpenTrade:
        trade = OpenTrade(signal=signal, open_time=datetime.now(timezone.utc),
                          open_price=signal.entry_price, size_usdc=signal.bet_size)
        state.open_trades.append(trade)
        log.info(f"TRADE ABIERTO — {signal.market.asset} {signal.side} @ {signal.entry_price:.3f} | Bet: ${signal.bet_size:.2f} | EV={signal.ev:.3f} Score={signal.score:.0f}")
        return trade

    def check_resolve(self, trade: OpenTrade, state: BotState, coingecko_id: str) -> bool:
        """
        Resolución realista: FV recalculado con precio actual de CoinGecko.
        Convergencia gradual + ruido gaussiano por asset.
        """
        now       = datetime.now(timezone.utc)
        elapsed   = (now - trade.open_time).total_seconds() / 60
        signal    = trade.signal
        asset     = signal.market.asset

        real_price = self.cg.get_price(coingecko_id)
        if real_price is None:
            return False

        engine     = SignalEngine(self.cg)
        current_fv = engine._calc_fair_value(real_price, signal.market.threshold, asset, signal.side)

        convergence = random.uniform(0.20, 0.40)
        noise       = random.gauss(0, {"BTC":0.025,"ETH":0.035,"SOL":0.055}.get(asset, 0.03))
        prev        = trade.exit_price if trade.exit_price > 0 else signal.entry_price
        current_tok = max(0.03, min(0.97, prev + (current_fv - prev) * convergence + noise))
        trade.exit_price = current_tok

        resolved, reason = False, ""
        if elapsed >= POSITION_TIMEOUT_MINS:
            resolved, reason = True, f"timeout_{elapsed:.1f}min"
        elif current_tok >= 0.85:
            resolved, reason = True, "target_profit"
        elif current_tok <= signal.entry_price * 0.60:
            resolved, reason = True, "stop_loss"

        if resolved:
            pnl_pct  = (current_tok - signal.entry_price) / signal.entry_price
            pnl_usdc = round(trade.size_usdc * pnl_pct, 4)
            trade.resolved = True
            trade.exit_price = current_tok
            trade.pnl = pnl_usdc
            trade.exit_reason = reason
            log.info(f"TRADE CERRADO [{'WIN' if pnl_usdc>=0 else 'LOSS'}] — {asset} {signal.side} | Entry={signal.entry_price:.3f} Exit={current_tok:.3f} | FV={current_fv:.3f} | PnL=${pnl_usdc:+.4f} ({pnl_pct:+.1%}) | {reason}")

        return resolved

# ─────────────────────────────────────────────────────────────────────────────
# CSV LOGGER
# ─────────────────────────────────────────────────────────────────────────────

CSV_HEADERS = ["timestamp","asset","side","question","entry_price","exit_price",
               "bet_size","pnl_usdc","pnl_pct","ev","bayesian","score",
               "real_price","threshold","mode","exit_reason"]

def init_csv():
    if not os.path.exists(TRADE_CSV):
        with open(TRADE_CSV,"w",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_HEADERS)

def log_trade_csv(trade: OpenTrade):
    s, m = trade.signal, trade.signal.market
    with open(TRADE_CSV,"a",newline="",encoding="utf-8") as f:
        csv.writer(f).writerow([
            datetime.now(timezone.utc).isoformat(), m.asset, s.side, m.question[:80],
            round(trade.open_price,4), round(trade.exit_price,4), round(trade.size_usdc,4),
            round(trade.pnl,4), round((trade.exit_price-trade.open_price)/max(trade.open_price,0.001),4),
            round(s.ev,4), round(s.bayesian_prob,4), round(s.score,1),
            round(m.real_price,2), round(m.threshold,2), s.mode, trade.exit_reason,
        ])

def read_last_trades(n: int = 10) -> list:
    if not os.path.exists(TRADE_CSV):
        return []
    with open(TRADE_CSV,"r",encoding="utf-8") as f:
        return list(csv.DictReader(f))[-n:]

# ─────────────────────────────────────────────────────────────────────────────
# DISCORD (opcional)
# ─────────────────────────────────────────────────────────────────────────────

def send_discord(text: str):
    """Webhook Discord. Solo activo si DISCORD_WEBHOOK_URL está configurado."""
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL,
                      json={"content": text.replace("*","**")[:2000]}, timeout=8)
    except Exception as e:
        log.error(f"Discord error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM — HTTP puro, sin python-telegram-bot
# ─────────────────────────────────────────────────────────────────────────────

class TelegramBot:
    BASE = "https://api.telegram.org/bot"

    def __init__(self, token: str, chat_id: str, state: BotState):
        self.token   = token
        self.chat_id = chat_id
        self.state   = state
        self.session = requests.Session()
        self.offset  = 0
        self._stop   = False

    def _url(self, m: str) -> str:
        return f"{self.BASE}{self.token}/{m}"

    def send(self, text: str) -> bool:
        if not self.token or "YOUR_" in self.token:
            log.info(f"[TG] {text[:80]}")
            return False
        try:
            r = self.session.post(self._url("sendMessage"),
                json={"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"}, timeout=10)
            return r.json().get("ok", False)
        except Exception as e:
            log.error(f"TG send: {e}")
            return False

    def _reply(self, chat_id: int, text: str):
        try:
            self.session.post(self._url("sendMessage"),
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}, timeout=10)
        except Exception as e:
            log.error(f"TG reply: {e}")

    def _handle_command(self, message: dict):
        text    = (message.get("text") or "").strip().lower()
        chat_id = message["chat"]["id"]

        if text.startswith("/status"):
            self._reply(chat_id, self.state.summary())

        elif text.startswith("/trades"):
            lines = []
            # ── Sección 1: TRADES ABIERTOS ────────────────────────────────────
            ot = self.state.open_trades
            if ot:
                lines.append("*📂 TRADES ABIERTOS*\n")
                for t in ot:
                    s       = t.signal
                    elapsed = (datetime.now(timezone.utc) - t.open_time).total_seconds() / 60
                    unreal  = (t.exit_price - t.open_price) if t.exit_price > 0 else 0
                    upnl    = round(t.size_usdc * unreal / max(t.open_price, 0.001), 2)
                    icon    = "📈" if upnl >= 0 else "📉"
                    lines.append(
                        f"{icon} `{s.market.asset}` {s.side} @ `{t.open_price:.3f}` | "
                        f"Bet `${t.size_usdc:.2f}` | `{elapsed:.0f}min` | PnL est. `${upnl:+.2f}`"
                    )
                lines.append("")
            else:
                lines.append("*📂 TRADES ABIERTOS*\n_Ninguna posición abierta_\n")
            # ── Sección 2: ÚLTIMOS 10 CERRADOS ────────────────────────────────
            rows = read_last_trades(10)
            if rows:
                lines.append("*📋 ÚLTIMOS 10 CERRADOS*\n")
                for r in rows:
                    pnl  = float(r.get("pnl_usdc", 0))
                    icon = "✅" if pnl >= 0 else "❌"
                    lines.append(f"{icon} `{r.get('asset')}` {r.get('side')} `{r.get('entry_price')}` → `{r.get('exit_price')}` | `${pnl:+.3f}` | {r.get('exit_reason','')}")
            else:
                lines.append("*📋 ÚLTIMOS 10 CERRADOS*\n_Sin trades cerrados aún_")
            self._reply(chat_id, "\n".join(lines))

        elif text.startswith("/active") or text.startswith("/open"):
            ot = self.state.open_trades
            if not ot:
                self._reply(chat_id, "Sin posiciones abiertas.")
                return
            lines = [f"*📂 POSICIONES ABIERTAS ({len(ot)})*\n"]
            for i, t in enumerate(ot, 1):
                s            = t.signal
                elapsed      = (datetime.now(timezone.utc) - t.open_time).total_seconds() / 60
                timeout_left = max(0, POSITION_TIMEOUT_MINS - elapsed)
                unreal       = (t.exit_price - t.open_price) if t.exit_price > 0 else 0
                upnl         = round(t.size_usdc * unreal / max(t.open_price, 0.001), 2)
                lines.append(
                    f"*#{i} — {s.market.asset} {s.side}*\n"
                    f"  Entrada: `{t.open_price:.3f}` | FV: `{s.fair_value:.3f}`\n"
                    f"  EV: `{s.ev:.3f}` | Bayes: `{s.bayesian_prob:.3f}` | Score: `{s.score:.0f}`\n"
                    f"  Bet: `${t.size_usdc:.2f}` | Modo: `{s.mode.upper()}`\n"
                    f"  Px entrada: `${s.market.real_price:,.2f}` vs umbral `${s.market.threshold:,.2f}`\n"
                    f"  Pregunta: _{s.market.question[:70]}_\n"
                    f"  Abierto: `{elapsed:.1f}min` | Cierre en: `{timeout_left:.1f}min`\n"
                    f"  PnL est.: `${upnl:+.2f}`"
                )
            self._reply(chat_id, "\n".join(lines))

        elif text.startswith("/pnl"):
            rows  = read_last_trades(500)
            total = len(rows)
            if total == 0:
                self._reply(chat_id, "Sin trades cerrados aún.")
                return
            wins  = sum(1 for r in rows if float(r.get("pnl_usdc",0)) >= 0)
            gross = sum(float(r.get("pnl_usdc",0)) for r in rows)
            best  = max(float(r.get("pnl_usdc",0)) for r in rows)
            worst = min(float(r.get("pnl_usdc",0)) for r in rows)
            avg_ev= sum(float(r.get("ev",0)) for r in rows) / total
            self._reply(chat_id,
                f"*📊 PnL HISTÓRICO*\n"
                f"────────────────\n"
                f"Trades: `{total}` | Wins: `{wins}` | WR: `{wins/total:.0%}`\n"
                f"PnL total: `${gross:+.4f}`\n"
                f"Mejor: `${best:+.4f}` | Peor: `${worst:+.4f}`\n"
                f"EV promedio: `{avg_ev:.3f}`\n"
                f"────────────────\n"
                f"Equity: `${self.state.equity:.2f}`\n"
                f"PnL hoy: `${self.state.daily_pnl:+.2f}`"
            )

        elif text.startswith("/pause"):
            self.state.paused = True
            self._reply(chat_id, "⏸ Pausado. Posiciones abiertas siguen activas.")

        elif text.startswith("/resume"):
            self.state.paused = False
            self._reply(chat_id, "▶ Reanudado.")

        elif text.startswith("/help"):
            self._reply(chat_id,
                "*Sentinel Polymarket v5.0*\n\n"
                "/status  — Estado completo\n"
                "/trades  — Abiertos + últimos 10 cerrados\n"
                "/active  — Solo posiciones abiertas (detalle)\n"
                "/pnl     — Resumen histórico PnL\n"
                "/pause   — Pausar entradas nuevas\n"
                "/resume  — Reanudar trading\n"
                "/help    — Esta ayuda\n\n"
                f"_v5.0 · Solo mercados reales · {'PAPER' if PAPER_MODE else 'LIVE'}_"
            )

    def _poll_loop(self):
        log.info("Telegram polling iniciado")
        while not self._stop:
            try:
                r    = self.session.get(self._url("getUpdates"),
                           params={"offset": self.offset, "timeout": 30, "limit": 10}, timeout=35)
                data = r.json()
                if not data.get("ok"):
                    time.sleep(5)
                    continue
                for update in data.get("result", []):
                    self.offset = update["update_id"] + 1
                    msg = update.get("message") or update.get("edited_message")
                    if msg and (msg.get("text","") or "").startswith("/"):
                        self._handle_command(msg)
            except Exception as e:
                log.error(f"TG poll: {e}")
                time.sleep(5)

    def start_polling(self):
        try:
            self.session.get(self._url("getUpdates"), params={"offset":-1,"timeout":1}, timeout=5)
        except Exception:
            pass
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def stop(self):
        self._stop = True


def send_telegram(tg: Optional[TelegramBot], text: str):
    if tg is None:
        log.info(f"[TG skip] {text[:80]}")
        return
    tg.send(text)

def notify(tg: Optional[TelegramBot], text: str):
    """Envía a Telegram y Discord simultáneamente."""
    send_telegram(tg, text)
    send_discord(text)

def build_telegram(state: BotState) -> TelegramBot:
    bot = TelegramBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, state)
    bot.start_polling()
    return bot

# ─────────────────────────────────────────────────────────────────────────────
# SCANNER PRINCIPAL — v5.0 sin mercados sintéticos
# ─────────────────────────────────────────────────────────────────────────────

def run_scan_cycle(state: BotState, poly: PolymarketClient, cg: CoinGeckoClient,
                   engine: SignalEngine, trader: PaperTrader,
                   tg_app: Optional[TelegramBot]) -> int:
    """
    Ciclo de escaneo completo.

    CAMBIO v5.0: Si no hay mercados reales para un asset → skip con log claro.
    No se generan mercados sintéticos bajo ninguna circunstancia.
    """
    state.cycles_run += 1
    signals_found    = 0
    markets_scanned  = 0
    assets_no_market = []

    log.info(f"━━━ CICLO #{state.cycles_run} | {'RELAJADO' if state.relaxed_mode else 'NORMAL'} | EV>={state.ev_threshold:.0%} Bayes>={state.bayesian_threshold:.0%} ━━━")

    # Una sola llamada a CoinGecko para los 3 assets
    cg.prefetch()

    # ── Gestionar posiciones abiertas ─────────────────────────────────────────
    for trade in list(state.open_trades):
        cg_id    = next((c for kw,c,_ in TARGET_MARKETS if kw in trade.signal.market.asset.lower()), "bitcoin")
        resolved = trader.check_resolve(trade, state, cg_id)
        if resolved:
            state.on_close(trade)
            log_trade_csv(trade)
            pnl  = trade.pnl
            icon = "✅" if pnl >= 0 else "❌"
            notify(tg_app,
                f"{icon} *TRADE CERRADO* ({'PAPER' if PAPER_MODE else 'LIVE'})\n"
                f"Asset: `{trade.signal.market.asset}` | Side: `{trade.signal.side}`\n"
                f"Entrada: `{trade.open_price:.3f}` → Salida: `{trade.exit_price:.3f}`\n"
                f"PnL: `${pnl:+.4f}` | Razón: `{trade.exit_reason}`\n"
                f"Equity: `${state.equity:.2f}` | DD: `{state.daily_dd:.1%}`"
            )

    # ── Circuit breaker ───────────────────────────────────────────────────────
    if state.circuit_broken:
        log.warning("Circuit breaker activo — sin entradas")
        log.info(f"━━━ Ciclo #{state.cycles_run} completo — circuit breaker ━━━\n")
        return 0

    can, reason = state.can_trade()

    # ── Escanear assets ───────────────────────────────────────────────────────
    for kw, cg_id, asset_name in TARGET_MARKETS:
        log.info(f"--- Escaneando {asset_name} ---")

        markets = poly.get_active_markets(kw)

        if not markets:
            # ═══════════════════════════════════════════════════════════════════
            # v5.0: NO hay mercados sintéticos aquí.
            # En v4.0 aqui se creaba un mercado sintetico. Eliminado en v5.0.
            # Si Polymarket no tiene mercados reales de 5min → skip limpio.
            # ═══════════════════════════════════════════════════════════════════
            log.info(f"  [SKIP] No real 5-min markets found for {asset_name} on Polymarket — skipping")
            assets_no_market.append(asset_name)
            continue

        log.info(f"  [FOUND] {len(markets)} mercado(s) real(es) para {asset_name}")

        for mkt in markets[:3]:
            markets_scanned += 1
            if not can:
                log.debug(f"  [SKIP trade] {reason}")
                continue

            signal = engine.analyze_market(mkt, asset_name, cg_id, state, poly)

            if signal:
                signals_found             += 1
                state.total_signals_found += 1
                state.last_signal_time     = datetime.now(timezone.utc)

                if PAPER_MODE:
                    trade      = trader.open_trade(signal, state)
                    order_info = poly.place_order_paper(signal)
                else:
                    log.warning("LIVE MODE — orden real (pendiente de implementar CLOB auth)")
                    order_info = {"order_id": "LIVE_TODO"}

                icon = "🚀" if signal.mode == "normal" else "📊"
                notify(tg_app,
                    f"{icon} *TRADE ABIERTO* ({'PAPER' if PAPER_MODE else 'LIVE'})\n"
                    f"Asset: `{asset_name}` | Side: `{signal.side}`\n"
                    f"Entrada: `{signal.entry_price:.3f}` | FV: `{signal.fair_value:.3f}`\n"
                    f"EV: `{signal.ev:.3f}` | Bayes: `{signal.bayesian_prob:.3f}` | Score: `{signal.score:.0f}`\n"
                    f"Bet: `${signal.bet_size:.2f}` | Modo: `{signal.mode.upper()}`\n"
                    f"Px real: `${signal.market.real_price:,.2f}` vs umbral `${signal.market.threshold:,.0f}`\n"
                    f"Pregunta: _{signal.market.question[:80]}_\n"
                    f"ID: `{order_info['order_id']}`"
                )
                can, reason = state.can_trade()

    # ── Resumen ───────────────────────────────────────────────────────────────
    if len(assets_no_market) == len(TARGET_MARKETS):
        state.cycles_no_markets += 1

    if assets_no_market:
        log.info(f"  [INFO] Sin mercados reales: {', '.join(assets_no_market)} — esperando apertura de mercados en Polymarket")

    state.last_cycle_scanned = markets_scanned
    log.info(f"━━━ Ciclo #{state.cycles_run} completo — {markets_scanned} mercados escaneados, {signals_found} señales ━━━\n")
    return signals_found

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 65)
    log.info("  SENTINEL POLYMARKET LAG BOT v5.0 — REAL MARKETS ONLY")
    log.info(f"  Modo: {'PAPER TRADING' if PAPER_MODE else 'LIVE TRADING'}")
    log.info(f"  Capital: ${INITIAL_CAPITAL} | Bet: {BET_SIZE_PCT:.0%}")
    log.info(f"  EV min: {EV_MIN_NORMAL:.0%}/{EV_MIN_RELAXED:.0%} | Bayes min: {BAYESIAN_MIN_NORMAL:.0%}/{BAYESIAN_MIN_RELAXED:.0%}")
    log.info(f"  Discord: {'activo' if DISCORD_WEBHOOK_URL else 'no configurado'}")
    log.info(f"  Mercados sintéticos: DESACTIVADOS")
    log.info("=" * 65)

    init_csv()
    state  = BotState()
    cg     = CoinGeckoClient()
    poly   = PolymarketClient()
    engine = SignalEngine(cg)
    trader = PaperTrader(cg)

    log.info("Pre-cargando precios de CoinGecko...")
    cg.prefetch()

    tg_app = None
    if TELEGRAM_TOKEN and "YOUR_" not in TELEGRAM_TOKEN:
        tg_app = build_telegram(state)
        time.sleep(1)
        log.info("Telegram bot iniciado (HTTP puro)")

    notify(tg_app,
        f"*Sentinel Polymarket v5.0 INICIADO*\n"
        f"Modo: `{'PAPER' if PAPER_MODE else 'LIVE'}`\n"
        f"Capital: `${INITIAL_CAPITAL}` | Bet: `{BET_SIZE_PCT:.0%}`\n"
        f"EV mín: `{EV_MIN_NORMAL:.0%}` | Bayes mín: `{BAYESIAN_MIN_NORMAL:.0%}`\n"
        f"Mercados sintéticos: *desactivados* — solo Polymarket real\n"
        f"Discord: `{'activo' if DISCORD_WEBHOOK_URL else 'no configurado'}`\n"
        f"/help para ver todos los comandos"
    )

    last_status_ts = 0.0
    while True:
        try:
            state.daily_reset()
            state.check_relaxed_mode()
            if not state.paused:
                run_scan_cycle(state, poly, cg, engine, trader, tg_app)
            else:
                log.info("Bot pausado — esperando /resume")
            now = time.time()
            if tg_app and (now - last_status_ts > 3600):
                send_telegram(tg_app, state.summary())
                last_status_ts = now
            time.sleep(SCAN_INTERVAL_SECS)
        except KeyboardInterrupt:
            log.info("Apagado por usuario")
            notify(tg_app, "Bot detenido.")
            if tg_app:
                tg_app.stop()
            break
        except Exception as e:
            log.error(f"Error en loop: {e}", exc_info=True)
            notify(tg_app, f"Error: `{str(e)[:200]}`")
            time.sleep(SCAN_INTERVAL_SECS * 2)

if __name__ == "__main__":
    main()
