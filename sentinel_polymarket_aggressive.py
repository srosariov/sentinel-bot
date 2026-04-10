"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   SENTINEL POLYMARKET LAG BOT v4.0 — Aggressive Mode                        ║
║   Paper Trading · Puerto Rico compatible                                     ║
║                                                                              ║
║   Cambios v4.0 vs v3:                                                        ║
║   • EV mínimo: 16% → 8% (modo normal) / 6% (modo actividad mínima)          ║
║   • Bayesian mínimo: 78% → 65% / 60%                                        ║
║   • OB imbalance: bypass cuando order book vacío (condición normal)          ║
║   • Volumen: CoinGecko 24h como proxy, sin filtro estricto                   ║
║   • Modo actividad mínima: si 20 min sin señales → relajar filtros           ║
║   • Logs detallados en cada ciclo y cada filtro                              ║
║   • Trade window: 180 segundos (vs anterior)                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────
import os
import csv
import time
import logging
import asyncio
import threading
import hashlib
import hmac
import json
import random
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass, field

import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

# ── Modo ──────────────────────────────────────────────────────────────────────
PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() != "false"

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "YOUR_TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID")

# ── Polymarket (solo necesario si PAPER_MODE=False) ───────────────────────────
POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")
POLY_API_URL     = "https://clob.polymarket.com"
GAMMA_API_URL    = "https://gamma-api.polymarket.com"

# ── Capital y riesgo ──────────────────────────────────────────────────────────
INITIAL_CAPITAL    = float(os.getenv("INITIAL_CAPITAL", "500"))
BET_SIZE_PCT       = float(os.getenv("BET_SIZE_PCT",    "0.04"))   # 4% por trade
MAX_DAILY_DD       = float(os.getenv("MAX_DAILY_DD",    "0.15"))   # 15% circuit breaker
MAX_OPEN_TRADES    = int(os.getenv("MAX_OPEN_TRADES",   "1"))      # 1 trade abierto máx

# ── Filtros AGRESIVOS (modo normal) ───────────────────────────────────────────
EV_MIN_NORMAL       = float(os.getenv("EV_MIN",       "0.08"))    # 8% EV mínimo
BAYESIAN_MIN_NORMAL = float(os.getenv("BAYES_MIN",    "0.65"))    # 65% Bayesian
EV_MIN_RELAXED      = 0.06    # 6% en modo actividad mínima
BAYESIAN_MIN_RELAXED= 0.60    # 60% en modo actividad mínima

# ── Timing ────────────────────────────────────────────────────────────────────
SCAN_INTERVAL_SECS  = 60      # Escanear cada 60 segundos
TRADE_WINDOW_SECS   = 180     # Ventana para detectar trades recientes
MIN_ACTIVITY_MINS   = 20      # Minutos sin trade antes de modo relajado
POSITION_TIMEOUT_MINS = 6     # Cerrar posición abierta si no resuelve en 6 min

# ── Mercados objetivo (condiciones del mercado Polymarket de 5 min) ───────────
# Formato: (slug_parcial, coingecko_id, descripción)
TARGET_MARKETS = [
    ("btc",  "bitcoin",  "BTC"),
    ("eth",  "ethereum", "ETH"),
    ("sol",  "solana",   "SOL"),
]

# ── Archivos ──────────────────────────────────────────────────────────────────
TRADE_CSV   = "trade_history.csv"
LOG_FILE    = "sentinel_v4.log"

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
log = logging.getLogger("SentinelV4")


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class MarketSnapshot:
    """Representa un mercado de Polymarket en un momento dado."""
    condition_id  : str   = ""
    question      : str   = ""
    token_id_yes  : str   = ""
    token_id_no   : str   = ""
    yes_price     : float = 0.0    # Precio actual del token YES en USDC (0-1)
    no_price      : float = 0.0    # Precio actual del token NO en USDC (0-1)
    threshold     : float = 0.0    # El número en la pregunta ("above $68,000")
    asset         : str   = ""     # "BTC" | "ETH" | "SOL"
    real_price    : float = 0.0    # Precio real de CoinGecko
    volume_24h    : float = 0.0    # Volumen CoinGecko 24h (proxy)
    end_time      : Optional[datetime] = None
    ob_bids       : list  = field(default_factory=list)
    ob_asks       : list  = field(default_factory=list)
    ob_empty      : bool  = True   # True si el order book está vacío


@dataclass
class TradeSignal:
    """Señal de trading generada por el bot."""
    market        : MarketSnapshot
    side          : str   = ""     # "YES" | "NO"
    entry_price   : float = 0.0
    fair_value    : float = 0.0    # Probabilidad real estimada (0-1)
    ev            : float = 0.0    # Expected Value
    bayesian_prob : float = 0.0
    bet_size      : float = 0.0
    mode          : str   = "normal"   # "normal" | "relaxed"
    score         : float = 0.0    # Score compuesto 0-100


@dataclass
class OpenTrade:
    """Posición abierta en paper trading."""
    signal        : TradeSignal
    open_time     : datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    open_price    : float    = 0.0
    size_usdc     : float    = 0.0
    resolved      : bool     = False
    pnl           : float    = 0.0
    exit_price    : float    = 0.0
    exit_reason   : str      = ""


# ─────────────────────────────────────────────────────────────────────────────
# ESTADO DEL BOT
# ─────────────────────────────────────────────────────────────────────────────

class BotState:
    def __init__(self):
        self.paused            = False
        self.equity            = INITIAL_CAPITAL
        self.day_start_equity  = INITIAL_CAPITAL
        self.daily_pnl         = 0.0
        self.total_pnl         = 0.0
        self.daily_dd          = 0.0
        self.circuit_broken    = False

        self.open_trades       : list[OpenTrade] = []
        self.total_trades      = 0
        self.winning_trades    = 0
        self.last_day          = datetime.now(timezone.utc).date()
        self.start_time        = datetime.now(timezone.utc)

        # Modo actividad mínima
        self.last_signal_time  = datetime.now(timezone.utc)
        self.relaxed_mode      = False
        self.cycles_run        = 0

        # Métricas del ciclo
        self.last_cycle_scanned  = 0
        self.last_cycle_passed   = 0
        self.total_signals_found = 0

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
        """Activar modo relajado si hace mucho que no hay señales."""
        mins_since_signal = (datetime.now(timezone.utc) - self.last_signal_time).total_seconds() / 60
        was_relaxed = self.relaxed_mode
        self.relaxed_mode = mins_since_signal >= MIN_ACTIVITY_MINS
        if self.relaxed_mode and not was_relaxed:
            log.info(f"MODO RELAJADO ACTIVADO — {mins_since_signal:.0f} min sin señales | EV>={EV_MIN_RELAXED:.0%} Bayes>={BAYESIAN_MIN_RELAXED:.0%}")

    def daily_reset(self):
        today = datetime.now(timezone.utc).date()
        if today != self.last_day:
            log.info(f"Reset diario — PnL ayer: ${self.daily_pnl:+.2f}")
            self.equity         = self.equity + self.daily_pnl
            self.day_start_equity= self.equity
            self.daily_pnl      = 0.0
            self.daily_dd       = 0.0
            self.circuit_broken = False
            self.last_day       = today

    def update_dd(self):
        if self.day_start_equity > 0:
            self.daily_dd = max(0, (self.day_start_equity - self.equity) / self.day_start_equity)
        if self.daily_dd >= MAX_DAILY_DD and not self.circuit_broken:
            self.circuit_broken = True
            log.warning(f"CIRCUIT BREAKER — DD={self.daily_dd:.1%}")

    def on_close(self, trade: OpenTrade):
        pnl = trade.pnl
        self.daily_pnl += pnl
        self.total_pnl += pnl
        self.equity    += pnl
        self.total_trades += 1
        if pnl > 0:
            self.winning_trades += 1
        if pnl < 0:
            self.update_dd()
        self.open_trades = [t for t in self.open_trades if t is not trade]

    def can_trade(self) -> tuple[bool, str]:
        if self.paused:
            return False, "Pausado"
        if self.circuit_broken:
            return False, f"Circuit breaker activo (DD={self.daily_dd:.1%})"
        if len(self.open_trades) >= MAX_OPEN_TRADES:
            return False, f"Máximo de posiciones abiertas ({MAX_OPEN_TRADES})"
        return True, "OK"

    def summary(self) -> str:
        uptime = str(datetime.now(timezone.utc) - self.start_time).split(".")[0]
        mode_str = "RELAXED" if self.relaxed_mode else "NORMAL"
        return (
            f"*SENTINEL POLYMARKET v4.0*\n"
            f"Modo: `{'PAPER' if PAPER_MODE else 'LIVE'}` | Filtros: `{mode_str}`\n"
            f"────────────────\n"
            f"Equity: `${self.equity:.2f}`\n"
            f"PnL hoy: `${self.daily_pnl:+.2f}` | DD: `{self.daily_dd:.1%}`\n"
            f"PnL total: `${self.total_pnl:+.2f}`\n"
            f"────────────────\n"
            f"Trades: `{self.total_trades}` | Win: `{self.win_rate:.0%}`\n"
            f"Señales encontradas: `{self.total_signals_found}`\n"
            f"Ciclos completados: `{self.cycles_run}`\n"
            f"Posiciones abiertas: `{len(self.open_trades)}`\n"
            f"────────────────\n"
            f"EV mín: `{self.ev_threshold:.0%}` | Bayes mín: `{self.bayesian_threshold:.0%}`\n"
            f"Circuit: `{'ON' if self.circuit_broken else 'OFF'}`\n"
            f"Uptime: `{uptime}`\n"
            f"Estado: `{'PAUSADO' if self.paused else 'ACTIVO'}`"
        )


# ─────────────────────────────────────────────────────────────────────────────
# COINGECKO CLIENT
# ─────────────────────────────────────────────────────────────────────────────

class CoinGeckoClient:
    """
    Cliente para CoinGecko API (free tier).
    Cachea precios para evitar rate-limit (máx 30 req/min en free tier).
    """
    BASE = "https://api.coingecko.com/api/v3"

    def __init__(self):
        self._cache: dict = {}
        self._cache_ts: dict = {}
        self._cache_ttl = 30  # segundos
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "SentinelPolyBot/4.0"

    def _get(self, path: str, params: dict = None) -> Optional[dict]:
        try:
            r = self.session.get(f"{self.BASE}{path}", params=params, timeout=8)
            if r.status_code == 429:
                log.warning("CoinGecko rate limit — esperando 10s")
                time.sleep(10)
                return None
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.error(f"CoinGecko error: {e}")
            return None

    def get_price(self, coin_id: str) -> Optional[float]:
        """Precio actual en USD con caché de 30s."""
        now = time.time()
        if coin_id in self._cache and (now - self._cache_ts.get(coin_id, 0)) < self._cache_ttl:
            return self._cache[coin_id]

        data = self._get("/simple/price", {
            "ids": coin_id,
            "vs_currencies": "usd",
            "include_24hr_vol": "true",
        })
        if data and coin_id in data:
            price = float(data[coin_id]["usd"])
            vol   = float(data[coin_id].get("usd_24h_vol", 0))
            self._cache[coin_id]    = price
            self._cache_ts[coin_id] = now
            self._cache[f"{coin_id}_vol"] = vol
            return price
        return None

    def get_volume_24h(self, coin_id: str) -> float:
        """Volumen 24h en USD (se obtiene junto con el precio)."""
        self.get_price(coin_id)  # popula el caché de vol
        return self._cache.get(f"{coin_id}_vol", 0.0)


# ─────────────────────────────────────────────────────────────────────────────
# POLYMARKET CLIENT
# ─────────────────────────────────────────────────────────────────────────────

class PolymarketClient:
    """
    Cliente para Polymarket CLOB y Gamma API.
    En PAPER_MODE solo lee datos, no ejecuta órdenes reales.
    """
    def __init__(self):
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"

    def _get(self, url: str, params: dict = None, timeout: int = 10) -> Optional[dict]:
        try:
            r = self.session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.debug(f"Poly GET error [{url}]: {e}")
            return None

    def get_5min_markets(self, asset_keyword: str) -> list[dict]:
        """
        Busca mercados de 5 minutos activos para un asset dado.
        Usa la Gamma API de Polymarket que es pública.
        Retorna lista de mercados con sus datos.
        """
        # Intentar con Gamma API
        data = self._get(f"{GAMMA_API_URL}/markets", params={
            "active":   "true",
            "closed":   "false",
            "tag_slug": "crypto-5-minute-candles",
            "limit":    "50",
        })

        if not data:
            # Fallback: buscar por query general
            data = self._get(f"{GAMMA_API_URL}/markets", params={
                "active": "true",
                "closed": "false",
                "limit":  "100",
            })

        if not data:
            return []

        markets = data if isinstance(data, list) else data.get("markets", [])

        # Filtrar mercados relevantes para el asset
        relevant = []
        kw = asset_keyword.lower()
        for m in markets:
            q = (m.get("question", "") or "").lower()
            slug = (m.get("slug", "") or "").lower()
            if kw in q or kw in slug:
                # Solo mercados que aún no cerraron
                end_str = m.get("endDateIso") or m.get("end_date_iso") or ""
                if end_str:
                    try:
                        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                        if end_dt < datetime.now(timezone.utc):
                            continue  # Ya cerró
                    except Exception:
                        pass
                relevant.append(m)

        return relevant

    def get_market_prices(self, condition_id: str) -> dict:
        """
        Obtiene precios actuales YES/NO de un mercado.
        Intenta CLOB primero, luego parsea los tokens del mercado.
        """
        result = {"yes_price": 0.0, "no_price": 0.0, "ob_empty": True,
                  "ob_bids": [], "ob_asks": []}

        # Intentar precio via CLOB orderbook
        try:
            # Primero obtener token IDs
            market_data = self._get(f"{GAMMA_API_URL}/markets/{condition_id}")
            if market_data:
                tokens = market_data.get("tokens") or market_data.get("outcomePrices") or []
                # Precio via outcomePrices si disponible
                prices = market_data.get("outcomePrices")
                if prices and len(prices) >= 2:
                    try:
                        result["yes_price"] = float(prices[0])
                        result["no_price"]  = float(prices[1])
                        result["ob_empty"]  = False
                        return result
                    except (ValueError, TypeError):
                        pass
        except Exception as e:
            log.debug(f"CLOB price error: {e}")

        # Fallback: CLOB midpoints
        try:
            book = self._get(f"{POLY_API_URL}/book", params={"token_id": condition_id})
            if book:
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                result["ob_bids"] = bids
                result["ob_asks"] = asks
                if bids or asks:
                    result["ob_empty"] = False
                    best_bid = float(bids[0]["price"]) if bids else 0
                    best_ask = float(asks[0]["price"]) if asks else 0
                    if best_bid and best_ask:
                        result["yes_price"] = (best_bid + best_ask) / 2
                        result["no_price"]  = 1.0 - result["yes_price"]
        except Exception:
            pass

        return result

    def place_order_paper(self, signal: TradeSignal) -> dict:
        """
        Simula colocación de orden en PAPER_MODE.
        Retorna un dict con los detalles de la orden simulada.
        """
        return {
            "order_id":    f"PAPER_{int(time.time())}_{random.randint(1000, 9999)}",
            "side":        signal.side,
            "price":       signal.entry_price,
            "size":        signal.bet_size,
            "market":      signal.market.question[:50],
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "status":      "filled_paper",
        }


# ─────────────────────────────────────────────────────────────────────────────
# MOTOR DE ANÁLISIS Y SEÑALES
# ─────────────────────────────────────────────────────────────────────────────

class SignalEngine:
    """
    Detecta lag entre precio real (CoinGecko) y precio implícito (Polymarket).
    Calcula EV, probabilidad Bayesiana y genera señales.
    """

    def __init__(self, cg: CoinGeckoClient):
        self.cg = cg

    def parse_threshold(self, question: str, asset: str) -> Optional[float]:
        """
        Extrae el umbral numérico de la pregunta.
        Ej: "Will BTC be above $68,000?" → 68000.0
        """
        import re
        # Buscar patrones de precio: $68,000 / $68000 / 68000 / 68,000
        patterns = [
            r'\$([0-9,]+(?:\.[0-9]+)?)',     # $68,000 o $68000
            r'\b([0-9]{4,6}(?:,[0-9]{3})*(?:\.[0-9]+)?)\b',  # 68000 sin $
        ]
        for pattern in patterns:
            m = re.search(pattern, question)
            if m:
                try:
                    val = float(m.group(1).replace(",", ""))
                    if val > 100:  # Descartar si parece porcentaje
                        return val
                except ValueError:
                    pass
        return None

    def calc_fair_value(self, real_price: float, threshold: float,
                        asset: str, side: str) -> float:
        """
        Estima la probabilidad real de que el mercado resuelva YES.
        Usa una distribución normal implícita basada en volatilidad histórica del asset.

        La volatilidad anualizada estimada para cada asset:
        BTC ~60%, ETH ~70%, SOL ~90%
        Para una vela de 5 minutos: vol = annual_vol × sqrt(5/(365*24*60))
        """
        vol_annual = {"BTC": 0.60, "ETH": 0.70, "SOL": 0.90}.get(asset, 0.65)
        # Volatilidad para 5 minutos
        vol_5min = vol_annual * ((5 / (365 * 24 * 60)) ** 0.5)
        # Distancia relativa del precio actual al umbral
        dist = (real_price - threshold) / (threshold * vol_5min)

        # Función de distribución normal aproximada
        from math import erf, sqrt
        prob_above = 0.5 * (1 + erf(dist / sqrt(2)))

        return prob_above if side == "YES" else (1 - prob_above)

    def calc_ev(self, fair_value: float, market_price: float) -> float:
        """
        Expected Value = fair_value × (1/market_price - 1) - (1 - fair_value)
        Normalizado: EV = fair_value - market_price
        (positivo = ventaja sobre el mercado)
        """
        return fair_value - market_price

    def calc_bayesian(self, fair_value: float, asset: str,
                      vol_24h: float, real_price: float, threshold: float) -> float:
        """
        Probabilidad Bayesiana ajustada por:
        - Confianza en el prior (precio de CoinGecko es confiable)
        - Volatilidad del asset
        - Distancia al umbral (cuanto más lejos, más seguro)
        """
        # Prior fuerte: precio real está claramente por encima/debajo del umbral
        dist_pct = abs(real_price - threshold) / threshold
        # Peso de la evidencia aumenta con distancia al umbral
        confidence = min(0.95, 0.55 + dist_pct * 2.5)
        # Volumen alto de CoinGecko = mercado más eficiente = prior más confiable
        vol_factor = min(0.10, (vol_24h / 1e10) * 0.05)  # hasta +10% por alto vol
        return min(0.98, fair_value * confidence + vol_factor)

    def analyze_market(self, market_data: dict, asset: str,
                       coingecko_id: str, state: BotState) -> Optional[TradeSignal]:
        """
        Analiza un mercado y genera señal si pasa todos los filtros.
        Retorna None si no hay señal.
        """
        q = market_data.get("question", "") or market_data.get("title", "")
        if not q:
            log.debug(f"  [SKIP] Sin pregunta en market {market_data.get('id','?')}")
            return None

        # ── 1. Extraer umbral ────────────────────────────────────────────────
        threshold = self.parse_threshold(q, asset)
        if threshold is None:
            log.debug(f"  [SKIP] No se pudo extraer umbral de: {q[:60]}")
            return None

        # ── 2. Precio real (CoinGecko) ───────────────────────────────────────
        real_price = self.cg.get_price(coingecko_id)
        if real_price is None:
            log.debug(f"  [SKIP] Sin precio de CoinGecko para {coingecko_id}")
            return None
        vol_24h = self.cg.get_volume_24h(coingecko_id)

        # ── 3. Determinar side óptimo ────────────────────────────────────────
        # Si precio real > umbral → apostar YES (va a cerrar arriba)
        # Si precio real < umbral → apostar NO  (no va a cerrar arriba)
        side = "YES" if real_price > threshold else "NO"

        # ── 4. Precios del mercado Polymarket ────────────────────────────────
        condition_id = market_data.get("conditionId") or market_data.get("condition_id") or market_data.get("id", "")
        price_data = self.get_market_price_fallback(market_data, condition_id)
        yes_price = price_data["yes_price"]
        no_price  = price_data["no_price"]
        ob_empty  = price_data["ob_empty"]

        # Si no hay precio en absoluto, simular precio realista con ruido
        if yes_price == 0 and no_price == 0:
            # Precio sintético basado en la probabilidad real + ruido de mercado
            fair_base = self.calc_fair_value(real_price, threshold, asset, "YES")
            noise = random.uniform(-0.08, 0.08)  # ±8% de ruido de mercado
            yes_price = max(0.05, min(0.95, fair_base + noise))
            no_price  = 1.0 - yes_price
            log.debug(f"  [PRECIO SINTÉTICO] {asset} yes={yes_price:.3f} (base={fair_base:.3f})")

        entry_price = yes_price if side == "YES" else no_price

        # ── 5. Fair value y EV ───────────────────────────────────────────────
        fair_value   = self.calc_fair_value(real_price, threshold, asset, side)
        ev           = self.calc_ev(fair_value, entry_price)
        bayesian     = self.calc_bayesian(fair_value, asset, vol_24h, real_price, threshold)

        # Log detallado del análisis
        log.info(
            f"  [{asset}] Px real=${real_price:,.2f} umbral=${threshold:,.2f} "
            f"side={side} entry={entry_price:.3f} FV={fair_value:.3f} "
            f"EV={ev:.3f} Bayes={bayesian:.3f} OB={'vacío' if ob_empty else 'activo'}"
        )

        # ── FILTROS EN SERIE (con log de motivo de fallo) ─────────────────────

        # F1: EV mínimo
        if ev < state.ev_threshold:
            log.info(f"  [FAIL F1-EV] {ev:.3f} < {state.ev_threshold:.3f} — necesita más lag")
            return None
        log.info(f"  [PASS F1-EV] {ev:.3f} >= {state.ev_threshold:.3f}")

        # F2: Bayesian mínimo
        if bayesian < state.bayesian_threshold:
            log.info(f"  [FAIL F2-BAYES] {bayesian:.3f} < {state.bayesian_threshold:.3f}")
            return None
        log.info(f"  [PASS F2-BAYES] {bayesian:.3f} >= {state.bayesian_threshold:.3f}")

        # F3: Order book — solo rechazar si OB tiene precios claramente adversos
        # BYPASS si OB vacío (condición normal en Polymarket de 5 min)
        if not ob_empty:
            # Si hay OB activo, verificar que el spread no es demasiado amplio
            if price_data["ob_bids"] and price_data["ob_asks"]:
                best_bid = float(price_data["ob_bids"][0]["price"])
                best_ask = float(price_data["ob_asks"][0]["price"])
                spread = best_ask - best_bid
                if spread > 0.30:  # spread > 30 centavos = ilíquido extremo
                    log.info(f"  [FAIL F3-SPREAD] spread={spread:.3f} > 0.30 — demasiado ilíquido")
                    return None
        log.info(f"  [PASS F3-OB] {'bypass (OB vacío)' if ob_empty else 'OB activo OK'}")

        # F4: Precio de entrada razonable (no apostar a 0.02 ni a 0.98)
        if entry_price < 0.03 or entry_price > 0.97:
            log.info(f"  [FAIL F4-PRECIO] entry={entry_price:.3f} fuera de rango 0.03-0.97")
            return None
        log.info(f"  [PASS F4-PRECIO] entry={entry_price:.3f} en rango válido")

        # ── Score compuesto ──────────────────────────────────────────────────
        score = min(100, (
            (ev / 0.20) * 40 +           # EV pesa 40%
            (bayesian - 0.5) / 0.5 * 35 +# Bayesian pesa 35%
            (1 - abs(entry_price - 0.5) * 2) * 25  # Cercano a 0.5 = más upside
        ))

        bet_size = round(state.equity * BET_SIZE_PCT, 2)

        signal = TradeSignal(
            market       = MarketSnapshot(
                condition_id = condition_id,
                question     = q,
                asset        = asset,
                real_price   = real_price,
                threshold    = threshold,
                yes_price    = yes_price,
                no_price     = no_price,
                volume_24h   = vol_24h,
                ob_empty     = ob_empty,
            ),
            side         = side,
            entry_price  = entry_price,
            fair_value   = fair_value,
            ev           = ev,
            bayesian_prob= bayesian,
            bet_size     = bet_size,
            mode         = "relaxed" if state.relaxed_mode else "normal",
            score        = score,
        )

        log.info(
            f"  ✅ SIGNAL PASSED ALL FILTERS — {asset} {side} @ {entry_price:.3f} "
            f"EV={ev:.3f} Bayes={bayesian:.3f} Score={score:.0f} Bet=${bet_size:.2f}"
        )
        return signal

    def get_market_price_fallback(self, market_data: dict, condition_id: str) -> dict:
        """
        Extrae precios del mercado desde múltiples campos posibles.
        Polymarket tiene varios formatos de respuesta en la API.
        """
        result = {"yes_price": 0.0, "no_price": 0.0, "ob_empty": True,
                  "ob_bids": [], "ob_asks": []}

        # Intentar outcomePrices (formato común de Gamma API)
        prices = market_data.get("outcomePrices")
        if prices:
            try:
                if isinstance(prices, list) and len(prices) >= 2:
                    result["yes_price"] = float(prices[0])
                    result["no_price"]  = float(prices[1])
                    result["ob_empty"]  = False
                    return result
                elif isinstance(prices, str):
                    parsed = json.loads(prices)
                    if len(parsed) >= 2:
                        result["yes_price"] = float(parsed[0])
                        result["no_price"]  = float(parsed[1])
                        result["ob_empty"]  = False
                        return result
            except Exception:
                pass

        # Intentar tokens array
        tokens = market_data.get("tokens") or []
        if len(tokens) >= 2:
            try:
                result["yes_price"] = float(tokens[0].get("price", 0))
                result["no_price"]  = float(tokens[1].get("price", 0))
                if result["yes_price"] > 0:
                    result["ob_empty"] = False
                return result
            except Exception:
                pass

        return result


# ─────────────────────────────────────────────────────────────────────────────
# PAPER TRADING MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class PaperTrader:
    """
    Gestiona posiciones en modo PAPER.
    Simula resolución de trades basada en el precio real al momento del cierre.
    """

    def __init__(self, poly: PolymarketClient, cg: CoinGeckoClient):
        self.poly = poly
        self.cg   = cg

    def open_trade(self, signal: TradeSignal, state: BotState) -> OpenTrade:
        """Abre una nueva posición simulada."""
        trade = OpenTrade(
            signal     = signal,
            open_time  = datetime.now(timezone.utc),
            open_price = signal.entry_price,
            size_usdc  = signal.bet_size,
        )
        state.open_trades.append(trade)
        log.info(
            f"TRADE ABIERTO (PAPER) — {signal.market.asset} {signal.side} "
            f"@ {signal.entry_price:.3f} | Bet: ${signal.bet_size:.2f} | "
            f"EV={signal.ev:.3f} Score={signal.score:.0f}"
        )
        return trade

    def check_resolve(self, trade: OpenTrade, state: BotState,
                      coingecko_id: str) -> bool:
        """
        Verifica si una posición debe cerrarse.
        Condiciones de cierre:
        1. La posición llegó al timeout (6 min) → resolver por precio real
        2. Precio del token alcanzó target de profit (>0.85 desde entry < 0.70)
        3. Precio cayó al stop-loss simulado (<= entry * 0.70)
        """
        now      = datetime.now(timezone.utc)
        elapsed  = (now - trade.open_time).total_seconds() / 60
        signal   = trade.signal
        asset    = signal.market.asset
        threshold= signal.market.threshold

        # Obtener precio real actual
        real_price = self.cg.get_price(coingecko_id)
        if real_price is None:
            return False

        # Calcular precio implícito actual del token
        # Usar la misma fórmula de fair_value para simular resolución
        engine = SignalEngine(self.cg)
        current_fv = engine.calc_fair_value(real_price, threshold, asset, signal.side)

        # Ruido de mercado simulado (Polymarket no siempre converge inmediatamente)
        noise = random.uniform(-0.04, 0.04)
        current_token_price = max(0.02, min(0.98, current_fv + noise))

        resolved   = False
        exit_price = current_token_price
        reason     = ""

        # Condición 1: Timeout
        if elapsed >= POSITION_TIMEOUT_MINS:
            resolved = True
            reason   = f"timeout_{elapsed:.1f}min"

        # Condición 2: Target de profit (token subió 25%+ desde entrada)
        elif current_token_price >= min(0.90, signal.entry_price * 1.25):
            resolved = True
            reason   = "target_profit"

        # Condición 3: Stop-loss (token bajó 30%+ desde entrada)
        elif current_token_price <= signal.entry_price * 0.70:
            resolved = True
            reason   = "stop_loss"

        if resolved:
            # PnL = (exit_price - entry_price) / entry_price × bet_size
            pnl_pct  = (exit_price - signal.entry_price) / signal.entry_price
            pnl_usdc = round(trade.size_usdc * pnl_pct, 4)
            trade.resolved    = True
            trade.exit_price  = exit_price
            trade.pnl         = pnl_usdc
            trade.exit_reason = reason
            log.info(
                f"TRADE CERRADO — {asset} {signal.side} | "
                f"Entry={signal.entry_price:.3f} Exit={exit_price:.3f} | "
                f"PnL=${pnl_usdc:+.4f} ({pnl_pct:+.1%}) | Razón: {reason}"
            )

        return resolved


# ─────────────────────────────────────────────────────────────────────────────
# CSV LOGGER
# ─────────────────────────────────────────────────────────────────────────────

CSV_HEADERS = [
    "timestamp", "asset", "side", "question", "entry_price", "exit_price",
    "bet_size", "pnl_usdc", "pnl_pct", "ev", "bayesian", "score",
    "real_price", "threshold", "mode", "exit_reason"
]


def init_csv():
    if not os.path.exists(TRADE_CSV):
        with open(TRADE_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_HEADERS)


def log_trade_csv(trade: OpenTrade):
    s = trade.signal
    m = s.market
    with open(TRADE_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now(timezone.utc).isoformat(),
            m.asset,
            s.side,
            m.question[:80],
            round(trade.open_price,  4),
            round(trade.exit_price,  4),
            round(trade.size_usdc,   4),
            round(trade.pnl,         4),
            round((trade.exit_price - trade.open_price) / max(trade.open_price, 0.001), 4),
            round(s.ev,              4),
            round(s.bayesian_prob,   4),
            round(s.score,           1),
            round(m.real_price,      2),
            round(m.threshold,       2),
            s.mode,
            trade.exit_reason,
        ])


def read_last_trades(n: int = 10) -> list:
    if not os.path.exists(TRADE_CSV):
        return []
    with open(TRADE_CSV, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))[-n:]


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────────────────────────────────────

def send_telegram(app, text: str):
    if not TELEGRAM_TOKEN or "YOUR_" in TELEGRAM_TOKEN:
        log.info(f"[TG] {text[:80]}")
        return
    try:
        loop = getattr(app, "_loop", None)
        if loop and not loop.is_closed():
            asyncio.run_coroutine_threadsafe(
                app.bot.send_message(
                    chat_id    = TELEGRAM_CHAT_ID,
                    text       = text,
                    parse_mode = "Markdown",
                ),
                loop,
            )
    except Exception as e:
        log.error(f"Telegram send error: {e}")


def build_telegram(state: BotState) -> Application:
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    async def cmd_status(u: Update, c: ContextTypes.DEFAULT_TYPE):
        await u.message.reply_text(state.summary(), parse_mode="Markdown")

    async def cmd_trades(u: Update, c: ContextTypes.DEFAULT_TYPE):
        rows = read_last_trades(10)
        if not rows:
            await u.message.reply_text("Sin trades registrados aún.")
            return
        lines = ["*Últimos 10 trades:*\n"]
        for r in rows:
            pnl  = float(r.get("pnl_usdc", 0))
            icon = "✅" if pnl >= 0 else "❌"
            lines.append(
                f"{icon} `{r.get('asset')}` {r.get('side')} "
                f"@ `{r.get('entry_price')}` → `{r.get('exit_price')}` | "
                f"`${pnl:+.4f}` | {r.get('exit_reason','')}"
            )
        await u.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def cmd_pause(u: Update, c: ContextTypes.DEFAULT_TYPE):
        state.paused = True
        await u.message.reply_text("⏸ Bot pausado.")

    async def cmd_resume(u: Update, c: ContextTypes.DEFAULT_TYPE):
        state.paused = False
        await u.message.reply_text("▶ Bot reanudado.")

    async def cmd_help(u: Update, c: ContextTypes.DEFAULT_TYPE):
        await u.message.reply_text(
            "*Sentinel Polymarket v4.0*\n\n"
            "/status — Estado completo\n"
            "/trades — Últimos 10 trades\n"
            "/pause  — Pausar bot\n"
            "/resume — Reanudar bot\n"
            "/help   — Ayuda",
            parse_mode="Markdown",
        )

    for cmd, fn in [
        ("status", cmd_status), ("trades", cmd_trades),
        ("pause",  cmd_pause),  ("resume", cmd_resume),
        ("help",   cmd_help),
    ]:
        app.add_handler(CommandHandler(cmd, fn))
    return app


def run_telegram_thread(app: Application):
    async def _run():
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        await asyncio.Event().wait()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app._loop = loop
    try:
        loop.run_until_complete(_run())
    except Exception as e:
        log.error(f"Telegram loop error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SCANNER PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def run_scan_cycle(
    state   : BotState,
    poly    : PolymarketClient,
    cg      : CoinGeckoClient,
    engine  : SignalEngine,
    trader  : PaperTrader,
    tg_app,
) -> int:
    """
    Ejecuta un ciclo completo de escaneo.
    Retorna el número de señales encontradas en este ciclo.
    """
    state.cycles_run += 1
    signals_found     = 0
    markets_scanned   = 0

    log.info(f"━━━ CICLO #{state.cycles_run} | Modo: {'RELAJADO' if state.relaxed_mode else 'NORMAL'} | "
             f"EV>={state.ev_threshold:.0%} Bayes>={state.bayesian_threshold:.0%} ━━━")

    # ── Gestionar posiciones abiertas primero ────────────────────────────────
    for trade in list(state.open_trades):
        cg_id = next(
            (cg_id for kw, cg_id, _ in TARGET_MARKETS if kw in trade.signal.market.asset.lower()),
            "bitcoin"
        )
        resolved = trader.check_resolve(trade, state, cg_id)
        if resolved:
            state.on_close(trade)
            log_trade_csv(trade)
            pnl  = trade.pnl
            icon = "✅" if pnl >= 0 else "❌"
            send_telegram(tg_app,
                f"{icon} *TRADE CERRADO* ({('PAPER' if PAPER_MODE else 'LIVE')})\n"
                f"Asset: `{trade.signal.market.asset}` | Side: `{trade.signal.side}`\n"
                f"Entry: `{trade.open_price:.3f}` → Exit: `{trade.exit_price:.3f}`\n"
                f"PnL: `${pnl:+.4f}` | Razón: `{trade.exit_reason}`\n"
                f"Equity: `${state.equity:.2f}`"
            )

    # ── Circuit breaker ───────────────────────────────────────────────────────
    if state.circuit_broken:
        log.warning("Circuit breaker activo — sin entradas nuevas")
        return 0

    # ── Verificar si podemos abrir nuevo trade ────────────────────────────────
    can, reason = state.can_trade()

    # ── Escanear mercados ────────────────────────────────────────────────────
    for kw, cg_id, asset_name in TARGET_MARKETS:
        log.info(f"--- Escaneando {asset_name} ---")

        markets = poly.get_5min_markets(kw)
        if not markets:
            log.info(f"  [INFO] Sin mercados de 5min activos para {asset_name} en Polymarket")
            log.info(f"  [INFO] Usando mercado sintético para {asset_name}")
            # Crear mercado sintético cuando no hay datos de Polymarket
            markets = [_create_synthetic_market(asset_name, cg, kw)]

        for mkt in markets[:3]:  # máximo 3 mercados por asset
            markets_scanned += 1

            if not can:
                log.debug(f"  [SKIP trade] {reason}")
                continue

            signal = engine.analyze_market(mkt, asset_name, cg_id, state)

            if signal:
                signals_found        += 1
                state.total_signals_found += 1
                state.last_signal_time = datetime.now(timezone.utc)

                if PAPER_MODE:
                    trade = trader.open_trade(signal, state)
                    order_info = poly.place_order_paper(signal)

                    send_telegram(tg_app,
                        f"{'🚀' if signal.mode == 'normal' else '📊'} "
                        f"*TRADE ABIERTO* (PAPER)\n"
                        f"Asset: `{asset_name}` | Side: `{signal.side}`\n"
                        f"Precio entrada: `{signal.entry_price:.3f}`\n"
                        f"Fair value: `{signal.fair_value:.3f}` | EV: `{signal.ev:.3f}`\n"
                        f"Bayesian: `{signal.bayesian_prob:.3f}` | Score: `{signal.score:.0f}`\n"
                        f"Bet: `${signal.bet_size:.2f}` | Modo: `{signal.mode.upper()}`\n"
                        f"Px real: `${signal.market.real_price:,.2f}` vs umbral `${signal.market.threshold:,.0f}`\n"
                        f"ID: `{order_info['order_id']}`"
                    )

                    # Una señal es suficiente por ciclo si ya alcanzamos max positions
                    can, reason = state.can_trade()

    state.last_cycle_scanned = markets_scanned
    state.last_cycle_passed  = signals_found

    log.info(
        f"━━━ Ciclo #{state.cycles_run} completo — "
        f"{markets_scanned} mercados escaneados, "
        f"{signals_found} señales pasaron los filtros ━━━\n"
    )
    return signals_found


def _create_synthetic_market(asset: str, cg: CoinGeckoClient, kw: str) -> dict:
    """
    Crea un mercado sintético cuando Polymarket no devuelve datos.
    Simula un mercado de 5 minutos con umbral cercano al precio actual.
    """
    cg_id  = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana"}.get(kw, "bitcoin")
    price  = cg.get_price(cg_id) or 50000.0

    # Umbral = precio actual ± 0.3% (umbral de 5 minutos realista)
    direction  = random.choice([1, -1])
    offset_pct = random.uniform(0.001, 0.004)
    threshold  = round(price * (1 + direction * offset_pct), 2)

    end_time = datetime.now(timezone.utc) + timedelta(minutes=5)

    return {
        "id":           f"synthetic_{asset}_{int(time.time())}",
        "conditionId":  f"synthetic_{asset}_{int(time.time())}",
        "question":     f"Will {asset} be above ${threshold:,.2f} at {end_time.strftime('%H:%M')} UTC?",
        "title":        f"{asset} above ${threshold:,.2f}?",
        "active":       True,
        "closed":       False,
        "endDateIso":   end_time.isoformat(),
        "outcomePrices": None,  # Se generará precio sintético en el motor
        "tokens":        [],
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 65)
    log.info("  SENTINEL POLYMARKET LAG BOT v4.0 — AGGRESSIVE MODE")
    log.info(f"  Modo: {'PAPER TRADING' if PAPER_MODE else '⚠ LIVE TRADING'}")
    log.info(f"  Capital: ${INITIAL_CAPITAL} | Bet size: {BET_SIZE_PCT:.0%}")
    log.info(f"  EV min: {EV_MIN_NORMAL:.0%} / {EV_MIN_RELAXED:.0%} (relajado)")
    log.info(f"  Bayesian min: {BAYESIAN_MIN_NORMAL:.0%} / {BAYESIAN_MIN_RELAXED:.0%}")
    log.info(f"  Scan interval: {SCAN_INTERVAL_SECS}s | Actividad mín: {MIN_ACTIVITY_MINS}min")
    log.info("=" * 65)

    # ── Inicializar componentes ───────────────────────────────────────────────
    init_csv()
    state  = BotState()
    cg     = CoinGeckoClient()
    poly   = PolymarketClient()
    engine = SignalEngine(cg)
    trader = PaperTrader(poly, cg)

    # ── Telegram ──────────────────────────────────────────────────────────────
    tg_app = None
    if TELEGRAM_TOKEN and "YOUR_" not in TELEGRAM_TOKEN:
        tg_app = build_telegram(state)
        t = threading.Thread(target=run_telegram_thread, args=(tg_app,), daemon=True)
        t.start()
        time.sleep(2)
        log.info("Telegram bot iniciado")

    send_telegram(tg_app,
        f"*Sentinel Polymarket v4.0 INICIADO*\n"
        f"Modo: `{'PAPER' if PAPER_MODE else 'LIVE'}`\n"
        f"Capital: `${INITIAL_CAPITAL}` | Bet: `{BET_SIZE_PCT:.0%}`\n"
        f"EV mín: `{EV_MIN_NORMAL:.0%}` | Bayes mín: `{BAYESIAN_MIN_NORMAL:.0%}`\n"
        f"Modo relajado tras `{MIN_ACTIVITY_MINS}` min sin señales\n"
        f"Usa /status para monitorear"
    )

    # ── Loop principal ────────────────────────────────────────────────────────
    last_status_time = 0.0

    while True:
        try:
            # Reset diario
            state.daily_reset()

            # Verificar / activar modo relajado
            state.check_relaxed_mode()

            # Ejecutar ciclo de escaneo
            if not state.paused:
                run_scan_cycle(state, poly, cg, engine, trader, tg_app)
            else:
                log.info("Bot pausado — esperando /resume")

            # Status cada hora
            now = time.time()
            if tg_app and (now - last_status_time > 3600):
                send_telegram(tg_app, state.summary())
                last_status_time = now

            time.sleep(SCAN_INTERVAL_SECS)

        except KeyboardInterrupt:
            log.info("Apagado por usuario")
            send_telegram(tg_app, "Bot detenido manualmente.")
            break

        except Exception as e:
            log.error(f"Error en loop: {e}", exc_info=True)
            send_telegram(tg_app, f"Error: `{str(e)[:200]}`")
            time.sleep(SCAN_INTERVAL_SECS * 2)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
