#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════════
#  bot.py — Sentinel Polymarket v7.0  (Definitive Edition)
# ───────────────────────────────────────────────────────────────────────────────
#  ROOT-CAUSE FIX (vs v3–v6):
#
#  Versiones anteriores usaban keyword search (/markets?search=...) — método
#  INCORRECTO para mercados efímeros de 5/15 minutos en Polymarket.
#
#  v7.0 usa SLUG DETERMINÍSTICO basado en timestamp (fuente: docs oficiales 2026):
#    Format:  {ticker}-updown-{N}m-{unix_ts_aligned_to_interval}
#    Example: btc-updown-5m-1744567800   (ts siempre % 300 == 0)
#    Query:   GET /events?slug={slug}    ← endpoint correcto
#
#  Polymarket crea estos mercados internamente con este mismo patrón.
#  Otros detalles clave de la investigación aplicados aquí:
#    - outcomePrices es un string JSON doblemente codificado → json.loads() obligatorio
#    - Outcomes son "Up"/"Down" (no "Yes"/"No") para estos mercados
#    - Fallback: GET /markets con filtro de rango de fechas
# ═══════════════════════════════════════════════════════════════════════════════

VERSION    = "7.0.0"
BUILD_DATE = "2026-04-12"

import os, sys, csv, json, time, math, uuid, logging, threading, traceback, re, random
from datetime    import datetime, timezone, timedelta
from collections import deque, defaultdict
from typing      import Optional, List, Dict, Any, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────────────────────
LOG_FILE = os.getenv("LOG_FILE", "sentinel_v7.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("sentinel")

# ──────────────────────────────────────────────────────────────────────────────
# ENVIRONMENT CONFIG
# ──────────────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN     = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
DISCORD_WEBHOOK    = os.getenv("DISCORD_WEBHOOK_URL", "")

PAPER_MODE         = os.getenv("PAPER_MODE", "true").lower() != "false"
INITIAL_BANKROLL   = float(os.getenv("INITIAL_BANKROLL", "100.0"))
TRADE_FRACTION     = float(os.getenv("TRADE_FRACTION",   "0.05"))
MAX_OPEN_TRADES    = int(os.getenv("MAX_OPEN_TRADES",    "8"))
CB_PCT             = float(os.getenv("CIRCUIT_BREAKER_PCT", "0.20"))
CYCLE_SLEEP        = int(os.getenv("CYCLE_SLEEP",        "60"))
CSV_FILE           = os.getenv("CSV_FILE",               "trades_v7.csv")
RELAXED_AFTER_MINS = int(os.getenv("RELAXED_AFTER_MINS", "15"))

# Filtros / Thresholds
EV_NORMAL     = float(os.getenv("EV_NORMAL",     "0.050"))
EV_RELAXED    = float(os.getenv("EV_RELAXED",    "0.025"))
BAYES_NORMAL  = float(os.getenv("BAYES_NORMAL",  "0.580"))
BAYES_RELAXED = float(os.getenv("BAYES_RELAXED", "0.520"))

# Assets y mapeos
ASSETS   = ["BTC", "ETH", "SOL"]
TICKERS  = {"BTC": "btc",      "ETH": "eth",      "SOL": "sol"}
CG_IDS   = {"BTC": "bitcoin",  "ETH": "ethereum", "SOL": "solana"}
KEYWORDS = {
    "BTC": ["btc", "bitcoin",  "xbt"],
    "ETH": ["eth", "ethereum"],
    "SOL": ["sol", "solana"],
}

# Timeframes: minutos → segundos de alineación
# 5-min  → timestamp % 300 == 0
# 15-min → timestamp % 900 == 0
TIMEFRAMES = {5: 300, 15: 900}

# APIs
GAMMA_BASE = "https://gamma-api.polymarket.com"
CG_BASE    = "https://api.coingecko.com/api/v3/simple/price"

# Factor de regresión al 50% en el modelo de señal (ver analyze())
FADE_FACTOR = 0.52

# ──────────────────────────────────────────────────────────────────────────────
# HTTP SESSION (con reintentos y User-Agent)
# ──────────────────────────────────────────────────────────────────────────────
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503],
        allowed_methods=["GET", "POST"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = f"SentinelBot/{VERSION}"
    return s

HTTP = _make_session()

# ──────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ──────────────────────────────────────────────────────────────────────────────
def parse_json_field(val: Any) -> list:
    """
    Parsea campos que pueden estar doblemente codificados como JSON string.
    Crítico: Polymarket retorna outcomePrices como '["0.51","0.49"]' (string)
    no como ["0.51","0.49"] (array). Detectado en investigación 2026.
    """
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            result = json.loads(val)
            if isinstance(result, list):
                return result
        except (json.JSONDecodeError, ValueError):
            pass
    return []


def normal_cdf(z: float) -> float:
    """CDF de distribución normal estándar vía función erf."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def half_kelly(edge: float) -> float:
    """Half-Kelly fraction, capped at TRADE_FRACTION."""
    return min(TRADE_FRACTION, max(0.0, edge * 0.5))


def ts_now() -> int:
    return int(time.time())


def dt_now() -> datetime:
    return datetime.now(timezone.utc)


# ──────────────────────────────────────────────────────────────────────────────
# SLUG DETERMINÍSTICO ← EL CAMBIO CLAVE VS v3-v6
# ──────────────────────────────────────────────────────────────────────────────
def compute_slugs(asset: str, tf_minutes: int) -> List[str]:
    """
    Calcula los slugs determinísticos para las ventanas de tiempo
    actual, anterior y siguiente de un mercado Up/Down.

    Formato oficial Polymarket (verificado 2026):
        {ticker}-updown-{N}m-{unix_ts_aligned}

    El timestamp SIEMPRE es divisible por el intervalo en segundos:
        5min  → ts % 300 == 0
        15min → ts % 900 == 0

    Ejemplo: btc-updown-5m-1744567800
             eth-updown-15m-1744568700
    """
    ticker   = TICKERS[asset]
    interval = TIMEFRAMES[tf_minutes]       # segundos
    duration = f"{tf_minutes}m"
    now_ts   = ts_now()

    # Ventana actual (mercado en curso)
    current  = now_ts - (now_ts % interval)
    # Ventana anterior (puede tener pocos minutos restantes)
    previous = current - interval
    # Ventana siguiente (ya puede estar abierta para trading)
    nxt      = current + interval

    return [
        f"{ticker}-updown-{duration}-{current}",
        f"{ticker}-updown-{duration}-{previous}",
        f"{ticker}-updown-{duration}-{nxt}",
    ]


# ──────────────────────────────────────────────────────────────────────────────
# PRICE FEED (CoinGecko con caché y rate-limit guard)
# ──────────────────────────────────────────────────────────────────────────────
class PriceFeed:
    TTL = 45  # segundos entre fetches reales

    def __init__(self):
        self._cache: Dict[str, float] = {}
        self._last  = 0.0

    def get(self) -> Dict[str, float]:
        now = time.time()
        if now - self._last < self.TTL and self._cache:
            return self._cache
        ids = ",".join(CG_IDS.values())
        try:
            r = HTTP.get(
                CG_BASE,
                params={"ids": ids, "vs_currencies": "usd"},
                timeout=10,
            )
            if r.status_code == 429:
                log.warning("CoinGecko rate limit — usando caché")
                return self._cache or {}
            r.raise_for_status()
            data = r.json()
            prices: Dict[str, float] = {
                asset: float(data.get(cg_id, {}).get("usd", 0))
                for asset, cg_id in CG_IDS.items()
            }
            self._cache = prices
            self._last  = now
            log.info(
                f"CoinGecko — "
                f"BTC=${prices.get('BTC', 0):,.0f}  "
                f"ETH=${prices.get('ETH', 0):,.0f}  "
                f"SOL=${prices.get('SOL', 0):,.2f}"
            )
            return prices
        except Exception as exc:
            log.warning(f"CoinGecko error: {exc} — usando caché")
            return self._cache or {}


# ──────────────────────────────────────────────────────────────────────────────
# MARKET SCANNER  ← SLUG-BASED PRIMARY + BROADCAST FALLBACK
# ──────────────────────────────────────────────────────────────────────────────
class MarketScanner:
    """
    Busca mercados Up/Down reales en Polymarket.

    Estrategia primaria: slugs determinísticos vía GET /events?slug=
    Fallback:            GET /markets con filtro end_date_min/max
    """

    # ── Fetchers ──────────────────────────────────────────────────────────────
    def _fetch_event(self, slug: str) -> Optional[Dict]:
        """Busca un evento de Polymarket por su slug exacto."""
        try:
            r = HTTP.get(
                f"{GAMMA_BASE}/events",
                params={"slug": slug},
                timeout=10,
            )
            if r.status_code != 200:
                return None
            data = r.json()
            if isinstance(data, list):
                return data[0] if data else None
            if isinstance(data, dict) and ("id" in data or "slug" in data):
                return data
        except Exception as exc:
            log.debug(f"    _fetch_event [{slug}]: {exc}")
        return None

    def _fetch_broadcast(self) -> List[Dict]:
        """
        Fallback: todos los mercados activos que expiran en los próximos 25 min.
        Usa filtro de rango de fechas — más eficiente que descargar todos los mercados.
        """
        now_ts  = ts_now()
        now_iso = datetime.fromtimestamp(now_ts, tz=timezone.utc).isoformat()
        max_iso = datetime.fromtimestamp(now_ts + 25 * 60, tz=timezone.utc).isoformat()
        try:
            r = HTTP.get(
                f"{GAMMA_BASE}/markets",
                params={
                    "active":       "true",
                    "closed":       "false",
                    "end_date_min": now_iso,
                    "end_date_max": max_iso,
                    "limit":        200,
                    "order":        "end_date_min",
                    "ascending":    "true",
                },
                timeout=15,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return data.get("markets", data.get("data", []))
        except Exception as exc:
            log.debug(f"    _fetch_broadcast: {exc}")
        return []

    # ── Helpers ───────────────────────────────────────────────────────────────
    @staticmethod
    def _extract_market(event: Dict) -> Optional[Dict]:
        """
        Extrae el mercado tradeable de un evento.
        Los eventos tienen un array 'markets'; los mercados standalone se devuelven tal cual.
        """
        markets = event.get("markets", [])
        if markets:
            # Primero buscar el que esté activo y abierto
            for m in markets:
                if m.get("active", False) and not m.get("closed", True):
                    return m
            return markets[0]
        return event  # ya es un mercado

    @staticmethod
    def _get_up_price(m: Dict) -> Optional[float]:
        """
        Extrae el precio del outcome "Up".
        CRÍTICO: outcomePrices viene como JSON string doblemente codificado.
        Ejemplo: '["0.51", "0.49"]'  ← string, NO array
        """
        outcomes = parse_json_field(m.get("outcomes",      []))
        prices   = parse_json_field(m.get("outcomePrices", []))
        if not outcomes or not prices:
            return None
        try:
            prices_f = [float(p) for p in prices]
        except (TypeError, ValueError):
            return None
        # Buscar outcome "Up", "Yes", o "1"
        for i, o in enumerate(outcomes):
            if str(o).lower() in ("up", "yes", "1", "true") and i < len(prices_f):
                return prices_f[i]
        return prices_f[0] if prices_f else None

    @staticmethod
    def _parse_end_dt(obj: Dict) -> Optional[datetime]:
        """Parsea endDate de un evento o mercado, manejando múltiples formatos."""
        for field in ("endDate", "end_date_iso", "endDateIso", "end_date"):
            val = obj.get(field)
            if not val:
                continue
            if isinstance(val, (int, float)):
                ts = val if val < 1e11 else val / 1000
                try:
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
                except Exception:
                    continue
            try:
                return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
        return None

    @staticmethod
    def _matches_asset(obj: Dict, asset: str) -> bool:
        text = " ".join(filter(None, [
            obj.get("title", ""),
            obj.get("question", ""),
            obj.get("description", ""),
            obj.get("slug", ""),
        ])).lower()
        return any(kw in text for kw in KEYWORDS.get(asset, [asset.lower()]))

    @staticmethod
    def _detect_tf(m: Dict, now_dt: datetime) -> Optional[int]:
        """Detecta si un mercado es de 5min o 15min por duración real."""
        for f_start in ("startDate", "start_date_iso", "startDateIso"):
            vs = m.get(f_start)
            if not vs:
                continue
            try:
                start_dt = datetime.fromisoformat(str(vs).replace("Z", "+00:00"))
                end_dt   = MarketScanner._parse_end_dt(m)
                if end_dt:
                    dur_mins = (end_dt - start_dt).total_seconds() / 60
                    if abs(dur_mins - 5)  <= 1.5:
                        return 5
                    if abs(dur_mins - 15) <= 2.0:
                        return 15
            except Exception:
                pass
        return None

    # ── Scan principal ────────────────────────────────────────────────────────
    def scan(self, asset: str, tf_minutes: int, spot_price: float) -> List[Dict]:
        """
        Escanea mercados Up/Down reales para (asset, tf_minutes).

        1. Compute slugs determinísticos → GET /events?slug=   [principal]
        2. Si nada → GET /markets con rango de fechas           [fallback]

        Retorna lista de dicts enriquecidos listos para analyze().
        """
        log.info(f"  ┌─ {asset} {tf_minutes}min")
        found:    List[Dict] = []
        seen_ids: set        = set()

        # ── ESTRATEGIA 1: Slug determinístico ─────────────────────────────────
        slugs = compute_slugs(asset, tf_minutes)
        log.debug(f"  │  Slugs calculados: {slugs}")

        for slug in slugs:
            event = self._fetch_event(slug)
            if not event:
                continue

            # Validar que el evento esté activo
            if event.get("closed", False):
                continue

            market = self._extract_market(event)
            if not market:
                continue

            mid = market.get("id") or market.get("conditionId")
            if not mid or mid in seen_ids:
                continue

            up_price = self._get_up_price(market)
            if up_price is None or not (0.03 <= up_price <= 0.97):
                continue

            end_dt = self._parse_end_dt(event) or self._parse_end_dt(market)
            if not end_dt:
                continue
            mins_left = (end_dt - dt_now()).total_seconds() / 60
            if mins_left < 0.5:
                continue  # ya expiró

            seen_ids.add(mid)
            enriched = {
                **market,
                "_asset":       asset,
                "_tf":          tf_minutes,
                "_up_price":    up_price,
                "_mins_left":   mins_left,
                "_slug":        slug,
                "_method":      "slug",
                "_spot_entry":  spot_price,
            }
            found.append(enriched)
            log.info(
                f"  │  ✅ [SLUG] {slug}\n"
                f"  │     Up={up_price:.3f}  Down={1-up_price:.3f}"
                f"  |  {mins_left:.1f}min restantes"
            )

        # ── ESTRATEGIA 2: Broadcast fallback ──────────────────────────────────
        if not found:
            log.debug(f"  │  Slugs sin resultado → broadcast fallback")
            for m in self._fetch_broadcast():
                if not self._matches_asset(m, asset):
                    continue
                mid = m.get("id") or m.get("conditionId")
                if not mid or mid in seen_ids:
                    continue

                up_price = self._get_up_price(m)
                if up_price is None or not (0.03 <= up_price <= 0.97):
                    continue

                end_dt = self._parse_end_dt(m)
                if not end_dt:
                    continue
                mins_left = (end_dt - dt_now()).total_seconds() / 60
                if mins_left < 0.5:
                    continue

                # Verificar timeframe por duración
                tf_detected = self._detect_tf(m, dt_now())
                if tf_detected is not None and tf_detected != tf_minutes:
                    continue
                # Si no podemos detectar TF, filtramos por ventana de tiempo
                if tf_detected is None:
                    if tf_minutes == 5  and not (0.5 <= mins_left <= 8):
                        continue
                    if tf_minutes == 15 and not (0.5 <= mins_left <= 20):
                        continue

                seen_ids.add(mid)
                enriched = {
                    **m,
                    "_asset":      asset,
                    "_tf":         tf_minutes,
                    "_up_price":   up_price,
                    "_mins_left":  mins_left,
                    "_slug":       m.get("slug", mid),
                    "_method":     "broadcast",
                    "_spot_entry": spot_price,
                }
                found.append(enriched)
                log.info(
                    f"  │  ✅ [BROADCAST] {m.get('slug', mid)[:50]}\n"
                    f"  │     Up={up_price:.3f}  Down={1-up_price:.3f}"
                    f"  |  {mins_left:.1f}min restantes"
                )

        # ── Resultado ─────────────────────────────────────────────────────────
        if not found:
            log.info(
                f"  └─ [SKIP] No real {tf_minutes}-min markets found"
                f" for {asset} on Polymarket — skipping"
            )
        else:
            log.info(f"  └─ {len(found)} mercado(s) encontrado(s)")

        return found


# ──────────────────────────────────────────────────────────────────────────────
# MOTOR DE SEÑALES
# ──────────────────────────────────────────────────────────────────────────────
#
# MODELO: Mercados Up/Down de Polymarket deberían tener probabilidad ~50%.
# Cuando el precio del mercado se desvía de 0.50, la multitud está sesgada.
# Nosotros "fadeamos" ese sesgo con un factor de regresión al 50%.
#
# Ejemplo: up_price = 0.65  (mercado bullish en BTC)
#   crowd_bias  = 0.65 - 0.50 = +0.15
#   our_up_prob = 0.50 - 0.35*0.15 = 0.4475
#   → Apostamos DOWN: ev = (1-0.4475) - (1-0.65) = 0.5525 - 0.35 = 0.2025 = 20% EV
#
# El modelo se ajusta al tiempo restante: cuanto menos tiempo queda,
# más peso damos al precio del mercado (más informado cerca del vencimiento).
#
def analyze(market: Dict, relaxed_mode: bool) -> Optional[Dict]:
    """
    Calcula señal de trading para un mercado Up/Down.
    Retorna dict de señal o None si no hay edge suficiente.
    """
    up_price   = market["_up_price"]
    asset      = market["_asset"]
    tf         = market["_tf"]
    mins_left  = market["_mins_left"]
    spot_entry = market.get("_spot_entry", 0.0)

    if mins_left < 1.0:  # Demasiado cerca del vencimiento
        return None

    # ── Estimación propia ──────────────────────────────────────────────────
    crowd_bias  = up_price - 0.50
    our_up_prob = 0.50 - FADE_FACTOR * crowd_bias

    # Ajuste por tiempo restante: confiar más en el mercado cerca del cierre
    pct_remaining = min(1.0, mins_left / tf)
    our_up_prob   = (our_up_prob * pct_remaining
                     + up_price * (1.0 - pct_remaining))
    our_up_prob   = max(0.05, min(0.95, our_up_prob))

    # ── Mejor lado ────────────────────────────────────────────────────────
    ev_up   = our_up_prob       - up_price
    ev_down = (1 - our_up_prob) - (1 - up_price)

    if ev_up >= ev_down and ev_up > 0:
        side      = "UP"
        ev        = ev_up
        bet_price = up_price
        our_prob  = our_up_prob
    elif ev_down > 0:
        side      = "DOWN"
        ev        = ev_down
        bet_price = 1.0 - up_price
        our_prob  = 1.0 - our_up_prob
    else:
        log.debug(f"    [NO-EDGE] {asset} {tf}min up={up_price:.3f}")
        return None

    # ── Clasificar por threshold ───────────────────────────────────────────
    if ev >= EV_NORMAL and our_prob >= BAYES_NORMAL:
        mode = "NORMAL"
    elif ev >= EV_RELAXED and our_prob >= BAYES_RELAXED:
        mode = "RELAXED"
    else:
        log.debug(
            f"    [BELOW-THRESHOLD] {asset} {tf}min | {side} | "
            f"EV={ev*100:.1f}% Bayes={our_prob*100:.1f}%"
        )
        return None

    return {
        "market_id":   market.get("id") or market.get("conditionId"),
        "slug":        market.get("_slug", ""),
        "asset":       asset,
        "tf":          tf,
        "side":        side,
        "ev":          ev,
        "bayes":       our_prob,
        "bet_price":   bet_price,
        "up_price":    up_price,
        "our_up_prob": our_up_prob,
        "mins_left":   mins_left,
        "mode":        mode,
        "spot_entry":  spot_entry,
        "question":    (
            market.get("question")
            or market.get("title")
            or f"{asset} Up/Down {tf}min"
        ),
    }


# ──────────────────────────────────────────────────────────────────────────────
# PAPER ENGINE
# ──────────────────────────────────────────────────────────────────────────────
CSV_FIELDS = [
    "trade_id", "market_id", "slug", "asset", "tf", "side",
    "bet_price", "stake", "entry_time", "spot_entry",
    "status", "pnl", "close_time", "mode",
]


class Trade:
    def __init__(self, tid: str, sig: Dict, stake: float, ts: datetime):
        self.trade_id   = tid
        self.market_id  = sig["market_id"]
        self.slug       = sig["slug"]
        self.asset      = sig["asset"]
        self.tf         = sig["tf"]
        self.side       = sig["side"]
        self.bet_price  = sig["bet_price"]
        self.stake      = stake
        self.entry_time = ts
        self.spot_entry = sig["spot_entry"]   # precio CoinGecko al abrir
        self.up_price   = sig["up_price"]     # precio Up del mercado al abrir
        self.mins_left  = sig["mins_left"]
        self.status     = "OPEN"
        self.pnl        = 0.0
        self.close_time = None
        self.mode       = sig["mode"]

    def row(self) -> Dict:
        return {
            "trade_id":   self.trade_id,
            "market_id":  self.market_id,
            "slug":       self.slug[:70],
            "asset":      self.asset,
            "tf":         self.tf,
            "side":       self.side,
            "bet_price":  f"{self.bet_price:.4f}",
            "stake":      f"{self.stake:.2f}",
            "entry_time": self.entry_time.isoformat(),
            "spot_entry": f"{self.spot_entry:.4f}",
            "status":     self.status,
            "pnl":        f"{self.pnl:.4f}",
            "close_time": self.close_time.isoformat() if self.close_time else "",
            "mode":       self.mode,
        }


class PaperEngine:
    def __init__(self, initial: float):
        self.bankroll       = initial
        self.peak           = initial
        self.open_trades:   List[Trade] = []
        self.closed_trades: deque       = deque(maxlen=500)
        self._lock          = threading.Lock()
        self._init_csv()

    def _init_csv(self):
        if not os.path.exists(CSV_FILE):
            with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()

    def _write_csv(self, t: Trade):
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow(t.row())

    # ── Lifecycle ─────────────────────────────────────────────────────────────
    def can_open(self, market_id: str) -> bool:
        with self._lock:
            if len(self.open_trades) >= MAX_OPEN_TRADES:
                return False
            return all(t.market_id != market_id for t in self.open_trades)

    def open_trade(self, sig: Dict, ts: datetime) -> Optional["Trade"]:
        if not self.can_open(sig["market_id"]):
            return None
        with self._lock:
            frac  = half_kelly(sig["ev"])
            stake = round(max(0.50, self.bankroll * frac), 2)
            t     = Trade(str(uuid.uuid4())[:8].upper(), sig, stake, ts)
            self.open_trades.append(t)
            self.bankroll -= stake
        self._write_csv(t)
        return t

    def resolve(self, t: Trade, won: bool, ts: datetime):
        with self._lock:
            if t.status != "OPEN":
                return
            if won:
                payout   = t.stake / t.bet_price  # Polymarket paga $1/share
                t.pnl    = payout - t.stake
                t.status = "WIN"
                self.bankroll += payout
            else:
                t.pnl    = -t.stake
                t.status = "LOSS"
            t.close_time = ts
            self.open_trades  = [x for x in self.open_trades if x.trade_id != t.trade_id]
            self.closed_trades.appendleft(t)
            if self.bankroll > self.peak:
                self.peak = self.bankroll
        self._write_csv(t)

    def resolve_by_price(self, prices: Dict[str, float]):
        """
        Paper mode: resuelve trades por comparación de precios spot.

        Método: compara precio CoinGecko actual vs precio al abrir el trade.
        Si el precio subió → Up gana. Si bajó → Down gana.
        Esto aproxima la resolución oracle de Chainlink que usa Polymarket.
        """
        now = dt_now()

        # Expirar trades muy viejos
        to_expire = []
        with self._lock:
            for t in self.open_trades:
                deadline = t.entry_time + timedelta(minutes=t.mins_left + 8)
                if now > deadline:
                    to_expire.append(t)
        for t in to_expire:
            log.info(f"[EXPIRE] {t.trade_id} {t.asset} {t.tf}min — forzando cierre")
            self.resolve(t, won=False, ts=now)

        # Resolver trades que ya vencieron
        for t in list(self.open_trades):
            if t.status != "OPEN":
                continue
            expected_close = t.entry_time + timedelta(minutes=t.mins_left)
            if now < expected_close:
                continue

            current_price = prices.get(t.asset, 0.0)
            if current_price <= 0:
                continue

            # Resolución: comparar precio actual vs precio al entrar
            if t.spot_entry > 0:
                price_went_up = current_price >= t.spot_entry
            else:
                # Fallback: simulación ponderada por precio de mercado
                price_went_up = random.random() >= (1 - t.up_price)

            if t.side == "UP":
                won = price_went_up
            else:  # DOWN
                won = not price_went_up

            self.resolve(t, won=won, ts=now)
            icon = "✅" if won else "❌"
            log.info(
                f"[RESOLVE] {icon} {t.trade_id} | {t.asset} {t.tf}min {t.side} | "
                f"spot_entry={t.spot_entry:.2f} → current={current_price:.2f} | "
                f"PnL: {'+' if t.pnl >= 0 else ''}{t.pnl:.2f}"
            )

    # ── Propiedades ───────────────────────────────────────────────────────────
    @property
    def drawdown(self) -> float:
        return (self.peak - self.bankroll) / self.peak if self.peak > 0 else 0.0

    @property
    def net_pnl(self) -> float:
        return self.bankroll - INITIAL_BANKROLL

    def summary(self) -> Dict:
        closed = list(self.closed_trades)
        wins   = [t for t in closed if t.status == "WIN"]
        losses = [t for t in closed if t.status == "LOSS"]
        total  = len(wins) + len(losses)
        return {
            "bankroll": self.bankroll,
            "net_pnl":  self.net_pnl,
            "peak":     self.peak,
            "drawdown": self.drawdown,
            "open":     len(self.open_trades),
            "closed":   total,
            "wins":     len(wins),
            "losses":   len(losses),
            "wr":       (len(wins) / total * 100) if total > 0 else 0.0,
        }


# ──────────────────────────────────────────────────────────────────────────────
# CIRCUIT BREAKER
# ──────────────────────────────────────────────────────────────────────────────
class CircuitBreaker:
    def __init__(self):
        self.tripped = False
        self.reason  = ""

    def check(self, engine: PaperEngine) -> bool:
        if engine.drawdown >= CB_PCT:
            self.tripped = True
            self.reason  = (
                f"Drawdown {engine.drawdown*100:.1f}% ≥ límite {CB_PCT*100:.0f}%"
            )
            return True
        return False

    def reset(self):
        self.tripped = False
        self.reason  = ""


# ──────────────────────────────────────────────────────────────────────────────
# DISCORD WEBHOOK (opcional)
# ──────────────────────────────────────────────────────────────────────────────
def discord(text: str):
    if not DISCORD_WEBHOOK:
        return
    try:
        requests.post(
            DISCORD_WEBHOOK,
            json={"content": text[:2000]},
            timeout=8,
        )
    except Exception as exc:
        log.debug(f"Discord error: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# BOT STATE
# ──────────────────────────────────────────────────────────────────────────────
class BotState:
    def __init__(self):
        self.cycle            = 0
        self.paused           = False
        self.relaxed_mode     = False
        self.last_signal_time = dt_now()
        self.signals_today    = 0
        self.engine           = PaperEngine(INITIAL_BANKROLL)
        self.cb               = CircuitBreaker()
        self._scan_now        = False  # flag para /scan_now de Telegram

    def update_relaxed_mode(self) -> bool:
        """Activa modo relajado automáticamente después de RELAXED_AFTER_MINS sin señales."""
        mins_since = (dt_now() - self.last_signal_time).total_seconds() / 60
        was_relaxed = self.relaxed_mode
        self.relaxed_mode = mins_since >= RELAXED_AFTER_MINS
        if self.relaxed_mode and not was_relaxed:
            log.info(
                f"[AUTO-RELAX] {mins_since:.0f}min sin señales "
                f"→ modo relajado activado "
                f"(EV≥{EV_RELAXED*100:.1f}% Bayes≥{BAYES_RELAXED*100:.0f}%)"
            )
        elif not self.relaxed_mode and was_relaxed:
            log.info("[AUTO-RELAX] Señal detectada → volviendo a modo normal")
        return self.relaxed_mode


# ──────────────────────────────────────────────────────────────────────────────
# TELEGRAM BOT (pure HTTP long-polling — sin librería externa)
# ──────────────────────────────────────────────────────────────────────────────
HELP_TEXT = f"""🤖 <b>Sentinel Polymarket v{VERSION}</b>

<b>Comandos disponibles:</b>
  /status    — Estado general del bot
  /active    — Trades abiertos con detalle completo
  /trades    — Abiertos + últimos 10 cerrados
  /pnl       — Resumen histórico de P&amp;L
  /scan_now  — Forzar escaneo inmediato
  /pause     — Pausar el bot
  /resume    — Reanudar / resetear circuit breaker
  /help      — Este mensaje

<b>Configuración:</b>
  Assets:    {', '.join(ASSETS)} × {list(TIMEFRAMES.keys())}min
  EV:        Normal {EV_NORMAL*100:.1f}% | Relajado {EV_RELAXED*100:.1f}%
  Bayes:     Normal {BAYES_NORMAL*100:.0f}% | Relajado {BAYES_RELAXED*100:.0f}%
  Relajado auto: después de {RELAXED_AFTER_MINS}min sin señales
  Mode:      {'📄 Paper' if PAPER_MODE else '💰 Live'}"""


class TelegramBot:
    def __init__(self, token: str, chat_id: str):
        self.token    = token
        self.chat_id  = chat_id
        self._base    = f"https://api.telegram.org/bot{token}"
        self._offset  = 0
        self._running = True

    # ── Envío ──────────────────────────────────────────────────────────────
    def send(self, text: str, parse_mode: str = "HTML", to: str = None) -> bool:
        cid = to or self.chat_id
        if not self.token or not cid:
            return False
        try:
            r = requests.post(
                f"{self._base}/sendMessage",
                json={
                    "chat_id":                  cid,
                    "text":                     text,
                    "parse_mode":               parse_mode,
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )
            return r.status_code == 200
        except Exception as exc:
            log.debug(f"Telegram send: {exc}")
            return False

    # ── Polling ────────────────────────────────────────────────────────────
    def _get_updates(self) -> List[Dict]:
        if not self.token:
            return []
        try:
            r = requests.get(
                f"{self._base}/getUpdates",
                params={"offset": self._offset, "timeout": 20, "limit": 20},
                timeout=25,
            )
            if r.status_code == 200:
                return r.json().get("result", [])
        except Exception:
            pass
        return []

    def poll_loop(self, state: BotState):
        log.info("Telegram polling iniciado")
        while self._running:
            try:
                updates = self._get_updates()
                for upd in updates:
                    self._offset = upd["update_id"] + 1
                    msg  = upd.get("message") or upd.get("edited_message") or {}
                    text = (msg.get("text") or "").strip()
                    cid  = str(msg.get("chat", {}).get("id", ""))
                    if text.startswith("/"):
                        cmd = text.split()[0].lower().split("@")[0]
                        try:
                            self._handle(cmd, cid, state)
                        except Exception as exc:
                            log.debug(f"Command handler error: {exc}")
            except Exception as exc:
                log.debug(f"Poll loop error: {exc}")
            time.sleep(1)

    def stop(self):
        self._running = False

    # ── Handlers de comandos ────────────────────────────────────────────────
    def _handle(self, cmd: str, cid: str, state: BotState):
        engine = state.engine

        if cmd == "/help":
            self.send(HELP_TEXT, to=cid)

        elif cmd == "/status":
            s     = engine.summary()
            cb    = "🔴 TRIPPED" if state.cb.tripped  else "🟢 OK"
            pa    = "⏸ PAUSADO"  if state.paused       else "▶ ACTIVO"
            mode  = "🔶 RELAJADO" if state.relaxed_mode else "🔵 NORMAL"
            mins_no_signal = (dt_now() - state.last_signal_time).total_seconds() / 60
            self.send(
                f"<b>🤖 Sentinel v{VERSION} — Status</b>\n\n"
                f"Estado:       {pa}\n"
                f"Modo señal:   {mode}\n"
                f"Sin señal:    {mins_no_signal:.0f}min\n"
                f"Ciclo:        #{state.cycle}\n"
                f"Bankroll:     <b>${s['bankroll']:.2f}</b>\n"
                f"P&amp;L neto: {'+' if s['net_pnl']>=0 else ''}{s['net_pnl']:.2f}\n"
                f"Peak:         ${s['peak']:.2f}\n"
                f"Drawdown:     {s['drawdown']*100:.1f}%\n"
                f"CB:           {cb}\n"
                f"Abiertos:     {s['open']}/{MAX_OPEN_TRADES}\n"
                f"Señales hoy:  {state.signals_today}",
                to=cid,
            )

        elif cmd == "/active":
            trades = engine.open_trades
            if not trades:
                self.send("📭 No hay trades abiertos.", to=cid)
                return
            lines = [f"<b>📂 Trades Abiertos ({len(trades)})</b>\n"]
            for t in trades:
                age_min = int((dt_now() - t.entry_time).total_seconds() / 60)
                lines.append(
                    f"🔹 <b>{t.trade_id}</b> | {t.asset} {t.tf}min\n"
                    f"   Lado:     {t.side} @ {t.bet_price:.3f}\n"
                    f"   Stake:    ${t.stake:.2f}\n"
                    f"   Entrada:  ${t.spot_entry:,.2f}\n"
                    f"   Elapsed:  {age_min}min\n"
                    f"   Modo:     {t.mode}\n"
                    f"   Slug:     {t.slug[:45]}…"
                )
            self.send("\n".join(lines), to=cid)

        elif cmd == "/trades":
            open_t   = engine.open_trades
            closed_t = list(engine.closed_trades)[:10]
            lines    = []
            if open_t:
                lines.append(f"<b>📂 Abiertos ({len(open_t)})</b>")
                for t in open_t:
                    lines.append(
                        f"  • {t.trade_id} | {t.asset} {t.tf}min "
                        f"| {t.side} @ {t.bet_price:.3f} | ${t.stake:.2f}"
                    )
            if closed_t:
                lines.append(f"\n<b>📋 Últimos 10 cerrados</b>")
                for t in closed_t:
                    icon = "✅" if t.status == "WIN" else "❌"
                    pnl  = f"{'+' if t.pnl >= 0 else ''}{t.pnl:.2f}"
                    lines.append(
                        f"  {icon} {t.trade_id} | {t.asset} {t.tf}min "
                        f"| {t.side} | PnL: {pnl}"
                    )
            if not lines:
                self.send("No hay trades registrados aún.", to=cid)
                return
            self.send("\n".join(lines), to=cid)

        elif cmd == "/pnl":
            s      = engine.summary()
            closed = list(engine.closed_trades)
            by_asset: Dict[str, Dict] = defaultdict(
                lambda: {"pnl": 0.0, "n": 0, "wins": 0}
            )
            for t in closed:
                d = by_asset[t.asset]
                d["pnl"]  += t.pnl
                d["n"]    += 1
                if t.status == "WIN":
                    d["wins"] += 1
            lines = [
                f"<b>📊 P&amp;L Histórico — Sentinel v{VERSION}</b>\n",
                f"Bankroll inicial: ${INITIAL_BANKROLL:.2f}",
                f"Bankroll actual:  <b>${s['bankroll']:.2f}</b>",
                f"P&amp;L neto:     {'+' if s['net_pnl']>=0 else ''}{s['net_pnl']:.2f}",
                f"Peak:             ${s['peak']:.2f}",
                f"Drawdown:         {s['drawdown']*100:.1f}%\n",
                f"Total trades: {s['closed']}  |  "
                f"{s['wins']}W / {s['losses']}L  |  WR {s['wr']:.1f}%\n",
                "<b>Por asset:</b>",
            ]
            for asset in ASSETS:
                d = by_asset.get(asset)
                if d and d["n"] > 0:
                    wr = d["wins"] / d["n"] * 100
                    pnl_str = f"{'+' if d['pnl']>=0 else ''}{d['pnl']:.2f}"
                    lines.append(
                        f"  {asset}: {pnl_str} "
                        f"({d['n']} trades | {d['wins']}W | {wr:.0f}% WR)"
                    )
            self.send("\n".join(lines), to=cid)

        elif cmd == "/scan_now":
            state._scan_now = True
            self.send(
                "🔍 Escaneo forzado en progreso...\n"
                "Recibirás una notificación con los resultados.",
                to=cid,
            )

        elif cmd == "/pause":
            state.paused = True
            log.info("[TELEGRAM] Bot pausado")
            self.send("⏸ Bot pausado. Usa /resume para reanudar.", to=cid)

        elif cmd == "/resume":
            state.paused = False
            state.cb.reset()
            log.info("[TELEGRAM] Bot reanudado + CB reseteado")
            self.send(
                "▶ Bot reanudado.\n"
                "Circuit breaker reseteado.\n"
                "Usa /status para ver el estado actual.",
                to=cid,
            )

        else:
            self.send(f"Comando no reconocido: <code>{cmd}</code>\nUsa /help.", to=cid)


# ──────────────────────────────────────────────────────────────────────────────
# UN CICLO DE ESCANEO (reutilizable por main loop y /scan_now)
# ──────────────────────────────────────────────────────────────────────────────
def run_scan_cycle(
    state:   BotState,
    scanner: MarketScanner,
    prices:  Dict[str, float],
    tg:      TelegramBot,
    forced:  bool = False,
) -> Tuple[int, int]:
    """
    Ejecuta un ciclo completo de escaneo sobre los 6 pares (asset × timeframe).
    Retorna (mercados_encontrados, señales_generadas).
    """
    now           = dt_now()
    relaxed       = state.update_relaxed_mode()
    mode_label    = (
        f"RELAJADO EV≥{EV_RELAXED*100:.1f}% Bayes≥{BAYES_RELAXED*100:.0f}%"
        if relaxed
        else f"NORMAL EV≥{EV_NORMAL*100:.1f}% Bayes≥{BAYES_NORMAL*100:.0f}%"
    )
    prefix        = "🔍 SCAN_NOW" if forced else f"CICLO #{state.cycle}"
    log.info(f"━━━ {prefix} | {mode_label} ━━━")

    total_found   = 0
    total_signals = 0
    no_mkt_assets: List[str] = []

    for asset in ASSETS:
        spot = prices.get(asset, 0.0)
        if spot <= 0:
            log.warning(f"  [SKIP] Sin precio para {asset}")
            no_mkt_assets.append(asset)
            continue

        log.info(f"━━ Escaneando {asset} (spot=${spot:,.2f}) ━━")
        asset_found = False

        for tf in TIMEFRAMES:
            markets = scanner.scan(asset, tf, spot)
            total_found += len(markets)
            if markets:
                asset_found = True

            for mkt in markets:
                sig = analyze(mkt, relaxed)
                if sig is None:
                    continue

                total_signals += 1
                state.signals_today += 1
                state.last_signal_time = dt_now()

                log.info(
                    f"  [SIGNAL] {asset} {tf}min | {sig['side']} | "
                    f"EV={sig['ev']*100:.1f}% Bayes={sig['bayes']*100:.1f}% "
                    f"[{sig['mode']}] | up_market={sig['up_price']:.3f}"
                )

                trade = state.engine.open_trade(sig, now)
                if trade:
                    msg = (
                        f"{'🔶' if sig['mode']=='RELAXED' else '🟢'} "
                        f"<b>TRADE #{trade.trade_id}</b> [{sig['mode']}]\n\n"
                        f"Asset:    {asset} {tf}min\n"
                        f"Lado:     <b>{trade.side}</b> @ {trade.bet_price:.3f}\n"
                        f"Stake:    ${trade.stake:.2f}\n"
                        f"EV:       {sig['ev']*100:.1f}%\n"
                        f"Bayes:    {sig['bayes']*100:.1f}%\n"
                        f"Precio:   ${spot:,.2f}\n"
                        f"Slug:     <code>{sig['slug'][:55]}</code>"
                    )
                    log.info(
                        f"  [TRADE ABIERTO] {trade.trade_id} | "
                        f"stake=${trade.stake:.2f} | {trade.side}"
                    )
                    tg.send(msg)
                    discord(
                        f"{'🔶' if sig['mode']=='RELAXED' else '🟢'} TRADE {trade.trade_id} | "
                        f"{asset} {tf}min | {trade.side} @ {trade.bet_price:.3f} | "
                        f"${trade.stake:.2f} | EV {sig['ev']*100:.1f}% [{sig['mode']}]"
                    )
                else:
                    log.info("  [SKIP TRADE] Max open trades o mercado duplicado")

        if not asset_found:
            no_mkt_assets.append(asset)

    # Resumen
    if set(no_mkt_assets) == set(ASSETS):
        log.info(
            f"  [INFO] Sin mercados reales: {', '.join(ASSETS)}"
            f" — esperando apertura de mercados en Polymarket"
        )
    elif no_mkt_assets:
        log.info(f"  [INFO] Sin mercados para: {', '.join(set(no_mkt_assets))}")

    log.info(
        f"━━━ {prefix} completo — "
        f"{total_found} mercados encontrados, "
        f"{total_signals} señales ━━━\n"
    )

    if forced:
        s = state.engine.summary()
        tg.send(
            f"🔍 <b>Scan forzado completado</b>\n\n"
            f"Mercados encontrados: {total_found}\n"
            f"Señales generadas:    {total_signals}\n"
            f"Modo:                 {'Relajado 🔶' if relaxed else 'Normal 🔵'}\n"
            f"Bankroll:             ${s['bankroll']:.2f}"
        )

    return total_found, total_signals


# ──────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ──────────────────────────────────────────────────────────────────────────────
def main():
    banner = "\n".join([
        "═" * 64,
        f"  SENTINEL POLYMARKET v{VERSION} — {BUILD_DATE}",
        f"  Mode:        {'📄 PAPER TRADING' if PAPER_MODE else '💰 LIVE TRADING'}",
        f"  Assets:      {', '.join(ASSETS)} × {list(TIMEFRAMES.keys())}min",
        f"  Búsqueda:    Slug determinístico + broadcast fallback",
        f"  EV:          normal={EV_NORMAL*100:.1f}%  relajado={EV_RELAXED*100:.1f}%",
        f"  Bayes:       normal={BAYES_NORMAL*100:.0f}%   relajado={BAYES_RELAXED*100:.0f}%",
        f"  Relajado:    auto después de {RELAXED_AFTER_MINS}min sin señales",
        f"  Bankroll:    ${INITIAL_BANKROLL:.2f}",
        f"  CB:          {CB_PCT*100:.0f}% drawdown",
        f"  Ciclo:       {CYCLE_SLEEP}s",
        f"  Discord:     {'✅ configurado' if DISCORD_WEBHOOK else '❌ no configurado'}",
        "═" * 64,
    ])
    log.info(banner)

    state   = BotState()
    scanner = MarketScanner()
    feed    = PriceFeed()
    tg      = TelegramBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

    # ── Iniciar Telegram polling en background ────────────────────────────────
    if TELEGRAM_TOKEN:
        tg_thread = threading.Thread(
            target=tg.poll_loop, args=(state,), daemon=True, name="tg-poll"
        )
        tg_thread.start()
        tg.send(
            f"🚀 <b>Sentinel v{VERSION} ACTIVADO y FUNCIONANDO correctamente</b>\n\n"
            f"Mode:    {'📄 Paper Trading' if PAPER_MODE else '💰 Live Trading'}\n"
            f"Assets:  {', '.join(ASSETS)} × {list(TIMEFRAMES.keys())}min\n"
            f"Capital: ${INITIAL_BANKROLL:.2f}\n"
            f"Método:  Slug determinístico (fix definitivo)\n\n"
            f"Usa /help para ver todos los comandos."
        )

    discord(
        f"🚀 Sentinel v{VERSION} iniciado | "
        f"{'Paper' if PAPER_MODE else 'Live'} | "
        f"${INITIAL_BANKROLL:.2f} | Slug-based scan"
    )

    # ── Loop principal ────────────────────────────────────────────────────────
    while True:
        try:
            state.cycle += 1

            # ── Guard: pause / circuit breaker ────────────────────────────────
            if state.paused:
                log.info("  [PAUSED] Bot pausado — esperando /resume")
                time.sleep(CYCLE_SLEEP)
                continue

            if state.cb.tripped:
                log.warning(f"  [CIRCUIT BREAKER] {state.cb.reason}")
                time.sleep(CYCLE_SLEEP)
                continue

            # ── Fetch precios ─────────────────────────────────────────────────
            prices = feed.get()

            # ── Resolver trades paper ─────────────────────────────────────────
            if PAPER_MODE:
                state.engine.resolve_by_price(prices)

            # ── Circuit breaker check ─────────────────────────────────────────
            if state.cb.check(state.engine):
                msg = f"🔴 <b>CIRCUIT BREAKER ACTIVADO</b>\n{state.cb.reason}"
                log.error(msg.replace("<b>", "").replace("</b>", ""))
                tg.send(msg + "\nUsa /resume para continuar.")
                discord(f"🔴 CIRCUIT BREAKER: {state.cb.reason}")
                time.sleep(CYCLE_SLEEP)
                continue

            # ── /scan_now flag ────────────────────────────────────────────────
            if state._scan_now:
                state._scan_now = False
                run_scan_cycle(state, scanner, prices, tg, forced=True)

            # ── Ciclo de escaneo normal ───────────────────────────────────────
            run_scan_cycle(state, scanner, prices, tg, forced=False)

            # ── Resumen horario (cada 60 ciclos ≈ 1 hora) ────────────────────
            if state.cycle % 60 == 0:
                s = state.engine.summary()
                tg.send(
                    f"📊 <b>Resumen horario — Ciclo #{state.cycle}</b>\n\n"
                    f"Bankroll:  ${s['bankroll']:.2f}\n"
                    f"P&amp;L:   {'+' if s['net_pnl']>=0 else ''}{s['net_pnl']:.2f}\n"
                    f"Trades:    {s['closed']} "
                    f"({s['wins']}W / {s['losses']}L | {s['wr']:.1f}% WR)\n"
                    f"Señales:   {state.signals_today}\n"
                    f"Modo:      {'🔶 Relajado' if state.relaxed_mode else '🔵 Normal'}"
                )

        except KeyboardInterrupt:
            log.info("Shutdown solicitado.")
            tg.stop()
            tg.send("🛑 Sentinel v7.0 detenido manualmente.")
            discord("🛑 Sentinel v7.0 detenido manualmente.")
            break

        except Exception as exc:
            log.error(
                f"Error no manejado en ciclo #{state.cycle}: {exc}\n"
                f"{traceback.format_exc()}"
            )

        time.sleep(CYCLE_SLEEP)


if __name__ == "__main__":
    main()
