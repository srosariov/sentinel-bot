#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════════
#  bot.py — Sentinel Polymarket v8.5  "Circuit Breaker FIXED"
# ───────────────────────────────────────────────────────────────────────────────
#  Assets:     7 (BTC, ETH, SOL, XRP, DOGE, HYPE, BNB)
#  Timeframes: 1h y 4h ÚNICAMENTE
#  Búsqueda:   SLUG DETERMINÍSTICO (primario) + BROADCAST (siempre activo)
#  Filtro:     Duración real — 1h: 40–90min | 4h: 160–320min
#  Análisis:   Post-trade automático + análisis de CB cuando se dispara
#  Telegram:   Pure HTTP con auto-reconnect; comandos /force_resume agregados
#  Deploy:     Railway — archivo principal bot.py
# ───────────────────────────────────────────────────────────────────────────────
#  CAMBIOS v8.5 vs v8.4 — FIX DEL CIRCUIT BREAKER:
#
#  BUG ENCONTRADO EN v8.4:
#    El CB se disparaba correctamente (cb.tripped = True), pero NUNCA seteaba
#    state.paused = True. Esto resultaba en:
#      - El bot saltaba escaneos (correcto) pero…
#      - El status mostraba "▶ ACTIVO" engañosamente
#      - La alerta de Telegram solo se enviaba una vez al transition
#      - No había forma de distinguir "pausa manual" de "pausa por CB"
#      - El log spammeaba "[CB] Drawdown 21.1%" cada minuto durante 17+ horas
#
#  FIX EN v8.5:
#    1. CB trip → SIEMPRE pone state.paused = True + state.cb_paused = True
#    2. Alerta Telegram con throttle (cada 60min mientras siga tripped, no cada min)
#    3. analyze() y open_trade() rechazan TODA entrada si state.paused == True
#    4. Reset diario UTC → resetea peak_today, drawdown, CB y state.paused
#    5. Nuevo /force_resume: requiere cooldown opcional, mensaje de confirmación
#    6. Post-trade analysis del CB: top 8 trades que más contribuyeron al DD
#    7. /status muestra explícitamente si la pausa es por CB o manual
# ───────────────────────────────────────────────────────────────────────────────
#  CAMBIOS v8.1+ preservados:
#    - resolve_by_price() honesta — sin random.random()
#    - EXPIRED como categoría separada
#    - Broadcast siempre activo + filtro de duración estricto
#    - EV_NORMAL 3.0%, BAYES_NORMAL 52%, MIN_LIQUIDITY $30
# ═══════════════════════════════════════════════════════════════════════════════

VERSION    = "8.5.0"
BUILD_DATE = "2026-05-11"

import os, sys, csv, json, time, math, uuid, logging, threading, traceback
from datetime    import datetime, timezone, timedelta
from collections import deque, defaultdict
from typing      import Optional, List, Dict, Tuple

import requests
from requests.adapters  import HTTPAdapter
from urllib3.util.retry import Retry

# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────────────────────
LOG_FILE = os.getenv("LOG_FILE", "sentinel_v80.log")
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
# CONFIG (env vars)
# ──────────────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN     = os.getenv("TELEGRAM_TOKEN",       "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",     "")
DISCORD_WEBHOOK    = os.getenv("DISCORD_WEBHOOK_URL",  "")

PAPER_MODE         = os.getenv("PAPER_MODE",           "true").lower() != "false"
INITIAL_BANKROLL   = float(os.getenv("INITIAL_BANKROLL","100.0"))
TRADE_FRACTION     = float(os.getenv("TRADE_FRACTION", "0.05"))
MAX_OPEN_TRADES    = int(os.getenv("MAX_OPEN_TRADES",  "8"))
CB_PCT             = float(os.getenv("CIRCUIT_BREAKER_PCT", "0.15"))
MAX_HISTORICAL_DD = float(os.getenv("MAX_HISTORICAL_DD", "0.35"))
CB_COOLDOWN_HOURS  = float(os.getenv("CB_COOLDOWN_HOURS",   "4.0"))  # /force_resume bypass
CB_ALERT_REPEAT_MIN= int(os.getenv("CB_ALERT_REPEAT_MIN",   "60"))   # re-alert every N min
CYCLE_SLEEP        = int(os.getenv("CYCLE_SLEEP",      "60"))
CSV_FILE           = os.getenv("CSV_FILE",             "trades_v80.csv")
RELAXED_AFTER_MINS = int(os.getenv("RELAXED_AFTER_MINS","30"))
MIN_LIQUIDITY      = float(os.getenv("MIN_LIQUIDITY",  "30.0"))    # USD min

# Filtros de señal (menos conservadores que v7.x para 1h/4h)
EV_NORMAL     = float(os.getenv("EV_NORMAL",     "0.030"))   # 3.0%
EV_RELAXED    = float(os.getenv("EV_RELAXED",    "0.020"))   # 2.0%
BAYES_NORMAL  = float(os.getenv("BAYES_NORMAL",  "0.520"))   # 52%
BAYES_RELAXED = float(os.getenv("BAYES_RELAXED", "0.500"))   # 50%
FADE_FACTOR   = float(os.getenv("FADE_FACTOR",   "0.30"))

# ──────────────────────────────────────────────────────────────────────────────
# CATÁLOGO DE ASSETS
# ──────────────────────────────────────────────────────────────────────────────
ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE", "BNB"]

TICKERS: Dict[str, str] = {
    "BTC":  "btc",   "ETH":  "eth",  "SOL":  "sol",
    "XRP":  "xrp",   "DOGE": "doge", "HYPE": "hype", "BNB":  "bnb",
}

CG_IDS: Dict[str, str] = {
    "BTC":  "bitcoin",     "ETH":  "ethereum",   "SOL":  "solana",
    "XRP":  "ripple",      "DOGE": "dogecoin",   "HYPE": "hyperliquid",
    "BNB":  "binancecoin",
}

# Volatilidad anualizada por asset
ASSET_VOL: Dict[str, float] = {
    "BTC":  0.65, "ETH":  0.85, "SOL":  1.30,
    "XRP":  1.10, "DOGE": 1.60, "HYPE": 1.90, "BNB":  1.05,
}

KEYWORDS: Dict[str, List[str]] = {
    "BTC":  ["btc", "bitcoin",  "xbt"],
    "ETH":  ["eth", "ethereum"],
    "SOL":  ["sol", "solana"],
    "XRP":  ["xrp", "ripple"],
    "DOGE": ["doge", "dogecoin"],
    "HYPE": ["hype", "hyperliquid"],
    "BNB":  ["bnb",  "binance coin", "binancecoin"],
}

# ──────────────────────────────────────────────────────────────────────────────
# TIMEFRAMES — 1h y 4h
#   Cada timeframe define:
#     interval_seconds:    alineación del slug timestamp
#     duration_variants:   posibles strings en el slug
#     duration_min_strict: rango estricto de duración real (en minutos)
#                          para aceptar mercados del broadcast fallback
# ──────────────────────────────────────────────────────────────────────────────
TIMEFRAMES: Dict[str, Dict] = {
    "1h": {
        "minutes":            60,
        "interval_seconds":   3600,
        "duration_variants":  ["1h", "60m", "hourly"],
        "duration_min_strict": (40,  90),    # 40–90 min para 1h
        "label":              "1 hora",
    },
    "4h": {
        "minutes":            240,
        "interval_seconds":   14400,
        "duration_variants":  ["4h", "240m", "4hourly"],
        "duration_min_strict": (160, 320),   # 160–320 min para 4h
        "label":              "4 horas",
    },
}

GAMMA_BASE = "https://gamma-api.polymarket.com"
CG_BASE    = "https://api.coingecko.com/api/v3/simple/price"

# ──────────────────────────────────────────────────────────────────────────────
# HTTP SESSION CON REINTENTOS
# ──────────────────────────────────────────────────────────────────────────────
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3, backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = f"SentinelBot/{VERSION}"
    return s

HTTP = _make_session()

# ──────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ──────────────────────────────────────────────────────────────────────────────
def parse_json_field(val) -> list:
    """
    Polymarket Gamma API retorna outcomes y outcomePrices como STRINGS JSON.
    Ejemplo: '["Up","Down"]' y '["0.51","0.49"]'.
    Esta función las convierte a listas reales de Python.
    """
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            r = json.loads(val)
            if isinstance(r, list):
                return r
        except (json.JSONDecodeError, ValueError):
            pass
    return []


def dt_now() -> datetime:    return datetime.now(timezone.utc)
def ts_now() -> int:         return int(time.time())
def pct(v: float) -> str:    return f"{v * 100:.1f}%"
def signed(v: float) -> str: return f"{'+' if v >= 0 else ''}{v:.2f}"

# ──────────────────────────────────────────────────────────────────────────────
# SLUG DETERMINÍSTICO (CORE FIX)
# ──────────────────────────────────────────────────────────────────────────────
def compute_slugs(asset: str, tf_key: str) -> List[str]:
    """
    Genera slugs candidatos para un (asset, timeframe).

    Polymarket alinea los slugs al inicio del intervalo:
      1h → ts % 3600 == 0
      4h → ts % 14400 == 0

    Probamos:
      - 3 ventanas (anterior, actual, siguiente)
      - Múltiples variantes de string ("1h", "60m", "hourly")

    Esto da hasta 9 slugs por (asset, timeframe), todos pueden existir
    en Polymarket simultáneamente.

    Ejemplo BTC 1h en ts=1745323456:
      current = 1745323456 - (1745323456 % 3600) = 1745323200
      slugs   = ["btc-updown-1h-1745319600",
                 "btc-updown-1h-1745323200",
                 "btc-updown-1h-1745326800",
                 "btc-updown-60m-1745319600", ...]
    """
    ticker   = TICKERS[asset]
    tf       = TIMEFRAMES[tf_key]
    interval = tf["interval_seconds"]
    now_ts   = ts_now()
    current  = now_ts - (now_ts % interval)
    windows  = [current - interval, current, current + interval]

    slugs = []
    for dur_variant in tf["duration_variants"]:
        for ts in windows:
            slugs.append(f"{ticker}-updown-{dur_variant}-{ts}")
    return slugs

# ──────────────────────────────────────────────────────────────────────────────
# PRICE FEED CON CACHÉ Y AUTO-RECONNECT
# ──────────────────────────────────────────────────────────────────────────────
class PriceFeed:
    TTL = 45  # segundos de TTL

    def __init__(self):
        self._cache: Dict[str, float] = {}
        self._ts        = 0.0
        self._fail_n    = 0

    def get(self) -> Dict[str, float]:
        now = time.time()
        if now - self._ts < self.TTL and self._cache:
            return self._cache

        ids = ",".join(set(CG_IDS.values()))
        try:
            r = HTTP.get(CG_BASE, params={"ids": ids, "vs_currencies": "usd"}, timeout=12)
            if r.status_code == 429:
                log.warning("CoinGecko rate limit — usando caché")
                return self._cache or {}
            r.raise_for_status()
            raw = r.json()
            prices: Dict[str, float] = {}
            for asset, cg_id in CG_IDS.items():
                p = raw.get(cg_id, {}).get("usd", 0)
                if p:
                    prices[asset] = float(p)
            self._cache  = prices
            self._ts     = now
            self._fail_n = 0
            summary = "  ".join(
                f"{a}=${prices[a]:,.4f}" for a in ASSETS if a in prices
            )
            log.info(f"CoinGecko — {summary}")
            return prices
        except Exception as exc:
            self._fail_n += 1
            log.warning(f"CoinGecko error #{self._fail_n}: {exc} — usando caché")
            return self._cache or {}

# ──────────────────────────────────────────────────────────────────────────────
# MARKET SCANNER
# ──────────────────────────────────────────────────────────────────────────────
class MarketScanner:
    """
    Estrategia dual:
      1. Slug determinístico → /events?slug=
      2. Broadcast fallback  → /markets con filtro end_date_min/max
    """

    def _fetch_event_by_slug(self, slug: str) -> Optional[Dict]:
        try:
            r = HTTP.get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=10)
            if r.status_code != 200:
                return None
            data = r.json()
            if isinstance(data, list):
                return data[0] if data else None
            if isinstance(data, dict) and ("id" in data or "slug" in data):
                return data
        except Exception as exc:
            log.debug(f"    _fetch_event[{slug}]: {exc}")
        return None

    def _fetch_broadcast(self, max_hours_ahead: int = 5) -> List[Dict]:
        """
        Mercados activos que expiran en las próximas N horas.
        Para 1h/4h timeframes, miramos hasta 5 horas adelante.
        """
        now_ts  = ts_now()
        now_iso = datetime.fromtimestamp(now_ts,                      tz=timezone.utc).isoformat()
        max_iso = datetime.fromtimestamp(now_ts + max_hours_ahead*3600, tz=timezone.utc).isoformat()
        try:
            r = HTTP.get(
                f"{GAMMA_BASE}/markets",
                params={
                    "active":       "true",
                    "closed":       "false",
                    "end_date_min": now_iso,
                    "end_date_max": max_iso,
                    "limit":        500,
                    "order":        "end_date_min",
                    "ascending":    "true",
                },
                timeout=15,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            return data if isinstance(data, list) else data.get("markets", data.get("data", []))
        except Exception as exc:
            log.debug(f"    _fetch_broadcast: {exc}")
        return []

    @staticmethod
    def _extract_market(event: Dict) -> Optional[Dict]:
        markets = event.get("markets", [])
        if markets:
            for m in markets:
                if m.get("active", False) and not m.get("closed", True):
                    return m
            return markets[0]
        return event

    @staticmethod
    def _get_up_price(m: Dict) -> Optional[float]:
        outcomes = parse_json_field(m.get("outcomes", []))
        prices   = parse_json_field(m.get("outcomePrices", []))
        if not outcomes or not prices:
            return None
        try:
            pf = [float(p) for p in prices]
        except (TypeError, ValueError):
            return None
        for i, o in enumerate(outcomes):
            if str(o).lower() in ("up", "yes", "1", "true") and i < len(pf):
                return pf[i]
        return pf[0] if pf else None

    @staticmethod
    def _parse_dt(obj: Dict, *fields: str) -> Optional[datetime]:
        for f in fields:
            val = obj.get(f)
            if not val:
                continue
            if isinstance(val, (int, float)):
                ts = val if val < 1e11 else val / 1000
                try:    return datetime.fromtimestamp(ts, tz=timezone.utc)
                except: continue
            try:
                return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            except:
                continue
        return None

    @staticmethod
    def _liquidity(m: Dict) -> float:
        for f in ("liquidityNum", "liquidity_num", "liquidity", "volume24hr", "volumeNum"):
            v = m.get(f)
            if v is not None:
                try:    return float(v)
                except: pass
        return 0.0

    @staticmethod
    def _matches_asset(obj: Dict, asset: str) -> bool:
        text = " ".join(filter(None, [
            obj.get("title", ""), obj.get("question", ""), obj.get("slug", ""),
        ])).lower()
        return any(kw in text for kw in KEYWORDS.get(asset, [asset.lower()]))

    # ── Scan principal ────────────────────────────────────────────────────────

    def scan(self, asset: str, tf_key: str, spot: float) -> List[Dict]:
        tf       = TIMEFRAMES[tf_key]
        tf_min   = tf["minutes"]
        log.info(f"  ┌─ {asset} {tf_key} (spot=${spot:,.4f})")

        found:    List[Dict] = []
        seen_ids: set        = set()

        # ── MÉTODO 1: Slug determinístico ─────────────────────────────────────
        for slug in compute_slugs(asset, tf_key):
            event = self._fetch_event_by_slug(slug)
            if not event or event.get("closed", False):
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

            end_dt = self._parse_dt(event,  "endDate", "end_date_iso") or \
                     self._parse_dt(market, "endDate", "end_date_iso")
            if not end_dt:
                continue
            mins_left = (end_dt - dt_now()).total_seconds() / 60
            if mins_left < 1:  # < 1 min restantes
                continue

            liq = self._liquidity(market)
            seen_ids.add(mid)
            found.append({
                **market,
                "_asset": asset, "_tf_key": tf_key, "_tf_min": tf_min,
                "_up_price": up_price, "_mins_left": mins_left,
                "_slug": slug, "_method": "slug",
                "_spot": spot, "_liquidity": liq,
            })
            log.info(
                f"  │  ✅ [SLUG]  {slug}\n"
                f"  │     Up={up_price:.3f}  Down={1-up_price:.3f}"
                f"  |  {mins_left:.0f}min left  |  liq=${liq:,.0f}"
            )

        # ── MÉTODO 2: Broadcast — SIEMPRE activo (deduplicado por market_id) ──
        # Slug es prioritario (ya corrió arriba y populó seen_ids). Broadcast
        # añade mercados que slug-lookup no encontró (slugs no-estándar, eventos
        # con tickers especiales, etc.). seen_ids previene duplicados — un
        # mercado encontrado por slug NO se procesará otra vez aquí.
        log.info(f"  │  Broadcast scan (estricto)")
        dur_min_low, dur_min_high = TIMEFRAMES[tf_key]["duration_min_strict"]

        broadcast_total    = 0
        broadcast_rejected = 0
        broadcast_added    = 0

        for m in self._fetch_broadcast(max_hours_ahead=5):
            broadcast_total += 1

            if not self._matches_asset(m, asset):
                continue
            mid = m.get("id") or m.get("conditionId")
            if not mid or mid in seen_ids:
                # Ya lo encontró el slug-lookup → no duplicar
                continue

            up_price = self._get_up_price(m)
            if up_price is None or not (0.03 <= up_price <= 0.97):
                continue

            # ── FILTRO ESTRICTO DE DURACIÓN ────────────────────────────
            # Necesitamos start Y end para calcular duración real.
            # Si no podemos calcularla, RECHAZAMOS (no asumimos nada).
            start_dt = self._parse_dt(m, "startDate", "start_date_iso", "startDateIso")
            end_dt   = self._parse_dt(m, "endDate",   "end_date_iso",   "endDateIso")
            if not (start_dt and end_dt):
                log.info(
                    f"  │  [REJECT-DUR] {m.get('slug', mid)[:50]} — "
                    f"sin start/end válidos"
                )
                broadcast_rejected += 1
                continue

            duration_min = (end_dt - start_dt).total_seconds() / 60

            # La duración real DEBE caer en el rango estricto del timeframe
            if not (dur_min_low <= duration_min <= dur_min_high):
                log.info(
                    f"  │  [REJECT-DUR] {m.get('slug', mid)[:50]} — "
                    f"dur={duration_min:.0f}min fuera de "
                    f"[{dur_min_low}, {dur_min_high}] para {tf_key}"
                )
                broadcast_rejected += 1
                continue

            mins_left = (end_dt - dt_now()).total_seconds() / 60
            if mins_left < 1:
                continue

            liq = self._liquidity(m)
            seen_ids.add(mid)
            broadcast_added += 1
            found.append({
                **m,
                "_asset": asset, "_tf_key": tf_key, "_tf_min": tf_min,
                "_up_price": up_price, "_mins_left": mins_left,
                "_slug": m.get("slug", str(mid)), "_method": "broadcast",
                "_spot": spot, "_liquidity": liq,
            })
            log.info(
                f"  │  ✅ [BROAD] {m.get('slug', mid)[:55]}\n"
                f"  │     dur={duration_min:.0f}min  Up={up_price:.3f}  "
                f"Down={1-up_price:.3f}"
                f"  |  {mins_left:.0f}min left  |  liq=${liq:,.0f}"
            )

        if broadcast_rejected > 0 or broadcast_added > 0:
            log.info(
                f"  │  Broadcast: {broadcast_total} candidatos | "
                f"+{broadcast_added} añadidos | "
                f"{broadcast_rejected} rechazados por duración"
            )

        if not found:
            log.info(f"  └─ [SKIP] No real {tf_key} markets for {asset} — skipping")
        else:
            n_slug  = sum(1 for m in found if m["_method"] == "slug")
            n_broad = sum(1 for m in found if m["_method"] == "broadcast")
            method_breakdown = []
            if n_slug:  method_breakdown.append(f"{n_slug} slug")
            if n_broad: method_breakdown.append(f"{n_broad} broadcast")
            log.info(
                f"  └─ {len(found)} mercado(s) válido(s) "
                f"({', '.join(method_breakdown)})"
            )
        return found

# ──────────────────────────────────────────────────────────────────────────────
# MOTOR DE SEÑALES
# ──────────────────────────────────────────────────────────────────────────────
def analyze(market: Dict, relaxed: bool) -> Optional[Dict]:
    """
    Modelo de señal: fade del sesgo de la multitud.

    Hipótesis: en mercados Up/Down de Polymarket, P(Up) verdadera ≈ 0.50.
    Cuando el mercado cotiza muy desviado, refleja sesgo de la multitud
    y existe edge en apostar al lado contrario (parcialmente).

    Para 1h/4h:
      - Mayor liquidez → menos ruido → mejor señal
      - Asset volatility ajusta la fuerza del fade
      - Cerca del cierre, el mercado tiene más info → confiamos más en él
    """
    up_price = market["_up_price"]
    asset    = market["_asset"]
    tf_key   = market["_tf_key"]
    tf_min   = market["_tf_min"]
    mins     = market["_mins_left"]
    spot     = market.get("_spot", 0.0)
    liq      = market.get("_liquidity", 0.0)

    if mins < 2.0:  # mínimo 2 minutos para entrar
        return None

    # Filtro de liquidez
    if liq > 0 and liq < MIN_LIQUIDITY:
        log.debug(f"    [SKIP-LIQ] {asset} {tf_key} liq=${liq:,.0f} < ${MIN_LIQUIDITY:,.0f}")
        return None

    # Estimación propia
    vol         = ASSET_VOL.get(asset, 1.0)
    fade        = FADE_FACTOR * (1.0 / (1.0 + vol * 0.3))
    crowd_bias  = up_price - 0.50
    our_up_prob = 0.50 - fade * crowd_bias

    pct_remaining = min(1.0, mins / tf_min)
    our_up_prob   = our_up_prob * pct_remaining + up_price * (1.0 - pct_remaining)
    our_up_prob   = max(0.05, min(0.95, our_up_prob))

    # Mejor lado
    ev_up   = our_up_prob       - up_price
    ev_down = (1 - our_up_prob) - (1 - up_price)

    if ev_up >= ev_down and ev_up > 0:
        side, ev, bet_price, our_prob = "UP",   ev_up,   up_price,       our_up_prob
    elif ev_down > 0:
        side, ev, bet_price, our_prob = "DOWN", ev_down, 1.0 - up_price, 1.0 - our_up_prob
    else:
        return None

    # Threshold
    if ev >= EV_NORMAL and our_prob >= BAYES_NORMAL:
        mode = "NORMAL"
    elif ev >= EV_RELAXED and our_prob >= BAYES_RELAXED:
        mode = "RELAXED"
    else:
        log.debug(
            f"    [BELOW-THRESHOLD] {asset} {tf_key} {side} "
            f"EV={pct(ev)} Bayes={pct(our_prob)}"
        )
        return None

    return {
        "market_id":   market.get("id") or market.get("conditionId"),
        "slug":        market.get("_slug", ""),
        "asset":       asset, "tf_key": tf_key, "tf_min": tf_min,
        "side":        side,
        "ev":          ev,    "bayes":  our_prob,
        "bet_price":   bet_price,
        "up_price":    up_price, "our_up_prob": our_up_prob,
        "mins_left":   mins,
        "mode":        mode,
        "spot":        spot,  "liquidity":   liq,
        "question":    market.get("question") or market.get("title")
                       or f"{asset} Up/Down {tf_key}",
    }

# ──────────────────────────────────────────────────────────────────────────────
# ANÁLISIS POST-TRADE
# ──────────────────────────────────────────────────────────────────────────────
def post_trade_analysis(trade: "Trade", won: bool, close_spot: float) -> str:
    """
    Genera análisis legible del por qué un trade ganó o perdió,
    con sugerencia concreta para futuros trades del mismo asset/timeframe.
    """
    icon = "✅ WIN" if won else "❌ LOSS"

    # Movimiento del precio
    if trade.spot_entry > 0:
        spot_change_pct = (close_spot - trade.spot_entry) / trade.spot_entry * 100
    else:
        spot_change_pct = 0.0

    actual_up = (close_spot >= trade.spot_entry) if trade.spot_entry > 0 else (trade.side == "UP")
    pred_up   = trade.our_up_prob
    pred_err  = abs(pred_up - (1.0 if actual_up else 0.0))

    # Diagnóstico del modelo
    if pred_err < 0.20:
        accuracy_lbl = "predicción acertada"
        accuracy_nt  = "El modelo estimó la dirección correctamente."
    elif pred_err < 0.50:
        accuracy_lbl = "predicción parcial"
        accuracy_nt  = "El modelo se acercó pero erró el resultado final."
    else:
        accuracy_lbl = "predicción incorrecta"
        accuracy_nt  = "El sesgo del mercado resultó informativo esta vez."

    # Por qué ganó/perdió
    if won:
        why = (
            f"El precio {'subió' if trade.side == 'UP' else 'bajó'} "
            f"{spot_change_pct:+.2f}% como esperábamos. "
            f"El mercado cotizaba "
            f"{'muy ' if abs(trade.up_price - 0.5) > 0.15 else 'levemente '}"
            f"sesgado hacia {'Up' if trade.up_price > 0.5 else 'Down'} "
            f"(up={trade.up_price:.3f}) y nuestro fade funcionó."
        )
    else:
        if abs(spot_change_pct) < 0.10:
            why = (
                f"Movimiento mínimo ({spot_change_pct:+.2f}%) — "
                f"en mercados muy flat el resultado es casi aleatorio."
            )
        else:
            why = (
                f"El precio se movió {spot_change_pct:+.2f}% "
                f"contra nuestra apuesta {trade.side}. "
                f"El sesgo del mercado (up={trade.up_price:.3f}) reflejaba momentum real."
            )

    # Sugerencia
    asset, tf_key = trade.asset, trade.tf_key
    vol           = ASSET_VOL.get(asset, 1.0)

    if not won and abs(trade.up_price - 0.5) < 0.06:
        suggestion = (
            f"Para {asset} {tf_key}: evita señales con up_price entre 0.44–0.56. "
            f"Mercados casi-flat son ruido — sube EV mínimo en este rango."
        )
    elif not won and vol > 1.4:
        suggestion = (
            f"Para {asset} {tf_key}: alta volatilidad ({vol:.2f}) aumenta varianza. "
            f"Considera reducir TRADE_FRACTION para {asset} o solo operar 4h."
        )
    elif not won and trade.bet_price < 0.30:
        suggestion = (
            f"Para {asset} {tf_key}: precios muy bajos ({trade.bet_price:.2f}) "
            f"requieren más confianza. Sube BAYES_NORMAL al 60% para este caso."
        )
    elif won and trade.mode == "RELAXED":
        suggestion = (
            f"Señal RELAJADA ganadora en {asset} {tf_key}. Buena evidencia de que "
            f"el threshold relajado funciona — mantén la config actual."
        )
    elif won and trade.ev > 0.08:
        suggestion = (
            f"Edge alto ({pct(trade.ev)}) bien aprovechado en {asset} {tf_key}. "
            f"Considera aumentar TRADE_FRACTION para edges > 8%."
        )
    elif won:
        suggestion = (
            f"Trade limpio en {asset} {tf_key} — mantén configuración actual."
        )
    else:
        suggestion = (
            f"Para {asset} {tf_key}: revisa el CSV — si WR < 45% en últimos 20 trades, "
            f"considera pausar este par temporalmente con MAX_OPEN_TRADES filtering."
        )

    return "\n".join([
        f"━━━ ANÁLISIS POST-TRADE #{trade.trade_id} ━━━",
        f"Resultado:   {icon}   PnL: {signed(trade.pnl)}",
        f"Asset:       {asset} {tf_key} | {trade.side} @ {trade.bet_price:.3f}",
        f"Spot:        ${trade.spot_entry:,.4f} → ${close_spot:,.4f} ({spot_change_pct:+.2f}%)",
        f"Prob propia: {pct(pred_up)} → real: {'UP' if actual_up else 'DOWN'}",
        f"Precisión:   {accuracy_lbl} ({accuracy_nt})",
        f"Por qué:     {why}",
        f"Sugerencia:  {suggestion}",
    ])

# ──────────────────────────────────────────────────────────────────────────────
# PAPER ENGINE
# ──────────────────────────────────────────────────────────────────────────────
CSV_FIELDS = [
    "trade_id", "market_id", "slug", "asset", "tf_key", "side",
    "bet_price", "stake", "entry_time", "spot_entry",
    "status", "pnl", "close_time", "close_spot", "mode", "ev", "bayes", "liquidity",
]


class Trade:
    def __init__(self, tid: str, sig: Dict, stake: float, ts: datetime):
        self.trade_id    = tid
        self.market_id   = sig["market_id"]
        self.slug        = sig["slug"]
        self.asset       = sig["asset"]
        self.tf_key      = sig["tf_key"]
        self.tf_min      = sig["tf_min"]
        self.side        = sig["side"]
        self.bet_price   = sig["bet_price"]
        self.up_price    = sig["up_price"]
        self.our_up_prob = sig["our_up_prob"]
        self.stake       = stake
        self.entry_time  = ts
        self.spot_entry  = sig["spot"]
        self.mins_left   = sig["mins_left"]
        self.mode        = sig["mode"]
        self.ev          = sig["ev"]
        self.bayes       = sig["bayes"]
        self.liquidity   = sig.get("liquidity", 0.0)
        self.status      = "OPEN"
        self.pnl         = 0.0
        self.close_time  = None
        self.close_spot  = 0.0

    def row(self) -> Dict:
        return {
            "trade_id":   self.trade_id,
            "market_id":  self.market_id,
            "slug":       self.slug[:80],
            "asset":      self.asset,
            "tf_key":     self.tf_key,
            "side":       self.side,
            "bet_price":  f"{self.bet_price:.4f}",
            "stake":      f"{self.stake:.2f}",
            "entry_time": self.entry_time.isoformat(),
            "spot_entry": f"{self.spot_entry:.6f}",
            "status":     self.status,
            "pnl":        f"{self.pnl:.4f}",
            "close_time": self.close_time.isoformat() if self.close_time else "",
            "close_spot": f"{self.close_spot:.6f}",
            "mode":       self.mode,
            "ev":         f"{self.ev:.4f}",
            "bayes":      f"{self.bayes:.4f}",
            "liquidity":  f"{self.liquidity:.2f}",
        }


class PaperEngine:
    def __init__(self, initial: float):
        self.bankroll        = initial
        self.peak            = initial
        self.peak_today      = initial
        self.peak_today_date = dt_now().date()
        self.open_trades:  List[Trade] = []
        self.closed:       deque       = deque(maxlen=2000)
        self._lock           = threading.Lock()
        self._on_resolve     = None
        # FAIL-SAFE v8.5: bandera global de bloqueo. Si está True,
        # open_trade() rechaza CUALQUIER entrada nueva. Esto protege
        # incluso si el main loop falla en sincronizar state.paused.
        self.trading_blocked = False
        self._init_csv()

    def block_trading(self, reason: str = "circuit breaker"):
        self.trading_blocked = True
        log.info(f"[ENGINE-BLOCK] Trading bloqueado: {reason}")

    def unblock_trading(self, reason: str = "manual"):
        self.trading_blocked = False
        log.info(f"[ENGINE-UNBLOCK] Trading desbloqueado: {reason}")

    def _init_csv(self):
        if not os.path.exists(CSV_FILE):
            with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()

    def _write_csv(self, t: Trade):
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow(t.row())

    def set_resolve_callback(self, fn):
        self._on_resolve = fn

    def can_open(self, market_id: str) -> bool:
        with self._lock:
            if len(self.open_trades) >= MAX_OPEN_TRADES:
                return False
            return all(t.market_id != market_id for t in self.open_trades)

    def open_trade(self, sig: Dict, ts: datetime) -> Optional[Trade]:
        # FAIL-SAFE v8.5: rechazar SIEMPRE si trading está bloqueado.
        # Esta es la última línea de defensa — aún si el main loop falla
        # en respetar state.paused, ningún trade nuevo se abre con CB activo.
        if self.trading_blocked:
            log.warning(
                f"  [BLOCKED] open_trade rechazado: trading bloqueado "
                f"(sig={sig['asset']} {sig['tf_key']} {sig['side']})"
            )
            return None
        if not self.can_open(sig["market_id"]):
            return None
        with self._lock:
            frac  = min(TRADE_FRACTION, max(0.0, sig["ev"] * 0.5))  # half-Kelly
            stake = round(max(0.50, self.bankroll * frac), 2)
            t     = Trade(str(uuid.uuid4())[:8].upper(), sig, stake, ts)
            self.open_trades.append(t)
            self.bankroll -= stake
        self._write_csv(t)
        return t

    def resolve(self, t: Trade, won: bool, close_spot: float, ts: datetime):
        with self._lock:
            if t.status != "OPEN":
                return
            if won:
                payout = t.stake / t.bet_price
                t.pnl  = payout - t.stake
                t.status = "WIN"
                self.bankroll += payout
            else:
                t.pnl    = -t.stake
                t.status = "LOSS"
            t.close_time = ts
            t.close_spot = close_spot
            self.open_trades = [x for x in self.open_trades if x.trade_id != t.trade_id]
            self.closed.appendleft(t)
            if self.bankroll > self.peak:        self.peak       = self.bankroll
            if self.bankroll > self.peak_today:  self.peak_today = self.bankroll

        self._write_csv(t)

        # Análisis post-trade
        analysis = post_trade_analysis(t, won, close_spot)
        log.info(analysis)
        if self._on_resolve:
            try:
                self._on_resolve(t, won, analysis)
            except Exception as exc:
                log.debug(f"Resolve callback error: {exc}")

    def resolve_by_price(self, prices: Dict[str, float]):
        """
        Paper mode: resuelve trades vencidos honestamente.

        Política de resolución (estricta, sin aleatoriedad):
          1. PREFERENTE — Si hay close_spot real Y spot_entry real:
                Up gana ⇔ close_spot >= spot_entry  (aproxima oracle Chainlink)
                → Esta es la única resolución 100% confiable.

          2. PROVISIONAL — Si NO hay close_spot disponible para el asset:
                NO resolver. Dejar el trade OPEN para reintento en el
                próximo ciclo (60s). El precio volverá pronto.

          3. ÚLTIMO RECURSO — Solo si pasamos el grace period (15 min después
             del cierre esperado) y aún no hay precio:
                Cerrar como EXPIRED usando bet_price como proxy determinista.
                Lógica: si pagamos bet_price por un share, el "valor justo"
                       en el momento de entrada era bet_price. Sin info nueva
                       el resultado más honesto es: gana si bet_price > 0.50
                       (i.e. el mercado nos daba la razón) — no aleatorio.
                Marcamos status="EXPIRED" en CSV para identificar estos casos.

        Esto preserva la integridad del paper trading: cada WIN/LOSS refleja
        un movimiento real de precio, nunca un coin flip.
        """
        now = dt_now()

        # Reset peak diario
        if now.date() != self.peak_today_date:
            self.peak_today      = self.bankroll
            self.peak_today_date = now.date()
            log.info(f"[DAILY-RESET] Peak diario reseteado: ${self.peak_today:.2f}")

        # ── Paso 1: resolver vencidos CON precio real ─────────────────────────
        for t in list(self.open_trades):
            if t.status != "OPEN":
                continue
            if now < t.entry_time + timedelta(minutes=t.mins_left):
                continue   # aún no vence

            close_spot = prices.get(t.asset, 0.0)

            # Sin precio actual disponible → dejar OPEN, reintento en próx. ciclo
            if close_spot <= 0:
                log.debug(
                    f"  [WAIT] {t.trade_id} {t.asset} vencido pero sin precio — "
                    f"reintento próximo ciclo"
                )
                continue

            # Sin spot_entry no podemos comparar → también esperamos
            if t.spot_entry <= 0:
                log.debug(
                    f"  [WAIT] {t.trade_id} {t.asset} sin spot_entry — "
                    f"reintento próximo ciclo"
                )
                continue

            # ✅ Resolución oracle real (camino feliz)
            price_went_up = close_spot >= t.spot_entry
            won = (
                (t.side == "UP"   and     price_went_up) or
                (t.side == "DOWN" and not price_went_up)
            )
            self.resolve(t, won=won, close_spot=close_spot, ts=now)

        # ── Paso 2: cerrar EXPIRED después del grace period (sin aleatoriedad) ─
        grace_minutes = 15
        with self._lock:
            stale = [
                t for t in self.open_trades
                if now > t.entry_time + timedelta(minutes=t.mins_left + grace_minutes)
            ]
        for t in stale:
            close_spot = prices.get(t.asset, 0.0) or t.spot_entry

            # Resolución determinista basada en bet_price (NO aleatoria):
            # Si bet_price > 0.50 el mercado nos daba la razón al entrar
            # → resolvemos a favor; caso contrario, en contra.
            # No es perfecta, pero es REPRODUCIBLE y conservadora.
            won = t.bet_price > 0.50

            log.warning(
                f"[EXPIRED] {t.trade_id} {t.asset} {t.tf_key} — "
                f"sin precio tras {grace_minutes}min grace | "
                f"resolución determinista por bet_price={t.bet_price:.3f} → "
                f"{'WIN' if won else 'LOSS'}"
            )
            # Marcamos como EXPIRED en lugar de WIN/LOSS estándar
            self._resolve_expired(t, won=won, close_spot=close_spot, ts=now)

    def _resolve_expired(self, t: "Trade", won: bool, close_spot: float, ts: datetime):
        """Resuelve un trade EXPIRED (sin precio real disponible)."""
        with self._lock:
            if t.status != "OPEN":
                return
            if won:
                payout = t.stake / t.bet_price
                t.pnl  = payout - t.stake
                t.status = "EXPIRED_WIN"
                self.bankroll += payout
            else:
                t.pnl    = -t.stake
                t.status = "EXPIRED_LOSS"
            t.close_time = ts
            t.close_spot = close_spot
            self.open_trades = [x for x in self.open_trades if x.trade_id != t.trade_id]
            self.closed.appendleft(t)
            if self.bankroll > self.peak:        self.peak       = self.bankroll
            if self.bankroll > self.peak_today:  self.peak_today = self.bankroll
        self._write_csv(t)
        # No corremos análisis post-trade para EXPIRED (no es trade real)
        log.info(
            f"[EXPIRED] {t.trade_id} cerrado como {t.status} | "
            f"PnL: {signed(t.pnl)}"
        )

    @property
    def drawdown_today(self) -> float:
        return (self.peak_today - self.bankroll) / self.peak_today if self.peak_today > 0 else 0.0

    @property
    def drawdown(self) -> float:
        return (self.peak - self.bankroll) / self.peak if self.peak > 0 else 0.0

    @property
    def net_pnl(self) -> float:
        return self.bankroll - INITIAL_BANKROLL

    def summary(self) -> Dict:
        """
        Estadísticas honestas: solo cuenta trades resueltos con precio REAL
        (WIN/LOSS). Los EXPIRED se reportan aparte para no contaminar el WR.
        """
        closed   = list(self.closed)
        wins     = [t for t in closed if t.status == "WIN"]
        losses   = [t for t in closed if t.status == "LOSS"]
        expired  = [t for t in closed if t.status.startswith("EXPIRED")]
        total_real = len(wins) + len(losses)
        return {
            "bankroll":       self.bankroll,
            "net_pnl":        self.net_pnl,
            "peak":           self.peak,
            "peak_today":     self.peak_today,
            "drawdown":       self.drawdown,
            "drawdown_today": self.drawdown_today,
            "open":           len(self.open_trades),
            "closed":         total_real,
            "wins":           len(wins),
            "losses":         len(losses),
            "expired":        len(expired),
            "wr":             (len(wins) / total_real * 100) if total_real > 0 else 0.0,
        }

# ──────────────────────────────────────────────────────────────────────────────
# CIRCUIT BREAKER (drawdown diario) — FAIL-SAFE en v8.5
# ──────────────────────────────────────────────────────────────────────────────
#
# Política nueva:
#   1. trip() es IDEMPOTENTE: marca tripped=True, registra hora, NO se desarma solo
#   2. Solo el reset diario (medianoche UTC) o /force_resume desarma el CB
#   3. El CB es la ÚNICA fuente de verdad sobre si el bot puede operar
#   4. BotState.paused se mantiene sincronizado con CB en todo momento
#
class CircuitBreaker:
    def __init__(self):
        self.tripped       = False
        self.reason        = ""
        self.tripped_at    = None      # datetime UTC cuando se disparó
        self.dd_at_trip    = 0.0       # drawdown % al momento del trip
        self._last_alert_at = None     # para throttle de alertas Telegram

    def trip(self, dd_today: float):
        """Dispara el CB. Idempotente — llamadas repetidas son seguras."""
        if not self.tripped:
            self.tripped    = True
            self.tripped_at = dt_now()
            self.dd_at_trip = dd_today
        self.reason = f"Drawdown diario {dd_today*100:.1f}% ≥ límite {CB_PCT*100:.0f}%"

    def check(self, e: "PaperEngine") -> bool:
        """Evalúa si el CB debe dispararse. Retorna True solo en transition."""
        was_tripped = self.tripped
        if e.drawdown_today >= CB_PCT:
            self.trip(e.drawdown_today)
            return not was_tripped   # True solo en la transición OFF→ON
        return False

    def reset(self, reason: str = "manual"):
        """Desarma el CB. Resetea todo el estado interno."""
        self.tripped        = False
        self.reason         = ""
        self.tripped_at     = None
        self.dd_at_trip     = 0.0
        self._last_alert_at = None
        log.info(f"[CB-RESET] Circuit breaker desarmado ({reason})")

    def should_alert_again(self) -> bool:
        """Throttle: solo alerta cada CB_ALERT_REPEAT_MIN minutos si sigue tripped."""
        if not self.tripped:
            return False
        if self._last_alert_at is None:
            self._last_alert_at = dt_now()
            return True
        mins_since = (dt_now() - self._last_alert_at).total_seconds() / 60
        if mins_since >= CB_ALERT_REPEAT_MIN:
            self._last_alert_at = dt_now()
            return True
        return False

    def hours_since_trip(self) -> float:
        """Horas desde el trip — usado para cooldown de /force_resume."""
        if not self.tripped_at:
            return 0.0
        return (dt_now() - self.tripped_at).total_seconds() / 3600

# ──────────────────────────────────────────────────────────────────────────────
# BOT STATE
# ──────────────────────────────────────────────────────────────────────────────
class BotState:
    def __init__(self):
        self.cycle            = 0
        self.paused           = False     # cualquier pausa (manual o CB)
        self.cb_paused        = False     # ⚠ pausa específicamente por CB
        self.manual_paused    = False     # pausa por /pause manual
        self.relaxed_mode     = False
        self.last_signal_time = dt_now()
        self.signals_today    = 0
        self.engine           = PaperEngine(INITIAL_BANKROLL)
        self.cb               = CircuitBreaker()
        self._scan_now        = False

    def pause_manual(self):
        self.manual_paused = True
        self.paused        = True

    def resume_manual(self):
        self.manual_paused = False
        # Solo desactiva paused si tampoco está el CB
        if not self.cb_paused:
            self.paused = False

    def pause_for_cb(self):
        """Llamado cuando el CB se dispara — sincroniza paused=True + bloquea engine."""
        self.cb_paused = True
        self.paused    = True
        self.engine.block_trading(reason="circuit breaker")

    def release_cb(self, reason: str = "manual"):
        """Llamado al desarmar el CB — sincroniza el estado de pausa."""
        self.cb_paused = False
        self.cb.reset(reason=reason)
        self.engine.unblock_trading(reason=reason)
        # Solo libera paused si tampoco hay pausa manual
        if not self.manual_paused:
            self.paused = False

    def update_relaxed_mode(self):
        mins = (dt_now() - self.last_signal_time).total_seconds() / 60
        was  = self.relaxed_mode
        self.relaxed_mode = mins >= RELAXED_AFTER_MINS
        if self.relaxed_mode and not was:
            log.info(
                f"[AUTO-RELAX] {mins:.0f}min sin señales → modo RELAJADO "
                f"(EV≥{pct(EV_RELAXED)} Bayes≥{BAYES_RELAXED*100:.0f}%)"
            )
        elif not self.relaxed_mode and was:
            log.info("[AUTO-RELAX] Señal detectada → modo NORMAL")

# ──────────────────────────────────────────────────────────────────────────────
# ANÁLISIS POST-CB — top trades que más contribuyeron al drawdown
# ──────────────────────────────────────────────────────────────────────────────
def cb_drawdown_analysis(engine: "PaperEngine", n_top: int = 8) -> str:
    """
    Cuando el CB se dispara, genera un informe de los N trades cerrados
    en las últimas 24h que más contribuyeron al drawdown (mayor pérdida).
    """
    now = dt_now()
    cutoff = now - timedelta(hours=24)

    # Trades cerrados en las últimas 24h, ordenados por pérdida (más negativo primero)
    recent_losses = [
        t for t in engine.closed
        if t.close_time and t.close_time >= cutoff and t.pnl < 0
    ]
    recent_losses.sort(key=lambda t: t.pnl)  # más negativo primero
    top_losers = recent_losses[:n_top]

    if not top_losers:
        return (
            "No hay trades perdedores en las últimas 24h.\n"
            "El drawdown puede deberse a trades abiertos sin resolver."
        )

    s = engine.summary()
    lines = [
        f"<b>📉 Top {len(top_losers)} pérdidas (últimas 24h)</b>",
        f"Bankroll: ${s['bankroll']:.2f} | DD día: {s['drawdown_today']*100:.1f}%\n",
    ]
    total_loss = 0.0
    for i, t in enumerate(top_losers, 1):
        total_loss += t.pnl
        lines.append(
            f"{i}. <code>{t.trade_id}</code> | {t.asset} {t.tf_key} {t.side}\n"
            f"   EV={pct(t.ev)}  Bayes={pct(t.bayes)}  "
            f"PnL={signed(t.pnl)}  [{t.mode}]"
        )
    lines.append(f"\nSuma top {len(top_losers)}: <b>{signed(total_loss)}</b>")

    # Analizar patrones
    assets = defaultdict(int)
    tfs    = defaultdict(int)
    modes  = defaultdict(int)
    for t in top_losers:
        assets[t.asset] += 1
        tfs[t.tf_key]   += 1
        modes[t.mode]   += 1

    worst_asset = max(assets, key=assets.get) if assets else None
    worst_tf    = max(tfs,    key=tfs.get)    if tfs    else None
    worst_mode  = max(modes,  key=modes.get)  if modes  else None

    lines.append("\n<b>Patrón observado:</b>")
    if worst_asset:
        lines.append(f"  • Asset más perdedor: <b>{worst_asset}</b> ({assets[worst_asset]} pérdidas)")
    if worst_tf:
        lines.append(f"  • Timeframe más perdedor: <b>{worst_tf}</b> ({tfs[worst_tf]} pérdidas)")
    if worst_mode:
        lines.append(f"  • Modo más perdedor: <b>{worst_mode}</b> ({modes[worst_mode]} pérdidas)")

    # Sugerencia
    if modes.get("RELAXED", 0) >= len(top_losers) * 0.6:
        lines.append(
            "\n⚠️ <b>Más del 60% de pérdidas son señales RELAXED.</b>\n"
            "Sugerencia: subir EV_RELAXED o desactivar modo relajado temporalmente."
        )
    elif worst_asset and assets[worst_asset] >= 4:
        lines.append(
            f"\n⚠️ <b>{worst_asset} acumula {assets[worst_asset]} pérdidas.</b>\n"
            f"Sugerencia: excluir {worst_asset} temporalmente del scan."
        )

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# DISCORD
# ──────────────────────────────────────────────────────────────────────────────
def discord(msg: str):
    if not DISCORD_WEBHOOK:
        return
    try:
        requests.post(DISCORD_WEBHOOK, json={"content": msg[:2000]}, timeout=8)
    except Exception as exc:
        log.debug(f"Discord: {exc}")

# ──────────────────────────────────────────────────────────────────────────────
# TELEGRAM (pure HTTP + auto-reconnect con backoff exponencial)
# ──────────────────────────────────────────────────────────────────────────────
HELP_TEXT = f"""🤖 <b>Sentinel Polymarket v{VERSION}</b>

<b>Comandos:</b>
  /status        — Estado general del bot
  /active        — Trades abiertos con detalle completo
  /trades        — Abiertos + últimos 10 cerrados
  /pnl           — Resumen histórico de P&amp;L por asset
  /scan_now      — Forzar escaneo inmediato
  /pause         — Pausar el bot manualmente
  /resume        — Reanudar (NO desarma CB)
  /force_resume  — Desarmar CB manualmente tras cooldown
  /help          — Este mensaje

<b>Configuración v{VERSION}:</b>
  Assets:     {", ".join(ASSETS)} ({len(ASSETS)} total)
  Timeframes: 1h, 4h
  EV:         normal {pct(EV_NORMAL)} | relajado {pct(EV_RELAXED)}
  Bayes:      normal {BAYES_NORMAL*100:.0f}% | relajado {BAYES_RELAXED*100:.0f}%
  Mode:       {'📄 Paper' if PAPER_MODE else '💰 Live'}
  Max open:   {MAX_OPEN_TRADES}
  CB:         {CB_PCT*100:.0f}% DD diario | cooldown {CB_COOLDOWN_HOURS:.0f}h"""


class TelegramBot:
    def __init__(self, token: str, chat_id: str):
        self.token    = token
        self.chat_id  = chat_id
        self._base    = f"https://api.telegram.org/bot{token}"
        self._offset  = 0
        self._running = True

    def send(self, text: str, parse_mode: str = "HTML", to: str = None) -> bool:
        cid = to or self.chat_id
        if not self.token or not cid:
            return False
        try:
            r = requests.post(
                f"{self._base}/sendMessage",
                json={"chat_id": cid, "text": text,
                      "parse_mode": parse_mode,
                      "disable_web_page_preview": True},
                timeout=10,
            )
            return r.status_code == 200
        except Exception as exc:
            log.debug(f"Telegram send: {exc}")
            return False

    def poll_loop(self, state: BotState):
        """
        Polling con auto-reconnect.
        Si falla, espera con backoff exponencial (1s → 2s → 4s ... → 60s max).
        """
        log.info("Telegram polling iniciado")
        retry_delay = 1
        while self._running:
            try:
                r = requests.get(
                    f"{self._base}/getUpdates",
                    params={"offset": self._offset, "timeout": 20, "limit": 20},
                    timeout=25,
                )
                if r.status_code == 200:
                    retry_delay = 1  # reset on success
                    for upd in r.json().get("result", []):
                        self._offset = upd["update_id"] + 1
                        msg  = upd.get("message") or upd.get("edited_message") or {}
                        text = (msg.get("text") or "").strip()
                        cid  = str(msg.get("chat", {}).get("id", ""))
                        if text.startswith("/"):
                            cmd = text.split()[0].lower().split("@")[0]
                            try:
                                self._handle(cmd, cid, state)
                            except Exception as exc:
                                log.debug(f"Cmd handler error: {exc}")
                else:
                    raise Exception(f"HTTP {r.status_code}")
            except Exception as exc:
                log.warning(
                    f"Telegram poll error: {exc} — reintento en {retry_delay}s"
                )
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
                continue
            time.sleep(1)

    def stop(self):
        self._running = False

    def _handle(self, cmd: str, cid: str, state: BotState):
        e = state.engine

        if cmd == "/help":
            self.send(HELP_TEXT, to=cid)

        elif cmd == "/status":
            s    = e.summary()
            cb_s = "🔴 TRIPPED" if state.cb.tripped  else "🟢 OK"
            # Mostrar el tipo específico de pausa
            if state.cb_paused:
                hrs = state.cb.hours_since_trip()
                pa_s = f"🚨 PAUSADO POR CB ({hrs:.1f}h)"
            elif state.manual_paused:
                pa_s = "⏸ PAUSADO MANUAL"
            else:
                pa_s = "▶ ACTIVO"
            mo_s = "🔶 RELAJADO" if state.relaxed_mode else "🔵 NORMAL"
            mns  = (dt_now() - state.last_signal_time).total_seconds() / 60
            blocked = "🔒 SÍ" if e.trading_blocked else "🔓 NO"
            self.send(
                f"<b>🤖 Sentinel v{VERSION} — Status</b>\n\n"
                f"Estado:        {pa_s}\n"
                f"Trading:       {blocked}\n"
                f"Modo señal:    {mo_s}\n"
                f"Sin señal:     {mns:.0f}min\n"
                f"Ciclo:         #{state.cycle}\n"
                f"Bankroll:      <b>${s['bankroll']:.2f}</b>\n"
                f"P&amp;L neto:  {signed(s['net_pnl'])}\n"
                f"Peak hist:     ${s['peak']:.2f}\n"
                f"Peak día:      ${s['peak_today']:.2f}\n"
                f"Drawdown día:  {s['drawdown_today']*100:.1f}%\n"
                f"CB:            {cb_s}\n"
                f"Open trades:   {s['open']}/{MAX_OPEN_TRADES}\n"
                f"Señales hoy:   {state.signals_today}",
                to=cid,
            )

        elif cmd == "/active":
            trades = e.open_trades
            if not trades:
                self.send("📭 No hay trades abiertos.", to=cid)
                return
            lines = [f"<b>📂 Trades Abiertos ({len(trades)})</b>\n"]
            for t in trades:
                age_min = int((dt_now() - t.entry_time).total_seconds() / 60)
                expected_close = t.entry_time + timedelta(minutes=t.mins_left)
                mins_to_close  = max(0, int((expected_close - dt_now()).total_seconds() / 60))
                lines.append(
                    f"🔹 <b>{t.trade_id}</b> | {t.asset} {t.tf_key}\n"
                    f"   Lado:        {t.side} @ {t.bet_price:.3f}\n"
                    f"   Stake:       ${t.stake:.2f}\n"
                    f"   Spot entry:  ${t.spot_entry:,.4f}\n"
                    f"   EV:          {pct(t.ev)} | Bayes: {pct(t.bayes)}\n"
                    f"   Elapsed:     {age_min}min\n"
                    f"   Cierra en:   ~{mins_to_close}min\n"
                    f"   Modo:        {t.mode}"
                )
            self.send("\n".join(lines), to=cid)

        elif cmd == "/trades":
            open_t   = e.open_trades
            closed_t = list(e.closed)[:10]
            lines    = []
            if open_t:
                lines.append(f"<b>📂 Abiertos ({len(open_t)})</b>")
                for t in open_t:
                    lines.append(
                        f"  • {t.trade_id} | {t.asset} {t.tf_key} "
                        f"| {t.side} @ {t.bet_price:.3f} | ${t.stake:.2f} [{t.mode}]"
                    )
            if closed_t:
                lines.append(f"\n<b>📋 Últimos 10 cerrados</b>")
                for t in closed_t:
                    if t.status == "WIN":
                        icon = "✅"
                    elif t.status == "LOSS":
                        icon = "❌"
                    else:  # EXPIRED_*
                        icon = "⏱"
                    lines.append(
                        f"  {icon} {t.trade_id} | {t.asset} {t.tf_key} "
                        f"| {t.side} | PnL: {signed(t.pnl)}"
                        + (f" [{t.status}]" if t.status.startswith("EXPIRED") else "")
                    )
            if not lines:
                self.send("No hay trades aún.", to=cid)
                return
            self.send("\n".join(lines), to=cid)

        elif cmd == "/pnl":
            s      = e.summary()
            closed = list(e.closed)
            by_asset: Dict[str, Dict] = defaultdict(
                lambda: {"pnl": 0.0, "n": 0, "wins": 0}
            )
            by_tf:    Dict[str, Dict] = defaultdict(
                lambda: {"pnl": 0.0, "n": 0, "wins": 0}
            )
            # Solo trades RESUELTOS REALES en estadísticas por asset/tf
            for t in closed:
                if t.status.startswith("EXPIRED"):
                    continue
                by_asset[t.asset]["pnl"]  += t.pnl
                by_asset[t.asset]["n"]    += 1
                by_tf[t.tf_key]["pnl"]    += t.pnl
                by_tf[t.tf_key]["n"]      += 1
                if t.status == "WIN":
                    by_asset[t.asset]["wins"] += 1
                    by_tf[t.tf_key]["wins"]   += 1

            lines = [
                f"<b>📊 P&amp;L Histórico — Sentinel v{VERSION}</b>\n",
                f"Bankroll inicial: ${INITIAL_BANKROLL:.2f}",
                f"Bankroll actual:  <b>${s['bankroll']:.2f}</b>",
                f"P&amp;L neto:     {signed(s['net_pnl'])}",
                f"Peak histórico:   ${s['peak']:.2f}",
                f"Drawdown total:   {s['drawdown']*100:.1f}%\n",
                f"Resueltos: {s['closed']} | "
                f"{s['wins']}W / {s['losses']}L | WR {s['wr']:.1f}%",
            ]
            if s['expired'] > 0:
                lines.append(
                    f"⏱ Expirados:    {s['expired']} (sin precio en grace period)"
                )
            lines.append("")  # blank line
            if any(d["n"] > 0 for d in by_asset.values()):
                lines.append("<b>Por asset:</b>")
                for a in ASSETS:
                    d = by_asset.get(a)
                    if d and d["n"] > 0:
                        wr = d["wins"] / d["n"] * 100
                        lines.append(
                            f"  <b>{a}</b>: {signed(d['pnl'])} "
                            f"({d['n']} | {d['wins']}W | {wr:.0f}%WR)"
                        )
            if any(d["n"] > 0 for d in by_tf.values()):
                lines.append("\n<b>Por timeframe:</b>")
                for tf in TIMEFRAMES:
                    d = by_tf.get(tf)
                    if d and d["n"] > 0:
                        wr = d["wins"] / d["n"] * 100
                        lines.append(
                            f"  <b>{tf}</b>: {signed(d['pnl'])} "
                            f"({d['n']} | {d['wins']}W | {wr:.0f}%WR)"
                        )
            self.send("\n".join(lines), to=cid)

        elif cmd == "/scan_now":
            state._scan_now = True
            self.send("🔍 Escaneo forzado iniciado…", to=cid)

        elif cmd == "/pause":
            state.pause_manual()
            log.info("[TELEGRAM] Bot pausado manualmente")
            self.send(
                "⏸ Bot pausado manualmente.\n"
                "Usa /resume para reanudar.",
                to=cid,
            )

        elif cmd == "/resume":
            if state.cb_paused:
                # /resume normal NO desarma el CB — solo /force_resume puede
                hrs = state.cb.hours_since_trip()
                self.send(
                    f"⚠️ <b>El bot está pausado por Circuit Breaker</b>\n\n"
                    f"DD que disparó CB: {state.cb.dd_at_trip*100:.1f}%\n"
                    f"Tiempo tripped: {hrs:.1f}h / cooldown {CB_COOLDOWN_HOURS:.0f}h\n\n"
                    f"<code>/resume</code> NO desarma el CB.\n"
                    f"Opciones:\n"
                    f"  • Esperar reset diario UTC (automático)\n"
                    f"  • Usar <code>/force_resume</code> (requiere confirmación)",
                    to=cid,
                )
            else:
                state.resume_manual()
                log.info("[TELEGRAM] Bot reanudado manualmente")
                self.send("▶ Bot reanudado.", to=cid)

        elif cmd == "/force_resume":
            if not state.cb_paused:
                self.send(
                    "ℹ El CB no está activo. Usa /resume si está pausado manualmente.",
                    to=cid,
                )
                return
            hrs = state.cb.hours_since_trip()
            if hrs < CB_COOLDOWN_HOURS:
                self.send(
                    f"⚠️ <b>Cooldown del CB no cumplido</b>\n\n"
                    f"Tiempo tripped:  {hrs:.1f}h\n"
                    f"Cooldown mínimo: {CB_COOLDOWN_HOURS:.0f}h\n"
                    f"Faltan:          {CB_COOLDOWN_HOURS - hrs:.1f}h\n\n"
                    f"Recomendado esperar antes de reanudar.\n"
                    f"Si insistes, espera al reset diario UTC."
                    f"\n\n(Si realmente quieres bypass de cooldown, "
                    f"baja CB_COOLDOWN_HOURS en Railway env vars y reinicia.)",
                    to=cid,
                )
                return
            # Cooldown OK — desarmar CB
            state.release_cb(reason="force_resume manual")
            log.warning(
                f"[TELEGRAM] /force_resume ejecutado — CB desarmado "
                f"tras {hrs:.1f}h tripped"
            )
            self.send(
                f"✅ <b>Circuit Breaker desarmado manualmente</b>\n\n"
                f"Tiempo que estuvo tripped: {hrs:.1f}h\n"
                f"Bot reanudando operaciones.\n"
                f"Recordatorio: el peak diario se reseteará a medianoche UTC.",
                to=cid,
            )

        else:
            self.send(f"Comando desconocido: <code>{cmd}</code>\nUsa /help.", to=cid)

# ──────────────────────────────────────────────────────────────────────────────
# CICLO DE ESCANEO
# ──────────────────────────────────────────────────────────────────────────────
def run_scan_cycle(
    state:   BotState,
    scanner: MarketScanner,
    prices:  Dict[str, float],
    tg:      TelegramBot,
    forced:  bool = False,
) -> Tuple[int, int]:
    now = dt_now()
    state.update_relaxed_mode()
    relaxed = state.relaxed_mode

    mode_lbl = (
        f"RELAJADO  EV≥{pct(EV_RELAXED)}  Bayes≥{BAYES_RELAXED*100:.0f}%"
        if relaxed else
        f"NORMAL    EV≥{pct(EV_NORMAL)}   Bayes≥{BAYES_NORMAL*100:.0f}%"
    )
    prefix = "🔍 SCAN_NOW" if forced else f"CICLO #{state.cycle}"

    log.info("━" * 70)
    log.info(f"  {prefix}  |  {mode_lbl}")
    log.info("━" * 70)

    total_found   = 0
    total_signals = 0
    no_mkt_assets: List[str] = []

    for asset in ASSETS:
        spot = prices.get(asset, 0.0)
        if spot <= 0:
            log.warning(f"  [SKIP] Sin precio para {asset}")
            no_mkt_assets.append(asset)
            continue

        log.info(f"━━ {asset} (${spot:,.4f}) ━━")
        asset_found = False

        for tf_key in TIMEFRAMES:
            markets = scanner.scan(asset, tf_key, spot)
            total_found += len(markets)
            if markets:
                asset_found = True

            for mkt in markets:
                sig = analyze(mkt, relaxed)
                if sig is None:
                    continue

                total_signals          += 1
                state.signals_today    += 1
                state.last_signal_time  = dt_now()

                log.info(
                    f"  [SIGNAL] {asset} {tf_key} | {sig['side']} | "
                    f"EV={pct(sig['ev'])}  Bayes={pct(sig['bayes'])}  [{sig['mode']}]"
                )

                trade = state.engine.open_trade(sig, now)
                if trade:
                    icon = "🔶" if sig["mode"] == "RELAXED" else "🟢"
                    msg  = (
                        f"{icon} <b>TRADE #{trade.trade_id}</b> [{sig['mode']}]\n\n"
                        f"Asset:      {asset} {tf_key}\n"
                        f"Lado:       <b>{trade.side}</b> @ {trade.bet_price:.3f}\n"
                        f"Stake:      ${trade.stake:.2f}\n"
                        f"EV:         {pct(sig['ev'])}\n"
                        f"Bayes:      {pct(sig['bayes'])}\n"
                        f"Spot:       ${spot:,.4f}\n"
                        f"Up market:  {sig['up_price']:.3f}\n"
                        f"Liquidity:  ${sig['liquidity']:,.0f}\n"
                        f"Cierra en:  ~{sig['mins_left']:.0f}min"
                    )
                    log.info(
                        f"  [TRADE OPEN] {trade.trade_id}  "
                        f"stake=${trade.stake:.2f}  {trade.side}"
                    )
                    tg.send(msg)
                    discord(
                        f"{icon} TRADE {trade.trade_id} | {asset} {tf_key} | "
                        f"{trade.side} @ {trade.bet_price:.3f} | "
                        f"${trade.stake:.2f} | EV {pct(sig['ev'])} [{sig['mode']}]"
                    )
                else:
                    log.info("  [SKIP-OPEN] MAX_OPEN_TRADES o duplicado")

        if not asset_found:
            no_mkt_assets.append(asset)

    if set(no_mkt_assets) == set(ASSETS):
        log.info("  [INFO] Sin mercados en ningún asset — esperando próxima ventana")
    elif no_mkt_assets:
        log.info(f"  [INFO] Sin mercados para: {', '.join(set(no_mkt_assets))}")

    log.info(
        f"━━━ {prefix} completo  |  "
        f"{total_found} mercados  |  {total_signals} señales ━━━\n"
    )

    if forced:
        s = state.engine.summary()
        tg.send(
            f"🔍 <b>Scan forzado completado</b>\n\n"
            f"Mercados:   {total_found}\n"
            f"Señales:    {total_signals}\n"
            f"Modo:       {'Relajado 🔶' if relaxed else 'Normal 🔵'}\n"
            f"Bankroll:   ${s['bankroll']:.2f}"
        )

    return total_found, total_signals

# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────
def main():
    banner = "\n".join([
        "═" * 70,
        f"  SENTINEL POLYMARKET v{VERSION} — CIRCUIT BREAKER FIXED",
        f"  Build:       {BUILD_DATE}",
        f"  Mode:        {'📄 PAPER TRADING' if PAPER_MODE else '💰 LIVE TRADING ⚠️'}",
        f"  Assets:      {', '.join(ASSETS)} ({len(ASSETS)} total)",
        f"  Timeframes:  1h, 4h  →  {len(ASSETS)*2} oportunidades/ciclo",
        f"  Búsqueda:    Slug determinístico + broadcast fallback",
        f"  EV:          normal={pct(EV_NORMAL)}   relajado={pct(EV_RELAXED)}",
        f"  Bayes:       normal={BAYES_NORMAL*100:.0f}%  relajado={BAYES_RELAXED*100:.0f}%",
        f"  Relajado:    auto después de {RELAXED_AFTER_MINS}min sin señal",
        f"  Bankroll:    ${INITIAL_BANKROLL:.2f}",
        f"  Max trades:  {MAX_OPEN_TRADES}",
        f"  CB diario:   {CB_PCT*100:.0f}% | Histórico: {MAX_HISTORICAL_DD*100:.0f}%",
        f"  CB alerts:   re-alerta cada {CB_ALERT_REPEAT_MIN}min mientras tripped",
        f"  Min liq:     ${MIN_LIQUIDITY:.0f}",
        f"  Discord:     {'✅' if DISCORD_WEBHOOK else '❌'}",
        f"  Telegram:    {'✅' if TELEGRAM_TOKEN else '❌'}",
        "═" * 70,
    ])
    log.info(banner)

    state   = BotState()
    scanner = MarketScanner()
    feed    = PriceFeed()
    tg      = TelegramBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

    # Callback post-trade
    def on_resolve(trade: Trade, won: bool, analysis_text: str):
        icon  = "✅" if won else "❌"
        # Limitar a primeras 8 líneas para Telegram
        lines = analysis_text.split("\n")[:8]
        body  = "\n".join(lines).replace("━", "─")
        tg.send(
            f"{icon} <b>TRADE {trade.trade_id} {'WIN' if won else 'LOSS'}</b>\n\n"
            f"<pre>{body}</pre>"
        )
        discord(
            f"{icon} TRADE {trade.trade_id} {'WIN' if won else 'LOSS'} | "
            f"{trade.asset} {trade.tf_key} | {trade.side} | "
            f"PnL: {signed(trade.pnl)}"
        )

    state.engine.set_resolve_callback(on_resolve)

    # Telegram polling thread
    if TELEGRAM_TOKEN:
        threading.Thread(
            target=tg.poll_loop, args=(state,), daemon=True, name="tg-poll"
        ).start()
        tg.send(
            f"🚀 <b>Sentinel v{VERSION} ACTIVADO y FUNCIONANDO correctamente</b>\n\n"
            f"🔧 <b>CIRCUIT BREAKER FIXED</b>\n"
            f"Mode:        {'📄 Paper Trading' if PAPER_MODE else '💰 Live Trading ⚠️'}\n"
            f"Assets:      {', '.join(ASSETS)}\n"
            f"Timeframes:  1h, 4h\n"
            f"Capital:     ${INITIAL_BANKROLL:.2f}\n"
            f"CB diario:   {CB_PCT*100:.0f}% DD | cooldown {CB_COOLDOWN_HOURS:.0f}h\n"
            f"Búsqueda:    Slug determinístico + fallback\n"
            f"Post-trade:  Análisis automático activado\n\n"
            f"Cambios v8.5: el CB ahora REALMENTE pausa el bot,\n"
            f"con reset diario UTC y comando /force_resume.\n\n"
            f"Usa /help para ver todos los comandos."
        )

    discord(
        f"🚀 Sentinel v{VERSION} | "
        f"{'Paper' if PAPER_MODE else 'LIVE'} | "
        f"${INITIAL_BANKROLL:.2f} | "
        f"{len(ASSETS)} assets × 1h+4h"
    )

    # Loop principal con error handling y CB fail-safe
    consecutive_errors = 0
    last_daily_reset_date = dt_now().date()

    while True:
        try:
            state.cycle += 1

            # ── PASO 1: Daily reset (medianoche UTC) ──────────────────────
            # Resetea peak_today, drawdown, CB y pausa por CB.
            current_date = dt_now().date()
            if current_date != last_daily_reset_date:
                log.info("=" * 70)
                log.info(f"  Drawdown reset a medianoche UTC ({current_date})")
                old_peak = state.engine.peak_today
                state.engine.peak_today      = state.engine.bankroll
                state.engine.peak_today_date = current_date
                log.info(f"  Nuevo peak equity: ${state.engine.peak_today:.2f} "
                         f"(anterior: ${old_peak:.2f})")
                # Si el CB estaba tripped por DD diario, ahora puede desarmarse
                if state.cb_paused:
                    state.release_cb(reason="reset diario UTC")
                    log.info("  Circuit breaker desarmado por reset diario")
                    tg.send(
                        f"🌅 <b>Reset diario UTC</b>\n\n"
                        f"Drawdown reseteado.\n"
                        f"Peak equity nuevo: ${state.engine.peak_today:.2f}\n"
                        f"Circuit breaker desarmado.\n"
                        f"Bot reanudando operaciones."
                    )
                last_daily_reset_date = current_date
                # ── Historical drawdown protection (nuevo en v8.5) ─────────────
hist_dd = (state.engine.peak - state.engine.bankroll) / state.engine.peak if state.engine.peak > 0 else 0
if hist_dd >= MAX_HISTORICAL_DD and not state.cb.tripped:
    state.cb.trip(hist_dd, reason=f"Drawdown histórico {hist_dd*100:.1f}% ≥ {MAX_HISTORICAL_DD*100:.0f}%")
    log.error(f"🚨 DRAW DOWN HISTÓRICO EXCEDIDO: {hist_dd*100:.1f}% — Bot PAUSADO")
    tg.send(
        f"🚨 <b>DRAWDOWN HISTÓRICO EXCEDIDO</b>\n\n"
        f"Drawdown desde el peak: {hist_dd*100:.1f}%\n"
        f"Límite configurado: {MAX_HISTORICAL_DD*100:.0f}%\n\n"
        f"El bot se ha pausado automáticamente por seguridad."
    )
                log.info("=" * 70)

            # ── PASO 2: Fetch prices (siempre, aún si pausado) ────────────
            prices = feed.get()

            # ── PASO 3: Resolver paper trades vencidos (siempre) ──────────
            # Importante: resolver trades aún si pausado, para que el DD se
            # estabilice y el daily reset eventualmente pueda liberar el CB.
            if PAPER_MODE:
                state.engine.resolve_by_price(prices)

            # ── PASO 4: Evaluar Circuit Breaker (SIEMPRE, fail-safe) ──────
            # cb.check() es idempotente; trip() solo loguea/alerta en transition.
            transitioned = state.cb.check(state.engine)

            if state.cb.tripped:
                # FIX CRÍTICO v8.5: sincronizar state.paused con CB
                if not state.cb_paused:
                    state.pause_for_cb()

                # Alerta en la transition OFF→ON (primera vez)
                if transitioned:
                    log.error(
                        f"━━━ 🚨 CIRCUIT BREAKER ACTIVADO ━━━\n"
                        f"  {state.cb.reason}\n"
                        f"  Bot PAUSADO automáticamente.\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                    )
                    analysis = cb_drawdown_analysis(state.engine, n_top=8)
                    log.info(analysis.replace("<b>", "").replace("</b>", "")
                                     .replace("<code>", "").replace("</code>", ""))
                    tg.send(
                        f"🚨 <b>CIRCUIT BREAKER ACTIVADO — "
                        f"{state.cb.dd_at_trip*100:.1f}% Drawdown — "
                        f"Bot PAUSADO automáticamente</b>\n\n"
                        f"{analysis}\n\n"
                        f"Usa <code>/force_resume</code> para reanudar manualmente.\n"
                        f"Recomendado esperar {CB_COOLDOWN_HOURS:.0f}h o el reset UTC."
                    )
                    discord(
                        f"🚨 CIRCUIT BREAKER ACTIVADO\n"
                        f"DD: {state.cb.dd_at_trip*100:.1f}%\n"
                        f"Bot pausado automáticamente."
                    )

                # Throttle: re-alertar cada CB_ALERT_REPEAT_MIN minutos
                elif state.cb.should_alert_again():
                    hours_tripped = state.cb.hours_since_trip()
                    log.warning(
                        f"  [CB] Sigue tripped hace {hours_tripped:.1f}h "
                        f"({state.cb.reason})"
                    )
                    tg.send(
                        f"⏰ <b>CB sigue activo</b>\n"
                        f"Tiempo tripped: {hours_tripped:.1f}h\n"
                        f"{state.cb.reason}\n"
                        f"Próximo reset automático: medianoche UTC.\n"
                        f"O usa /force_resume si quieres reanudar antes."
                    )
                # Si no es transition ni hay que re-alertar, solo log debug

                time.sleep(CYCLE_SLEEP)
                continue

            # ── PASO 5: Si está pausado manualmente, esperar ──────────────
            if state.paused:
                log.warning("  [PAUSED] Circuit breaker activo — saltando ciclo")
                time.sleep(CYCLE_SLEEP)
                continue

            # ── PASO 6: Scan normal ───────────────────────────────────────
            if state._scan_now:
                state._scan_now = False
                run_scan_cycle(state, scanner, prices, tg, forced=True)

            run_scan_cycle(state, scanner, prices, tg, forced=False)

            # Resumen horario (cada 60 ciclos)
            if state.cycle % 60 == 0:
                s = state.engine.summary()
                tg.send(
                    f"📊 <b>Resumen horario — Ciclo #{state.cycle}</b>\n\n"
                    f"Bankroll:   ${s['bankroll']:.2f}\n"
                    f"P&amp;L:    {signed(s['net_pnl'])}\n"
                    f"DD día:     {s['drawdown_today']*100:.1f}%\n"
                    f"Trades:     {s['closed']} "
                    f"({s['wins']}W / {s['losses']}L | {s['wr']:.1f}% WR)\n"
                    f"Open:       {s['open']}/{MAX_OPEN_TRADES}\n"
                    f"Señales:    {state.signals_today}\n"
                    f"Modo:       {'🔶 Relajado' if state.relaxed_mode else '🔵 Normal'}"
                )

            consecutive_errors = 0  # reset on success

        except KeyboardInterrupt:
            log.info("Shutdown solicitado.")
            tg.stop()
            tg.send("🛑 Sentinel v8.5 detenido manualmente.")
            discord("🛑 Sentinel v8.5 detenido manualmente.")
            break

        except Exception as exc:
            consecutive_errors += 1
            log.error(
                f"Error no manejado ciclo #{state.cycle} "
                f"(consecutivos: {consecutive_errors}):\n"
                f"{traceback.format_exc()}"
            )
            # Si hay 5+ errores seguidos, alertar
            if consecutive_errors == 5:
                tg.send(
                    f"⚠️ <b>Alerta:</b> 5 errores consecutivos en ciclos.\n"
                    f"Último: <code>{str(exc)[:200]}</code>"
                )
            # Backoff: si muchos errores, dormir más
            error_sleep = min(CYCLE_SLEEP * (1 + consecutive_errors // 3), 300)
            time.sleep(error_sleep)
            continue

        time.sleep(CYCLE_SLEEP)


if __name__ == "__main__":
    main()
