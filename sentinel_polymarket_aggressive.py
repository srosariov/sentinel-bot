#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════════
#  sentinel_polymarket_aggressive.py  —  v6.0  (Aggressive Edition)
# ───────────────────────────────────────────────────────────────────────────────
#  Búsqueda:  BTC, ETH, SOL × 5-min + 15-min = 6 oportunidades por ciclo
#  Filtros:   EV normal 5% | relajado 3.5% | Bayes normal 58% | relajado 52%
#  Extras:    Telegram puro HTTP | Discord webhook | Paper mode | Circuit breaker
#             Compounding | CSV export | /status /active /trades /pnl /pause /resume
# ───────────────────────────────────────────────────────────────────────────────
#  FIX v6.0 vs v5.x:
#    - Estrategias de búsqueda múltiples por asset/timeframe (7 queries cada una)
#    - Ventanas de tiempo más amplias (1-8 min para 5min, 5-22 min para 15min)
#    - Broadcast scan de TODOS los mercados activos como fallback
#    - Solo mercados reales; nunca sintéticos
#    - Logging claro [SKIP] / [FOUND] / [SIGNAL] / [TRADE]
# ═══════════════════════════════════════════════════════════════════════════════

VERSION    = "6.0.0"
BUILD_DATE = "2026-04-12"

import os, sys, csv, json, time, math, uuid, logging, threading, traceback, re
from datetime      import datetime, timezone, timedelta
from collections   import deque, defaultdict
from typing        import Optional, Tuple, List, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════
LOG_FILE = os.getenv("LOG_FILE", "sentinel_v6.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("sentinel")

# ══════════════════════════════════════════════════════════════════════════════
# ENV CONFIG
# ══════════════════════════════════════════════════════════════════════════════
TELEGRAM_TOKEN      = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
DISCORD_WEBHOOK     = os.getenv("DISCORD_WEBHOOK_URL", "")

PAPER_MODE          = os.getenv("PAPER_MODE", "true").lower() != "false"
INITIAL_BANKROLL    = float(os.getenv("INITIAL_BANKROLL", "100.0"))
TRADE_FRACTION      = float(os.getenv("TRADE_FRACTION", "0.05"))  # max % bankroll per trade
MAX_OPEN_TRADES     = int(os.getenv("MAX_OPEN_TRADES", "4"))
CIRCUIT_BREAKER_PCT = float(os.getenv("CIRCUIT_BREAKER_PCT", "0.20"))  # 20% drawdown
CYCLE_SLEEP         = int(os.getenv("CYCLE_SLEEP", "60"))
CSV_FILE            = os.getenv("CSV_FILE", "trades_v6.csv")

# ── Thresholds ────────────────────────────────────────────────────────────────
EV_NORMAL     = float(os.getenv("EV_NORMAL",     "0.050"))   # 5%
EV_RELAXED    = float(os.getenv("EV_RELAXED",    "0.035"))   # 3.5%
BAYES_NORMAL  = float(os.getenv("BAYES_NORMAL",  "0.580"))   # 58%
BAYES_RELAXED = float(os.getenv("BAYES_RELAXED", "0.520"))   # 52%

# ── Assets & Timeframes ───────────────────────────────────────────────────────
ASSETS     = ["BTC", "ETH", "SOL"]
TIMEFRAMES = [5, 15]   # minutes

ASSET_NAMES = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
}
ASSET_KEYWORDS = {
    "BTC": ["btc", "bitcoin", "xbt"],
    "ETH": ["eth", "ethereum"],
    "SOL": ["sol", "solana"],
}
# Approximate annualized volatility (log-normal model)
ASSET_VOL = {
    "BTC": 0.65,
    "ETH": 0.85,
    "SOL": 1.30,
}

# ── API endpoints ─────────────────────────────────────────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com"
CG_API    = "https://api.coingecko.com/api/v3/simple/price"
CG_IDS    = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana"}

# ══════════════════════════════════════════════════════════════════════════════
# HTTP SESSION (with retries)
# ══════════════════════════════════════════════════════════════════════════════
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": f"SentinelBot/{VERSION}"})
    return s

HTTP = _make_session()

# ══════════════════════════════════════════════════════════════════════════════
# MATH HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def normal_cdf(z: float) -> float:
    """Approximation of the standard normal CDF via erf."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def kelly_fraction(edge: float, odds: float = 1.0) -> float:
    """Fractional Kelly: capped at TRADE_FRACTION."""
    if odds <= 0 or edge <= 0:
        return 0.0
    return min(TRADE_FRACTION, max(0.0, edge / odds * 0.5))   # half-Kelly

# ══════════════════════════════════════════════════════════════════════════════
# PRICE FEED  (CoinGecko with caching + rate-limit guard)
# ══════════════════════════════════════════════════════════════════════════════
class PriceFeed:
    TTL = 45  # seconds between real fetches

    def __init__(self):
        self._cache: Dict[str, float] = {}
        self._last_fetch = 0.0

    def get(self) -> Dict[str, float]:
        now = time.time()
        if now - self._last_fetch < self.TTL and self._cache:
            log.debug("CoinGecko — usando caché")
            return self._cache

        ids = ",".join(CG_IDS.values())
        try:
            resp = HTTP.get(
                CG_API,
                params={"ids": ids, "vs_currencies": "usd"},
                timeout=10,
            )
            if resp.status_code == 429:
                log.warning("CoinGecko rate limit — usando caché")
                return self._cache or {}
            resp.raise_for_status()
            data = resp.json()
            prices: Dict[str, float] = {}
            for asset, cg_id in CG_IDS.items():
                prices[asset] = float(data.get(cg_id, {}).get("usd", 0))
            self._cache = prices
            self._last_fetch = now
            log.info(
                f"CoinGecko — BTC=${prices.get('BTC', 0):,.0f} "
                f"ETH=${prices.get('ETH', 0):,.0f} "
                f"SOL=${prices.get('SOL', 0):,.0f}"
            )
            return prices
        except Exception as exc:
            log.warning(f"CoinGecko error: {exc} — usando caché")
            return self._cache or {}

# ══════════════════════════════════════════════════════════════════════════════
# POLYMARKET MARKET SCANNER
# ══════════════════════════════════════════════════════════════════════════════
class MarketScanner:
    """
    Scans Polymarket Gamma API for REAL crypto prediction markets.
    Strategy: multiple keyword queries + broadcast scan as fallback.
    Never returns synthetic markets.
    """

    # ── low-level fetchers ───────────────────────────────────────────────────
    def _fetch_by_search(self, search: str, limit: int = 50) -> List[Dict]:
        try:
            resp = HTTP.get(
                f"{GAMMA_API}/markets",
                params={"search": search, "active": "true", "closed": "false", "limit": limit},
                timeout=12,
            )
            if resp.status_code != 200:
                return []
            data = resp.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return data.get("markets", data.get("data", []))
        except Exception as exc:
            log.debug(f"_fetch_by_search [{search}]: {exc}")
        return []

    def _fetch_broadcast(self, limit: int = 200) -> List[Dict]:
        """Fetch the most-imminent active markets (sorted by endDate ascending)."""
        try:
            resp = HTTP.get(
                f"{GAMMA_API}/markets",
                params={
                    "active": "true",
                    "closed": "false",
                    "limit": limit,
                    "order": "end_date_min",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                return []
            data = resp.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return data.get("markets", data.get("data", []))
        except Exception as exc:
            log.debug(f"_fetch_broadcast: {exc}")
        return []

    # ── helpers ──────────────────────────────────────────────────────────────
    @staticmethod
    def _parse_end_dt(m: Dict) -> Optional[datetime]:
        for field in ("endDate", "end_date_iso", "endDateIso", "end_date", "endDateTimestamp"):
            val = m.get(field)
            if not val:
                continue
            # Handle UNIX timestamp (seconds or milliseconds)
            if isinstance(val, (int, float)):
                ts = val if val < 1e11 else val / 1000
                try:
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
                except:
                    continue
            # ISO string
            try:
                return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            except:
                continue
        return None

    @staticmethod
    def _get_yes_price(m: Dict) -> Optional[float]:
        outcomes = m.get("outcomes", [])
        prices   = m.get("outcomePrices", [])
        if not outcomes or not prices:
            return None
        try:
            prices_f = [float(p) for p in prices]
        except:
            return None
        for i, o in enumerate(outcomes):
            if str(o).lower() in ("yes", "1", "true") and i < len(prices_f):
                return prices_f[i]
        return prices_f[0] if prices_f else None

    @staticmethod
    def _matches_asset(m: Dict, asset: str) -> bool:
        text = " ".join([
            m.get("question", ""),
            m.get("description", ""),
            m.get("slug", ""),
            m.get("category", ""),
        ]).lower()
        return any(kw in text for kw in ASSET_KEYWORDS.get(asset, [asset.lower()]))

    @staticmethod
    def _detect_timeframe(m: Dict, now: datetime) -> Optional[int]:
        """
        Returns 5 or 15 (minutes) if the market is a short-term crypto market
        with an appropriate amount of time remaining. Returns None otherwise.
        """
        end_dt = MarketScanner._parse_end_dt(m)
        if end_dt is None:
            return None

        mins_left = (end_dt - now).total_seconds() / 60

        # Hard gates: must have at least 1 minute and at most 25 minutes
        if mins_left < 1.0 or mins_left > 25.0:
            return None

        # First try explicit keyword detection in the question
        question = (m.get("question") or "").lower()
        tf_hints: Dict[int, List[str]] = {
            5:  ["5 minute", "5-minute", "5 min", "5min", "five minute", " 5m "],
            15: ["15 minute", "15-minute", "15 min", "15min", "fifteen minute", " 15m "],
        }
        for tf, hints in tf_hints.items():
            for h in hints:
                if h in question:
                    return tf

        # Infer from time remaining:
        #   1–8  min remaining  → classify as 5-min opportunity
        #   8–22 min remaining  → classify as 15-min opportunity
        if 1.0 <= mins_left <= 8.0:
            return 5
        if 8.0 < mins_left <= 22.0:
            return 15

        return None

    # ── public scan method ───────────────────────────────────────────────────
    def scan(self, asset: str, timeframe: int, now: datetime) -> List[Dict]:
        """
        Returns enriched market dicts for (asset, timeframe) that are ready
        for signal analysis.  Only returns REAL markets (never synthetic).
        """
        log.info(f"  --- Escaneando {asset} {timeframe}min ---")

        # Build search queries (multiple strategies)
        queries = [
            f"{asset} {timeframe} minute",
            f"{asset} {timeframe} min",
            f"Will {asset} be above",
            f"Will {asset} be below",
            f"{asset} price",
            ASSET_NAMES[asset],
            f"{ASSET_NAMES[asset]} {timeframe}",
        ]

        seen:       set        = set()
        candidates: List[Dict] = []

        # Strategy 1: targeted keyword searches
        for q in queries:
            for m in self._fetch_by_search(q):
                mid = m.get("id") or m.get("conditionId") or m.get("slug")
                if not mid or mid in seen:
                    continue
                if not m.get("active", True) or m.get("closed", False):
                    continue
                if not self._matches_asset(m, asset):
                    continue
                yes_price = self._get_yes_price(m)
                if yes_price is None or not (0.03 <= yes_price <= 0.97):
                    continue
                tf = self._detect_timeframe(m, now)
                if tf != timeframe:
                    continue
                end_dt    = self._parse_end_dt(m)
                mins_left = (end_dt - now).total_seconds() / 60 if end_dt else 0
                seen.add(mid)
                m["_yes_price"]    = yes_price
                m["_mins_left"]    = mins_left
                m["_asset"]        = asset
                m["_timeframe"]    = timeframe
                candidates.append(m)

        # Strategy 2: broadcast fallback if nothing found
        if not candidates:
            for m in self._fetch_broadcast(200):
                mid = m.get("id") or m.get("conditionId") or m.get("slug")
                if not mid or mid in seen:
                    continue
                if not m.get("active", True) or m.get("closed", False):
                    continue
                if not self._matches_asset(m, asset):
                    continue
                yes_price = self._get_yes_price(m)
                if yes_price is None or not (0.03 <= yes_price <= 0.97):
                    continue
                tf = self._detect_timeframe(m, now)
                if tf != timeframe:
                    continue
                end_dt    = self._parse_end_dt(m)
                mins_left = (end_dt - now).total_seconds() / 60 if end_dt else 0
                seen.add(mid)
                m["_yes_price"]    = yes_price
                m["_mins_left"]    = mins_left
                m["_asset"]        = asset
                m["_timeframe"]    = timeframe
                candidates.append(m)

        if not candidates:
            log.info(
                f"  [SKIP] No real {timeframe}-min markets found for {asset}"
                f" on Polymarket — skipping"
            )
        else:
            log.info(
                f"  [FOUND] {len(candidates)} mercado(s) real(es) "
                f"{timeframe}-min para {asset}"
            )

        return candidates

# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL ENGINE
# ══════════════════════════════════════════════════════════════════════════════
def _prob_above(current: float, target: float, asset: str, mins: float) -> float:
    """
    Log-normal Brownian motion estimate of P(price ≥ target) at expiry.
    """
    if current <= 0 or mins <= 0:
        return 0.5
    annual_vol  = ASSET_VOL.get(asset, 0.80)
    vol_per_min = annual_vol / math.sqrt(365 * 24 * 60)
    sigma       = vol_per_min * math.sqrt(mins)
    if sigma < 1e-9:
        return 1.0 if current >= target else 0.0
    log_k = math.log(target / current)
    z     = -log_k / sigma          # P(S_T >= K)
    return max(0.02, min(0.98, normal_cdf(z)))


def _parse_direction_target(
    question: str,
    current: float,
) -> Tuple[Optional[str], Optional[float]]:
    """
    Extract (direction, target_price) from a Polymarket question string.
    E.g. "Will BTC be above $73,000?" → ('above', 73000.0)
    """
    q_low = question.lower()

    if any(w in q_low for w in ("above", "over", "higher than", "exceed")):
        direction = "above"
    elif any(w in q_low for w in ("below", "under", "lower than", "less than")):
        direction = "below"
    else:
        return None, None

    # Match price patterns: $73,000  /  73000  /  $73k  /  73.5k
    patterns = [
        r"\$[\d,]+(?:\.\d+)?",          # $73,000
        r"\$[\d]+(?:\.\d+)?[kK]",       # $73k
        r"(?<!\w)[\d]{4,}(?:,\d{3})*(?:\.\d+)?(?!\w)",  # 73000 or 73,000
    ]
    for pat in patterns:
        for raw in re.findall(pat, question):
            clean = raw.replace("$", "").replace(",", "").strip()
            multiplier = 1
            if clean.lower().endswith("k"):
                clean = clean[:-1]
                multiplier = 1_000
            try:
                price = float(clean) * multiplier
                if price <= 0:
                    continue
                # Accept if within ±25% of current price (sanity check)
                if current > 0 and not (0.75 <= price / current <= 1.25):
                    continue
                return direction, price
            except ValueError:
                continue

    # Fallback: use current price as pivot (e.g. "Will BTC be above its open?")
    return direction, current


def analyze(market: Dict, current_price: float) -> Optional[Dict]:
    """
    Full EV + Bayesian analysis for one candidate market.
    Returns a signal dict, or None if no qualifying edge.
    """
    asset     = market["_asset"]
    yes_price = market["_yes_price"]
    mins      = market["_mins_left"]
    question  = market.get("question", "")

    direction, target = _parse_direction_target(question, current_price)
    if direction is None or target is None or target <= 0:
        log.debug(f"  [SKIP-PARSE] {question[:70]}")
        return None

    prob_above = _prob_above(current_price, target, asset, mins)

    if direction == "above":
        our_prob    = prob_above
        market_prob = yes_price
    else:
        our_prob    = 1.0 - prob_above
        market_prob = yes_price

    edge = our_prob - market_prob

    # Choose best side
    if edge >= 0:
        side        = "YES"
        sig_ev      = edge
        sig_bayes   = our_prob
        bet_price   = yes_price
    else:
        side        = "NO"
        no_price    = 1.0 - yes_price
        sig_ev      = -edge
        sig_bayes   = 1.0 - our_prob
        bet_price   = no_price

    # Classify
    if sig_ev >= EV_NORMAL and sig_bayes >= BAYES_NORMAL:
        mode = "NORMAL"
    elif sig_ev >= EV_RELAXED and sig_bayes >= BAYES_RELAXED:
        mode = "RELAXED"
    else:
        log.debug(
            f"  [SKIP-EV] EV={sig_ev*100:.1f}% Bayes={sig_bayes*100:.1f}%"
            f"  {question[:50]}"
        )
        return None

    return {
        "market_id":    market.get("id") or market.get("conditionId"),
        "question":     question,
        "asset":        asset,
        "timeframe":    market["_timeframe"],
        "direction":    direction,
        "target":       target,
        "current":      current_price,
        "side":         side,
        "market_prob":  market_prob,
        "our_prob":     our_prob,
        "ev":           sig_ev,
        "bayes":        sig_bayes,
        "bet_price":    bet_price,
        "mins":         mins,
        "mode":         mode,
        "market":       market,
    }

# ══════════════════════════════════════════════════════════════════════════════
# PAPER TRADING ENGINE
# ══════════════════════════════════════════════════════════════════════════════
class Trade:
    __slots__ = (
        "trade_id", "market_id", "question", "asset", "timeframe",
        "side", "bet_price", "stake", "entry_time", "status",
        "pnl", "close_time", "close_price",
        "mins", "target", "current", "direction", "mode",
    )

    def __init__(self, trade_id: str, sig: Dict, stake: float, ts: datetime):
        self.trade_id   = trade_id
        self.market_id  = sig["market_id"]
        self.question   = sig["question"]
        self.asset      = sig["asset"]
        self.timeframe  = sig["timeframe"]
        self.side       = sig["side"]
        self.bet_price  = sig["bet_price"]
        self.stake      = stake
        self.entry_time = ts
        self.status     = "OPEN"
        self.pnl        = 0.0
        self.close_time = None
        self.close_price= None
        self.mins       = sig["mins"]
        self.target     = sig["target"]
        self.current    = sig["current"]
        self.direction  = sig["direction"]
        self.mode       = sig["mode"]

    def as_row(self) -> Dict:
        return {
            "trade_id":   self.trade_id,
            "market_id":  self.market_id,
            "question":   self.question[:100],
            "asset":      self.asset,
            "timeframe":  self.timeframe,
            "side":       self.side,
            "bet_price":  f"{self.bet_price:.4f}",
            "stake":      f"{self.stake:.2f}",
            "entry_time": self.entry_time.isoformat(),
            "status":     self.status,
            "pnl":        f"{self.pnl:.4f}",
            "close_time": self.close_time.isoformat() if self.close_time else "",
            "mode":       self.mode,
        }

CSV_FIELDS = [
    "trade_id", "market_id", "question", "asset", "timeframe",
    "side", "bet_price", "stake", "entry_time",
    "status", "pnl", "close_time", "mode",
]


class PaperEngine:
    def __init__(self, initial: float):
        self.bankroll     = initial
        self.peak         = initial
        self.open_trades: List[Trade]  = []
        self.closed: deque             = deque(maxlen=500)
        self._lock        = threading.Lock()
        self._init_csv()

    def _init_csv(self):
        if not os.path.exists(CSV_FILE):
            with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()

    def _append_csv(self, trade: Trade):
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow(trade.as_row())

    # ── trade lifecycle ──────────────────────────────────────────────────────
    def can_open(self, market_id: str) -> bool:
        with self._lock:
            if len(self.open_trades) >= MAX_OPEN_TRADES:
                return False
            return all(t.market_id != market_id for t in self.open_trades)

    def open_trade(self, sig: Dict, ts: datetime) -> Optional[Trade]:
        if not self.can_open(sig["market_id"]):
            return None
        with self._lock:
            frac  = kelly_fraction(sig["ev"])
            stake = round(max(0.50, self.bankroll * frac), 2)
            trade = Trade(str(uuid.uuid4())[:8].upper(), sig, stake, ts)
            self.open_trades.append(trade)
            self.bankroll -= stake  # reserve stake upfront
        self._append_csv(trade)
        return trade

    def resolve(self, trade: Trade, won: bool, close_price: float, ts: datetime):
        with self._lock:
            if trade.status != "OPEN":
                return
            if won:
                payout     = trade.stake / trade.bet_price  # polymarket pays $1/share
                trade.pnl  = payout - trade.stake
                trade.status = "WIN"
                self.bankroll += payout
            else:
                trade.pnl    = -trade.stake
                trade.status = "LOSS"
                # stake already deducted at open
            trade.close_time  = ts
            trade.close_price = close_price
            self.open_trades  = [t for t in self.open_trades if t.trade_id != trade.trade_id]
            self.closed.appendleft(trade)
            if self.bankroll > self.peak:
                self.peak = self.bankroll
        self._append_csv(trade)

    def expire_stale(self, now: datetime):
        to_expire = []
        with self._lock:
            for t in self.open_trades:
                deadline = t.entry_time + timedelta(minutes=t.mins + 6)
                if now > deadline:
                    to_expire.append(t)
        for t in to_expire:
            log.info(f"[EXPIRE] {t.trade_id} {t.asset} expirado sin resolución")
            self.resolve(t, won=False, close_price=0.0, ts=now)

    # ── properties & summary ─────────────────────────────────────────────────
    @property
    def drawdown(self) -> float:
        return (self.peak - self.bankroll) / self.peak if self.peak > 0 else 0.0

    @property
    def net_pnl(self) -> float:
        return self.bankroll - INITIAL_BANKROLL

    def summary(self) -> Dict:
        closed = list(self.closed)
        wins   = [t for t in closed if t.status == "WIN"]
        losses = [t for t in closed if t.status == "LOSS"]
        total  = len(wins) + len(losses)
        return {
            "bankroll":  self.bankroll,
            "net_pnl":   self.net_pnl,
            "peak":      self.peak,
            "drawdown":  self.drawdown,
            "open":      len(self.open_trades),
            "closed":    total,
            "wins":      len(wins),
            "losses":    len(losses),
            "win_rate":  (len(wins) / total * 100) if total > 0 else 0.0,
        }

# ══════════════════════════════════════════════════════════════════════════════
# CIRCUIT BREAKER
# ══════════════════════════════════════════════════════════════════════════════
class CircuitBreaker:
    def __init__(self):
        self.tripped = False
        self.reason  = ""

    def check(self, engine: PaperEngine) -> bool:
        if engine.drawdown >= CIRCUIT_BREAKER_PCT:
            self.tripped = True
            self.reason  = (
                f"Drawdown {engine.drawdown*100:.1f}% ≥ "
                f"límite {CIRCUIT_BREAKER_PCT*100:.0f}%"
            )
            return True
        return False

    def reset(self):
        self.tripped = False
        self.reason  = ""

# ══════════════════════════════════════════════════════════════════════════════
# TRADE RESOLUTION (paper simulation)
# ══════════════════════════════════════════════════════════════════════════════
def resolve_open_trades(engine: PaperEngine, prices: Dict[str, float]):
    """
    Paper mode: at or after expected resolution time, settle trade
    by comparing current market price vs target.
    """
    now = datetime.now(timezone.utc)
    engine.expire_stale(now)

    for trade in list(engine.open_trades):
        if trade.status != "OPEN":
            continue
        current = prices.get(trade.asset, 0.0)
        if current <= 0:
            continue
        expected_close = trade.entry_time + timedelta(minutes=trade.mins)
        if now < expected_close:
            continue  # not yet expired

        price_above = current >= trade.target
        if trade.direction == "above":
            market_won = price_above
        else:
            market_won = not price_above

        won = (trade.side == "YES" and market_won) or (trade.side == "NO" and not market_won)
        engine.resolve(trade, won=won, close_price=current, ts=now)
        icon = "✅" if won else "❌"
        log.info(
            f"[RESOLVE] {icon} {trade.trade_id} | {trade.asset} {trade.side} | "
            f"PnL: {'+' if trade.pnl >= 0 else ''}{trade.pnl:.2f} | "
            f"target={trade.target:.0f} actual={current:.0f}"
        )

# ══════════════════════════════════════════════════════════════════════════════
# DISCORD WEBHOOK
# ══════════════════════════════════════════════════════════════════════════════
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
        log.debug(f"Discord send error: {exc}")

# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM BOT  (pure HTTP, long-polling)
# ══════════════════════════════════════════════════════════════════════════════
HELP_MSG = (
    f"<b>🤖 Sentinel Polymarket v{VERSION}</b>\n\n"
    "<b>Comandos:</b>\n"
    "  /status   — Estado general del bot\n"
    "  /active   — Trades abiertos con detalle\n"
    "  /trades   — Abiertos + últimos 10 cerrados\n"
    "  /pnl      — Resumen histórico de P&L\n"
    "  /pause    — Pausar el bot\n"
    "  /resume   — Reanudar / reset circuit breaker\n"
    "  /help     — Este mensaje\n\n"
    f"Assets: {', '.join(ASSETS)} × {TIMEFRAMES}min\n"
    f"EV: {EV_NORMAL*100:.1f}% normal | {EV_RELAXED*100:.1f}% relajado\n"
    f"Bayes: {BAYES_NORMAL*100:.0f}% normal | {BAYES_RELAXED*100:.0f}% relajado\n"
    f"Mode: {'📄 Paper' if PAPER_MODE else '💰 Live'}"
)


class TelegramBot:
    def __init__(self, token: str, chat_id: str):
        self.token    = token
        self.chat_id  = chat_id
        self._base    = f"https://api.telegram.org/bot{token}"
        self._offset  = 0
        self._running = True

    # ── send ─────────────────────────────────────────────────────────────────
    def send(self, text: str, parse_mode: str = "HTML", to: str = None) -> bool:
        cid = to or self.chat_id
        if not self.token or not cid:
            return False
        try:
            r = requests.post(
                f"{self._base}/sendMessage",
                json={"chat_id": cid, "text": text, "parse_mode": parse_mode,
                      "disable_web_page_preview": True},
                timeout=10,
            )
            return r.status_code == 200
        except Exception as exc:
            log.debug(f"Telegram send: {exc}")
            return False

    # ── polling ───────────────────────────────────────────────────────────────
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
        except:
            pass
        return []

    def poll_loop(self, state: "BotState"):
        log.info("Telegram poll loop iniciado")
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
                        self._handle(cmd, cid, state)
            except Exception as exc:
                log.debug(f"poll_loop error: {exc}")
            time.sleep(1)

    # ── command handlers ──────────────────────────────────────────────────────
    def _handle(self, cmd: str, cid: str, state: "BotState"):
        engine = state.engine

        if cmd == "/help":
            self.send(HELP_MSG, to=cid)

        elif cmd == "/status":
            s  = engine.summary()
            cb = "🔴 TRIPPED" if state.cb.tripped else "🟢 OK"
            pa = "⏸ PAUSADO"  if state.paused     else "▶ ACTIVO"
            self.send(
                f"<b>🤖 Sentinel v{VERSION}</b>\n\n"
                f"Estado:     {pa}\n"
                f"Ciclo:      #{state.cycle}\n"
                f"Bankroll:   <b>${s['bankroll']:.2f}</b>\n"
                f"P&amp;L neto: {'+' if s['net_pnl']>=0 else ''}{s['net_pnl']:.2f}\n"
                f"Peak:       ${s['peak']:.2f}\n"
                f"Drawdown:   {s['drawdown']*100:.1f}%\n"
                f"CB:         {cb}\n"
                f"Señales:    {state.signals_today}\n"
                f"Abiertos:   {s['open']}/{MAX_OPEN_TRADES}",
                to=cid,
            )

        elif cmd == "/active":
            trades = engine.open_trades
            if not trades:
                self.send("📭 No hay trades abiertos.", to=cid)
                return
            lines = [f"<b>📂 Trades Abiertos ({len(trades)})</b>\n"]
            for t in trades:
                age = int((datetime.now(timezone.utc) - t.entry_time).total_seconds() / 60)
                lines.append(
                    f"🔹 <b>{t.trade_id}</b> | {t.asset} {t.timeframe}min\n"
                    f"   Side: {t.side} @ {t.bet_price:.3f}\n"
                    f"   Stake: ${t.stake:.2f} | {age}min transcurridos\n"
                    f"   Target: {t.direction} ${t.target:,.0f}\n"
                    f"   Q: {t.question[:60]}…"
                )
            self.send("\n".join(lines), to=cid)

        elif cmd == "/trades":
            open_t   = engine.open_trades
            closed_t = list(engine.closed)[:10]
            lines    = []
            if open_t:
                lines.append(f"<b>📂 Abiertos ({len(open_t)})</b>")
                for t in open_t:
                    lines.append(f"  • {t.trade_id} {t.asset} {t.side} ${t.stake:.2f}")
            if closed_t:
                lines.append(f"\n<b>📋 Últimos 10 cerrados</b>")
                for t in closed_t:
                    icon = "✅" if t.status == "WIN" else "❌"
                    lines.append(
                        f"  {icon} {t.trade_id} | {t.asset} {t.timeframe}min "
                        f"| {t.side} | {'+' if t.pnl>=0 else ''}{t.pnl:.2f}"
                    )
            if not lines:
                self.send("No hay trades registrados aún.", to=cid)
                return
            self.send("\n".join(lines), to=cid)

        elif cmd == "/pnl":
            s      = engine.summary()
            closed = list(engine.closed)
            by_asset: Dict[str, Dict] = defaultdict(lambda: {"pnl": 0.0, "n": 0, "w": 0})
            for t in closed:
                d = by_asset[t.asset]
                d["pnl"] += t.pnl
                d["n"]   += 1
                if t.status == "WIN":
                    d["w"] += 1
            lines = [
                f"<b>📊 P&amp;L Histórico — Sentinel v{VERSION}</b>\n",
                f"Bankroll inicial: ${INITIAL_BANKROLL:.2f}",
                f"Bankroll actual:  ${s['bankroll']:.2f}",
                f"P&amp;L neto:     {'+' if s['net_pnl']>=0 else ''}{s['net_pnl']:.2f}",
                f"Peak:             ${s['peak']:.2f}",
                f"Drawdown actual:  {s['drawdown']*100:.1f}%\n",
                f"Total trades:     {s['closed']} | "
                f"{s['wins']}W / {s['losses']}L | WR {s['win_rate']:.1f}%\n",
                "<b>Por asset:</b>",
            ]
            for asset in ASSETS:
                d = by_asset.get(asset)
                if d and d["n"] > 0:
                    wr = d["w"] / d["n"] * 100
                    lines.append(
                        f"  {asset}: {'+' if d['pnl']>=0 else ''}{d['pnl']:.2f} "
                        f"({d['n']} trades | {d['w']}W | {wr:.0f}% WR)"
                    )
            self.send("\n".join(lines), to=cid)

        elif cmd == "/pause":
            state.paused = True
            self.send("⏸ Bot pausado. Usa /resume para reanudar.", to=cid)
            log.info("[TELEGRAM] Bot pausado por el usuario")

        elif cmd == "/resume":
            state.paused = False
            state.cb.reset()
            self.send("▶ Bot reanudado. Circuit breaker reseteado.", to=cid)
            log.info("[TELEGRAM] Bot reanudado por el usuario")

        else:
            self.send(f"Comando no reconocido: {cmd}\nUsa /help.", to=cid)

    def stop(self):
        self._running = False

# ══════════════════════════════════════════════════════════════════════════════
# BOT STATE
# ══════════════════════════════════════════════════════════════════════════════
class BotState:
    def __init__(self):
        self.cycle         = 0
        self.paused        = False
        self.signals_today = 0
        self.engine        = PaperEngine(INITIAL_BANKROLL)
        self.cb            = CircuitBreaker()

# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════════
def main():
    banner = (
        f"{'='*62}\n"
        f"  SENTINEL POLYMARKET v{VERSION} — {BUILD_DATE}\n"
        f"  Mode:      {'📄 PAPER' if PAPER_MODE else '💰 LIVE'}\n"
        f"  Assets:    {', '.join(ASSETS)} × {TIMEFRAMES}min\n"
        f"  EV:        normal={EV_NORMAL*100:.1f}%  relajado={EV_RELAXED*100:.1f}%\n"
        f"  Bayes:     normal={BAYES_NORMAL*100:.0f}%   relajado={BAYES_RELAXED*100:.0f}%\n"
        f"  Bankroll:  ${INITIAL_BANKROLL:.2f}\n"
        f"  CB limit:  {CIRCUIT_BREAKER_PCT*100:.0f}% drawdown\n"
        f"  Cycle:     {CYCLE_SLEEP}s\n"
        f"{'='*62}"
    )
    log.info(banner)

    state   = BotState()
    scanner = MarketScanner()
    feed    = PriceFeed()
    tg      = TelegramBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

    # ── Start Telegram polling thread ─────────────────────────────────────────
    if TELEGRAM_TOKEN:
        tg_thread = threading.Thread(
            target=tg.poll_loop, args=(state,), daemon=True, name="tg-poll"
        )
        tg_thread.start()
        tg.send(
            f"🚀 <b>Sentinel v{VERSION}</b> iniciado\n"
            f"Mode: {'Paper 📄' if PAPER_MODE else 'Live 💰'} | "
            f"Bankroll: ${INITIAL_BANKROLL:.2f}\n"
            f"Assets: {', '.join(ASSETS)} × {TIMEFRAMES}min\n"
            f"Usa /help para ver comandos disponibles."
        )
    discord(
        f"🚀 Sentinel v{VERSION} iniciado | "
        f"{'Paper' if PAPER_MODE else 'Live'} | "
        f"Bankroll ${INITIAL_BANKROLL:.2f}"
    )

    # ── Main cycle ────────────────────────────────────────────────────────────
    while True:
        try:
            state.cycle += 1
            now = datetime.now(timezone.utc)
            mode_label = (
                f"EV>={EV_RELAXED*100:.0f}% "
                f"Bayes>={BAYES_RELAXED*100:.0f}%"
            )
            log.info(f"━━━ CICLO #{state.cycle} | RELAJADO | {mode_label} ━━━")

            # ── Guard: pause / circuit breaker ────────────────────────────
            if state.paused:
                log.info("  [PAUSED] Bot pausado, esperando /resume…")
                time.sleep(CYCLE_SLEEP)
                continue

            if state.cb.tripped:
                log.warning(f"  [CIRCUIT BREAKER] {state.cb.reason}")
                time.sleep(CYCLE_SLEEP)
                continue

            # ── Prices ───────────────────────────────────────────────────
            prices = feed.get()

            # ── Resolve paper trades ──────────────────────────────────────
            if PAPER_MODE:
                resolve_open_trades(state.engine, prices)

            # ── Circuit breaker evaluation ────────────────────────────────
            if state.cb.check(state.engine):
                msg = f"🔴 CIRCUIT BREAKER: {state.cb.reason}"
                log.error(msg)
                tg.send(f"<b>{msg}</b>\nUsa /resume para continuar.")
                discord(msg)
                time.sleep(CYCLE_SLEEP)
                continue

            # ── Scan 6 opportunities: 3 assets × 2 timeframes ─────────────
            total_scanned = 0
            total_signals = 0
            no_mkt_assets: List[str] = []

            for asset in ASSETS:
                log.info(f"--- Escaneando {asset} ---")
                asset_price = prices.get(asset, 0.0)

                if asset_price <= 0:
                    log.warning(f"  [SKIP] Precio no disponible para {asset}")
                    no_mkt_assets.append(asset)
                    continue

                asset_has_market = False

                for tf in TIMEFRAMES:
                    markets = scanner.scan(asset, tf, now)
                    total_scanned += len(markets)

                    if markets:
                        asset_has_market = True

                    for mkt in markets:
                        sig = analyze(mkt, asset_price)
                        if sig is None:
                            continue

                        total_signals  += 1
                        state.signals_today += 1

                        log.info(
                            f"  [SIGNAL] {asset} {tf}min | {sig['side']} | "
                            f"EV={sig['ev']*100:.1f}% Bayes={sig['bayes']*100:.1f}% "
                            f"[{sig['mode']}] | {sig['question'][:60]}"
                        )

                        trade = state.engine.open_trade(sig, now)
                        if trade:
                            msg = (
                                f"🟢 <b>TRADE #{trade.trade_id}</b> [{sig['mode']}]\n"
                                f"Asset:  {asset} {tf}min | {sig['direction'].upper()}\n"
                                f"Side:   {trade.side} @ {trade.bet_price:.3f}\n"
                                f"Stake:  ${trade.stake:.2f}\n"
                                f"EV:     {sig['ev']*100:.1f}% | Bayes: {sig['bayes']*100:.1f}%\n"
                                f"Target: ${sig['target']:,.0f} | Now: ${asset_price:,.0f}\n"
                                f"Q: {sig['question'][:70]}"
                            )
                            log.info(
                                f"  [TRADE OPENED] {trade.trade_id} | "
                                f"stake=${trade.stake:.2f} | {trade.side}"
                            )
                            tg.send(msg)
                            discord(
                                f"🟢 TRADE {trade.trade_id} | {asset} {tf}min | "
                                f"{trade.side} @ {trade.bet_price:.3f} | "
                                f"${trade.stake:.2f} | EV {sig['ev']*100:.1f}% | "
                                f"[{sig['mode']}]"
                            )
                        else:
                            log.info("  [SKIP TRADE] Max open trades o mercado duplicado")

                if not asset_has_market:
                    no_mkt_assets.append(asset)

            # ── Summary ───────────────────────────────────────────────────
            if set(no_mkt_assets) == set(ASSETS):
                log.info(
                    f"  [INFO] Sin mercados reales: {', '.join(ASSETS)}"
                    f" — esperando apertura de mercados en Polymarket"
                )
            elif no_mkt_assets:
                log.info(f"  [INFO] Sin mercados para: {', '.join(set(no_mkt_assets))}")

            log.info(
                f"━━━ Ciclo #{state.cycle} completo — "
                f"{total_scanned} mercados escaneados, "
                f"{total_signals} señales ━━━"
            )

            # ── Hourly Telegram summary (every 60 cycles) ─────────────────
            if state.cycle % 60 == 0:
                s = state.engine.summary()
                tg.send(
                    f"📊 <b>Resumen horario — Ciclo #{state.cycle}</b>\n"
                    f"Bankroll:  ${s['bankroll']:.2f}\n"
                    f"P&amp;L:   {'+' if s['net_pnl']>=0 else ''}{s['net_pnl']:.2f}\n"
                    f"Trades:    {s['closed']} "
                    f"({s['wins']}W / {s['losses']}L | {s['win_rate']:.1f}% WR)\n"
                    f"Señales:   {state.signals_today}"
                )

        except KeyboardInterrupt:
            log.info("Shutdown solicitado por el usuario.")
            tg.stop()
            tg.send("🛑 Sentinel detenido manualmente.")
            discord("🛑 Sentinel detenido manualmente.")
            break

        except Exception as exc:
            log.error(f"Error no manejado ciclo #{state.cycle}: {exc}\n{traceback.format_exc()}")

        time.sleep(CYCLE_SLEEP)


if __name__ == "__main__":
    main()
