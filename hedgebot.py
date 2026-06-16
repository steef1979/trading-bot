"""
HedgeBot v6 — إعادة هيكلة كاملة
═══════════════════════════════════
الرافعة المالية:
  Grid         : x9 ثابت
  EMA/Multi/Vol: 4/4 → x20 | 3/4 → x10 | 2/4 → x5
الاستراتيجيات: Grid + EMA + MultiMA + Volume
75 عملة — Binance Futures Testnet
"""

import asyncio, json, logging, os, time, math, aiohttp
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from dotenv import load_dotenv
from binance import AsyncClient, BinanceSocketManager
from binance.enums import (
    SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET,
    FUTURE_ORDER_TYPE_STOP_MARKET,
    FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
)

# ═══════════════════════════════════════════════════════
# ⚙️  إعدادات
# ═══════════════════════════════════════════════════════
load_dotenv()
API_KEY    = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
if not API_KEY or not API_SECRET:
    raise EnvironmentError("❌ أضف BINANCE_API_KEY و BINANCE_API_SECRET في .env")

TG_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

SYMBOLS = [
    'BTCUSDT',  'ETHUSDT',  'SOLUSDT',  'BNBUSDT',  'ADAUSDT',
    'XRPUSDT',  'DOTUSDT',  'LTCUSDT',  'LINKUSDT', 'MATICUSDT',
    'AVAXUSDT', 'UNIUSDT',  'ATOMUSDT', 'NEARUSDT', 'ALGOUSDT',
    'VETUSDT',  'FILUSDT',  'FTMUSDT',  'SANDUSDT',
    'MANAUSDT', 'EGLDUSDT', 'AXSUSDT',  'AAVEUSDT', 'THETAUSDT',
    'DOGEUSDT', 'TRXUSDT',  'BCHUSDT',  'XLMUSDT',  'INJUSDT',
    'APEUSDT',  'HBARUSDT', 'XTZUSDT',  'EOSUSDT',  'KLAYUSDT',
    'CHZUSDT',  'OPUSDT',   'ARBUSDT',  'APTUSDT',  'SUIUSDT',
    'WLDUSDT',  'RNDRUSDT', 'FETUSDT',  'IMXUSDT',  'SEIUSDT',
    'GALAUSDT', 'SNXUSDT',  'CRVUSDT',  'DYDXUSDT', 'QNTUSDT',
    'LDOUSDT',  'FXSUSDT',  'MINAUSDT', 'CFXUSDT',  'RUNEUSDT',
    'KAVAUSDT', 'GMXUSDT',  'LRCUSDT',  'ANKRUSDT', 'ENJUSDT',
    'BATUSDT',  'IOTXUSDT', 'ZENUSDT',  'ZILUSDT',  'ONEUSDT',
    'ICXUSDT',  'ONTUSDT',  'STORJUSDT','SUSHIUSDT','CTSIUSDT',
    'SKLUSDT',  'RVNUSDT',  'MKRUSDT',  'TAOUSDT',  'OCEANUSDT',
]

# رأس المال
BALANCE_USDT       = 10000.0
RISK_PCT           = 0.01       # 1% لكل صفقة
MIN_NOTIONAL       = 55.0       # حد Binance الأدنى

# الرافعة المالية
GRID_LEVERAGE      = 9          # Grid ثابت x9
LEV_4OF4           = 20         # 4/4 مؤشرات
LEV_3OF4           = 10         # 3/4 مؤشرات
LEV_2OF4           = 5          # 2/4 مؤشرات
MAX_LEVERAGE       = 20

# SL/TP
ATR_PERIOD         = 14
ATR_SL_MULT        = 1.2
ATR_TP_MULT        = 3.0
MIN_SL_PCT         = 0.005      # 0.5% حد أدنى
MIN_TP_PCT         = 0.008      # 0.8% حد أدنى

# Trailing
TRAILING_MULT      = 2.0
TRAILING_TRIGGER   = 0.01
TRAILING_ACTIVATE  = 2.0    # تفعيل بعد 2×ATR ربح       # 1% ربح لتفعيل Trailing

# تشغيل
MAX_POSITIONS      = 15
COOLDOWN_SEC       = 120
CANDLE_BUF         = 100
MAX_STREAMS        = 200
RECONNECT_SEC      = 5
MONITOR_SEC        = 15

# Fear & Greed
FNG_LONG_MIN       = 5
FNG_SHORT_MAX      = 95
FNG_CACHE_SEC      = 300

# Persistence
POS_FILE           = "positions.json"
WGT_FILE           = "weights.json"

# Logging
_fmt = logging.Formatter('%(asctime)s | %(levelname)-8s | %(name)s | %(message)s', '%H:%M:%S')
_fh  = logging.FileHandler("bot.log", encoding="utf-8"); _fh.setFormatter(_fmt)
_sh  = logging.StreamHandler(); _sh.setFormatter(_fmt)
logging.root.setLevel(logging.INFO)
logging.root.addHandler(_fh); logging.root.addHandler(_sh)
logger = logging.getLogger("HedgeBotV6")

# ═══════════════════════════════════════════════════════
# 📦  هياكل البيانات
# ═══════════════════════════════════════════════════════
class Sig(Enum):
    BUY  = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"

class PosSide(Enum):
    LONG  = "LONG"
    SHORT = "SHORT"

@dataclass
class Candle:
    t: int; o: float; h: float; l: float; c: float; v: float; closed: bool

@dataclass
class Ticker:
    symbol: str; price: float; bid: float; ask: float
    volume: float; change: float; ts: float = field(default_factory=time.time)

@dataclass
class Signal:
    symbol: str; side: Sig; strategy: str
    score: int           # عدد المؤشرات الموافقة (1-4)
    price: float; bid: float = 0.0; ask: float = 0.0

@dataclass
class Position:
    symbol: str; side: PosSide; entry: float; qty: float
    leverage: int; sl: float; tp: float; strategy: str
    opened: float = field(default_factory=time.time)
    sl_id: str = ""; tp_id: str = ""
    highest_lowest: float = 0.0
    trailing_activated: bool = False
    partial_exited: bool = False
    partial_exited: bool = False

    def pnl_pct(self, p):
        return (p-self.entry)/self.entry if self.side==PosSide.LONG else (self.entry-p)/self.entry

    def pnl_usdt(self, p):
        return self.pnl_pct(p) * self.entry * self.qty * self.leverage

# ═══════════════════════════════════════════════════════
# 🔌  ConnectionManager
# ═══════════════════════════════════════════════════════
class ConnectionManager:
    def __init__(self):
        self.client = None; self.bm = None; self._ok = False

    async def connect(self):
        logger.info("Connecting to Binance Futures Testnet...")
        self.client = await AsyncClient.create(API_KEY, API_SECRET, testnet=True)
        self.bm     = BinanceSocketManager(self.client)
        self._ok    = True
        logger.info("✅ Connected")

    async def disconnect(self):
        if self.client: await self.client.close_connection()
        self._ok = False; logger.info("🔌 Disconnected")

    @property
    def ok(self): return self._ok

# ═══════════════════════════════════════════════════════
# 📊  MarketDataHub
# ═══════════════════════════════════════════════════════
class MarketDataHub:
    def __init__(self, conn: ConnectionManager):
        self.conn     = conn
        self.sem      = asyncio.Semaphore(MAX_STREAMS)
        self.candles  = {s: deque(maxlen=CANDLE_BUF) for s in SYMBOLS}
        self.tickers  : dict[str, Ticker] = {}
        self.steps    : dict[str, float] = {s: 0.001 for s in SYMBOLS}
        self.ticks    : dict[str, int]   = {s: 2     for s in SYMBOLS}
        self._cbs     = []

    def subscribe(self, cb): self._cbs.append(cb)

    async def _notify(self, t: Ticker):
        for cb in self._cbs:
            try: await cb(t)
            except Exception as e: logger.error(f"CB error [{t.symbol}]: {e}")

    async def load_filters(self):
        logger.info("⏳ Loading exchange filters...")
        try:
            info = await self.conn.client.futures_exchange_info()
            for si in info.get('symbols', []):
                s = si['symbol']
                if s not in self.steps: continue
                for f in si.get('filters', []):
                    if f['filterType'] == 'LOT_SIZE':
                        step = float(f['stepSize'])
                        if step > 0:
                            self.steps[s] = step
                            self.ticks[s] = max(0, round(-math.log10(step)))
                    if f['filterType'] == 'PRICE_FILTER':
                        tick = float(f['tickSize'])
                        if tick > 0:
                            self.ticks[s] = max(0, round(-math.log10(tick)))
            logger.info("✅ Filters loaded")
        except Exception as e:
            logger.warning(f"Filters failed: {e}")

    async def prefetch(self):
        logger.info("⏳ Prefetching 200 candles...")
        sem = asyncio.Semaphore(10)
        async def _f(s):
            async with sem:
                try:
                    kl = await self.conn.client.futures_klines(symbol=s, interval='1m', limit=CANDLE_BUF)
                    for k in kl:
                        self.candles[s].append(Candle(k[0],float(k[1]),float(k[2]),float(k[3]),float(k[4]),float(k[5]),True))
                except Exception as e:
                    logger.warning(f"Prefetch [{s}]: {e}")
                await asyncio.sleep(0.05)
        await asyncio.gather(*[_f(s) for s in SYMBOLS])
        ready = sum(1 for s in SYMBOLS if len(self.candles[s]) >= 60)
        logger.info(f"✅ Prefetch done — {ready}/{len(SYMBOLS)} ready")

    async def _stream(self, symbol: str):
        while True:
            try:
                streams = [f"{symbol.lower()}@ticker", f"{symbol.lower()}@kline_1m"]
                async with self.conn.bm.multiplex_socket(streams) as ws:
                    while True:
                        msg  = await ws.recv()
                        data = msg.get('data', {}); ev = data.get('e', '')
                        if ev == '24hrTicker':
                            t = Ticker(symbol, float(data.get('c',0)), float(data.get('b',0)),
                                       float(data.get('a',0)), float(data.get('v',0)), float(data.get('P',0)))
                            self.tickers[symbol] = t
                            await self._notify(t)
                        elif ev == 'kline':
                            k = data['k']
                            c = Candle(k['t'],float(k['o']),float(k['h']),float(k['l']),float(k['c']),float(k['v']),k['x'])
                            buf = self.candles[symbol]
                            if c.closed: buf.append(c)
                            elif buf and not buf[-1].closed: buf[-1] = c
                            else: buf.append(c)
            except asyncio.CancelledError: return
            except Exception as e:
                logger.warning(f"Stream [{symbol}]: {e} — retry {RECONNECT_SEC}s")
                await asyncio.sleep(RECONNECT_SEC)

    async def start(self):
        await self.load_filters()
        await self.prefetch()
        logger.info(f"📡 Streaming {len(SYMBOLS)} pairs")
        tasks = []
        for s in SYMBOLS:
            tasks.append(asyncio.create_task(self._stream(s)))
            await asyncio.sleep(0.2)
        await asyncio.gather(*tasks)

    def closes(self, s): return [c.c for c in self.candles[s]]
    def volumes(self, s): return [c.v for c in self.candles[s]]
    def fix_qty(self, s, q):
        step = self.steps[s]
        q = math.floor(q / step) * step
        return round(q, max(0, round(-math.log10(step))) if step < 1 else 0)
    def fix_price(self, s, p):
        prec = self.ticks[s]
        return round(p, prec)

    def adx(self, s, period=14) -> float:
        """Average Directional Index — قوة الاتجاه"""
        candles = list(self.candles[s])
        if len(candles) < period + 1: return 0.0
        trs, plus_dms, minus_dms = [], [], []
        for i in range(1, len(candles)):
            h,l,pc = candles[i].h, candles[i].l, candles[i-1].c
            ph,pl  = candles[i-1].h, candles[i-1].l
            trs.append(max(h-l, abs(h-pc), abs(l-pc)))
            up = h - ph; dn = pl - l
            plus_dms.append(up  if up > dn  and up  > 0 else 0)
            minus_dms.append(dn if dn > up  and dn  > 0 else 0)
        atr = sum(trs[-period:]) / period
        if atr == 0: return 0.0
        pdi = (sum(plus_dms[-period:])  / period / atr) * 100
        mdi = (sum(minus_dms[-period:]) / period / atr) * 100
        return abs(pdi - mdi) / (pdi + mdi + 1e-9) * 100

    def adx(self, s, period=14) -> float:
        """Average Directional Index — قوة الاتجاه"""
        candles = list(self.candles[s])
        if len(candles) < period + 1: return 0.0
        trs, plus_dms, minus_dms = [], [], []
        for i in range(1, len(candles)):
            h,l,pc = candles[i].h, candles[i].l, candles[i-1].c
            ph,pl  = candles[i-1].h, candles[i-1].l
            trs.append(max(h-l, abs(h-pc), abs(l-pc)))
            up = h - ph; dn = pl - l
            plus_dms.append(up  if up > dn  and up  > 0 else 0)
            minus_dms.append(dn if dn > up  and dn  > 0 else 0)
        atr = sum(trs[-period:]) / period
        if atr == 0: return 0.0
        pdi = (sum(plus_dms[-period:])  / period / atr) * 100
        mdi = (sum(minus_dms[-period:]) / period / atr) * 100
        return abs(pdi - mdi) / (pdi + mdi + 1e-9) * 100

# ═══════════════════════════════════════════════════════
# 🧠  LearningEngine
# ═══════════════════════════════════════════════════════
class LearningEngine:
    def __init__(self):
        self.w      = {s: 1.0 for s in SYMBOLS}
        self.wins   = {s: 0 for s in SYMBOLS}
        self.loss   = {s: 0 for s in SYMBOLS}
        self.streak = {s: deque(maxlen=5) for s in SYMBOLS}
        self._load()

    def _load(self):
        if not os.path.exists(WGT_FILE): return
        try:
            d = json.load(open(WGT_FILE))
            for s in SYMBOLS:
                self.w[s]    = d.get('w',{}).get(s, 1.0)
                self.wins[s] = d.get('wins',{}).get(s, 0)
                self.loss[s] = d.get('loss',{}).get(s, 0)
                self.streak[s].extend(d.get('streak',{}).get(s, []))
            logger.info(f"🧠 Loaded weights for {len(SYMBOLS)} symbols")
        except Exception as e: logger.warning(f"Weights load: {e}")

    def save(self):
        try:
            json.dump({
                'w': self.w, 'wins': self.wins, 'loss': self.loss,
                'streak': {s: list(self.streak[s]) for s in SYMBOLS}
            }, open(WGT_FILE,'w'), indent=2)
        except Exception as e: logger.warning(f"Weights save: {e}")

    def update(self, s, pnl):
        old = self.w[s]
        if pnl > 0:
            self.w[s] = min(2.0, old + 0.10 * (1 + pnl*10))
            self.wins[s] += 1
            self.streak[s].append(1)
        else:
            self.w[s] = max(0.2, old - 0.15 * (1 + abs(pnl)*10))
            self.loss[s] += 1
            self.streak[s].append(0)
        self.save()
        logger.info(f"📚 Learn [{s}]: pnl={pnl:+.3%} w {old:.2f}→{self.w[s]:.2f} (W:{self.wins[s]} L:{self.loss[s]})")

    def allow(self, s): return self.w[s] >= 0.25
    def size_mult(self, s):
        w = self.w[s]
        if w < 0.4:   base = 0.5
        elif w < 0.8: base = 0.8
        elif w < 1.5: base = 1.0
        else:         base = 1.5
        # تعديل بناءً على سلسلة الأداء الأخير
        recent = list(self.streak[s])
        if len(recent) >= 3:
            wr = sum(recent) / len(recent)
            if wr >= 0.8:   base = min(base * 1.3, 1.5)  # سلسلة ربح → زيادة
            elif wr <= 0.2: base = max(base * 0.7, 0.3)  # سلسلة خسارة → تخفيض
        return base
    def early_exit(self, s): return self.w[s] <= 0.25

# ═══════════════════════════════════════════════════════
# 💾  StateTracker
# ═══════════════════════════════════════════════════════
class StateTracker:
    def __init__(self):
        self.pos: dict[str, Position] = {}
        self.closed_pnl = 0.0; self.trades = 0
        self._load()

    def _load(self):
        if not os.path.exists(POS_FILE): return
        try:
            d = json.load(open(POS_FILE))
            for s, p in d.get('positions', {}).items():
                self.pos[s] = Position(
                    symbol=p['symbol'], side=PosSide(p['side']),
                    entry=p['entry'], qty=p['qty'], leverage=p['leverage'],
                    sl=p['sl'], tp=p['tp'], strategy=p['strategy'],
                    opened=p.get('opened', time.time()),
                    sl_id=p.get('sl_id',''), tp_id=p.get('tp_id',''),
                )
                self.pos[s].highest_lowest = p.get('hl', self.pos[s].entry)
                self.pos[s].trailing_activated = p.get('trail', False)
            self.closed_pnl = d.get('closed_pnl', 0.0)
            self.trades     = d.get('trades', 0)
            if self.pos: logger.info(f"💾 Loaded {len(self.pos)} positions")
        except Exception as e: logger.warning(f"Positions load: {e}")

    def _save(self):
        try:
            json.dump({
                'closed_pnl': self.closed_pnl, 'trades': self.trades,
                'positions': {s: {
                    'symbol': p.symbol, 'side': p.side.value,
                    'entry': p.entry, 'qty': p.qty, 'leverage': p.leverage,
                    'sl': p.sl, 'tp': p.tp, 'strategy': p.strategy,
                    'opened': p.opened, 'sl_id': p.sl_id, 'tp_id': p.tp_id,
                    'hl': p.highest_lowest, 'trail': p.trailing_activated
                } for s, p in self.pos.items()}
            }, open(POS_FILE,'w'), indent=2)
        except Exception as e: logger.warning(f"Positions save: {e}")

    def has(self, s): return s in self.pos
    def count(self): return len(self.pos)
    def can_open(self): return self.count() < MAX_POSITIONS

    def add(self, p: Position):
        self.pos[p.symbol] = p; self._save()
        logger.info(f"📂 Opened: {p.symbol} {p.side.value} @ {p.entry:.4f} x{p.leverage} qty={p.qty}")

    def remove(self, s, exit_price) -> Optional[float]:
        p = self.pos.pop(s, None)
        if p:
            pnl = p.pnl_pct(exit_price)
            self.closed_pnl += p.pnl_usdt(exit_price)
            self.trades += 1; self._save()
            logger.info(f"📁 Closed: {s} pnl={pnl:+.3%} | total={self.closed_pnl:+.2f} USDT")
            return pnl
        return None

    def upnl(self, tickers):
        return sum(p.pnl_usdt(tickers[s].price) for s,p in self.pos.items() if s in tickers)

    def summary(self, tickers):
        return f"Open={self.count()} | Trades={self.trades} | Closed={self.closed_pnl:+.2f} | uPnL={self.upnl(tickers):+.2f}"

# ═══════════════════════════════════════════════════════
# 📐  مؤشرات فنية
# ═══════════════════════════════════════════════════════
def ema(p, n):
    if len(p) < n: return []
    k = 2/(n+1); r = [sum(p[:n])/n]
    for x in p[n:]: r.append(x*k + r[-1]*(1-k))
    return r

def sma(p, n):
    if len(p) < n: return []
    return [sum(p[i:i+n])/n for i in range(len(p)-n+1)]

def rsi_val(p, n=14):
    if len(p) < n+1: return None
    d = [p[i+1]-p[i] for i in range(len(p)-1)]
    g = [x for x in d[-n:] if x>0]; l = [-x for x in d[-n:] if x<0]
    ag = sum(g)/n if g else 0; al = sum(l)/n if l else 1e-9
    return 100 - 100/(1+ag/al)

def macd_val(p):
    if len(p) < 35: return None, None, None
    f = ema(p, 12); s = ema(p, 26)
    ml = [f[-len(s)+i] - s[i] for i in range(len(s))]
    sg = ema(ml, 9)
    if not sg: return None, None, None
    return ml[-1], sg[-1], ml[-1]-sg[-1]

def stoch_rsi_val(p, n=14):
    if len(p) < n*2: return None, None
    rv = []
    for i in range(n, len(p)+1):
        r = rsi_val(p[max(0,i-n-1):i], n)
        if r is not None: rv.append(r)
    if len(rv) < n: return None, None
    seg = rv[-n:]; lo,hi = min(seg),max(seg)
    if hi==lo: return 50.0, 50.0
    ks = [(rv[i]-min(rv[i-n:i]))/(max(rv[i-n:i])-min(rv[i-n:i])+1e-9)*100
          for i in range(n, len(rv))]
    if len(ks) < 3: return 50.0, 50.0
    k = sum(ks[-3:])/3
    d = sum(ks[-6:-3])/3 if len(ks)>=6 else k
    return k, d

def calc_atr(candles, n=ATR_PERIOD):
    if len(candles) < n+1: return 0.0
    trs = [max(candles[i].h-candles[i].l,
               abs(candles[i].h-candles[i-1].c),
               abs(candles[i].l-candles[i-1].c))
           for i in range(1, len(candles))]
    return sum(trs[-n:])/n

# ═══════════════════════════════════════════════════════
# 🔀  الاستراتيجيات
# ═══════════════════════════════════════════════════════
class GridStrategy:
    """Grid: نطاق ±2%، يشتري تحت المتوسط، يبيع فوقه — x9 دائماً"""
    def analyze(self, closes, price) -> Optional[tuple]:
        if len(closes) < 10: return None
        mid = sum(closes[-10:]) / 10
        lo  = mid * 0.98
        hi  = mid * 1.02
        if hi <= lo: return None
        pos = (price - lo) / (hi - lo)  # 0..1
        # تحت المتوسط (pos < 0.5) → شراء
        # فوق المتوسط (pos > 0.5) → بيع
        if pos < 0.40:
            strength = 1.0 - pos
            return (Sig.BUY,  1, strength)
        elif pos > 0.60:
            strength = pos
            return (Sig.SELL, 1, strength)
        return None

class EMAStrategy:
    """EMA 9/21: تقاطع + اتجاه مستمر"""
    def analyze(self, closes, price) -> Optional[tuple]:
        if len(closes) < 23: return None
        f = ema(closes, 9); s = ema(closes, 21)
        if len(f)<2 or len(s)<2: return None
        gap = abs(f[-1]-s[-1])/(s[-1]+1e-9)
        cross_up   = f[-2]<s[-2] and f[-1]>s[-1]
        cross_down = f[-2]>s[-2] and f[-1]<s[-1]
        trend_up   = f[-1]>s[-1] and f[-2]>s[-2] and gap>0.001
        trend_down = f[-1]<s[-1] and f[-2]<s[-2] and gap>0.001
        if cross_up:    return (Sig.BUY,  1, min(gap*200,1.0))
        if cross_down:  return (Sig.SELL, 1, min(gap*200,1.0))
        if trend_up:    return (Sig.BUY,  1, min(gap*100,0.6))
        if trend_down:  return (Sig.SELL, 1, min(gap*100,0.6))
        return None

class MultiMAStrategy:
    """RSI + MACD + EMA + Stoch: يحسب score 0-4"""
    def analyze(self, closes, price) -> Optional[tuple]:
        if len(closes) < 60: return None
        e9=ema(closes,9); e21=ema(closes,21); s50=sma(closes,50)
        if not e9 or not e21 or not s50: return None
        r = rsi_val(closes)
        ml, sg, hist = macd_val(closes)
        k, d = stoch_rsi_val(closes)

        bull = [
            e9[-1] > e21[-1] > s50[-1],
            r is not None and r < 50,
            hist is not None and hist > 0 and ml > sg,
            k is not None and k < 40,
        ]
        bear = [
            e9[-1] < e21[-1] < s50[-1],
            r is not None and r > 50,
            hist is not None and hist < 0 and ml < sg,
            k is not None and k > 60,
        ]
        bs = sum(bull); ss = sum(bear)
        if bs >= 2 and bs > ss:
            return (Sig.BUY,  bs, bs/4)
        if ss >= 2 and ss > bs:
            return (Sig.SELL, ss, ss/4)
        return None

class VolumeStrategy:
    """Volume: حجم عالٍ + اتجاه واضح"""
    def analyze(self, closes, volumes, candles, price) -> Optional[tuple]:
        if len(closes)<22 or not candles: return None
        avg = sum(volumes[-20:])/20
        if avg == 0: return None
        vr = volumes[-1]/avg
        lc = candles[-1]
        if vr > 2.0:
            if lc.c > (lc.h+lc.l)/2 and len(closes)>=3 and closes[-1]>closes[-2]>closes[-3]:
                return (Sig.BUY,  1, min(vr/3,1.0))
            if lc.c < (lc.h+lc.l)/2 and len(closes)>=3 and closes[-1]<closes[-2]<closes[-3]:
                return (Sig.SELL, 1, min(vr/3,1.0))
        return None

# ═══════════════════════════════════════════════════════
# 🔀  StrategyRouter — القلب
# ═══════════════════════════════════════════════════════
class StrategyRouter:
    def __init__(self, hub: MarketDataHub, le: LearningEngine):
        self.hub    = hub; self.le = le
        self.grid   = GridStrategy()
        self.ema_s  = EMAStrategy()
        self.multi  = MultiMAStrategy()
        self.volume = VolumeStrategy()

    def route(self, symbol: str, price: float) -> Optional[Signal]:
        closes  = self.hub.closes(symbol)
        volumes = self.hub.volumes(symbol)
        candles = list(self.hub.candles[symbol])
        ticker  = self.hub.tickers.get(symbol)
        if not closes: return None
        w = self.le.w.get(symbol, 1.0)

        def mk(side, strat, score):
            return Signal(symbol, side, strat, score, price,
                          ticker.bid if ticker else 0,
                          ticker.ask if ticker else 0)

        # ── Grid: تنفَّذ فوراً بمجرد الإشارة ──
        if w >= 0.25:
            try:
                gr = self.grid.analyze(closes, price)
                if gr:
                    return mk(gr[0], "grid", 1)
            except: pass

        # ── باقي الاستراتيجيات ──
        if not self.le.allow(symbol):
            return None

        results = []
        for fn in [
            lambda: self.ema_s.analyze(closes, price),
            lambda: self.multi.analyze(closes, price),
            lambda: self.volume.analyze(closes, volumes, candles, price),
        ]:
            try:
                r = fn()
                if r: results.append(r)
            except: pass

        if not results: return None

        buy_score  = sum(r[1] for r in results if r[0]==Sig.BUY)
        sell_score = sum(r[1] for r in results if r[0]==Sig.SELL)

        if buy_score == 0 and sell_score == 0: return None
        if buy_score > 0 and sell_score > 0 and abs(buy_score-sell_score) <= 1: return None

        if buy_score > sell_score:
            return mk(Sig.BUY,  "mixed", buy_score)
        else:
            return mk(Sig.SELL, "mixed", sell_score)

# ═══════════════════════════════════════════════════════
# 🛡️  RiskManager
# ═══════════════════════════════════════════════════════
class RiskManager:
    def __init__(self, hub: MarketDataHub, le: LearningEngine):
        self.hub = hub; self.le = le

    def leverage(self, sig: Signal) -> int:
        if sig.strategy == "grid":
            return GRID_LEVERAGE
        score = sig.score
        if score >= 4: return LEV_4OF4
        if score >= 3: return LEV_3OF4
        return LEV_2OF4

    def qty(self, sym, price, balance, lev) -> float:
        notional = max(balance * RISK_PCT * lev * self.le.size_mult(sym), MIN_NOTIONAL)
        q = notional / price
        q = self.hub.fix_qty(sym, q)
        # تأكد من الحد الأدنى بعد التقريب
        while q * price < MIN_NOTIONAL:
            q += self.hub.steps[sym]
            q  = self.hub.fix_qty(sym, q)
        return q

    def sl_tp(self, sym, side, price, candles):
        atr = calc_atr(candles)
        min_atr = price * MIN_SL_PCT
        atr = max(atr, min_atr)
        if side == Sig.BUY:
            sl = self.hub.fix_price(sym, price - ATR_SL_MULT * atr)
            tp = self.hub.fix_price(sym, price + ATR_TP_MULT * atr)
            sl = min(sl, self.hub.fix_price(sym, price * (1 - MIN_SL_PCT)))
            tp = max(tp, self.hub.fix_price(sym, price * (1 + MIN_SL_PCT*2)))
        else:
            sl = self.hub.fix_price(sym, price + ATR_SL_MULT * atr)
            tp = self.hub.fix_price(sym, price - ATR_TP_MULT * atr)
            sl = max(sl, self.hub.fix_price(sym, price * (1 + MIN_SL_PCT)))
            tp = min(tp, self.hub.fix_price(sym, price * (1 - MIN_SL_PCT*2)))
        return sl, tp

    def check(self, pos: Position, price) -> Optional[str]:
        if pos.side == PosSide.LONG:
            if price <= pos.sl: return "SL"
            if price >= pos.tp: return "TP"
        else:
            if price >= pos.sl: return "SL"
            if price <= pos.tp: return "TP"
        return None

# ═══════════════════════════════════════════════════════
# 💼  OrderExecutor
# ═══════════════════════════════════════════════════════
class OrderExecutor:
    def __init__(self, conn: ConnectionManager, risk: RiskManager, state: StateTracker):
        self.conn = conn; self.risk = risk; self.state = state
        self._lock = asyncio.Lock()
        self.balance = BALANCE_USDT
        self._pending: dict[str, float] = {}
        self._blacklist: set[str] = set()  # عملات مغلقة في Testnet

    async def refresh_balance(self):
        try:
            acc = await self.conn.client.futures_account()
            self.balance = float(acc['totalWalletBalance'])
            logger.debug(f"💰 Balance: {self.balance:.2f}")
        except Exception as e: logger.warning(f"Balance: {e}")

    async def open(self, sig: Signal):
        sym = sig.symbol
        if sym in self._blacklist: return
        async with self._lock:
            if self.state.has(sym) or not self.state.can_open(): return

        lev     = self.risk.leverage(sig)
        qty     = self.risk.qty(sym, sig.price, self.balance, lev)
        candles = list(self.conn.bm._conns.get(sym, {}).get('candles', [])) if False else \
                  list(getattr(self.risk.hub, 'candles', {}).get(sym, []))
        sl, tp  = self.risk.sl_tp(sym, sig.side, sig.price, candles)

        if qty <= 0: return

        # تحقق من صحة SL/TP
        if sig.side == Sig.BUY:
            if sl >= sig.price or tp <= sig.price:
                logger.warning(f"⚠️ Invalid SL/TP [{sym}] BUY: sl={sl} tp={tp} price={sig.price}")
                return
        else:
            if sl <= sig.price or tp >= sig.price:
                logger.warning(f"⚠️ Invalid SL/TP [{sym}] SELL: sl={sl} tp={tp} price={sig.price}")
                return

        order_side = SIDE_BUY if sig.side == Sig.BUY else SIDE_SELL
        pos_side   = PosSide.LONG if sig.side == Sig.BUY else PosSide.SHORT

        try:
            await self.conn.client.futures_change_leverage(symbol=sym, leverage=lev)
            await self.conn.client.futures_create_order(
                symbol=sym, side=order_side, type=ORDER_TYPE_MARKET, quantity=qty)
            logger.info(f"✅ {sym} {sig.side.value} qty={qty} lev={lev}x strat={sig.strategy}(score={sig.score})")

            pos = Position(sym, pos_side, sig.price, qty, lev, sl, tp, sig.strategy)
            pos.highest_lowest = sig.price
            self.state.add(pos)

            close_side = SIDE_SELL if sig.side==Sig.BUY else SIDE_BUY
            try:
                sl_o = await self.conn.client.futures_create_order(
                    symbol=sym, side=close_side, type=FUTURE_ORDER_TYPE_STOP_MARKET,
                    stopPrice=sl, closePosition=True, timeInForce="GTE_GTC")
                tp_o = await self.conn.client.futures_create_order(
                    symbol=sym, side=close_side, type=FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
                    stopPrice=tp, closePosition=True, timeInForce="GTE_GTC")
                pos.sl_id = str(sl_o.get('orderId',''))
                pos.tp_id = str(tp_o.get('orderId',''))
                self.state._save()
                logger.info(f"🛡️  SL={sl:.4f} TP={tp:.4f} [{sym}]")
            except Exception as e:
                logger.warning(f"SL/TP failed [{sym}]: {e}")
        except Exception as e:
            err = str(e)
            if "-4141" in err:
                logger.warning(f"⛔ [{sym}] Symbol closed on Testnet — blacklisting")
                self._blacklist.add(sym)
            else:
                logger.error(f"Order failed [{sym}]: {e}")

    async def close(self, sym, reason, price):
        if not self.state.has(sym): return
        pos = self.state.pos[sym]
        close_side = SIDE_SELL if pos.side==PosSide.LONG else SIDE_BUY
        try:
            await self.conn.client.futures_create_order(
                symbol=sym, side=close_side, type=ORDER_TYPE_MARKET,
                quantity=pos.qty, reduceOnly=True)
            logger.info(f"🔴 Closed [{sym}] reason={reason} @ {price:.4f}")
        except Exception as e:
            if "-2022" in str(e): logger.info(f"🔴 [{sym}] closed by exchange @ {price:.4f}")
            else: logger.error(f"Close failed [{sym}]: {e}")
        finally:
            pnl = self.state.remove(sym, price)
            if pnl is not None: self._pending[sym] = pnl

    async def partial_exit(self, pos: Position, price: float) -> bool:
        """إغلاق 50% من المركز لحجز الربح"""
        sym = pos.symbol
        half = self.risk.hub.fix_qty(sym, pos.qty / 2)
        if half <= 0: return False
        close_side = SIDE_SELL if pos.side == PosSide.LONG else SIDE_BUY
        try:
            await self.conn.client.futures_create_order(
                symbol=sym, side=close_side, type=ORDER_TYPE_MARKET,
                quantity=half, reduceOnly=True)
            pos.qty -= half
            self.state._dirty = True; self.state._save()
            logger.info(f"✂️ Partial exit [{sym}]: {half} @ {price:.4f} | remaining={pos.qty}")
            return True
        except Exception as e:
            logger.warning(f"Partial exit failed [{sym}]: {e}")
            return False

    async def update_sl(self, pos: Position, new_sl: float):
        sym = pos.symbol
        try:
            # إلغاء كل الأوامر المفتوحة للرمز (يحل -4130 نهائياً)
            try:
                await self.conn.client.futures_cancel_all_open_orders(symbol=sym)
            except Exception as e:
                logger.debug(f"Cancel all orders [{sym}]: {e}")
            pos.tp_id = ""; pos.sl_id = ""
            await asyncio.sleep(0.2)

            # تحقق أن المركز لا يزال مفتوحاً
            try:
                info = await self.conn.client.futures_position_information(symbol=sym)
                if not info or float(info[0].get('positionAmt', 0)) == 0:
                    if self.state.has(sym):
                        self.state.remove(sym, pos.entry)
                    return
            except: pass

            close_side = SIDE_SELL if pos.side==PosSide.LONG else SIDE_BUY
            o = await self.conn.client.futures_create_order(
                symbol=sym, side=close_side,
                type=FUTURE_ORDER_TYPE_STOP_MARKET,
                stopPrice=new_sl, closePosition=True,
                timeInForce="GTE_GTC")
            pos.sl_id = str(o.get('orderId',''))
            self.state._dirty = True
            self.state._save()
        except Exception as e:
            logger.warning(f"SL update failed [{sym}]: {e}")

# ═══════════════════════════════════════════════════════
# 😱  Fear & Greed
# ═══════════════════════════════════════════════════════
class FGSensor:
    def __init__(self):
        self._v = 50.0; self._ts = 0.0; self._sess = None

    async def _sess_get(self):
        if not self._sess or self._sess.closed:
            self._sess = aiohttp.ClientSession()
        return self._sess

    async def get(self) -> float:
        if time.time() - self._ts < FNG_CACHE_SEC: return self._v
        try:
            s = await self._sess_get()
            async with s.get("https://api.alternative.me/fng/?limit=1",
                             timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    d = await r.json()
                    self._v  = float(d['data'][0]['value'])
                    self._ts = time.time()
                    logger.info(f"😱 F&G: {self._v:.0f} ({d['data'][0].get('value_classification','')})")
        except Exception as e: logger.debug(f"F&G: {e}")
        return self._v

    def ok_long(self, v): return v >= FNG_LONG_MIN
    def ok_short(self, v): return v <= FNG_SHORT_MAX
    async def close(self):
        if self._sess and not self._sess.closed: await self._sess.close()

# ═══════════════════════════════════════════════════════
# 📲  Telegram
# ═══════════════════════════════════════════════════════
class TG:
    def __init__(self):
        self.enabled = bool(TG_TOKEN and TG_CHAT_ID)
        self._sess = None

    async def _s(self):
        if not self._sess or self._sess.closed:
            self._sess = aiohttp.ClientSession()
        return self._sess

    async def send(self, txt):
        if not self.enabled: return
        try:
            s = await self._s()
            async with s.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                              json={"chat_id":TG_CHAT_ID,"text":txt,"parse_mode":"HTML"},
                              timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status != 200: logger.warning(f"TG: {r.status}")
        except Exception as e:
            logger.warning(f"TG failed: {e}")
            if self._sess and not self._sess.closed:
                try: await self._sess.close()
                except: pass
            self._sess = None

    async def opened(self, p: Position):
        e = "🟢" if p.side==PosSide.LONG else "🔴"
        await self.send(f"{e} <b>صفقة جديدة</b>\n{p.symbol} {p.side.value}\nسعر: {p.entry:.4f} × {p.leverage}x\nSL: {p.sl:.4f} | TP: {p.tp:.4f}\nاستراتيجية: {p.strategy}")

    async def closed(self, sym, pnl_pct, pnl_u, reason):
        e = "✅" if pnl_pct>0 else "❌"
        await self.send(f"{e} <b>إغلاق</b> {sym}\nالسبب: {reason}\nPnL: {pnl_pct:+.2%} ({pnl_u:+.2f} USDT)")

    async def close(self):
        if self._sess and not self._sess.closed: await self._sess.close()

# ═══════════════════════════════════════════════════════
# 🤖  HedgeBotV6
# ═══════════════════════════════════════════════════════
class HedgeBotV6:
    def __init__(self):
        self.conn   = ConnectionManager()
        self.hub    = MarketDataHub(self.conn)
        self.le     = LearningEngine()
        self.state  = StateTracker()
        self.risk   = RiskManager(self.hub, self.le)
        self.exec   = OrderExecutor(self.conn, self.risk, self.state)
        self.router = StrategyRouter(self.hub, self.le)
        self.tg     = TG()
        self.fg     = FGSensor()
        self._cd    : dict[str,float] = {}
        self._fng   = 50.0
        self._tick  = 0
        self._pending_orders: set[str] = set()  # منع تكرار الصفقة
        self._pending_orders: set[str] = set()  # منع تكرار الصفقات

    async def _trailing(self):
        while True:
            await asyncio.sleep(MONITOR_SEC)
            for sym, pos in list(self.state.pos.items()):
                t = self.hub.tickers.get(sym)
                if not t: continue
                price = t.price
                atr = calc_atr(list(self.hub.candles.get(sym,[])))
                if atr == 0: continue

                # تحديث highest_lowest
                if pos.side == PosSide.LONG:
                    if price > pos.highest_lowest: pos.highest_lowest = price
                else:
                    if price < pos.highest_lowest or pos.highest_lowest == 0:
                        pos.highest_lowest = price

                trail_dist = TRAILING_MULT * atr

                if pos.side == PosSide.LONG:
                    nsl = self.hub.fix_price(sym, pos.highest_lowest - trail_dist)
                    if not pos.trailing_activated:
                        if pos.highest_lowest - pos.entry >= TRAILING_ACTIVATE * atr:
                            # خروج جزئي 50% عند تفعيل Trailing
                            if not getattr(pos, 'partial_exited', False):
                                ok = await self.exec.partial_exit(pos, price)
                                if ok:
                                    pos.partial_exited = True
                                    await self.tg.send(f"✂️ <b>خروج جزئي 50%</b> [{sym}]")
                            pos.trailing_activated = True
                            pos.sl = nsl; self.state._save()
                            logger.info(f"🎯 Trail ON [{sym}]: SL→{nsl:.4f}")
                            await self.exec.update_sl(pos, nsl)
                    elif nsl > pos.sl:
                        old = pos.sl; pos.sl = nsl; self.state._save()
                        logger.info(f"📈 Trail [{sym}]: SL {old:.4f}→{nsl:.4f}")
                        await self.exec.update_sl(pos, nsl)
                else:
                    nsl = self.hub.fix_price(sym, pos.highest_lowest + trail_dist)
                    if not pos.trailing_activated:
                        if pos.entry - pos.highest_lowest >= TRAILING_ACTIVATE * atr:
                            if not getattr(pos, 'partial_exited', False):
                                ok = await self.exec.partial_exit(pos, price)
                                if ok:
                                    pos.partial_exited = True
                                    await self.tg.send(f"✂️ <b>خروج جزئي 50%</b> [{sym}]")
                            pos.trailing_activated = True
                            pos.sl = nsl; self.state._save()
                            logger.info(f"🎯 Trail ON [{sym}]: SL→{nsl:.4f}")
                            await self.exec.update_sl(pos, nsl)
                    elif nsl < pos.sl:
                        old = pos.sl; pos.sl = nsl; self.state._save()
                        logger.info(f"📉 Trail [{sym}]: SL {old:.4f}→{nsl:.4f}")
                        await self.exec.update_sl(pos, nsl)

    async def _monitor(self):
        while True:
            await asyncio.sleep(MONITOR_SEC)
            if not self.conn.ok: continue
            try:
                info = await self.conn.client.futures_position_information()
                open_ex = {p['symbol'] for p in info if float(p['positionAmt'])!=0}
                for sym in list(self.state.pos.keys()):
                    if sym not in open_ex:
                        # منع التكرار
                        if not self.state.has(sym): continue
                        t = self.hub.tickers.get(sym)
                        pos = self.state.pos[sym]
                        price = t.price if t else pos.entry
                        pct = pos.pnl_pct(price); usdt = pos.pnl_usdt(price)
                        logger.info(f"🔔 [{sym}] closed by exchange @ {price:.4f}")
                        pnl = self.state.remove(sym, price)
                        if pnl: self.le.update(sym, pnl)
                        self._cd[sym] = time.time()
                        self._pending_orders.discard(sym)
                        await self.tg.closed(sym, pct, usdt, "exchange_SL/TP")
            except Exception as e: logger.debug(f"Monitor: {e}")

    def trend_filter(self, symbol: str, side: Sig) -> bool:
        """فلتر EMA50 — لا تشتري تحت المتوسط، لا تبيع فوقه"""
        closes = self.hub.closes(symbol)
        if len(closes) < 50: return True   # بيانات غير كافية → اسمح
        e50   = ema(closes, 50)
        price = closes[-1]
        if side == Sig.BUY  and price < e50[-1]: return False
        if side == Sig.SELL and price > e50[-1]: return False
        return True

    def atr_ok(self, symbol: str, price: float) -> bool:
        """فلتر التقلب — تجنب السوق الراكدة (ATR < 0.5% من السعر)"""
        atr = calc_atr(list(self.hub.candles[symbol]))
        return atr > 0 and (atr / price) >= 0.005

    async def _on_ticker(self, t: Ticker):
        sym = t.symbol; price = t.price

        # تحديث التعلم
        pnl = self.exec._pending.pop(sym, None)
        if pnl is not None:
            self.le.update(sym, pnl); self._cd[sym] = time.time()

        # فحص المراكز
        if self.state.has(sym):
            pos = self.state.pos[sym]
            reason = self.risk.check(pos, price)

            # TP hit → فعّل Trailing
            if reason == "TP":
                atr = calc_atr(list(self.hub.candles.get(sym,[])))
                if atr > 0:
                    if pos.side==PosSide.LONG:
                        nsl = self.hub.fix_price(sym, price - TRAILING_MULT*atr)
                        if nsl > pos.sl:
                            old=pos.sl; pos.sl=nsl
                            pos.tp = price*(1+0.5)
                            logger.info(f"🎯 TP→Trail [{sym}]: SL {old:.4f}→{nsl:.4f}")
                            await self.exec.update_sl(pos, nsl)
                            await self.tg.send(f"🎯 <b>TP→Trailing</b> [{sym}]\nSL جديد: {nsl:.4f}")
                            return
                    else:
                        nsl = self.hub.fix_price(sym, price + TRAILING_MULT*atr)
                        if nsl < pos.sl:
                            old=pos.sl; pos.sl=nsl
                            pos.tp = price*0.01
                            logger.info(f"🎯 TP→Trail [{sym}]: SL {old:.4f}→{nsl:.4f}")
                            await self.exec.update_sl(pos, nsl)
                            await self.tg.send(f"🎯 <b>TP→Trailing</b> [{sym}]\nSL جديد: {nsl:.4f}")
                            return

            if reason is None and self.le.early_exit(sym): reason = "early_exit"
            if reason:
                pct = pos.pnl_pct(price); usdt = pos.pnl_usdt(price)
                await self.exec.close(sym, reason, price)
                await self.tg.closed(sym, pct, usdt, reason)
            return

        # cooldown + منع التكرار
        if time.time() - self._cd.get(sym, 0) < COOLDOWN_SEC: return
        if sym in self._pending_orders: return

        # فلتر التقلب — لا تتداول في السوق الراكدة
        if not self.atr_ok(sym, price): return

        # ADX: إذا اتجاه قوي (>25) → اعطِ أولوية للدخول
        adx_val = self.hub.adx(sym)

        # ADX: إذا اتجاه قوي (>25) → اعطِ أولوية للدخول
        adx_val = self.hub.adx(sym)

        # إشارة
        sig = self.router.route(sym, price)
        if sig and adx_val > 25:
            logger.debug(f"💪 Strong trend [{sym}] ADX={adx_val:.1f}")
        if sig and adx_val > 25:
            logger.debug(f"💪 Strong trend [{sym}] ADX={adx_val:.1f}")
        if not sig or sig.side == Sig.HOLD: pass
        else:
            # فلتر EMA50 (على الجميع بما فيهم Grid)
            if not self.trend_filter(sym, sig.side):
                logger.debug(f"📊 Trend filter blocked [{sym}] {sig.side.value}")
                return

            # فلتر BTC EMA200 (ليس على Grid)
            if sig.strategy != "grid":
                btc_c = self.hub.closes("BTCUSDT")
                btc_t = self.hub.tickers.get("BTCUSDT")
                if len(btc_c) >= 200 and btc_t:
                    e200 = ema(btc_c, 200)
                    bull = btc_t.price > e200[-1]
                    if not bull and sig.side==Sig.BUY  and sym!="BTCUSDT": return
                    if bull  and sig.side==Sig.SELL and sym!="BTCUSDT": return

            # فلتر F&G
            if sig.side==Sig.BUY  and not self.fg.ok_long(self._fng):  return
            if sig.side==Sig.SELL and not self.fg.ok_short(self._fng): return

            before = self.state.count()
            self._pending_orders.add(sym)
            try:
                await self.exec.open(sig)
            finally:
                self._pending_orders.discard(sym)
            if self.state.count() > before and self.state.has(sym):
                await self.tg.opened(self.state.pos[sym])

        self._tick += 1
        if self._tick % 500 == 0:
            s = self.state.summary(self.hub.tickers)
            logger.info(f"📊 {s}")
            await self.exec.refresh_balance()
            self._fng = await self.fg.get()
            if self._tick % 5000 == 0: await self.tg.send(f"📊 <b>إحصاء</b>\n{s}")

    async def start(self):
        await self.conn.connect()
        self._fng = await self.fg.get()
        self.hub.subscribe(self._on_ticker)
        logger.info("🚀 HedgeBot v6 — Started")
        logger.info(f"   Pairs      : {len(SYMBOLS)}")
        logger.info(f"   Grid lev   : x{GRID_LEVERAGE}")
        logger.info(f"   EMA/Multi  : 4→x{LEV_4OF4} | 3→x{LEV_3OF4} | 2→x{LEV_2OF4}")
        logger.info(f"   Fear&Greed : {self._fng:.0f}")
        await self.tg.send(
            f"🚀 <b>HedgeBot v6 يعمل</b>\n"
            f"العملات: {len(SYMBOLS)}\n"
            f"Grid: x{GRID_LEVERAGE} | 4/4→x{LEV_4OF4} | 3/4→x{LEV_3OF4} | 2/4→x{LEV_2OF4}\n"
            f"F&G: {self._fng:.0f}"
        )
        asyncio.create_task(self._trailing())
        asyncio.create_task(self._monitor())
        await self.hub.start()

    async def stop(self):
        logger.info("🛑 Stopping...")
        for sym in list(self.state.pos.keys()):
            t = self.hub.tickers.get(sym)
            if t:
                pos = self.state.pos[sym]
                await self.exec.close(sym, "shutdown", t.price)
                await self.tg.closed(sym, pos.pnl_pct(t.price), pos.pnl_usdt(t.price), "shutdown")
        await self.tg.send(f"🛑 <b>البوت توقف</b>\nPnL: {self.state.closed_pnl:+.2f} USDT")
        await self.tg.close(); await self.fg.close()
        await self.conn.disconnect()
        logger.info(f"Final PnL: {self.state.closed_pnl:+.2f} USDT")

async def main():
    bot = HedgeBotV6()
    try:
        await bot.start()
    except KeyboardInterrupt:
        await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
