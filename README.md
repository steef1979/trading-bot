hedgebot.py

"""
╔══════════════════════════════════════════════════════════╗
║        HedgeBot v11 Fixed — نسخة الإنتاج المحسّنة        ║
╠══════════════════════════════════════════════════════════╣
║ ✅ إصلاح حجم الصفقة (استخدام الرصيد الفعلي)             ║
║ ✅ فلاتر Grid ذكية (منع SHORT في سوق صاعد)              ║
║ ✅ Anti-Revenge + time decay                            ║
║ ✅ فلاتر خاصة بكل عملة                                  ║
║ ✅ أمر /تحليل تفاعلي عبر Telegram                       ║
╚══════════════════════════════════════════════════════════╝
"""

import asyncio, json, logging, os, time, math, aiohttp, random
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from dotenv import load_dotenv
from binance import AsyncClient, BinanceSocketManager
from binance.enums import (
    SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET,
    FUTURE_ORDER_TYPE_STOP_MARKET,
)

# ═══════════════════════════════════════════════════════
# ⚙️ الإعدادات
# ═══════════════════════════════════════════════════════
load_dotenv()
API_KEY    = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")
if not API_KEY or not API_SECRET:
    raise EnvironmentError("❌ أضف BINANCE_API_KEY و BINANCE_API_SECRET في .env")

TG_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

SYMBOLS = [
    'BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','ADAUSDT',
    'XRPUSDT','DOTUSDT','LTCUSDT','LINKUSDT','MATICUSDT',
    'AVAXUSDT','UNIUSDT','ATOMUSDT','NEARUSDT','ALGOUSDT',
    'VETUSDT','FILUSDT','FTMUSDT','SANDUSDT',
    'MANAUSDT','EGLDUSDT','AXSUSDT','AAVEUSDT','THETAUSDT',
    'DOGEUSDT','TRXUSDT','BCHUSDT','XLMUSDT','INJUSDT',
    'APEUSDT','HBARUSDT','XTZUSDT','EOSUSDT','KLAYUSDT',
    'CHZUSDT','OPUSDT','ARBUSDT','APTUSDT','SUIUSDT',
    'WLDUSDT','RNDRUSDT','FETUSDT','IMXUSDT','SEIUSDT',
    'GALAUSDT','SNXUSDT','CRVUSDT','DYDXUSDT','QNTUSDT',
    'LDOUSDT','FXSUSDT','MINAUSDT','CFXUSDT','RUNEUSDT',
    'KAVAUSDT','GMXUSDT','LRCUSDT','ANKRUSDT','ENJUSDT',
    'BATUSDT','IOTXUSDT','ZENUSDT','ZILUSDT','ONEUSDT',
    'ICXUSDT','ONTUSDT','STORJUSDT','SUSHIUSDT','CTSIUSDT',
    'SKLUSDT','RVNUSDT','MKRUSDT','TAOUSDT','OCEANUSDT',
]

# ═══════════════════════════════════════════════════════
# 💰 إعدادات رأس المال (🆕 مصلحة)
# ═══════════════════════════════════════════════════════
# ⚠️ هذا الرقم يُحدَّث تلقائياً من refresh_balance()
# لكن نضع القيمة الأولية هنا
BALANCE_USDT             = 11635.0   # 🆕 رصيدك الفعلي
TOTAL_CAPITAL_ALLOCATION = 0.25      # 25% من الرصيد للتداول
MAX_POSITIONS            = 10        # 🆕 قلّلنا من 15 إلى 10 لزيادة حجم كل صفقة

# حساب تلقائي:
# 11635 × 25% = 2909 USDT إجمالي
# 2909 / 10 = 290 USDT للصفقة الواحدة
# × رافعة 10x = 2900 USDT حجم فعلي
# ربح 10% = 290 USDT بدل 16 USDT السابقة!

# --- إعدادات المخاطرة ---
RISK_PCT               = 0.02
MIN_NOTIONAL           = 100.0
GRID_LEVERAGE          = 5           # 🆕 قلّلنا رافعة Grid من 10 إلى 5 للأمان
MAX_LEVERAGE           = 20
ATR_PERIOD             = 14
ATR_SL_MULT            = 1.5
ATR_TRAIL_MULT         = 1.5
TRAILING_ACTIVATE      = 2.0
MIN_SL_PCT             = 0.02
COOLDOWN_SEC           = 60
CANDLE_BUF             = 500
RECONNECT_MIN_SEC      = 7
RECONNECT_MAX_SEC      = 15
MONITOR_SEC            = 15
FNG_LONG_MIN           = 5
FNG_SHORT_MAX          = 95
FNG_CACHE_SEC          = 300
POS_FILE               = "positions_final.json"
WGT_FILE               = "weights_final.json"
ADX_THRESHOLD          = 20
MIN_ATR_PCT            = 0.001
ALLOW_THRESHOLD        = 0.2
GRID_UPDATE_HOURS      = 6
MAX_DAILY_LOSS_PCT     = 0.05
COOLDOWN_AFTER_LOSS    = 3
ANTI_REVENGE_TIMEOUT_HOURS = 6

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[logging.FileHandler("bot_final.log", encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger("HedgeBot")

# ═══════════════════════════════════════════════════════
# 📦 هياكل البيانات
# ═══════════════════════════════════════════════════════
class Sig(Enum): BUY = "BUY"; SELL = "SELL"; HOLD = "HOLD"
class PosSide(Enum): LONG = "LONG"; SHORT = "SHORT"

@dataclass
class Candle:
    t: int; o: float; h: float; l: float; c: float; v: float; closed: bool

@dataclass
class Ticker:
    symbol: str; price: float; bid: float; ask: float
    volume: float; change: float; ts: float = field(default_factory=time.time)

@dataclass
class Signal:
    symbol: str; side: Sig; strategy: str; score: int; price: float
    filters_passed: int = 0; has_cross: bool = False; has_high_volume: bool = False
    bid: float = 0.0; ask: float = 0.0

@dataclass
class Position:
    symbol: str; side: PosSide; entry: float; qty: float; leverage: int; sl: float
    strategy: str; opened: float = field(default_factory=time.time)
    sl_id: str = ""; highest_lowest: float = 0.0
    trailing_activated: bool = False; partial_exited: bool = False
    def pnl_pct(self, p):
        return (p-self.entry)/self.entry*self.leverage if self.side==PosSide.LONG else (self.entry-p)/self.entry*self.leverage
    def pnl_usdt(self, p):
        return self.pnl_pct(p)*self.entry*self.qty/self.leverage

# ═══════════════════════════════════════════════════════
# 🔌 ConnectionManager
# ═══════════════════════════════════════════════════════
class ConnectionManager:
    def __init__(self): self.client = None; self.bm = None; self._ok = False
    async def connect(self):
        for i in range(5):
            try:
                self.client = await AsyncClient.create(API_KEY, API_SECRET, testnet=True)
                self.bm = BinanceSocketManager(self.client); self._ok = True
                logger.info("✅ Connected"); return
            except Exception as e: logger.warning(f"Connect {i+1}/5: {e}"); await asyncio.sleep(5)
        raise RuntimeError("❌ فشل الاتصال")
    async def disconnect(self):
        if self.client: await self.client.close_connection()
    @property
    def ok(self): return self._ok

# ═══════════════════════════════════════════════════════
# 📊 MarketDataHub
# ═══════════════════════════════════════════════════════
class MarketDataHub:
    def __init__(self, conn):
        self.conn = conn; self.candles = {s: deque(maxlen=CANDLE_BUF) for s in SYMBOLS}
        self.tickers: dict[str, Ticker] = {}
        self.steps = {s: 0.001 for s in SYMBOLS}; self.ticks = {s: 2 for s in SYMBOLS}
        self._cbs = []
    def subscribe(self, cb): self._cbs.append(cb)
    async def _notify(self, t):
        for cb in self._cbs:
            try: await cb(t)
            except: pass
    async def load_filters(self):
        info = await self.conn.client.futures_exchange_info()
        for si in info['symbols']:
            s = si['symbol']
            if s not in self.steps: continue
            for f in si['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    step = float(f['stepSize'])
                    if step > 0: self.steps[s] = step
                if f['filterType'] == 'PRICE_FILTER':
                    tick = float(f['tickSize'])
                    if tick > 0: self.ticks[s] = max(0, round(-math.log10(tick)))
    async def prefetch(self):
        sem = asyncio.Semaphore(15)
        async def _f(s):
            async with sem:
                try:
                    kl = await self.conn.client.futures_klines(symbol=s, interval='1m', limit=CANDLE_BUF)
                    for k in kl:
                        self.candles[s].append(Candle(k[0],float(k[1]),float(k[2]),float(k[3]),float(k[4]),float(k[5]),True))
                except: pass
                await asyncio.sleep(0.02)
        await asyncio.gather(*[_f(s) for s in SYMBOLS])
    async def _stream(self, symbol):
        while True:
            try:
                async with self.conn.bm.multiplex_socket([f"{symbol.lower()}@ticker", f"{symbol.lower()}@kline_1m"]) as ws:
                    while True:
                        msg = await ws.recv(); data = msg.get('data', {}); ev = data.get('e', '')
                        if ev == '24hrTicker':
                            self.tickers[symbol] = Ticker(symbol, float(data.get('c',0)), float(data.get('b',0)),
                                                           float(data.get('a',0)), float(data.get('v',0)), float(data.get('P',0)))
                            await self._notify(self.tickers[symbol])
                        elif ev == 'kline':
                            k = data['k']; c = Candle(k['t'],float(k['o']),float(k['h']),float(k['l']),float(k['c']),float(k['v']),k['x'])
                            buf = self.candles[symbol]
                            if c.closed: buf.append(c)
                            elif buf and not buf[-1].closed: buf[-1] = c
                            else: buf.append(c)
            except asyncio.CancelledError: return
            except Exception as e:
                wait = random.uniform(RECONNECT_MIN_SEC, RECONNECT_MAX_SEC)
                logger.warning(f"Stream {symbol}: retry in {wait:.1f}s"); await asyncio.sleep(wait)
    async def start(self):
        await self.load_filters(); await self.prefetch()
        tasks = []
        for i, s in enumerate(SYMBOLS):
            tasks.append(asyncio.create_task(self._stream(s)))
            await asyncio.sleep(0.3 if i < 30 else 0.6)
        logger.info(f"📡 {len(SYMBOLS)} streams started")
        await asyncio.gather(*tasks)
    def closes(self, s): return [c.c for c in self.candles[s]]
    def volumes(self, s): return [c.v for c in self.candles[s]]
    def fix_qty(self, s, q):
        step = self.steps[s]; q = max(math.floor(q/step)*step, step)
        return round(q, max(0, round(-math.log10(step))) if step<1 else 0)
    def fix_price(self, s, p): return round(p, self.ticks[s])
    def adx(self, s, period=14):
        candles = list(self.candles[s])
        if len(candles) < period*2+1: return 0.0
        trs, plus_dms, minus_dms = [], [], []
        for i in range(1, len(candles)):
            h,l,c = candles[i].h, candles[i].l, candles[i].c
            ph,pl,pc = candles[i-1].h, candles[i-1].l, candles[i-1].c
            trs.append(max(h-l, abs(h-pc), abs(l-pc)))
            up, dn = h-ph, pl-l
            plus_dms.append(up if up>dn and up>0 else 0.0)
            minus_dms.append(dn if dn>up and dn>0 else 0.0)
        def wilder(d, n):
            if len(d) < n: return 0.0
            v = sum(d[:n])
            for x in d[n:]: v = v - v/n + x
            return v
        atr = wilder(trs, period)
        if atr == 0: return 0.0
        pdi = wilder(plus_dms, period)/atr*100
        mdi = wilder(minus_dms, period)/atr*100
        dx = abs(pdi-mdi)/(pdi+mdi+1e-9)*100
        dx_list = []
        for i in range(period, len(trs)):
            a = wilder(trs[:i+1], period)
            if a == 0: continue
            dx_list.append(abs(wilder(plus_dms[:i+1],period)/a*100 - wilder(minus_dms[:i+1],period)/a*100)/((wilder(plus_dms[:i+1],period)+wilder(minus_dms[:i+1],period))/a*100+1e-9)*100)
        if not dx_list: return dx
        adx_v = sum(dx_list[:period])/period
        for x in dx_list[period:]: adx_v = (adx_v*(period-1)+x)/period
        return adx_v

# ═══════════════════════════════════════════════════════
# 📐 المؤشرات الفنية
# ═══════════════════════════════════════════════════════
def ema(p, n):
    if len(p) < n: return []
    k = 2/(n+1); r = [sum(p[:n])/n]
    for x in p[n:]: r.append(x*k + r[-1]*(1-k))
    return r
def rsi_val(p, n=14):
    if len(p) < n+1: return None
    d = [p[i+1]-p[i] for i in range(len(p)-1)]
    g = [x for x in d[-n:] if x>0]; l = [-x for x in d[-n:] if x<0]
    ag = sum(g)/n if g else 0; al = sum(l)/n if l else 1e-9
    return 100 - 100/(1+ag/al) if al != 0 else 100.0
def calc_atr(candles, n=14):
    if len(candles) < n+1: return 0.0
    trs = [max(candles[i].h-candles[i].l, abs(candles[i].h-candles[i-1].c), abs(candles[i].l-candles[i-1].c)) for i in range(1, len(candles))]
    return sum(trs[-n:])/n

# ═══════════════════════════════════════════════════════
# 🧠 FilterEngine
# ═══════════════════════════════════════════════════════
class FilterEngine:
    def __init__(self, hub): self.hub = hub
    def _compute(self, symbol, side, price):
        closes = self.hub.closes(symbol); volumes = self.hub.volumes(symbol)
        candles = list(self.hub.candles[symbol])
        btc_closes = self.hub.closes("BTCUSDT"); btc_ticker = self.hub.tickers.get("BTCUSDT")
        result = {'M50': False, 'B200': False, 'ATR': False, 'RSI': False, 'VOL': False, 'CRS': False, 'ADX': False}
        has_cross = False; has_high_volume = False
        if len(closes) >= 50:
            e50 = ema(closes, 50)[-1]
            if (side == Sig.BUY and closes[-1] > e50) or (side == Sig.SELL and closes[-1] < e50): result['M50'] = True
        if len(btc_closes) >= 200 and btc_ticker:
            e200 = ema(btc_closes, 200)[-1]; bull = btc_ticker.price > e200
            if (side == Sig.BUY and bull) or (side == Sig.SELL and not bull): result['B200'] = True
        atr = calc_atr(candles)
        if atr > 0 and (atr/price) >= MIN_ATR_PCT: result['ATR'] = True
        rsi = rsi_val(closes)
        if rsi and ((side == Sig.BUY and rsi < 70) or (side == Sig.SELL and rsi > 30)): result['RSI'] = True
        if len(volumes) >= 20:
            avg_vol = sum(volumes[-20:])/20
            if avg_vol > 0 and volumes[-1] > avg_vol*1.5: result['VOL'] = True; has_high_volume = True
        if len(closes) >= 23:
            ema9 = ema(closes, 9); ema21 = ema(closes, 21)
            if len(ema9) >= 3 and len(ema21) >= 3:
                golden = ema9[-2] < ema21[-2] and ema9[-1] > ema21[-1]
                death = ema9[-2] > ema21[-2] and ema9[-1] < ema21[-1]
                if (side == Sig.BUY and golden) or (side == Sig.SELL and death): result['CRS'] = True; has_cross = True
        if self.hub.adx(symbol) >= ADX_THRESHOLD: result['ADX'] = True
        count = sum(1 for v in result.values() if v)
        return result, count, has_cross, has_high_volume
    def count_filters(self, symbol, side, price):
        _, count, has_cross, has_high_volume = self._compute(symbol, side, price)
        return count, has_cross, has_high_volume
    def get_filters_dict(self, symbol, side, price):
        result, _, _, _ = self._compute(symbol, side, price)
        return result
    def leverage_by_filters(self, filters_count, has_cross=False, has_high_volume=False):
        if has_cross and has_high_volume: return 48
        if filters_count >= 7: return 20
        elif filters_count >= 5: return 12
        elif filters_count >= 3: return 5
        else: return 2

# ═══════════════════════════════════════════════════════
# 🎯 DynamicGridStrategy (🆕 مع فلاتر ذكية)
# ═══════════════════════════════════════════════════════
class DynamicGridStrategy:
    def __init__(self, hub): self.hub = hub; self.last_update = 0; self.regime = "neutral"

    def update_regime(self):
        now = time.time()
        if now - self.last_update < GRID_UPDATE_HOURS*3600: return
        self.last_update = now
        btc = self.hub.closes("BTCUSDT")
        lookback = min(1440, len(btc)-1)
        if lookback < 60: return
        change = (btc[-1] - btc[-lookback])/btc[-lookback]
        if change > 0.01: self.regime = "up"
        elif change < -0.01: self.regime = "down"
        else: self.regime = "neutral"
        logger.info(f"🔄 Grid Regime: {self.regime} (BTC 24h: {change:+.2%})")

    def _is_market_bullish(self):
        """🆕 فحص اتجاه السوق العام عبر BTC EMA200"""
        btc_closes = self.hub.closes("BTCUSDT")
        if len(btc_closes) < 200: return True  # افتراض صاعد عند عدم البيانات
        btc_e200 = ema(btc_closes, 200)[-1]
        return btc_closes[-1] > btc_e200

    def _check_short_filters(self, symbol, price):
        """
        🆕 فلاتر Grid SHORT الذكية
        يعيد True فقط إذا كانت الظروف مناسبة للشورت
        """
        closes = self.hub.closes(symbol)

        # فلتر 1: BTC يجب أن يكون تحت EMA200
        if self._is_market_bullish():
            logger.info(f"🚫 [GRID] منع SHORT على {symbol}: BTC فوق EMA200 (سوق صاعد)")
            return False

        # فلتر 2: RSI العملة يجب أن يكون فوق 55 (زخم هبوطي)
        rsi = rsi_val(closes) if len(closes) >= 15 else None
        if rsi is not None and rsi < 55:
            logger.info(f"🚫 [GRID] منع SHORT على {symbol}: RSI={rsi:.1f} < 55 (زخم شرائي)")
            return False

        # فلتر 3: السعر يجب أن يكون تحت EMA50 (اتجاه هبوطي)
        if len(closes) >= 50:
            sym_e50 = ema(closes, 50)[-1]
            if price > sym_e50:
                logger.info(f"🚫 [GRID] منع SHORT على {symbol}: السعر {price:.4f} فوق EMA50 {sym_e50:.4f}")
                return False

        # فلتر 4: لا شورت على العملات الممنوعة أبداً
        SHORT_BLACKLIST = ['DOGEUSDT', 'INJUSDT']
        if symbol in SHORT_BLACKLIST:
            logger.info(f"🚫 [GRID] منع SHORT على {symbol}: قائمة سوداء")
            return False

        return True

    def analyze(self, symbol, price):
        self.update_regime()
        closes = self.hub.closes(symbol)
        if len(closes) < 60: return None
        candles = list(self.hub.candles[symbol])[-1440:]
        if not candles: return None
        high = max(c.h for c in candles); low = min(c.l for c in candles)
        if high <= low: return None
        pos = (price - low) / (high - low)

        if self.regime == "up":
            # في السوق الصاعد: فقط LONG
            if pos < 0.30:
                return (Sig.BUY, "grid_long", 1.0 - pos)
            # ❌ لا شورت في سوق صاعد أبداً

        elif self.regime == "down":
            if pos > 0.70:
                # 🆕 فحص الفلاتر قبل فتح SHORT
                if not self._check_short_filters(symbol, price):
                    return None
                return (Sig.SELL, "grid_short", pos)

        elif self.regime == "neutral":
            if pos < 0.25:
                return (Sig.BUY, "grid_long", 1.0 - pos)
            elif pos > 0.75:
                # 🆕 فحص الفلاتر قبل فتح SHORT
                if not self._check_short_filters(symbol, price):
                    return None
                return (Sig.SELL, "grid_short", pos)

        return None

# ═══════════════════════════════════════════════════════
# 🛡️ TradingLogicOptimizer v2
# ═══════════════════════════════════════════════════════
class TradingLogicOptimizer:
    def __init__(self, hub, config=None):
        self.hub = hub
        self.config = config or {}
        self.last_trade_direction: dict[str, str] = {}
        self.last_trade_result: dict[str, str] = {}
        self.last_trade_time: dict[str, float] = {}
        self.cooldown_counter: dict[str, int] = {}
        self.trade_history: list[dict] = []

    def _get_rsi(self, symbol):
        closes = self.hub.closes(symbol)
        if len(closes) >= 15: return rsi_val(closes)
        return None

    def apply_global_blacklist(self, symbol, signal_direction):
        SHORT_BLACKLIST = ['DOGEUSDT', 'INJUSDT']
        if symbol in SHORT_BLACKLIST and signal_direction == 'SHORT':
            logger.info(f"⛔ [BLACKLIST] SHORT على {symbol} ممنوع.")
            return False
        return True

    def apply_cooldown(self, symbol):
        if symbol in self.cooldown_counter and self.cooldown_counter[symbol] > 0:
            logger.info(f"⏳ [COOLDOWN] {symbol}: متبقي {self.cooldown_counter[symbol]} تجاهل.")
            self.cooldown_counter[symbol] -= 1
            return False
        return True

    def apply_anti_revenge_logic(self, symbol, new_signal_direction):
        if symbol not in self.last_trade_direction or symbol not in self.last_trade_result:
            return True
        last_dir = self.last_trade_direction[symbol]
        last_res = self.last_trade_result[symbol]
        last_time = self.last_trade_time.get(symbol, 0)
        timeout_seconds = self.config.get('anti_revenge_timeout_hours', ANTI_REVENGE_TIMEOUT_HOURS) * 3600
        if time.time() - last_time > timeout_seconds:
            return True
        if last_res == 'LOSS' and new_signal_direction != last_dir:
            remaining_hours = (timeout_seconds - (time.time() - last_time)) / 3600
            logger.warning(f"🛑 [ANTI-REVENGE] منع انعكاس {symbol}: آخر {last_dir} خاسرة. يرفع بعد {remaining_hours:.1f}h.")
            return False
        return True

    def apply_symbol_specific_filters(self, signal, filters_dict):
        symbol = signal.symbol
        strength = signal.filters_passed
        filters = filters_dict

        if symbol == 'XRPUSDT':
            rsi = self._get_rsi(symbol)
            if rsi is not None and not (40 <= rsi <= 60):
                logger.info(f"🔍 [XRP] إشارة مرفوضة: RSI={rsi:.1f} خارج 40-60.")
                return False
        elif symbol == 'LINKUSDT':
            if not filters.get('VOL', False):
                logger.info(f"🔍 [LINK] إشارة مرفوضة: فلتر VOL إلزامي.")
                return False
        elif symbol in ('BTCUSDT', 'ETHUSDT'):
            if strength <= 3:
                logger.info(f"🔍 [{symbol}] إشارة مرفوضة: ضعيفة ({strength}/7).")
                return False
        elif symbol == 'NEARUSDT':
            if strength == 4:
                logger.info(f"🔍 [NEAR] إشارة مرفوضة: متوسطة (4/7).")
                return False
            if filters.get('ATR', False):
                logger.info(f"🔍 [NEAR] إشارة مرفوضة: ATR غير مسموح.")
                return False
        elif symbol in ('DOGEUSDT', 'INJUSDT'):
            if signal.side == Sig.SELL:
                return False
        return True

    def update_trade_state(self, symbol, direction, pnl):
        now = time.time()
        self.last_trade_direction[symbol] = direction
        self.last_trade_result[symbol] = 'WIN' if pnl > 0 else 'LOSS'
        self.last_trade_time[symbol] = now
        self.trade_history.append({'symbol': symbol, 'direction': direction, 'pnl': pnl, 'time': now})
        if len(self.trade_history) > 500: self.trade_history = self.trade_history[-500:]
        if pnl < 0:
            cooldown_period = self.config.get('cooldown_after_loss', COOLDOWN_AFTER_LOSS)
            self.cooldown_counter[symbol] = cooldown_period
            logger.info(f"🔴 خسارة {symbol}: تهدئة {cooldown_period} إشارات.")

    def validate_signal(self, signal, filters_dict):
        symbol = signal.symbol
        direction = 'LONG' if signal.side == Sig.BUY else 'SHORT'
        if not self.apply_global_blacklist(symbol, direction): return False
        if not self.apply_symbol_specific_filters(signal, filters_dict): return False
        if not self.apply_cooldown(symbol): return False
        if not self.apply_anti_revenge_logic(symbol, direction): return False
        return True

# ═══════════════════════════════════════════════════════
# 🔀 StrategyRouter
# ═══════════════════════════════════════════════════════
class StrategyRouter:
    def __init__(self, hub, le):
        self.hub, self.le = hub, le
        self.grid = DynamicGridStrategy(hub); self.filters = FilterEngine(hub)
    def route(self, symbol, price):
        closes = self.hub.closes(symbol)
        if not closes: return None
        ticker = self.hub.tickers.get(symbol)
        gr = self.grid.analyze(symbol, price)
        if gr: return Signal(symbol, gr[0], gr[1], 1, price, 7, False, False, ticker.bid if ticker else 0, ticker.ask if ticker else 0)
        if not self.le.allow(symbol): return None
        bf, bc, bv = self.filters.count_filters(symbol, Sig.BUY, price)
        sf, sc, sv = self.filters.count_filters(symbol, Sig.SELL, price)
        if bf >= 2 and bf >= sf: return Signal(symbol, Sig.BUY, "mixed", bf, price, bf, bc, bv, ticker.bid if ticker else 0, ticker.ask if ticker else 0)
        elif sf >= 2: return Signal(symbol, Sig.SELL, "mixed", sf, price, sf, sc, sv, ticker.bid if ticker else 0, ticker.ask if ticker else 0)
        return None

# ═══════════════════════════════════════════════════════
# 🛡️ RiskManager (🆕 إصلاح حجم الصفقة)
# ═══════════════════════════════════════════════════════
class RiskManager:
    def __init__(self, hub, le, filters):
        self.hub, self.le, self.filters = hub, le, filters
        # 🆕 سيتحدث من refresh_balance() تلقائياً
        self._actual_balance = BALANCE_USDT

    def update_balance(self, balance):
        """🆕 تحديث الرصيد الفعلي لحساب الأحجام بدقة"""
        self._actual_balance = balance

    @property
    def capital_per_trade(self):
        """🆕 حساب ديناميكي بناءً على الرصيد الفعلي"""
        return (self._actual_balance * TOTAL_CAPITAL_ALLOCATION) / MAX_POSITIONS

    def leverage(self, sig):
        if "grid" in sig.strategy: return GRID_LEVERAGE
        return min(self.filters.leverage_by_filters(sig.filters_passed, sig.has_cross, sig.has_high_volume), MAX_LEVERAGE)

    def qty(self, sym, price, balance, lev):
        """
        🆕 إصلاح: استخدام capital_per_trade مباشرة بدلاً من min()
        كان: risk_capital = min(capital_per_trade, balance * RISK_PCT * lev)
        المشكلة: balance * 0.02 * 10 = 2327 → يأخذ الأصغر = 166 USDT فقط
        الحل: استخدام capital_per_trade مباشرة
        """
        # 🆕 حجم الصفقة = رأس المال المخصص × مضاعف التعلم
        risk_capital = self.capital_per_trade * self.le.size_mult(sym)

        # حساب الكمية
        notional = max(risk_capital * lev, MIN_NOTIONAL)
        q = self.hub.fix_qty(sym, notional / price)

        # ضمان الحد الأدنى
        while q * price < MIN_NOTIONAL:
            q += self.hub.steps[sym]
            q = self.hub.fix_qty(sym, q)

        return q

    def initial_sl(self, sym, side, price, candles):
        atr = calc_atr(candles)
        is_long = side in [Sig.BUY, "grid_long"] or (isinstance(side, str) and "long" in side.lower())
        dist = max(price*0.02, atr) if isinstance(side, str) and "grid" in side else max(ATR_SL_MULT*atr, price*MIN_SL_PCT)
        return self.hub.fix_price(sym, price-dist if is_long else price+dist)

# ═══════════════════════════════════════════════════════
# 💼 OrderExecutor
# ═══════════════════════════════════════════════════════
class OrderExecutor:
    def __init__(self, conn, risk, state):
        self.conn, self.risk, self.state = conn, risk, state
        self._lock = asyncio.Lock(); self.balance = BALANCE_USDT; self._pending = {}
        self._daily_loss = 0.0; self._daily_loss_day = -1
    async def refresh_balance(self):
        try:
            self.balance = float((await self.conn.client.futures_account())['totalWalletBalance'])
            self.risk.update_balance(self.balance)  # 🆕 تحديث RiskManager بالرصيد الفعلي
            logger.info(f"💰 الرصيد الفعلي: {self.balance:.2f} USDT | لكل صفقة: {self.risk.capital_per_trade:.2f} USDT")
        except: pass
    @property
    def hub(self): return self.risk.hub
    def _check_daily_loss(self):
        today = time.localtime().tm_yday
        if today != self._daily_loss_day: self._daily_loss = 0.0; self._daily_loss_day = today
        if self._daily_loss >= self.balance*MAX_DAILY_LOSS_PCT:
            logger.warning(f"⛔ Daily loss limit: {self._daily_loss:.2f}"); return False
        return True
    async def open(self, sig):
        sym = sig.symbol
        async with self._lock:
            if self.state.has(sym) or not self.state.can_open() or not self._check_daily_loss(): return
        await self.refresh_balance()
        lev = self.risk.leverage(sig); qty = self.risk.qty(sym, sig.price, self.balance, lev)
        sl = self.risk.initial_sl(sym, sig.side, sig.price, list(self.hub.candles.get(sym,[])))
        if qty <= 0: return
        side_str = sig.side if isinstance(sig.side, Sig) else (Sig.BUY if "long" in sig.side else Sig.SELL)
        order_side = SIDE_BUY if side_str == Sig.BUY else SIDE_SELL
        pos_side = PosSide.LONG if side_str == Sig.BUY else PosSide.SHORT
        try:
            await self.conn.client.futures_change_leverage(symbol=sym, leverage=lev)
            order = await self.conn.client.futures_create_order(symbol=sym, side=order_side, type=ORDER_TYPE_MARKET, quantity=qty)
            actual_price = float(order.get('avgPrice', sig.price))
            if actual_price <= 0: return
            pos = Position(sym, pos_side, actual_price, qty, lev, sl, sig.strategy)
            pos.highest_lowest = actual_price; self.state.add(pos)
            close_side = SIDE_SELL if pos_side==PosSide.LONG else SIDE_BUY
            sl_o = await self.conn.client.futures_create_order(symbol=sym, side=close_side, type=FUTURE_ORDER_TYPE_STOP_MARKET,
                                                               quantity=qty, stopPrice=sl, reduceOnly=True, timeInForce="GTE_GTC")
            pos.sl_id = str(sl_o.get('orderId','')); self.state._save()
            logger.info(f"✅ {sym} {pos_side.value} qty={qty} lev={lev}x @ {actual_price:.4f} SL={sl:.4f} | رأس مال: {self.risk.capital_per_trade:.0f} USDT")
        except Exception as e:
            logger.error(f"Open failed [{sym}]: {e}")
            if self.state.has(sym): self.state.remove(sym, sig.price)
    async def close(self, sym, reason, price):
        pos = self.state.pos.get(sym)
        if not pos: return
        safe_qty = self.hub.fix_qty(sym, max(pos.qty, self.hub.steps[sym]))
        close_side = SIDE_SELL if pos.side==PosSide.LONG else SIDE_BUY
        try:
            await self.conn.client.futures_create_order(symbol=sym, side=close_side, type=ORDER_TYPE_MARKET, quantity=safe_qty, reduceOnly=True)
        except Exception as e:
            if "-2022" not in str(e) and "-1111" not in str(e): logger.error(f"Close failed [{sym}]: {e}")
        finally:
            pnl_usdt = pos.pnl_usdt(price)
            if pnl_usdt < 0: self._daily_loss += abs(pnl_usdt)
            pnl = self.state.remove(sym, price)
            if pnl is not None: self._pending[sym] = pnl
    async def partial_exit(self, pos, price):
        sym = pos.symbol
        half = self.hub.fix_qty(sym, pos.qty/2)
        if half < self.hub.steps[sym]: return False
        close_side = SIDE_SELL if pos.side==PosSide.LONG else SIDE_BUY
        try:
            info = await self.conn.client.futures_position_information(symbol=sym)
            if not info or float(info[0].get('positionAmt', 0)) == 0:
                if self.state.has(sym): self.state.pos.pop(sym, None); self.state._save()
                return False
            await self.conn.client.futures_create_order(symbol=sym, side=close_side, type=ORDER_TYPE_MARKET, quantity=half, reduceOnly=True)
            pos.qty -= half; pos.qty = self.hub.fix_qty(sym, pos.qty); self.state._save()
            logger.info(f"💰 Partial exit {sym}: {half}")
            return True
        except Exception as e:
            err = str(e)
            if "-2022" in err:
                if self.state.has(sym): self.state.pos.pop(sym, None); self.state._save()
            else: logger.warning(f"Partial exit failed [{sym}]: {e}")
            return False
    async def update_sl(self, pos, new_sl, current_price):
        sym = pos.symbol
        gap = current_price*0.001
        if (pos.side==PosSide.LONG and new_sl >= current_price-gap) or (pos.side==PosSide.SHORT and new_sl <= current_price+gap): return
        try:
            try: await self.conn.client.futures_cancel_all_open_orders(symbol=sym)
            except: pass
            pos.sl_id = ""; await asyncio.sleep(0.2)
            info = await self.conn.client.futures_position_information(symbol=sym)
            if not info or float(info[0].get('positionAmt',0)) == 0:
                if self.state.has(sym): self.state.remove(sym, pos.entry)
                return
        except: return
        try:
            safe_qty = self.hub.fix_qty(sym, pos.qty)
            close_side = SIDE_SELL if pos.side==PosSide.LONG else SIDE_BUY
            new_order = await self.conn.client.futures_create_order(symbol=sym, side=close_side, type=FUTURE_ORDER_TYPE_STOP_MARKET,
                                                                    quantity=safe_qty, stopPrice=new_sl, reduceOnly=True, timeInForce="GTE_GTC")
            pos.sl_id = str(new_order.get('orderId','')); pos.sl = new_sl; self.state._save()
        except Exception as e:
            if "-4509" in str(e) and self.state.has(sym): self.state.remove(sym, pos.entry)

# ═══════════════════════════════════════════════════════
# 😱 Fear & Greed
# ═══════════════════════════════════════════════════════
class FGSensor:
    def __init__(self): self._v, self._ts, self._sess = 50.0, 0.0, None
    async def get(self):
        if time.time()-self._ts < FNG_CACHE_SEC: return self._v
        try:
            if not self._sess or self._sess.closed: self._sess = aiohttp.ClientSession()
            async with self._sess.get("https://api.alternative.me/fng/?limit=1", timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200: d = await r.json(); self._v = float(d['data'][0]['value']); self._ts = time.time()
        except: pass
        return self._v
    def ok_long(self, v): return v >= FNG_LONG_MIN
    def ok_short(self, v): return v <= FNG_SHORT_MAX
    async def close(self):
        if self._sess and not self._sess.closed: await self._sess.close()

# ═══════════════════════════════════════════════════════
# 💾 StateTracker
# ═══════════════════════════════════════════════════════
class StateTracker:
    def __init__(self):
        self.pos: dict[str, Position] = {}; self.closed_pnl = 0.0; self.trades = 0; self._load()
    def _load(self):
        if not os.path.exists(POS_FILE): return
        try:
            d = json.load(open(POS_FILE))
            for s, p in d.get('positions',{}).items():
                self.pos[s] = Position(p['symbol'], PosSide(p['side']), p['entry'], p['qty'], p['leverage'],
                                       p['sl'], p['strategy'], p.get('opened',time.time()), p.get('sl_id',''),
                                       p.get('hl',p['entry']), p.get('trail',False), p.get('partial',False))
            self.closed_pnl = d.get('closed_pnl',0.0); self.trades = d.get('trades',0)
        except: pass
    def _save(self):
        try:
            json.dump({'closed_pnl':self.closed_pnl,'trades':self.trades,
                       'positions':{s:{'symbol':p.symbol,'side':p.side.value,'entry':p.entry,'qty':p.qty,
                                       'leverage':p.leverage,'sl':p.sl,'strategy':p.strategy,'opened':p.opened,
                                       'sl_id':p.sl_id,'hl':p.highest_lowest,'trail':p.trailing_activated,
                                       'partial':p.partial_exited} for s,p in self.pos.items()}},
                      open(POS_FILE,'w'), indent=2)
        except: pass
    def has(self, s): return s in self.pos
    def count(self): return len(self.pos)
    def can_open(self): return self.count() < MAX_POSITIONS
    def add(self, p): self.pos[p.symbol] = p; self._save()
    def remove(self, s, price):
        p = self.pos.pop(s, None)
        if p: pct, usdt = p.pnl_pct(price), p.pnl_usdt(price); self.closed_pnl += usdt; self.trades += 1; self._save(); return pct
        return None

# ═══════════════════════════════════════════════════════
# 🧠 LearningEngine
# ═══════════════════════════════════════════════════════
class LearningEngine:
    def __init__(self):
        self.w = {s:1.0 for s in SYMBOLS}; self.wins = {s:0 for s in SYMBOLS}; self.loss = {s:0 for s in SYMBOLS}
        self.streak = {s: deque(maxlen=5) for s in SYMBOLS}; self._load()
    def _load(self):
        if not os.path.exists(WGT_FILE): return
        try:
            d = json.load(open(WGT_FILE))
            for s in SYMBOLS:
                self.w[s] = d['w'].get(s,1.0); self.wins[s] = d['wins'].get(s,0); self.loss[s] = d['loss'].get(s,0)
                self.streak[s].extend(d.get('streak',{}).get(s,[]))
        except: pass
    def save(self):
        try: json.dump({'w':self.w,'wins':self.wins,'loss':self.loss,'streak':{s:list(self.streak[s]) for s in SYMBOLS}}, open(WGT_FILE,'w'), indent=2)
        except: pass
    def update(self, s, pnl):
        if pnl > 0: self.w[s] = min(2.0, self.w[s]+0.08*(1+pnl*10)); self.wins[s] += 1; self.streak[s].append(1)
        else: self.w[s] = max(0.2, self.w[s]-0.12*(1+abs(pnl)*10)); self.loss[s] += 1; self.streak[s].append(0)
        self.save()
    def allow(self, s): return self.w[s] >= ALLOW_THRESHOLD
    def size_mult(self, s):
        w = self.w[s]
        if w >= 1.8: base = 2.0
        elif w >= 1.5: base = 1.5
        elif w >= 1.2: base = 1.2
        elif w >= 1.0: base = 1.0
        elif w >= 0.8: base = 0.8
        elif w >= 0.6: base = 0.6
        else: base = 0.4
        recent = list(self.streak[s])
        if len(recent) >= 3:
            wr = sum(recent)/len(recent)
            if wr >= 0.8: base *= 1.2
            elif wr <= 0.2: base *= 0.7
        return base

# ═══════════════════════════════════════════════════════
# 📲 Telegram
# ═══════════════════════════════════════════════════════
class TG:
    def __init__(self, bot_ref=None):
        self.enabled = bool(TG_TOKEN and TG_CHAT_ID)
        self._sess = None; self._last_update_id = 0
        self.bot_ref = bot_ref; self._commands_task = None

    async def _s(self):
        if not self._sess or self._sess.closed: self._sess = aiohttp.ClientSession()
        return self._sess

    async def send(self, txt):
        if not self.enabled: return
        try:
            s = await self._s()
            for i in range(0, len(txt), 4000):
                chunk = txt[i:i+4000]
                async with s.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                                  json={"chat_id":TG_CHAT_ID,"text":chunk,"parse_mode":"HTML"},
                                  timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status != 200: logger.warning(f"TG: {r.status}")
        except Exception as e: logger.error(f"TG send: {e}")

    def format_filters_line(self, fd):
        return " ".join(f"{'✅' if fd.get(n) else '❌'} {n}" for n in ['M50','B200','ATR','RSI','VOL','CRS','ADX'])

    async def opened(self, sig, lev, sl, fd, total, hg):
        e = "🟢" if sig.side==Sig.BUY else "🔴"; t = "LONG" if sig.side==Sig.BUY else "SHORT"
        h = "🔥🔥🔥 إشارة ذهبية 🔥🔥🔥" if hg else "⭐ إشارة قوية ⭐" if total>=6 else "📈 إشارة متوسطة" if total>=4 else "📉 إشارة ضعيفة"
        await self.send(f"{h}\n{e} {t} | {sig.symbol} | ×{lev}\n━━━━━━━━━━━━━━━━━━━\n{self.format_filters_line(fd)}\n📊 {total}/7 فلتر | {'🔥 تقاطع + حجم' if hg else 'استراتيجية: '+sig.strategy}\n━━━━━━━━━━━━━━━━━━━\n💰 سعر: {sig.price:,.4f} USDT\n🛡️ SL: {sl:,.4f}\n⏱️ {time.strftime('%Y-%m-%d %H:%M:%S')}")

    async def opened_grid(self, pos):
        e = "🟢" if pos.side==PosSide.LONG else "🔴"
        await self.send(f"📐 <b>Grid {pos.strategy}</b>\n{e} {pos.symbol} {pos.side.value}\nسعر: {pos.entry:.4f} × {pos.leverage}x\nSL: {pos.sl:.4f}")

    async def closed(self, sym, pct, usdt, reason, info=""):
        e = "✅" if pct>0 else "❌"
        msg = f"{e} إغلاق | {sym}\n━━━━━━━━━━━━━━━━━━━\n📊 السبب: {reason}\n💰 PnL: {pct:+.2%} ({usdt:+.2f} USDT)\n⏱️ {time.strftime('%Y-%m-%d %H:%M:%S')}"
        if info: msg += f"\n📝 {info}"
        await self.send(msg)

    async def daily_limit(self, loss): await self.send(f"⛔ <b>Daily Loss Limit</b>\nتم إيقاف التداول اليوم\nالخسارة: {loss:.2f} USDT ({MAX_DAILY_LOSS_PCT:.0%})")
    async def partial_exit(self, sym): await self.send(f"✂️ <b>خروج جزئي 50%</b> {sym}")

    async def start_command_listener(self):
        if not self.enabled: return
        self._commands_task = asyncio.create_task(self._poll_commands())

    async def _poll_commands(self):
        while True:
            try: await self._check_updates()
            except asyncio.CancelledError: return
            except: pass
            await asyncio.sleep(2)

    async def _check_updates(self):
        try:
            s = await self._s()
            params = {"offset": self._last_update_id+1, "timeout": 1, "allowed_updates": ["message"]}
            async with s.get(f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates", params=params, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status != 200: return
                data = await r.json()
                if not data.get("ok"): return
            for update in data.get("result", []):
                self._last_update_id = update["update_id"]
                msg = update.get("message", {})
                text = msg.get("text", "").strip()
                if text in ["/تحليل", "/تحليل@MyTradingAlertBot"]: asyncio.create_task(self._handle_analysis())
                elif text in ["/حالة", "/status"]: asyncio.create_task(self._handle_status())
                elif text in ["/help", "/مساعدة"]: await self.send("🤖 <b>HedgeBot v11 Fixed</b>\n\n/تحليل - تحليل فوري للسوق\n/حالة - حالة البوت والصفقات\n/مساعدة - هذه القائمة")
        except: pass

    async def _handle_analysis(self):
        if not self.bot_ref: await self.send("⚠️ النظام غير جاهز."); return
        try:
            hub = self.bot_ref.hub; le = self.bot_ref.le; state = self.bot_ref.state
            filters = self.bot_ref.filters; grid = self.bot_ref.grid; optimizer = self.bot_ref.optimizer
            grid.update_regime()
            regime_emoji = {"up":"🟢 صاعد","down":"🔴 هابط","neutral":"⚪ محايد"}
            buy_cand, sell_cand = [], []
            for sym in SYMBOLS:
                t = hub.tickers.get(sym)
                if not t: continue
                fd_b = filters.get_filters_dict(sym, Sig.BUY, t.price)
                fd_s = filters.get_filters_dict(sym, Sig.SELL, t.price)
                bc = sum(1 for v in fd_b.values() if v)
                sc = sum(1 for v in fd_s.values() if v)
                rsi = rsi_val(hub.closes(sym)) if len(hub.closes(sym))>=15 else None
                w = le.w.get(sym,1.0); wins=le.wins.get(sym,0); losses=le.loss.get(sym,0)
                total_t = wins+losses; wr = (wins/total_t*100) if total_t>0 else 0
                entry = {'symbol':sym,'price':t.price,'buy_filters':bc,'sell_filters':sc,'rsi':rsi,'weight':w,'wr':wr,'change':t.change}
                if bc>=4: buy_cand.append(entry)
                if sc>=4: sell_cand.append(entry)
            buy_cand.sort(key=lambda x:(x['buy_filters'],x['weight'],x['wr']), reverse=True)
            sell_cand.sort(key=lambda x:(x['sell_filters'],x['weight'],x['wr']), reverse=True)
            risk = self.bot_ref.risk
            lines = [f"📊 <b>تحليل السوق</b>\n⏱️ {time.strftime('%Y-%m-%d %H:%M:%S')}\n🌐 Grid: {regime_emoji.get(grid.regime,'⚪')}\n📈 مفتوحة: {state.count()}/{MAX_POSITIONS}\n💰 صافي: {state.closed_pnl:+.2f} USDT\n💵 لكل صفقة: {risk.capital_per_trade:.0f} USDT\n\n🟢 <b>أفضل 5 شراء:</b>"]
            for i,c in enumerate(buy_cand[:5],1):
                rsi_s = f"RSI={c['rsi']:.0f}" if c['rsi'] else "RSI=?"
                lines.append(f"{i}. {c['symbol']} | {c['buy_filters']}/7 | {rsi_s} | {c['price']:.4f} | {c['change']:+.1f}% | ن.فوز={c['wr']:.0f}%")
            lines.append("\n🔴 <b>أفضل 5 بيع:</b>")
            for i,c in enumerate(sell_cand[:5],1):
                rsi_s = f"RSI={c['rsi']:.0f}" if c['rsi'] else "RSI=?"
                lines.append(f"{i}. {c['symbol']} | {c['sell_filters']}/7 | {rsi_s} | {c['price']:.4f} | {c['change']:+.1f}% | ن.فوز={c['wr']:.0f}%")
            if optimizer.trade_history:
                recent = optimizer.trade_history[-50:]
                wins_c = sum(1 for t in recent if t['pnl']>0)
                total_p = sum(t['pnl'] for t in recent)
                lines.append(f"\n📋 آخر 50: {wins_c} فوز | PnL={total_p:+.2f} USDT")
            await self.send("\n".join(lines))
        except Exception as e: await self.send(f"❌ خطأ: {str(e)[:200]}")

    async def _handle_status(self):
        if not self.bot_ref: await self.send("⚠️ النظام غير جاهز."); return
        try:
            state = self.bot_ref.state; ex = self.bot_ref.exec; opt = self.bot_ref.optimizer
            risk = self.bot_ref.risk
            lines = [f"🤖 <b>حالة HedgeBot v11 Fixed</b>\n⏱️ {time.strftime('%Y-%m-%d %H:%M:%S')}\n💰 الرصيد: {ex.balance:.0f} USDT\n💵 لكل صفقة: {risk.capital_per_trade:.0f} USDT\n📊 صافي: {state.closed_pnl:+.2f} USDT\n📂 مفتوحة: {state.count()}/{MAX_POSITIONS}\n\n<b>الصفقات المفتوحة:</b>"]
            if state.pos:
                for sym,pos in state.pos.items():
                    t = self.bot_ref.hub.tickers.get(sym)
                    cp = t.price if t else pos.entry
                    pnl_p = pos.pnl_pct(cp); pnl_u = pos.pnl_usdt(cp)
                    lines.append(f"{'🟢' if pnl_p>=0 else '🔴'} {sym} {pos.side.value} ×{pos.leverage} | {pos.entry:.4f}→{cp:.4f} | {pnl_p:+.2%} ({pnl_u:+.2f}) | SL={pos.sl:.4f}")
            else: lines.append("   لا توجد")
            if opt.cooldown_counter:
                cooled = {s:c for s,c in opt.cooldown_counter.items() if c>0}
                if cooled:
                    lines.append("\n⏳ <b>تهدئة:</b>")
                    for s,c in cooled.items(): lines.append(f"   {s}: {c} متبقية")
            await self.send("\n".join(lines))
        except Exception as e: await self.send(f"❌ خطأ: {str(e)[:200]}")

    async def close(self):
        if self._commands_task: self._commands_task.cancel()
        if self._sess and not self._sess.closed: await self._sess.close()

# ═══════════════════════════════════════════════════════
# 🤖 HedgeBot v11 Fixed الرئيسي
# ═══════════════════════════════════════════════════════
class HedgeBot:
    def __init__(self):
        self.conn = ConnectionManager(); self.hub = MarketDataHub(self.conn)
        self.le = LearningEngine(); self.state = StateTracker()
        self.filters = FilterEngine(self.hub)
        self.risk = RiskManager(self.hub, self.le, self.filters)
        self.exec = OrderExecutor(self.conn, self.risk, self.state)
        self.router = StrategyRouter(self.hub, self.le)
        self.grid = self.router.grid  # مرجع مباشر لـ Grid
        self.tg = TG(bot_ref=self); self.fg = FGSensor()
        self.optimizer = TradingLogicOptimizer(self.hub, config={'cooldown_after_loss':COOLDOWN_AFTER_LOSS,'anti_revenge_timeout_hours':ANTI_REVENGE_TIMEOUT_HOURS})
        self._cd = {}; self._fng = 50.0; self._tick = 0
        self._pending_orders = set(); self._pending_lock = asyncio.Lock()

    async def _trailing(self):
        while True:
            await asyncio.sleep(MONITOR_SEC)
            for sym, pos in list(self.state.pos.items()):
                t = self.hub.tickers.get(sym)
                if not t: continue
                price, atr = t.price, calc_atr(list(self.hub.candles.get(sym,[])))
                if atr == 0: continue
                if pos.side == PosSide.LONG and price > pos.highest_lowest: pos.highest_lowest = price
                elif pos.side == PosSide.SHORT and price < pos.highest_lowest: pos.highest_lowest = price
                trail_dist = ATR_TRAIL_MULT*atr; sl_updated = False
                if pos.side == PosSide.LONG:
                    pot = self.hub.fix_price(sym, pos.highest_lowest-trail_dist)
                    if not pos.trailing_activated:
                        if pos.highest_lowest - pos.entry >= TRAILING_ACTIVATE*atr:
                            if not pos.partial_exited:
                                ok = await self.exec.partial_exit(pos, price)
                                if ok: pos.partial_exited = True; pos.trailing_activated = True; await self.exec.update_sl(pos, self.hub.fix_price(sym, pos.highest_lowest-2.2*atr), price); await self.tg.partial_exit(sym); sl_updated = True
                            else: pos.trailing_activated = True; await self.exec.update_sl(pos, pot, price); sl_updated = True
                    elif pot > pos.sl: await self.exec.update_sl(pos, pot, price); sl_updated = True
                else:
                    pot = self.hub.fix_price(sym, pos.highest_lowest+trail_dist)
                    if not pos.trailing_activated:
                        if pos.entry - pos.highest_lowest >= TRAILING_ACTIVATE*atr:
                            if not pos.partial_exited:
                                ok = await self.exec.partial_exit(pos, price)
                                if ok: pos.partial_exited = True; pos.trailing_activated = True; await self.exec.update_sl(pos, self.hub.fix_price(sym, pos.highest_lowest+2.2*atr), price); await self.tg.partial_exit(sym); sl_updated = True
                            else: pos.trailing_activated = True; await self.exec.update_sl(pos, pot, price); sl_updated = True
                    elif pot < pos.sl: await self.exec.update_sl(pos, pot, price); sl_updated = True
                if not sl_updated:
                    if (pos.side==PosSide.LONG and price<=pos.sl) or (pos.side==PosSide.SHORT and price>=pos.sl):
                        await self.exec.close(sym, "وقف خسارة", price)
                        await self.tg.closed(sym, pos.pnl_pct(price), pos.pnl_usdt(price), "وقف خسارة", f"دخل: {pos.entry:.4f} | خرج: {price:.4f} | {pos.strategy}")

    async def _monitor(self):
        while True:
            await asyncio.sleep(MONITOR_SEC)
            if not self.conn.ok: continue
            try:
                info = await self.conn.client.futures_position_information()
                open_ex = {p['symbol'] for p in info if float(p['positionAmt'])!=0}
                for sym in list(self.state.pos.keys()):
                    if sym not in open_ex:
                        t = self.hub.tickers.get(sym); pos = self.state.pos[sym]
                        price = t.price if t else pos.entry
                        pct, usdt = pos.pnl_pct(price), pos.pnl_usdt(price)
                        self.state.remove(sym, price); self._cd[sym] = time.time()
                        await self.tg.closed(sym, pct, usdt, "exchange", f"دخل: {pos.entry:.4f} | خرج: {price:.4f}")
                        async with self._pending_lock: self._pending_orders.discard(sym)
            except: pass

    async def _on_ticker(self, t):
        sym, price = t.symbol, t.price
        pnl = self.exec._pending.pop(sym, None)
        if pnl is not None:
            pos = self.state.pos.get(sym)
            direction = pos.side.value if pos else ('LONG' if pnl > 0 else 'SHORT')
            self.le.update(sym, pnl); self.optimizer.update_trade_state(sym, direction, pnl); self._cd[sym] = time.time()
        async with self._pending_lock:
            if self.state.has(sym) or sym in self._pending_orders: return
            if time.time() - self._cd.get(sym,0) < COOLDOWN_SEC: return
        sig = self.router.route(sym, price)
        if not sig or sig.side == Sig.HOLD: return
        if sig.side==Sig.BUY and not self.fg.ok_long(self._fng): return
        if sig.side==Sig.SELL and not self.fg.ok_short(self._fng): return
        if "grid" not in sig.strategy:
            fd = self.filters.get_filters_dict(sym, sig.side, price)
            if not self.optimizer.validate_signal(sig, fd): return
        before = self.state.count()
        async with self._pending_lock: self._pending_orders.add(sym)
        try: await self.exec.open(sig)
        finally:
            async with self._pending_lock: self._pending_orders.discard(sym)
        if self.state.count() > before and self.state.has(sym):
            if "grid" not in sig.strategy:
                fd = self.filters.get_filters_dict(sym, sig.side, price)
                total = sum(1 for v in fd.values() if v); hg = fd.get('VOL') and fd.get('CRS')
                await self.tg.opened(sig, self.risk.leverage(sig), self.state.pos[sym].sl, fd, total, hg)
            else: await self.tg.opened_grid(self.state.pos[sym])
        self._tick += 1
        if self._tick % 300 == 0:  # 🆕 تحديث أكثر تكراراً (كل 300 tick بدل 500)
            await self.exec.refresh_balance()
            self._fng = await self.fg.get()

    async def start(self):
        await self.conn.connect()
        await self.exec.refresh_balance()  # 🆕 تحديث الرصيد فور الاتصال
        self._fng = await self.fg.get(); self.hub.subscribe(self._on_ticker)
        logger.info("🚀 HedgeBot v11 Fixed — Started")
        cap_per_trade = self.risk.capital_per_trade
        await self.tg.send(
            f"🚀 <b>HedgeBot v11 Fixed</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"💰 الرصيد: {self.exec.balance:.0f} USDT\n"
            f"💵 لكل صفقة: {cap_per_trade:.0f} USDT × رافعة = حجم فعلي\n"
            f"📊 أقصى صفقات: {MAX_POSITIONS}\n"
            f"🛡️ Grid رافعة: {GRID_LEVERAGE}x (مخفضة للأمان)\n"
            f"✅ فلاتر Grid الذكية: مفعّلة\n"
            f"✅ Anti-Revenge: مفعّل\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"/تحليل | /حالة | /مساعدة"
        )
        await self.tg.start_command_listener()
        asyncio.create_task(self._trailing())
        asyncio.create_task(self._monitor())
        await self.hub.start()

    async def stop(self):
        for sym in list(self.state.pos.keys()):
            t = self.hub.tickers.get(sym)
            if t: await self.exec.close(sym, "shutdown", t.price)
        await self.tg.close(); await self.fg.close(); await self.conn.disconnect()

async def main():
    bot = HedgeBot()
    try: await bot.start()
    except KeyboardInterrupt:
        logger.info("⏹️ Shutting down..."); await bot.stop()

if __name__ == "__main__":
    print("🚀 HedgeBot v11 Fixed — Starting...")
    print(f"💰 الرصيد الأولي: {BALANCE_USDT:.0f} USDT")
    print(f"📊 رأس مال التداول: {BALANCE_USDT * TOTAL_CAPITAL_ALLOCATION:.0f} USDT (25%)")
    print(f"📊 لكل صفقة: {(BALANCE_USDT * TOTAL_CAPITAL_ALLOCATION) / MAX_POSITIONS:.0f} USDT")
    print(f"📊 أقصى صفقات: {MAX_POSITIONS}")
    print(f"📊 رافعة Grid: {GRID_LEVERAGE}x")
    print(f"📊 أقصى رافعة: {MAX_LEVERAGE}x")
    print(f"🛡️ Grid فلاتر ذكية: ✅ مفعّلة")
    print(f"🛡️ Anti-Revenge Timeout: {ANTI_REVENGE_TIMEOUT_HOURS}h")
    print(f"📲 Telegram: {'✅ Enabled' if TG_TOKEN and TG_CHAT_ID else '❌ Disabled'}")
    print("═" * 50)
    asyncio.run(main())
