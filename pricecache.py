"""In-memory кеш цін усіх Bybit-перпів (легкий, для слабкої машини).

Фоновий цикл раз на PRICECACHE_POLL_SEC робить ОДИН запит
/v5/market/tickers?category=linear (усі ~500 символів за раз) і оновлює:
  - останню ціну кожного символу;
  - похвилинні максимуми (для reference_high за REF_LOOKBACK_MIN хв).

Мета: на сигналі делістингу мати ціну + 5-хв максимум У ПАМʼЯТІ, без мережевого
запиту на гарячому шляху відкриття. Ключ — сирий bybit-символ (напр. "DOGEUSDT").
"""
import asyncio
import time

import aiohttp

import config
import fastjson
import logbook as log

_SNAP_URL = "https://api.bybit.com/v5/market/tickers?category=linear"
_WS_URL = "wss://stream.bybit.com/v5/public/linear"
_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
_ws_msgs = 0  # лічильник WS-оновлень (для heartbeat-статистики)

_last: dict[str, float] = {}        # sym -> ціна
_last_ts: dict[str, int] = {}       # sym -> коли оновлено (ms)
_mhigh: dict[str, dict[int, float]] = {}  # sym -> {хвилина(epoch_min): максимум}
_updated_ms: int = 0


def _now_ms() -> int:
    return int(time.time() * 1000)


_subscribers: list = []  # cb(sym, price, now_ms) — викликається на КОЖНОМУ оновленні


def subscribe(cb) -> None:
    """Підписка на потік оновлень цін (для детектора обвалу). Колбек летить на
    гарячому WS-шляху (~86 оновлень/с), тому має бути дешевим і без await."""
    _subscribers.append(cb)


def _update(sym: str, price: float, now_ms: int, notify: bool = True) -> None:
    """notify=False для масового знімка: інакше один прохід по 800+ символах смикав
    детектор обвалу 800 разів підряд і затирав event-loop на ~50мс кожні 2с (виміряно
    монітором лагу). Детектор має жити лише на WS-дельтах — вони й так реал-тайм."""
    _last[sym] = price
    _last_ts[sym] = now_ms
    minute = now_ms // 60000
    buckets = _mhigh.setdefault(sym, {})
    if price > buckets.get(minute, 0.0):
        buckets[minute] = price
    keep = config.REF_LOOKBACK_MIN + 1
    if len(buckets) > keep:  # прибрати застарілі хвилини
        for m in sorted(buckets)[:-keep]:
            buckets.pop(m, None)
    if notify:
        for cb in _subscribers:
            try:
                cb(sym, price, now_ms)
            except Exception:  # noqa: BLE001
                pass  # детектор не має права зламати кеш цін


def get_price(sym: str):
    """(ціна, вік_сек) або None, якщо символу нема в кеші."""
    p = _last.get(sym)
    if p is None:
        return None
    age = (_now_ms() - _last_ts.get(sym, 0)) / 1000.0
    return p, age


def reference_high(sym: str, lookback_min: int):
    """Максимум ціни за останні lookback_min хв з похвилинних бакетів (або None)."""
    buckets = _mhigh.get(sym)
    if not buckets:
        return None
    cutoff = (_now_ms() // 60000) - lookback_min + 1
    highs = [h for m, h in buckets.items() if m >= cutoff]
    return max(highs) if highs else None


def stats() -> dict:
    age = (_now_ms() - _updated_ms) / 1000.0 if _updated_ms else None
    return {"symbols": len(_last), "last_update_age_sec": age, "ws_msgs": _ws_msgs}


async def ws_run() -> None:
    """РЕАЛ-ТАЙМ шар: WS-стрім tickers усіх Bybit-перпів. Оновлює кеш у мить зміни ціни.
    Символи бере зі знімка (run() сідить їх першим). Ціну — з поля lastPrice дельти."""
    global _ws_msgs
    if not config.PRICECACHE_WS:
        return
    while True:
        try:
            # чекаємо, поки знімок заповнить перелік символів (сід)
            for _ in range(30):
                if _last:
                    break
                await asyncio.sleep(1)
            symbols = list(_last.keys())
            if not symbols:
                await asyncio.sleep(3)
                continue
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(_WS_URL, heartbeat=20, timeout=25) as ws:
                    # підписка чанками (обмеження на кількість args у запиті)
                    for i in range(0, len(symbols), 50):
                        args = [f"tickers.{sym}" for sym in symbols[i:i + 50]]
                        await ws.send_json({"op": "subscribe", "args": args})
                    log.event("pricecache_ws_connected", topics=len(symbols))
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        try:
                            d = fastjson.loads(msg.data)
                        except Exception:  # noqa: BLE001
                            continue
                        topic = d.get("topic", "")
                        if not topic.startswith("tickers."):
                            continue
                        lp = (d.get("data") or {}).get("lastPrice")
                        if lp is None:
                            continue  # дельта без зміни ціни
                        try:
                            _update(topic.split(".", 1)[1], float(lp), _now_ms())
                            _ws_msgs += 1
                        except (ValueError, IndexError):
                            continue
        except Exception:  # noqa: BLE001
            log.exception("pricecache WS помилка")
        await asyncio.sleep(5)


async def run() -> None:
    """Фоновий цикл знімків. Публічні дані — без ключів."""
    global _updated_ms
    if config.PRICECACHE_POLL_SEC <= 0:
        log.info("price-cache вимкнено (PRICECACHE_POLL_SEC=0)")
        return
    cycle, primed = 0, False
    async with aiohttp.ClientSession() as s:  # персистентна сесія → теплий TLS
        while True:
            try:
                async with s.get(_SNAP_URL, headers=_HEADERS,
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        # ~200КБ на 800+ символів раз на 2с — парсимо orjson-ом, щоб не
                        # тримати луп і не сипати обʼєктами під ноги збирачу.
                        data = fastjson.loads(await r.read())
                        now_ms = _now_ms()
                        rows = (data.get("result") or {}).get("list") or []
                        for i, t in enumerate(rows):
                            try:
                                _update(t["symbol"], float(t["lastPrice"]), now_ms, notify=False)
                            except (KeyError, ValueError, TypeError):
                                continue
                            if i % 250 == 249:
                                await asyncio.sleep(0)  # віддати луп: 800 символів за раз
                        _updated_ms = now_ms
                        if not primed:  # перший успішний знімок
                            log.event("pricecache_primed", **stats())
                            primed = True
            except Exception:  # noqa: BLE001
                pass
            cycle += 1
            if cycle % 150 == 0:  # ~кожні 5 хв — heartbeat, що кеш живий
                log.event("pricecache_stats", **stats())
            await asyncio.sleep(config.PRICECACHE_POLL_SEC)
