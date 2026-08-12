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

_SNAP_URL = "https://api.bybit.com/v5/market/tickers?category=linear"
_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

_last: dict[str, float] = {}        # sym -> ціна
_last_ts: dict[str, int] = {}       # sym -> коли оновлено (ms)
_mhigh: dict[str, dict[int, float]] = {}  # sym -> {хвилина(epoch_min): максимум}
_updated_ms: int = 0


def _now_ms() -> int:
    return int(time.time() * 1000)


def _update(sym: str, price: float, now_ms: int) -> None:
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
    return {"symbols": len(_last), "last_update_age_sec": age}


async def run() -> None:
    """Фоновий цикл знімків. Публічні дані — без ключів."""
    global _updated_ms
    if config.PRICECACHE_POLL_SEC <= 0:
        return
    async with aiohttp.ClientSession() as s:  # персистентна сесія → теплий TLS
        while True:
            try:
                async with s.get(_SNAP_URL, headers=_HEADERS,
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        data = await r.json()
                        now_ms = _now_ms()
                        for t in ((data.get("result") or {}).get("list") or []):
                            try:
                                _update(t["symbol"], float(t["lastPrice"]), now_ms)
                            except (KeyError, ValueError, TypeError):
                                continue
                        _updated_ms = now_ms
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(config.PRICECACHE_POLL_SEC)
