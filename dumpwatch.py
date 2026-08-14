"""Детектор обвалу на потоці цін Bybit — найшвидший тригер, який нам фізично доступний.

ЧОМУ ВІН ПОТРІБЕН (усе виміряно з нашого сервера у Франкфурті):
  • Анонси Binance (apex і composite) віддаються через CloudFront із TTL ~120с.
    Обійти кеш не вийшло: невідомий query-параметр → HTTP 400, ротація pageSize → 400,
    заголовок Cache-Control: no-cache → ігнорується (знову Hit, Age=66).
    Отже ПОЛЛІНГ новин принципово не може бути свіжішим за десятки секунд.
  • Нативний push Binance (Odin/NATS) віддає анонімному клієнту лише приватні
    subjects push.inbox.<свій appId> — публічних broadcast-топіків там немає.
  • А от РУХ ЦІНИ доходить до нас за 81мс (замір Bybit tickers, один бік) і нічим
    не кешується. Делістинг завжди дає різкий обвал — тому обвал і є сигналом.

Бюджет такого тригера: 81мс (push) + ~0мс (наша обробка) + 165мс (ордер) ≈ 250мс
від моменту, коли ціна реально пішла.

ВАЖЛИВО: це вже не «торгівля по новині», а торгівля по реакції ринку. Тому за
замовчуванням детектор працює у ТІНЬОВОМУ режимі: фіксує в лог і сповіщає в Telegram,
але угод НЕ відкриває (DUMPWATCH_TRADE=0). Увімкнення торгівлі — окреме рішення.
"""
from collections import deque

import config
import logbook as log

# sym -> deque[(ts_ms, price)] у межах вікна; коротко, бо оновлень небагато на символ
_hist: dict[str, deque] = {}
_alerted: dict[str, int] = {}   # sym -> коли останній раз алертили (антиспам)
_recent_hits: deque = deque()   # (ts_ms, sym) — щоб відрізнити обвал ринку від одиночного
_on_dump = None                 # колбек (sym, drop_pct, from_price, to_price, span_ms)
_stats = {"checked": 0, "alerts": 0, "suppressed_market": 0, "suppressed_cooldown": 0}


def set_handler(cb) -> None:
    """cb(sym, drop_pct, from_price, to_price, span_ms) — синхронний, дешевий."""
    global _on_dump
    _on_dump = cb


def stats() -> dict:
    return dict(_stats, tracked=len(_hist))


def on_price(sym: str, price: float, now_ms: int) -> None:
    """Викликається price-cache-ом на КОЖНОМУ оновленні. Мусить бути дешевим:
    підрізаємо вікно, беремо максимум, рахуємо просадку. O(довжина вікна)."""
    if price <= 0:
        return
    h = _hist.get(sym)
    if h is None:
        h = _hist[sym] = deque(maxlen=config.DUMP_MAXLEN)
    cutoff = now_ms - int(config.DUMP_WINDOW_SEC * 1000)
    while h and h[0][0] < cutoff:
        h.popleft()
    h.append((now_ms, price))
    _stats["checked"] += 1
    if len(h) < 3:
        return

    top_ts, top = max(h, key=lambda x: x[1])
    if top <= 0 or top_ts > now_ms:
        return
    drop = (top - price) / top * 100.0
    if drop < config.DUMP_PCT:
        return

    last = _alerted.get(sym, 0)
    if now_ms - last < config.DUMP_COOLDOWN_SEC * 1000:
        _stats["suppressed_cooldown"] += 1
        return

    # Обвал ринку (BTC поїхав — валиться все) відрізняємо від одиночного обвалу монети:
    # якщо за останні DUMP_MARKET_SEC стільких же символів просіло — це не делістинг.
    while _recent_hits and _recent_hits[0][0] < now_ms - int(config.DUMP_MARKET_SEC * 1000):
        _recent_hits.popleft()
    others = {s for _t, s in _recent_hits if s != sym}
    _recent_hits.append((now_ms, sym))
    if len(others) >= config.DUMP_MARKET_WIDE_N:
        _stats["suppressed_market"] += 1
        log.event("dump_suppressed_market", symbol=sym, drop_pct=round(drop, 2),
                  concurrent=len(others))
        return

    _alerted[sym] = now_ms
    _stats["alerts"] += 1
    span_ms = now_ms - top_ts
    log.event("dump_detected", symbol=sym, drop_pct=round(drop, 2),
              from_price=top, to_price=price, span_ms=span_ms,
              window_sec=config.DUMP_WINDOW_SEC, trade=config.DUMPWATCH_TRADE)
    if _on_dump:
        _on_dump(sym, drop, top, price, span_ms)


def install() -> bool:
    """Підписує детектор на потік price-cache. Повертає False, якщо вимкнено."""
    if not config.DUMPWATCH:
        log.info("dumpwatch вимкнено (DUMPWATCH=0)")
        return False
    import pricecache
    pricecache.subscribe(on_price)
    log.event("dumpwatch_installed", drop_pct=config.DUMP_PCT,
              window_sec=config.DUMP_WINDOW_SEC, cooldown_sec=config.DUMP_COOLDOWN_SEC,
              market_wide_n=config.DUMP_MARKET_WIDE_N, trade=config.DUMPWATCH_TRADE)
    return True
