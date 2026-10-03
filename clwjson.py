"""Резервний тригер без ключа: публічний JSON постачальника cryptolisting.

НАВІЩО (2026-10-03). Швидких тригерів у бота зараз НУЛЬ. WebSocket приймає
з'єднання, але не віддає нічого — доведено офіційною самоперевіркою
`{"type":"test"}`: відповіді нема, вітального кадру нема, heartbeat (документовані
30с) немає. Телеграм-сесію відкликано 34 доби тому. Лишається власний поллінг
каталогу Binance із заміряною медіаною детекту 46с і максимумом 95.6с — а на
кривій PnL 15с це вже +5.2% на угоду проти +26% при 1-2с.

Обидва мертві шляхи лікуються лише ДІЯМИ КОРИСТУВАЧА (новий ключ у постачальника,
новий вхід у Telegram з телефона). Цей модуль — те, що можна зробити без них.

ЩО ЦЕ. Постачальник сам публікує безкоштовний keyless JSON і в документації
прямо називає його «fallback source when the WebSocket is unreachable». Чесна
вимога — не частіше ніж один запит на 30с; вона тут зашита жорстким підлоговим
обмеженням, а не лише значенням за замовчуванням.

ЧОМУ ЦЬОМУ ДЖЕРЕЛУ МОЖНА ДОВІРЯТИ РЕАЛЬНІ ГРОШІ. Звірено 2026-10-03 на всій
їхній історії (274 події, квітень–жовтень 2026): КОЖНА подія `binance` +
`spot_delisting` відповідає рівно повному анонсу «Binance Will Delist X, Y, Z»
з каталогу Binance. Один до одного, без винятків:
    USDP · ICX,SCRT,STORJ · ACX,HFT,PIVX,PYR,VANRY,VIC · ALCX,ARDR,NFP,POND
    COS,D,HIGH,MBOX · ATA,FARM,MLN,PHB,SYS · DEGO,DENT,TRU · UTK · BIFI,FIO,…
Жодного margin-делістингу, жодного прибирання пар, жодного Binance Alpha —
тобто саме ті три категорії, які наш власний класифікатор відсіює вручну, сюди
не потрапляють узагалі. Monitoring Tag у них окремий тип і теж не змішується.

ЗАМІРЯНА ЗАТРИМКА (на їхній же історії, зшитій із releaseDate Binance):
    їхній детект спот-делістингу   +1.21 .. +2.38с (медіана 2.28с, n=7)
    публікація файлу після детекту +0.51с
    наш опит                        0 .. 30с (у середньому 15с)
Разом медіана ~18с проти наших 46с, і — головне — стеля 33с замість 96с.
Це не заміна швидкому тригеру, це страховка від найгірших випадків поллінга.

БЕЗПЕКА. Вік сигналу рахується від ЇХНЬОГО detected_at_us, тому застарілий файл
не може спровокувати угоду: спрацюють ті самі ворота MAX_SIGNAL_AGE_SEC, що й
скрізь. Якщо фід замре — ми просто нічого не відкриємо, а сторож поскаржиться.
"""
import asyncio
import time

import aiohttp

import alerts
import config
import fastjson
import logbook as log

_handler = None
_notifier = None

# Жорстка підлога: постачальник просить не частіше ніж раз на 30с, і це його
# безкоштовний сервіс. Значення з .env не може опуститись нижче.
_MIN_PERIOD_SEC = 30.0
_MAX_SEEN = 4000
# Наскільки близькі мітки детекту вважати одним анонсом. Два РІЗНІ спот-делістинги
# Binance у межах секунди неможливі: за їхньою ж історією вони трапляються раз на
# ~19 діб. Зате розрив одного анонсу на кілька сигналів цілком реальний — і
# коштує двох ордерних раундів затримки для останнього токена.
_GROUP_WINDOW_US = 1_000_000

_seen: set = set()
_stats = {"polls": 0, "not_modified": 0, "errors": 0, "events_new": 0,
          "delist_groups": 0, "signals": 0, "skipped_stale": 0,
          "last_ok_ms": 0, "last_event_ms": 0, "feed_updated_us": 0,
          "last_error": None}
_etag = None
_last_modified = None


def set_handler(fn) -> None:
    """fn(tickers: list[str], age_sec: float, source: str) -> awaitable."""
    global _handler
    _handler = fn


def set_notifier(fn) -> None:
    global _notifier
    _notifier = fn


def stats() -> dict:
    d = dict(_stats)
    now = time.time()
    d["last_ok_age_sec"] = (round(now - _stats["last_ok_ms"] / 1000, 1)
                            if _stats["last_ok_ms"] else None)
    d["feed_age_sec"] = (round(now - _stats["feed_updated_us"] / 1e6, 1)
                         if _stats["feed_updated_us"] else None)
    d["seen"] = len(_seen)
    return d


def healthy() -> bool:
    """Доказ — успішно прочитаний файл за останні кілька періодів опитування."""
    if not config.CLW_JSON:
        return False
    ms = _stats["last_ok_ms"]
    return bool(ms) and (time.time() - ms / 1000) <= config.CLW_JSON_POLL_SEC * 4


def _key(e: dict) -> str:
    return "|".join((str(e.get("cex")), str(e.get("ticker")),
                     str(e.get("type")), str(e.get("detected_at_us"))))


def parse_events(data) -> list:
    """Витягує список подій. Форма: {"updated_at_us": …, "events": [...]}."""
    if isinstance(data, dict):
        evs = data.get("events")
        upd = data.get("updated_at_us")
        if isinstance(upd, (int, float)) and upd > 0:
            _stats["feed_updated_us"] = int(upd)
    else:
        evs = data
    return [e for e in (evs or []) if isinstance(e, dict)]


async def _handle_new(events: list) -> None:
    """Нові події одного зчитування. Події одного анонсу приходять окремими
    рядками з ОДНАКОВИМ detected_at_us — групуємо, щоб усі тикери анонсу пішли
    в один сигнал і відкривались паралельно, а не по черзі."""
    # Групуємо за моментом детекту, ЗАОКРУГЛЕНИМ до секунди. У живих даних усі
    # тикери одного анонсу мають однаковий detected_at_us до мікросекунди
    # (перевірено: HOOK і D — обидва 1790926200429435), але покладатись на точну
    # рівність крихко. Ціна помилки конкретна: розгрупований анонс пішов би
    # кількома послідовними викликами, і третій токен заходив би на два ордерні
    # раунди пізніше — тобто ми б самі відтворили затримку, заради усунення якої
    # і робився паралельний _fire_tickers. Два РІЗНІ спот-делістинги в межах
    # однієї секунди неможливі: вони трапляються раз на ~19 діб.
    hits = []
    for e in events:
        if str(e.get("cex", "")).lower() != "binance":
            continue
        if str(e.get("type", "")) != "spot_delisting":
            continue
        tk = str(e.get("ticker") or "").strip().upper()
        us = e.get("detected_at_us")
        if not tk or not isinstance(us, (int, float)) or isinstance(us, bool):
            continue
        hits.append((int(us), tk))

    # Кластеризація за БЛИЗЬКІСТЮ, не за фіксованим відром: відро ділить події на
    # межі секунди, а це рівно той крихкий випадок, якого ми й позбуваємось.
    groups = []
    for us, tk in sorted(hits):
        if groups and us - groups[-1]["us"] <= _GROUP_WINDOW_US:
            g = groups[-1]
        else:
            g = {"us": us, "tickers": []}
            groups.append(g)
        if tk not in g["tickers"]:
            g["tickers"].append(tk)

    for g in groups:
        us, tickers = g["us"], g["tickers"]
        _stats["delist_groups"] += 1
        since_detect = round(time.time() - us / 1e6, 2)
        est_age = round(since_detect + config.FEED_DETECT_LAG_SEC, 2)
        log.event("clw_delisting", tickers=tickers, since_detect_sec=since_detect,
                  est_age_sec=est_age, detected_at_us=us)
        if _notifier is not None:
            _notifier("📄 <b>Публічний фід: спот-делістинг Binance</b>" + chr(10)
                      + "Токени: " + ", ".join(tickers) + chr(10)
                      + "Вік сигналу ~" + str(est_age) + "с")
        if not config.CLW_JSON_TRADE:
            log.event("clw_no_trade", tickers=tickers, reason="CLW_JSON_TRADE=0")
            continue
        if est_age > config.MAX_SIGNAL_AGE_SEC:
            # Саме це робить джерело безпечним: вік рахується від ЇХНЬОГО детекту,
            # тож завислий файл не може відкрити позицію в уже відпрацьований дамп.
            _stats["skipped_stale"] += 1
            log.event("clw_stale_no_trade", tickers=tickers, est_age_sec=est_age,
                      limit=config.MAX_SIGNAL_AGE_SEC)
            continue
        if _handler is None:
            log.error("clwjson: обробник не встановлено — сигнал втрачено")
            continue
        _stats["signals"] += 1
        log.event("clw_signal", tickers=tickers, est_age_sec=est_age,
                  since_detect_sec=since_detect)
        await _handler(tickers, est_age, "clw_json")


async def _poll_once(sess: aiohttp.ClientSession) -> None:
    global _etag, _last_modified
    headers = {}
    # Умовний запит: якщо файл не змінився, сервер віддасть 304 і не платитиме
    # за трафік. Дрібниця, але це безкоштовний сервіс чужої людини.
    if _etag:
        headers["If-None-Match"] = _etag
    if _last_modified:
        headers["If-Modified-Since"] = _last_modified
    async with sess.get(config.CLW_JSON_URL, headers=headers) as r:
        if r.status == 304:
            _stats["not_modified"] += 1
            _stats["last_ok_ms"] = int(time.time() * 1000)
            return
        if r.status != 200:
            raise RuntimeError("HTTP " + str(r.status))
        _etag = r.headers.get("ETag") or _etag
        _last_modified = r.headers.get("Last-Modified") or _last_modified
        body = await r.read()
    data = fastjson.loads(body)
    _stats["polls"] += 1
    _stats["last_ok_ms"] = int(time.time() * 1000)
    events = parse_events(data)
    first = not _seen
    fresh = []
    for e in events:
        k = _key(e)
        if k in _seen:
            continue
        _seen.add(k)
        if not first:
            fresh.append(e)
    if first:
        log.event("clw_primed", events=len(events),
                  feed_age_sec=stats()["feed_age_sec"])
        return
    if not fresh:
        return
    _stats["events_new"] += len(fresh)
    _stats["last_event_ms"] = int(time.time() * 1000)
    log.event("clw_new_events", n=len(fresh),
              kinds=sorted({str(e.get("cex")) + ":" + str(e.get("type"))
                            for e in fresh})[:10])
    await _handle_new(fresh)
    if len(_seen) > _MAX_SEEN:          # файл — ковзне вікно, памʼять не має рости
        _seen.clear()
        for e in events:
            _seen.add(_key(e))


async def run() -> None:
    if not config.CLW_JSON:
        log.event("loop_disabled", loop="clw_json", reason="CLW_JSON=0")
        return
    period = max(_MIN_PERIOD_SEC, config.CLW_JSON_POLL_SEC)
    log.event("clw_start", url=config.CLW_JSON_URL, period_sec=period,
              trade=config.CLW_JSON_TRADE)
    timeout = aiohttp.ClientTimeout(total=20)
    hdrs = {"User-Agent": "delisting-bot/1.0 (+fallback feed reader)",
            "Accept": "application/json"}
    backoff = period
    async with aiohttp.ClientSession(timeout=timeout, headers=hdrs) as sess:
        while True:
            t0 = time.time()
            try:
                await _poll_once(sess)
                backoff = period
                if _stats["errors"]:
                    await alerts.clear_alert("Публічний фід не читається")
                    _stats["errors"] = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                _stats["errors"] += 1
                _stats["last_error"] = (type(e).__name__ + ": " + str(e))[:200]
                if _stats["errors"] in (1, 10, 100):
                    log.event("clw_error", err=_stats["last_error"],
                              errors=_stats["errors"])
                if _stats["errors"] == 10:
                    await alerts.raise_alert(
                        "Публічний фід не читається",
                        "Резервне джерело детекту недоступне: "
                        + str(_stats["last_error"]) + chr(10)
                        + "Лишається власний поллінг (медіана 46с).")
                backoff = min(10 * period, backoff * 2)
            await asyncio.sleep(max(0.0, backoff - (time.time() - t0)))
