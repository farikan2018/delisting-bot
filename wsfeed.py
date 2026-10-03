"""Швидкий тригер №1: WebSocket-фід cryptolisting.ws.

ЩО ВИЯВИЛОСЬ 2026-10-03 І ЧОМУ ЦЕЙ ФАЙЛ ТАКИЙ.

З 30.08 по 01.10 у логах 20 подій `ws_connected` — транспорт живий, реконекти
рідкі. Подій `ws_delisting` за той самий час — НУЛЬ. 01.10 о 02:45:14 UTC
cryptolisting задетектив три ф'ючерсні делістинги Binance (видно в їхньому
публічному фіді), наш поллінг зловив ту саму статтю на +33с — а WS не сказав
нічого. Але відрізнити «фід мовчить, бо не було подій» від «фід не працює» було
НЕМОЖЛИВО: старий код читав `async for msg in ws` і не лишав по кадру жодного
сліду, якщо кадр не збігся з фільтром.

Документація постачальника знімає двозначність повністю:
    welcome   — JSON ОДРАЗУ після рукостискання (тариф, ліміти, термін ключа);
    heartbeat — JSON кожні 30с, саме щоб клієнт бачив тиху зупинку;
    PING      — контрольний кадр кожні 15с, на нього відповідає бібліотека.
Тобто справний фід зобов'язаний давати TEXT-кадр щопівхвилини НАВІТЬ коли подій
нема. Після розгортання інструментованої версії ми отримали за 5 хвилин: нуль
welcome, нуль heartbeat, нуль будь-чого. Висновок однозначний: з'єднання
приймається, але даних по ньому не йде — і так, найпевніше, усі 34 доби.

ЗВІДСИ ТРИ РЕЧІ В ЦЬОМУ МОДУЛІ:
1. Кожен кадр лишає слід, включно з не-текстовими і з кодом закриття.
2. Жива САМОПЕРЕВІРКА. Постачальник приймає `{"type":"test"}` і відповідає
   синтетичним анонсом (DUMMYTOKEN, ліміт 1/хв). Це перетворює «сподіваємось,
   що працює» на «перевірено сьогодні о 15:40». Робимо це на кожному підключенні
   і на вимогу через /wstest.
3. Тиша тепер вимірювана: нема кадру довше WS_IDLE_ALERT_SEC при документованих
   30с — це аварія, а не затишшя.

ЩО ТОРГУЄМО. Тільки повний спот-делістинг Binance. Три незалежні ворота:
publisher має бути binance (фільтр ?cex= серверний, але покладатись лише на
нього не можна: URL у .env), listingType має бути делістингом, і ОРИГІНАЛЬНИЙ
заголовок має класифікуватись як SPOT_DELIST тим самим binance_watcher.classify,
що й поллінг. Margin-делістинг ми міряли окремо: −0.24% за хвилину і НУЛЬ
обвалів ≥10% із 48 пар, тобто це угода на шум.
"""
import asyncio
import time

import aiohttp

import alerts
import binance_watcher as bw
import config
import fastjson
import logbook as log

_handler = None
_notifier = None
_ws = None                 # живе з'єднання — для самоперевірки на вимогу

_FULL_FRAMES = 30          # перші N кадрів сирими: так видно welcome і реальну схему
_RAW_CLIP = 1500
_COMPACT_AFTER = 500
_SAMPLE_EVERY = 25
_SELFTEST_MIN_GAP = 70.0   # постачальник обмежує test 1/хв на ключ

# Коди закриття з документації — щоб «просто реконект» не ховав мертвий ключ.
_CLOSE_MEANING = {
    1000: "штатне закриття (можливо key_expired або key_invalidated — дивись reason)",
    1008: "порушення політики (too_slow або rate_limit_exceeded)",
    1009: "кадр завеликий",
    1011: "внутрішня помилка сервера",
}

_stats = {
    "frames": 0, "non_text": 0, "binary_frames": 0, "acks": 0, "parse_fails": 0,
    "welcomes": 0, "heartbeats": 0, "announcements": 0, "delist_frames": 0,
    "signals": 0, "skipped_stale": 0, "skipped_category": 0, "skipped_publisher": 0,
    "schema_drift": 0, "errors_from_server": 0,
    "connects": 0, "disconnects": 0, "conn_errors": 0,
    "last_frame_ms": 0, "last_heartbeat_ms": 0, "last_selftest_ok_ms": 0,
    "last_selftest_ms": 0,
    "connected_since_ms": 0, "last_error": None, "last_close": None,
    "tier": None, "allowed_cex": None, "key_expires_at": None,
    "selftests_sent": 0, "selftests_ok": 0,
}
_types: dict[str, int] = {}
_listing_types: dict[str, int] = {}


def set_handler(fn) -> None:
    """fn(tickers: list[str], age_sec: float | None, source: str) -> awaitable."""
    global _handler
    _handler = fn


def set_notifier(fn) -> None:
    """fn(text: str) -> None — сповіщення про анонс (не лише торгований)."""
    global _notifier
    _notifier = fn


def stats() -> dict:
    d = dict(_stats)
    now = time.time()
    for src, dst in (("last_frame_ms", "last_frame_age_sec"),
                     ("last_heartbeat_ms", "last_heartbeat_age_sec"),
                     ("last_selftest_ok_ms", "last_selftest_age_sec")):
        d[dst] = round(now - _stats[src] / 1000, 1) if _stats[src] else None
    d["connected_sec"] = (round(now - _stats["connected_since_ms"] / 1000, 1)
                          if _stats["connected_since_ms"] else None)
    d["key_expires_in_sec"] = (round(_stats["key_expires_at"] - now)
                               if _stats["key_expires_at"] else None)
    d["types"] = dict(_types)
    d["listing_types"] = dict(_listing_types)
    return d


def healthy() -> bool:
    """Чи є ДОКАЗ, що фід живий саме як джерело даних.

    Доказ — свіжий heartbeat або свіжа вдала самоперевірка. З'єднання без кадрів
    доказом НЕ є: рівно в такому стані фід простояв 34 доби, а бот усі ці 34 доби
    звітував «⚡ WS». Документований інтервал heartbeat — 30с, тому вимога
    «кадр за останні WS_IDLE_ALERT_SEC» м'яка навіть із запасом.
    """
    if not config.CL_WS_KEY:
        return False
    now = time.time()
    for key in ("last_heartbeat_ms", "last_selftest_ok_ms"):
        ms = _stats[key]
        if ms and (now - ms / 1000) <= config.WS_IDLE_ALERT_SEC:
            return True
    return False


def parse(raw: str) -> dict:
    """Розбирає кадр за документованою схемою. Нічого не вирішує.

    Повертає і те, що потрібно для торгівлі, і те, що потрібно для діагностики
    дрейфу схеми: верхньорівневі ключі та чи є в сирому тексті слово delist.
    """
    text = raw if isinstance(raw, str) else str(raw)
    out = {
        "ok": False, "type": "", "listing_type": "", "publisher": "",
        "tickers": [], "title": "", "category": "",
        "dispatch_us": None, "detected_us": None,
        "transport_sec": None, "since_detect_sec": None,
        "abnormal_latency": False, "keys": [],
        "mentions_delist": "delist" in text.lower(), "is_delist_type": False,
    }
    try:
        d = fastjson.loads(text)
    except Exception:  # noqa: BLE001
        return out
    if not isinstance(d, dict):
        # Масив/рядок/число — валідний JSON, але не наша схема. Фіксуємо факт,
        # щоб зміна обгортки (напр. батчинг у список) не виглядала як тиша.
        out["ok"] = True
        out["type"] = "_" + type(d).__name__
        return out
    out["ok"] = True
    out["keys"] = sorted(d.keys())[:20]
    out["type"] = str(d.get("type") or "")
    out["listing_type"] = str(d.get("listingType") or "")
    out["publisher"] = str(d.get("publisher") or "").lower()
    out["title"] = str(d.get("title") or "")[:300]
    out["abnormal_latency"] = bool(d.get("abnormalDetectionLatency"))
    tick = d.get("ticker")
    if isinstance(tick, str):
        out["tickers"] = [t.strip().upper() for t in tick.split(",") if t.strip()]
    elif isinstance(tick, list):
        out["tickers"] = [str(t).strip().upper() for t in tick if str(t).strip()]
    now_ms = time.time() * 1000
    for field, us_key, sec_key in (("dispatchTimestampUs", "dispatch_us", "transport_sec"),
                                   ("detectedTimestampUs", "detected_us", "since_detect_sec")):
        v = d.get(field)
        if isinstance(v, (int, float)) and v > 0:
            out[us_key] = int(v)
            # Мітка в МІКРОсекундах: /1000 -> мс, далі /1000 -> с.
            out[sec_key] = round((now_ms - v / 1000) / 1000, 3)
    out["is_delist_type"] = out["listing_type"] in ("spot_delisting", "futures_delisting")
    if out["title"]:
        out["category"] = bw.classify(out["title"])
    return out


def _bump(counter: dict, key: str) -> None:
    if key:
        counter[key] = counter.get(key, 0) + 1


async def _selftest_send(reason: str) -> bool:
    """Просить у постачальника синтетичний анонс. Це ЄДИНИЙ спосіб довести, що
    шлях «сокет -> парсер -> наш обробник» живий, не чекаючи делістингу."""
    ws = _ws
    if ws is None or ws.closed:
        log.event("ws_selftest_skipped", reason="нема з'єднання", trigger=reason)
        return False
    last = _stats["last_selftest_ms"]
    if last and (time.time() - last) < _SELFTEST_MIN_GAP:
        log.event("ws_selftest_skipped", reason="ліміт постачальника 1/хв",
                  trigger=reason)
        return False
    try:
        await ws.send_json({"type": "test"})
    except Exception as e:  # noqa: BLE001
        log.event("ws_selftest_send_failed", err=f"{type(e).__name__}: {e}"[:160])
        return False
    _stats["last_selftest_ms"] = time.time()
    _stats["selftests_sent"] += 1
    log.event("ws_selftest_sent", trigger=reason, sent=_stats["selftests_sent"])
    return True


async def selftest(reason: str = "manual", wait_sec: float = 10.0) -> dict:
    """Самоперевірка з очікуванням відповіді. Для /wstest і для перевірки після
    підключення. Повертає знімок із полем ok."""
    before = _stats["selftests_ok"]
    sent = await _selftest_send(reason)
    if not sent:
        return {"ok": False, "sent": False, "why": "не вдалось надіслати запит"}
    deadline = time.time() + wait_sec
    while time.time() < deadline:
        await asyncio.sleep(0.25)
        if _stats["selftests_ok"] > before:
            return {"ok": True, "sent": True,
                    "took_sec": round(wait_sec - (deadline - time.time()), 2)}
    return {"ok": False, "sent": True,
            "why": "відповіді немає за " + str(wait_sec) + "с"}


async def _on_welcome(p: dict, d_raw: str) -> None:
    _stats["welcomes"] += 1
    try:
        d = fastjson.loads(d_raw)
    except Exception:  # noqa: BLE001
        return
    _stats["tier"] = d.get("tier")
    _stats["allowed_cex"] = d.get("allowedCex")
    exp = d.get("expiresInSecs")
    if isinstance(exp, (int, float)) and exp > 0:
        _stats["key_expires_at"] = time.time() + float(exp)
    log.event("ws_welcome", tier=_stats["tier"], allowed_cex=_stats["allowed_cex"],
              expires_in_sec=exp, max_distinct_ips=d.get("maxDistinctIps"),
              max_conn_per_ip=d.get("maxConnectionsPerIp"))
    # Безкоштовні ключі видаються на ТИЖДЕНЬ і продовжуються на запит. Дізнатись
    # про це після того, як ключ помер, означає пропустити делістинг — тому
    # попереджаємо завчасно і завчасно ж пишемо, що робити.
    if isinstance(exp, (int, float)) and 0 < exp < config.WS_KEY_WARN_SEC:
        await alerts.raise_alert(
            "Ключ WS скоро протермінується",
            "Постачальник каже: лишилось " + str(round(exp / 3600, 1)) + " год."
            + chr(10) + "Тариф: " + str(_stats["tier"])
            + ". Безкоштовні ключі видаються на тиждень і продовжуються на запит "
            + "у @CLWfeed." + chr(10)
            + "Без ключа швидкий тригер зникає, детект падає на поллінг (медіана 46с).",
            cooldown_sec=12 * 3600)


async def _on_frame(raw: str, binary: bool = False) -> None:
    now_ms = int(time.time() * 1000)
    _stats["frames"] += 1
    if binary:
        _stats["binary_frames"] += 1
    _stats["last_frame_ms"] = now_ms
    n = _stats["frames"]
    p = parse(raw)

    if not p["ok"]:
        _stats["parse_fails"] += 1
    _bump(_types, p["type"] or "_unparsed")
    _bump(_listing_types, p["listing_type"])

    # Перші кадри — сирими. Саме тут буде welcome із тарифом, лімітами й терміном
    # ключа; без нього ми одного разу вже діагностували 429 як «термін ключа»,
    # хоча насправді це був maxDistinctIps=1 і друге з'єднання з іншої машини.
    if n <= _FULL_FRAMES:
        log.event("ws_frame", n=n, raw=raw[:_RAW_CLIP], keys=p["keys"],
                  type=p["type"], listing_type=p["listing_type"],
                  binary=binary)
    elif p["type"] == "heartbeat":
        pass          # кожні 30с: у лог не пишемо, лише лічильник і мітка часу
    elif n <= _COMPACT_AFTER or n % _SAMPLE_EVERY == 0:
        log.event("ws_frame", n=n, type=p["type"], listing_type=p["listing_type"],
                  publisher=p["publisher"], tickers=p["tickers"],
                  title=p["title"][:120], sampled=n > _COMPACT_AFTER)

    if p["type"] == "heartbeat":
        _stats["heartbeats"] += 1
        _stats["last_heartbeat_ms"] = now_ms
        return
    if p["type"] == "welcome":
        await _on_welcome(p, raw)
        return
    if p["type"] == "test_announcement":
        # Синтетичний анонс від постачальника. ТОРГУВАТИ ЙОГО НЕ МОЖНА НІКОЛИ —
        # це і є сенс окремого типу. Для нас це доказ, що шлях живий.
        _stats["selftests_ok"] += 1
        _stats["last_selftest_ok_ms"] = now_ms
        log.event("ws_selftest_ok", tickers=p["tickers"], listing_type=p["listing_type"],
                  ok_total=_stats["selftests_ok"])
        await alerts.clear_alert("WS: фід мовчить")
        await alerts.clear_alert("WS: жодного кадру від фіда")
        return
    if p["type"] == "error":
        _stats["errors_from_server"] += 1
        log.event("ws_server_error", raw=raw[:400])
        return

    if p["type"] != "announcement":
        # Невідомий тип. Якщо в ньому є слово delist — це вже не цікавинка, а
        # ризик пропустити подію: саме так виглядав би дрейф схеми.
        if p["mentions_delist"]:
            await _drift(n, p, raw)
        elif p["type"] not in ("", "changelog", "renewal_notice"):
            log.event("ws_unknown_type", n=n, type=p["type"], keys=p["keys"])
        return

    _stats["announcements"] += 1
    if not p["is_delist_type"]:
        # Лістинги теж приходять — це нормальна робота фіда, не наша подія.
        if p["mentions_delist"]:
            await _drift(n, p, raw)
        return
    _stats["delist_frames"] += 1

    log.event("ws_delisting", listing_type=p["listing_type"], publisher=p["publisher"],
              tickers=p["tickers"], title=p["title"], category=p["category"],
              transport_sec=p["transport_sec"], since_detect_sec=p["since_detect_sec"],
              abnormal_latency=p["abnormal_latency"])
    if _notifier is not None:
        _notifier("⚡ <b>WS: " + p["listing_type"] + " (" + (p["publisher"] or "?")
                  + ")</b>" + chr(10) + "Токени: " + (", ".join(p["tickers"]) or "—")
                  + chr(10) + "<i>" + p["title"] + "</i>")

    # --- ворота до реальних грошей ---
    # 1) Біржа. Серверний фільтр ?cex= задається в .env і може змінитись; подія з
    # Upbit/Bithumb має іншу економіку, яку ми не підтверджували.
    if p["publisher"] and p["publisher"] != "binance":
        _stats["skipped_publisher"] += 1
        log.event("ws_publisher_no_trade", publisher=p["publisher"],
                  tickers=p["tickers"])
        return
    # 2) Тип. Ф'ючерсний делістинг не чіпає спот — ми його не торгуємо.
    if p["listing_type"] != "spot_delisting":
        return
    # 3) Категорія з ОРИГІНАЛЬНОГО заголовка — та сама класифікація, що в поллінгу.
    if p["category"] and p["category"] != bw.SPOT_DELIST:
        _stats["skipped_category"] += 1
        log.event("ws_category_no_trade", category=p["category"],
                  tickers=p["tickers"], title=p["title"][:160])
        return
    if not p["tickers"]:
        log.event("ws_no_tickers", title=p["title"], keys=p["keys"])
        return
    if "DUMMYTOKEN" in p["tickers"]:
        # Подвійний запобіжник: синтетика має відсікатись типом вище, але ціна
        # помилки тут — реальний ордер на неіснуючий токен.
        log.event("ws_dummy_no_trade", tickers=p["tickers"])
        return

    # Вік рахуємо від ЇХНЬОГО детекту — та сама опора, що й у телеграм-фіда, тож
    # обидва шляхи міряються однаково. Плюс явна константа «їхній детект відносно
    # release_ms», яку ми не знаємо і тому не вдаємо, що знаємо.
    base = p["since_detect_sec"] if p["since_detect_sec"] is not None else p["transport_sec"]
    est_age = round(base + config.FEED_DETECT_LAG_SEC, 2) if base is not None else None
    if est_age is None:
        log.event("ws_age_unknown", tickers=p["tickers"], keys=p["keys"])
    elif est_age > config.MAX_SIGNAL_AGE_SEC:
        _stats["skipped_stale"] += 1
        log.event("ws_stale_no_trade", tickers=p["tickers"], est_age_sec=est_age,
                  limit=config.MAX_SIGNAL_AGE_SEC)
        return
    if _handler is None:
        log.error("wsfeed: обробник не встановлено — сигнал втрачено")
        return

    _stats["signals"] += 1
    log.event("ws_signal", tickers=p["tickers"], est_age_sec=est_age,
              since_detect_sec=p["since_detect_sec"], transport_sec=p["transport_sec"],
              detect_lag_const=config.FEED_DETECT_LAG_SEC,
              abnormal_latency=p["abnormal_latency"])
    await _handler(p["tickers"], est_age, "ws_cryptolisting")


async def _drift(n: int, p: dict, raw: str) -> None:
    """Кадр говорить про делістинг, а наші ворота його не впізнали."""
    _stats["schema_drift"] += 1
    log.event("ws_schema_drift", n=n, type=p["type"], listing_type=p["listing_type"],
              keys=p["keys"], raw=raw[:_RAW_CLIP])
    await alerts.raise_alert(
        "WS: кадр про делістинг не підпадає під фільтр",
        "Прилетів кадр зі словом delist, але type=" + repr(p["type"])
        + " listingType=" + repr(p["listing_type"]) + "." + chr(10)
        + "Схема фіда, найпевніше, змінилась — торговий фільтр її не впізнає."
        + chr(10) + "Дивись події ws_schema_drift у events.jsonl.",
        cooldown_sec=3600)


async def _watch_loop() -> None:
    """Зведення і сторож тиші. Документований heartbeat — 30с, тому мовчання
    довше за WS_IDLE_ALERT_SEC означає поломку, а не затишшя на ринку."""
    while True:
        await asyncio.sleep(config.WS_HEARTBEAT_SEC)
        s = stats()
        log.event("ws_health", **{k: v for k, v in s.items()
                                  if k not in ("last_error", "last_close")})
        if not config.CL_WS_KEY or _ws is None or _ws.closed:
            continue
        age = s["last_frame_age_sec"]
        if age is None:
            await alerts.raise_alert(
                "WS: жодного кадру від фіда",
                "З'єднання тримається " + str(round((s["connected_sec"] or 0) / 60))
                + " хв, реконектів " + str(s["connects"])
                + ", але не прийшло ЖОДНОГО кадру — навіть вітального." + chr(10)
                + "Постачальник документує welcome одразу і heartbeat кожні 30с, "
                + "тож це поломка, а не тиша на ринку." + chr(10)
                + "Швидкий тригер де-факто мертвий: детект падає на поллінг (~46с).")
        elif age > config.WS_IDLE_ALERT_SEC:
            await alerts.raise_alert(
                "WS: фід мовчить",
                "Останній кадр " + str(round(age / 60, 1)) + " хв тому при "
                + "документованому heartbeat кожні 30с." + chr(10)
                + "Кадрів усього " + str(s["frames"]) + ", heartbeat "
                + str(s["heartbeats"]) + ".")
        else:
            await alerts.clear_alert("WS: фід мовчить")
            await alerts.clear_alert("WS: жодного кадру від фіда")


async def run() -> None:
    """Тримає підписку на фід. Повертається лише якщо ключа нема."""
    global _ws
    if not config.CL_WS_KEY:
        log.event("loop_disabled", loop="ws", reason="нема CL_WS_KEY")
        log.info("WS: CL_WS_KEY не заданий — швидкий тригер вимкнено (лишається поллінг)")
        return
    asyncio.ensure_future(_watch_loop())
    backoff = 5.0
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(config.CL_WS_URL,
                                        headers={"X-API-Key": config.CL_WS_KEY},
                                        heartbeat=20, timeout=25) as ws:
                    _ws = ws
                    _stats["connects"] += 1
                    _stats["connected_since_ms"] = int(time.time() * 1000)
                    log.event("ws_connected", url=config.CL_WS_URL,
                              connects=_stats["connects"], frames_total=_stats["frames"])
                    backoff = 5.0
                    # Доказ життя одразу після підключення, не чекаючи делістингу.
                    asyncio.ensure_future(_selftest_after_connect())
                    async for msg in ws:
                        # BINARY нарівні з TEXT (2026-10-03). Саме це й ховало
                        # фід 34 доби: код читав ТІЛЬКИ TEXT, а лічильник
                        # non_text показав 482 відкинуті кадри за 4 години —
                        # рівно один на 30с, тобто документований heartbeat.
                        # Транспортний кадр — це спосіб доставки, а не формат
                        # даних; розбирати треба вміст, а не тип кадру.
                        if msg.type in (aiohttp.WSMsgType.TEXT,
                                        aiohttp.WSMsgType.BINARY):
                            try:
                                data = msg.data
                                if isinstance(data, (bytes, bytearray)):
                                    data = bytes(data).decode("utf-8", "replace")
                                await _on_frame(data, binary=msg.type is
                                                aiohttp.WSMsgType.BINARY)
                            except Exception:  # noqa: BLE001
                                # Виняток на одному кадрі не має вбивати підписку:
                                # наступний анонс важливіший за цей.
                                log.exception("wsfeed: обробка кадру впала")
                        elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                          aiohttp.WSMsgType.ERROR):
                            break
                        else:
                            # BINARY/PING/PONG. Раніше вони зникали безслідно, і
                            # «сервер шле лише бінарне» виглядало як повна тиша.
                            _stats["non_text"] += 1
                    code = getattr(ws, "close_code", None)
                    _stats["last_close"] = code
                    _stats["disconnects"] += 1
                    _stats["connected_since_ms"] = 0
                    _ws = None
                    log.event("ws_disconnected", close_code=code,
                              meaning=_CLOSE_MEANING.get(code, "невідомий код"),
                              **{k: _stats[k] for k in
                                 ("frames", "heartbeats", "signals", "connects")})
                    if code in (1008, 1009) or (code == 1000 and _stats["frames"] == 0):
                        await alerts.raise_alert(
                            "WS: сервер закрив з'єднання (" + str(code) + ")",
                            _CLOSE_MEANING.get(code, "невідомий код") + chr(10)
                            + "Кадрів за це з'єднання: " + str(_stats["frames"])
                            + ". Найчастіші причини: ключ протермінований або "
                            + "відкликаний, або перевищено ліміт IP/з'єднань.")
        except Exception as e:  # noqa: BLE001
            _ws = None
            _stats["conn_errors"] += 1
            _stats["last_error"] = (type(e).__name__ + ": " + str(e))[:200]
            # HTTP-статус рукостискання — найцінніше тут: 401 це мертвий ключ,
            # 429 це ліміт (одного разу вже сплутали з протермінуванням ключа).
            log.event("ws_error", err=_stats["last_error"],
                      status=getattr(e, "status", None),
                      errors=_stats["conn_errors"], retry_sec=round(backoff, 1))
            if _stats["conn_errors"] in (3, 30, 300):
                await alerts.raise_alert(
                    "WS: не вдається підключитись",
                    "Спроб поспіль: " + str(_stats["conn_errors"]) + chr(10)
                    + str(_stats["last_error"]) + chr(10)
                    + "Швидкий тригер недоступний — лишається поллінг (~46с).")
        await asyncio.sleep(backoff)
        backoff = min(120.0, backoff * 2)


async def _selftest_after_connect() -> None:
    """Через кілька секунд після підключення: welcome уже мав прийти, і тепер
    питання лише в тому, чи йдуть дані взагалі."""
    await asyncio.sleep(5.0)
    res = await selftest("after_connect", wait_sec=12.0)
    if res.get("ok"):
        return
    log.event("ws_selftest_failed", **res)
    await alerts.raise_alert(
        "WS: самоперевірка не пройшла",
        "Після підключення надіслано {\"type\":\"test\"} — синтетичного анонсу у "
        "відповідь НЕ прийшло." + chr(10)
        + "Причина: " + str(res.get("why")) + chr(10)
        + "Тобто сокет відкритий, але даних по ньому нема. Швидкий тригер не "
        + "працює; детект впаде на поллінг (медіана 46с: ~+5% на угоду замість ~+26%).",
        cooldown_sec=6 * 3600)
