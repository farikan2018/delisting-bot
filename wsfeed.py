"""Швидкий тригер №1: WebSocket-фід cryptolisting.ws.

ЧОМУ ЦЕ ВИНЕСЕНО З main.py І ОБВІШАНО ЛІЧИЛЬНИКАМИ (2026-10-03).

Стан, який це виявив. З 30.08 по 01.10 у логах 20 подій `ws_connected` — тобто
транспорт живий і реконекти рідкі. За той самий час подій `ws_delisting` — НУЛЬ.
01.10 Binance опублікував реальний анонс «Binance Futures Will Delist Multiple
USDⓈ-M Perpetual Contracts», який під фільтр `futures_delisting` підпадає, — і WS
про нього не сказав нічого; подію зловив лише власний поллінг, на +33с.

Проблема була не стільки в цьому, скільки в тому, що ми НЕ МОГЛИ ВІДРІЗНИТИ
три різні світи:
    (а) фід здоровий, просто делістингів не було (вони бувають раз на 3-6 тижнів);
    (б) тариф FreeDelayed делістинги взагалі не шле;
    (в) постачальник змінив схему, кадри йдуть, а наш фільтр їх мовчки викидає.
Старий код читав `async for msg in ws` і не лишав по кадру ЖОДНОГО сліду, якщо
кадр не збігся з фільтром. Тобто (б) і (в) виглядали рівно як (а) — вічно.

Що тепер. Кожен кадр лишає слід. Перші `_FULL_FRAMES` — сирими (саме так видно
вітальний кадр із тарифом і лімітами). Далі — компактно, з придушенням, якщо фід
раптом стане балакучим. Раз на `WS_HEARTBEAT_SEC` — зведення, тож ТИША ТЕЖ ВИДНА.
І головне: якщо в сирому кадрі є слово delist, але наш фільтр його не впізнав —
це подія `ws_schema_drift` плюс сповіщення в Telegram. Тобто випадок (в) тепер
гучний, а не вічний.

ЩО НЕ ЗМІНИЛОСЬ СВІДОМО: умова, за якою відкривається РЕАЛЬНА позиція. Вона
точно та сама, що була. Дрейф схеми ми ПОМІЧАЄМО, але не намагаємось вгадати
новий формат і торгувати по здогадці — на це є живі гроші й $9.35 балансу.
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

_FULL_FRAMES = 25          # перші N кадрів — сирими: так видно вітальний кадр і схему
_RAW_CLIP = 1500
_COMPACT_AFTER = 400       # якщо фід виявиться балакучим — переходимо на вибірку
_SAMPLE_EVERY = 25

_stats = {
    "frames": 0, "acks": 0, "parse_fails": 0, "announcements": 0,
    "delist_frames": 0, "signals": 0, "skipped_stale": 0, "schema_drift": 0,
    "connects": 0, "disconnects": 0, "conn_errors": 0,
    "last_frame_ms": 0, "connected_since_ms": 0, "last_error": None,
}
_types: dict[str, int] = {}
_listing_types: dict[str, int] = {}


def set_handler(fn) -> None:
    """fn(tickers: list[str], age_sec: float | None, source: str) -> awaitable."""
    global _handler
    _handler = fn


def set_notifier(fn) -> None:
    """fn(text: str) -> None — сповіщення про БУДЬ-ЯКИЙ анонс (не лише торгований)."""
    global _notifier
    _notifier = fn


def stats() -> dict:
    d = dict(_stats)
    now = time.time()
    d["last_frame_age_sec"] = (round(now - _stats["last_frame_ms"] / 1000, 1)
                               if _stats["last_frame_ms"] else None)
    d["connected_sec"] = (round(now - _stats["connected_since_ms"] / 1000, 1)
                          if _stats["connected_since_ms"] else None)
    d["types"] = dict(_types)
    d["listing_types"] = dict(_listing_types)
    return d


def healthy() -> bool:
    """Чи є ДОКАЗ, що фід живий саме як джерело даних, а не лише як сокет.

    Доказ — отриманий кадр. З'єднання без жодного кадру доказом не є: рівно в
    такому стані фід простояв 34 доби й ми вважали його робочим.
    """
    return bool(config.CL_WS_KEY) and _stats["frames"] > 0


def parse(raw: str) -> dict:
    """Розбирає кадр. Нічого не вирішує — рішення ухвалює викликач.

    Повертає і те, що потрібно для торгівлі, і те, що потрібно для діагностики
    дрейфу схеми: верхньорівневі ключі та чи є в сирому тексті слово delist.
    """
    text = raw if isinstance(raw, str) else str(raw)
    low = text.lower()
    out = {
        "ok": False, "type": "", "listing_type": "", "tickers": [], "title": "",
        "dispatch_us": None, "transport_sec": None, "keys": [],
        "is_ack": False, "mentions_delist": "delist" in low, "tradeable": False,
    }
    try:
        d = fastjson.loads(text)
    except Exception:  # noqa: BLE001
        return out
    if not isinstance(d, dict):
        # Масив/рядок/число — теж валідний JSON, але не наша схема. Фіксуємо факт,
        # щоб зміна обгортки (напр. батчинг у список) не виглядала як тиша.
        out["ok"] = True
        out["type"] = "_" + type(d).__name__
        return out
    out["ok"] = True
    out["keys"] = sorted(d.keys())[:20]
    out["type"] = str(d.get("type") or "")
    out["listing_type"] = str(d.get("listingType") or "")
    out["title"] = str(d.get("title") or "")[:200]
    # Вітальний кадр / ack: нема ні типу, ні тикера. Саме в ньому постачальник
    # повідомляє тариф і maxDistinctIps — тому він нам потрібен у логу цілим.
    out["is_ack"] = not out["type"] and not d.get("ticker")
    tick = d.get("ticker")
    if isinstance(tick, str):
        out["tickers"] = [t.strip().upper() for t in tick.split(",") if t.strip()]
    elif isinstance(tick, list):
        out["tickers"] = [str(t).strip().upper() for t in tick if str(t).strip()]
    disp = d.get("dispatchTimestampUs")
    if isinstance(disp, (int, float)) and disp > 0:
        out["dispatch_us"] = int(disp)
        # Мітка в МІКРОсекундах: /1000 -> мс, далі /1000 -> с.
        out["transport_sec"] = round((time.time() * 1000 - disp / 1000) / 1000, 3)
    out["tradeable"] = (out["type"] == "announcement"
                        and out["listing_type"] in ("spot_delisting", "futures_delisting"))
    return out


def _bump(counter: dict, key: str) -> None:
    if key:
        counter[key] = counter.get(key, 0) + 1


async def _on_frame(raw: str) -> None:
    now_ms = int(time.time() * 1000)
    _stats["frames"] += 1
    _stats["last_frame_ms"] = now_ms
    n = _stats["frames"]
    p = parse(raw)

    if not p["ok"]:
        _stats["parse_fails"] += 1
    _bump(_types, p["type"] or ("_ack" if p["is_ack"] else "_unknown"))
    _bump(_listing_types, p["listing_type"])
    if p["is_ack"]:
        _stats["acks"] += 1

    # Перші кадри — сирими. Саме тут буде вітальний кадр із тарифом; без нього ми
    # одного разу вже діагностували 429 як «термін ключа», хоча насправді це був
    # maxDistinctIps=1 і друге з'єднання з іншої машини.
    if n <= _FULL_FRAMES:
        log.event("ws_frame", n=n, raw=raw[:_RAW_CLIP], keys=p["keys"],
                  type=p["type"], listing_type=p["listing_type"])
    elif n <= _COMPACT_AFTER or n % _SAMPLE_EVERY == 0:
        log.event("ws_frame", n=n, type=p["type"], listing_type=p["listing_type"],
                  tickers=p["tickers"], title=p["title"][:120],
                  sampled=n > _COMPACT_AFTER)

    # Дрейф схеми: у кадрі є слово delist, а фільтр його не впізнав. Це рівно той
    # стан, який раніше був невидимий — і який коштував би нам пропущеного анонсу.
    if p["mentions_delist"] and not p["tradeable"]:
        _stats["schema_drift"] += 1
        log.event("ws_schema_drift", n=n, type=p["type"],
                  listing_type=p["listing_type"], keys=p["keys"], raw=raw[:_RAW_CLIP])
        await alerts.raise_alert(
            "WS: кадр про делістинг не підпадає під фільтр",
            "Прилетів кадр зі словом delist, але type=" + repr(p["type"])
            + " listingType=" + repr(p["listing_type"]) + "." + chr(10)
            + "Схема фіда, найпевніше, змінилась — торговий фільтр її не впізнає."
            + chr(10) + "Дивись події ws_schema_drift у events.jsonl.",
            cooldown_sec=3600)
        return

    if not p["tradeable"]:
        return

    _stats["announcements"] += 1
    _stats["delist_frames"] += 1
    log.event("ws_delisting", listing_type=p["listing_type"], tickers=p["tickers"],
              title=p["title"], transport_age_sec=p["transport_sec"])
    if _notifier is not None:
        _notifier("⚡ <b>WS-сигнал: " + p["listing_type"] + "</b>" + chr(10)
                  + "Токени: " + (", ".join(p["tickers"]) or "—") + chr(10)
                  + "<i>" + p["title"] + "</i>")

    if p["listing_type"] != "spot_delisting":
        return

    # transport_sec — це ЛИШЕ транспорт від їхньої відправки. Справжній вік сигналу
    # більший на час їхнього власного детекту (заміряно 2.28с на живому делістингу
    # 20.08) — звідси явна константа, яку видно поруч із компонентами.
    est_age = (round(p["transport_sec"] + config.FEED_DETECT_LAG_SEC, 2)
               if p["transport_sec"] is not None else None)
    if est_age is None:
        # Мітки часу нема — ворота застарілості застосувати ні до чого. Пуш по WS
        # майже напевно свіжий, тож НЕ блокуємо угоду (пропустити делістинг гірше,
        # ніж увійти без заміру), але лишаємо гучний слід: зникнення поля означає
        # зміну схеми, а її краще помітити одразу.
        log.event("ws_age_unknown", tickers=p["tickers"], keys=p["keys"])
    elif est_age > config.MAX_SIGNAL_AGE_SEC:
        _stats["skipped_stale"] += 1
        log.event("ws_stale_no_trade", tickers=p["tickers"], est_age_sec=est_age,
                  transport_sec=p["transport_sec"], limit=config.MAX_SIGNAL_AGE_SEC)
        return
    if not p["tickers"]:
        log.event("ws_no_tickers", title=p["title"], keys=p["keys"])
        return
    if _handler is None:
        log.error("wsfeed: обробник не встановлено — сигнал втрачено")
        return

    _stats["signals"] += 1
    log.event("ws_signal", tickers=p["tickers"], est_age_sec=est_age,
              transport_sec=p["transport_sec"],
              detect_lag_const=config.FEED_DETECT_LAG_SEC)
    await _handler(p["tickers"], est_age, "ws_cryptolisting")


async def _heartbeat_loop() -> None:
    """Зведення раз на WS_HEARTBEAT_SEC — щоб ТИША була видимою величиною, а не
    відсутністю рядків. Плюс сповіщення, якщо кадрів нема аж надто довго."""
    while True:
        await asyncio.sleep(config.WS_HEARTBEAT_SEC)
        s = stats()
        log.event("ws_heartbeat", **{k: v for k, v in s.items()
                                     if k not in ("last_error",)})
        if not config.CL_WS_KEY:
            continue
        age = s["last_frame_age_sec"]
        if age is None:
            # Жодного кадру за весь час роботи процесу. Саме так виглядав фід 34 доби.
            if s["connected_sec"] and s["connected_sec"] > config.WS_IDLE_ALERT_SEC:
                await alerts.raise_alert(
                    "WS: жодного кадру від фіда",
                    "З'єднання тримається " + str(int(s["connected_sec"] // 3600))
                    + " год, реконектів " + str(s["connects"])
                    + ", але не прийшло ЖОДНОГО кадру — навіть вітального." + chr(10)
                    + "Швидкий тригер де-факто мертвий: детект впаде на поллінг (~46с).")
        elif age > config.WS_IDLE_ALERT_SEC:
            await alerts.raise_alert(
                "WS: фід мовчить",
                "Останній кадр був " + str(round(age / 3600, 1)) + " год тому."
                + chr(10) + "Кадрів усього " + str(s["frames"])
                + ", реконектів " + str(s["connects"]) + ".")
        else:
            await alerts.clear_alert("WS: фід мовчить")
            await alerts.clear_alert("WS: жодного кадру від фіда")


async def run() -> None:
    """Тримає підписку на фід. Повертається лише якщо ключа нема."""
    if not config.CL_WS_KEY:
        log.event("loop_disabled", loop="ws", reason="нема CL_WS_KEY")
        log.info("WS: CL_WS_KEY не заданий — швидкий тригер вимкнено (лишається поллінг)")
        return
    asyncio.ensure_future(_heartbeat_loop())
    backoff = 5.0
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(config.CL_WS_URL,
                                        headers={"X-API-Key": config.CL_WS_KEY},
                                        heartbeat=15, timeout=25) as ws:
                    _stats["connects"] += 1
                    _stats["connected_since_ms"] = int(time.time() * 1000)
                    log.event("ws_connected", url=config.CL_WS_URL,
                              connects=_stats["connects"], frames_total=_stats["frames"])
                    backoff = 5.0
                    async for msg in ws:
                        if msg.type is aiohttp.WSMsgType.TEXT:
                            try:
                                await _on_frame(msg.data)
                            except Exception:  # noqa: BLE001
                                # Виняток на одному кадрі не має вбивати підписку:
                                # наступний анонс важливіший за цей.
                                log.exception("wsfeed: обробка кадру впала")
                        elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                          aiohttp.WSMsgType.ERROR):
                            break
                    _stats["disconnects"] += 1
                    _stats["connected_since_ms"] = 0
                    log.event("ws_disconnected", close_code=getattr(ws, "close_code", None),
                              **{k: _stats[k] for k in ("frames", "signals", "connects")})
        except Exception as e:  # noqa: BLE001
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
