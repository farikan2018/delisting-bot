"""Швидкий тригер №2: телеграм-канал анонсів @CLWfeed (Telethon userbot).

ЧОМУ ЦЕ ІСНУЄ. Власний поллінг CMS Binance дає ~15с — і це НЕ через частоту
опитування (CloudFront не кешує, кожен хост опитується раз на 500мс), а через
лаг індексу бекенда: стаття з'являється у відповіді
`/bapi/apex/v1/public/apex/cms/article/list/query` приблизно через 13с після
`release_ms`. Опитувати частіше безсенсу.

Заміряно на живому делістингу ICX/SCRT/STORJ (2026-08-20):
    release_ms                          0
    cryptolisting задетектив       +2.28с   (їхня мітка, µs)
    ринок поїхав                   +2.31с
    @CLWfeed доставив нам          +4.51с   <- цей модуль
    наш поллінг CMS               +14.97с
І крива PnL по 63 тикер-подіях: 1-2с → +26%, 5с → +12.8%, 15с → +5.2% на угоду.
Тобто цей канал утричі цінніший за поллінг і при цьому безкоштовний — потрібна
лише Telethon-сесія, платний WS-ключ не потрібен.

ФОРМАТ ПОВІДОМЛЕННЯ (реальний приклад):
    BINANCE Delisting Announcement – $ICX, $SCRT, $STORJ
    Binance Will Delist ICX, SCRT, STORJ on 2026-09-03
    Detected (UTC): 2026-08-20T06:00:08.885836Z (µs precision)
    Telegram message delayed by 150ms.

ЩО САМЕ ТОРГУЄМО. Тільки `BINANCE` + делістинг. Канал шле також UPBIT і BITHUMB —
це інші події з іншою економікою (корейський «유의 종목» ми міряли окремо: після
поправок ~+8..13% при граничній значущості), і змішувати їх у той самий шлях
означало б торгувати статистику, якої ми не перевіряли.

ПРО ЧЕСНІСТЬ ЗАМІРУ ВІКУ СИГНАЛУ. Ми не знаємо `release_ms` — його знає лише сам
CMS. Тому вік вважаємо як `(зараз − їхній Detected)` плюс явна константа
`FEED_DETECT_LAG_SEC` (їхнє відставання від release, заміряне як 2.28с на
одній події). Константа СВІДОМО окрема й логується поруч із компонентами, щоб її
можна було уточнити на наступних подіях, а не щоб вдавати точність.
"""
import asyncio
import datetime as dt
import re
import time

import alerts
import binance_watcher as bw
import config
import logbook as log

_handler = None
_stats = {"msgs": 0, "binance_delist": 0, "signals": 0, "skipped_stale": 0,
          "connects": 0, "last_msg_ms": 0, "errors": 0, "fatal": None}

# Винятки Telethon, яких ретрай НЕ ЛІКУЄ: ключ авторизації відкликано назавжди,
# новий можна отримати лише повторним входом із телефону (tg_login.py).
# Чому це окремий список. 2026-08-30 о 14:21 сесія померла з AuthKeyDuplicatedError
# (її використали з двох IP одночасно), і цикл нижче 34 доби поспіль перепідключався
# кожні 120с: 24411 однакових подій у events.jsonl, нуль сповіщень, швидкий тригер
# де-факто мертвий. Ретрай на такій помилці — не стійкість, а шум, який ХОВАЄ аварію.
_FATAL_ERRORS = (
    "AuthKeyDuplicatedError", "AuthKeyUnregisteredError", "AuthKeyInvalidError",
    "SessionRevokedError", "SessionExpiredError", "UserDeactivatedError",
    "UserDeactivatedBanError", "PhoneNumberBannedError",
)

# «$ICX, $SCRT» -> ICX, SCRT. Канал завжди префіксує тикери доларом, тож це
# надійніше за витягування великих слів із довільного тексту.
# Одна літера дозволена свідомо: реальний анонс «Binance Will Delist COS, D, HIGH,
# MBOX» містить тикер D, і старий мінімум у два символи його губив. binance_watcher
# однолітерні тикери приймає, і розбіжність між двома шляхами означала б, що та сама
# подія торгується або ні залежно від того, яке джерело спрацювало першим.
_TICKER_RE = re.compile(r"\$([A-Z][A-Z0-9]{0,9})\b")
_DETECTED_RE = re.compile(r"Detected \(UTC\):\s*([0-9T:.\-]+)Z")
# Канал цитує ОРИГІНАЛЬНИЙ заголовок Binance окремим рядком між переліком тикерів
# і міткою Detected. Він потрібен, щоб прогнати повідомлення через ту саму
# класифікацію, що й головний шлях (binance_watcher.classify).
_TITLE_RE = re.compile(r"\$[A-Z][A-Z0-9]{0,9}(?:\s*,\s*\$[A-Z][A-Z0-9]{0,9})*\s+"
                       r"(.+?)\s+Detected \(UTC\):")


def set_handler(fn) -> None:
    """fn(tickers: list[str], age_sec: float, source: str) -> awaitable."""
    global _handler
    _handler = fn


def stats() -> dict:
    d = dict(_stats)
    d["last_msg_age_sec"] = (round(time.time() - _stats["last_msg_ms"] / 1000, 1)
                             if _stats["last_msg_ms"] else None)
    return d


def healthy() -> bool:
    """Чи є ДОКАЗ, що канал живий саме як джерело даних.

    Доказ — отримане повідомлення, а не наявність кредів і не вдале підключення.
    Саме ця різниця коштувала 34 діб: креди були на місці, `cap_tg_feed` світився
    зеленим, одне `tg_feed_connected` у логу було — а повідомлень нуль.
    """
    return (bool(config.TG_API_ID and config.TG_API_HASH and config.TG_SESSION)
            and _stats["fatal"] is None and _stats["msgs"] > 0)


def parse(text: str) -> dict:
    """Розбирає пост каналу. Повертає що зміг; рішення ухвалює викликач."""
    flat = " ".join((text or "").split())
    low = flat.lower()
    m = _DETECTED_RE.search(flat)
    detected_ms = None
    if m:
        try:
            detected_ms = int(dt.datetime.fromisoformat(m.group(1))
                              .replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
        except Exception:  # noqa: BLE001
            detected_ms = None
    first = flat.split(" ", 1)[0].upper() if flat else ""
    tm = _TITLE_RE.search(flat)
    title = tm.group(1).strip() if tm else ""
    return {
        "text": flat,
        "exchange": first if first.isalpha() else "",
        "is_delist": "delist" in low,
        "is_listing": "listing" in low,
        "tickers": _TICKER_RE.findall(flat),
        "feed_detected_ms": detected_ms,
        "binance_title": title,
        # Та сама класифікація, що й у поллінг-шляху. Без неї цей шлях торгував би
        # БУДЬ-ЯКИЙ «BINANCE Delisting Announcement» — включно з margin-делістингом
        # (заміряно: -0.24% за хвилину, обвал >=10% у НУЛЯ з 48 пар) і з
        # ф'ючерсним. Тобто дві з трьох категорій, які головний шлях свідомо не
        # торгує, тут відкривали б реальну позицію на $12 номіналу.
        "category": bw.classify(title) if title else "",
    }


async def _on_message(text: str) -> None:
    now_ms = int(time.time() * 1000)
    _stats["msgs"] += 1
    _stats["last_msg_ms"] = now_ms
    p = parse(text)

    transport = (round((now_ms - p["feed_detected_ms"]) / 1000, 3)
                 if p["feed_detected_ms"] else None)
    age = (round(transport + config.FEED_DETECT_LAG_SEC, 3)
           if transport is not None else None)

    log.event("tg_feed_msg", exchange=p["exchange"], is_delist=p["is_delist"],
              tickers=p["tickers"], transport_sec=transport, est_age_sec=age,
              category=p["category"], title=p["text"][:160])

    if p["exchange"] != "BINANCE" or not p["is_delist"] or not p["tickers"]:
        return
    _stats["binance_delist"] += 1

    # Категорія з ОРИГІНАЛЬНОГО заголовка Binance. Торгуємо лише повний спот-
    # делістинг — рівно як поллінг і WS. Якщо заголовка в пості нема, категорію
    # визначити ні з чого: тоді торгуємо (пропустити делістинг дорожче за зайву
    # угоду на $12), але лишаємо гучний слід, щоб побачити реальні формати каналу
    # і уточнити правило на даних, а не на здогадці.
    if p["category"] and p["category"] != bw.SPOT_DELIST:
        _stats["skipped_category"] = _stats.get("skipped_category", 0) + 1
        log.event("tg_feed_category_no_trade", tickers=p["tickers"],
                  category=p["category"], title=p["binance_title"][:160])
        return
    if not p["category"]:
        log.event("tg_feed_no_title", tickers=p["tickers"], text=p["text"][:200])

    if not config.TG_FEED_TRADE:
        log.event("tg_feed_no_trade", tickers=p["tickers"],
                  reason="TG_FEED_TRADE=0")
        return
    # Захист від «фід підвис і вивалив пачку»: транспорт міряє саме це.
    if age is not None and age > config.MAX_SIGNAL_AGE_SEC:
        _stats["skipped_stale"] += 1
        log.event("tg_feed_stale_no_trade", tickers=p["tickers"],
                  est_age_sec=age, limit=config.MAX_SIGNAL_AGE_SEC)
        return
    if _handler is None:
        log.error("tg_feed: обробник не встановлено — сигнал втрачено")
        return

    _stats["signals"] += 1
    log.event("tg_feed_signal", tickers=p["tickers"], est_age_sec=age,
              transport_sec=transport,
              detect_lag_const=config.FEED_DETECT_LAG_SEC)
    await _handler(p["tickers"], age, "tg_clwfeed")


async def run() -> None:
    """Тримає userbot-підписку. Повертається лише якщо можливість вимкнена —
    тоді це видно в `startup` через cap_tg_feed і в події нижче."""
    if not (config.TG_API_ID and config.TG_API_HASH and config.TG_SESSION):
        log.event("loop_disabled", loop="tg_feed",
                  reason="нема TG_API_ID/TG_API_HASH/TG_SESSION")
        return
    try:
        from telethon import TelegramClient, events
        from telethon.sessions import StringSession
        from telethon.tl.functions.channels import JoinChannelRequest
    except Exception as e:  # noqa: BLE001
        log.event("loop_disabled", loop="tg_feed",
                  reason=f"telethon недоступний: {type(e).__name__}")
        return

    client = TelegramClient(StringSession(config.TG_SESSION),
                            config.TG_API_ID, config.TG_API_HASH)

    @client.on(events.NewMessage(chats=config.TG_FEED_CHANNEL))
    async def _h(event):  # noqa: ANN001
        # Виняток тут не має вбивати підписку: наступний анонс важливіший за цей.
        try:
            await _on_message(event.message.message or "")
        except Exception:  # noqa: BLE001
            log.exception("tg_feed: обробка повідомлення впала")

    backoff = 5.0
    while True:
        try:
            await client.start()
            try:
                await client(JoinChannelRequest(config.TG_FEED_CHANNEL))
            except Exception:  # noqa: BLE001
                pass  # уже підписані або канал приватний — не привід падати
            me = await client.get_me()
            _stats["connects"] += 1
            log.event("tg_feed_connected", account=getattr(me, "username", None),
                      channel=config.TG_FEED_CHANNEL, connects=_stats["connects"])
            backoff = 5.0
            await client.run_until_disconnected()
            log.event("tg_feed_disconnected", **stats())
        except Exception as e:  # noqa: BLE001
            name = type(e).__name__
            _stats["errors"] += 1
            if name in _FATAL_ERRORS:
                # Ретрай тут безсенсу: сесію відкликано, лікує лише новий вхід.
                # Виходимо з циклу — краще ОДНА гучна аварія, ніж вічний шум.
                _stats["fatal"] = name
                log.event("tg_feed_fatal", err=name, detail=str(e)[:200],
                          errors=_stats["errors"])
                log.error("tg_feed: сесію Telegram відкликано (" + name
                          + ") — читач ЗУПИНЕНО, потрібен новий TG_SESSION")
                await alerts.raise_alert(
                    "Телеграм-фід мертвий: " + name,
                    "Сесію Telegram відкликано — ретрай це не лікує, читач зупинено."
                    + chr(10) + "Швидкий тригер №2 недоступний." + chr(10)
                    + "Лікується лише новою сесією на сервері:" + chr(10)
                    + "<code>cd ~/delisting-bot &amp;&amp; .venv/bin/python tg_login.py</code>"
                    + chr(10) + "далі покласти новий TG_SESSION у .env і "
                    + "<code>sudo systemctl restart delisting-bot</code>",
                    cooldown_sec=24 * 3600)
                return
            # Не-фатальні: мережа, флуд-вейт, тимчасовий збій ДЦ. Ретраїмо, але
            # НЕ пишемо 24 тисячі однакових рядків — лише перші кілька й далі рідко.
            if _stats["errors"] <= 3 or _stats["errors"] % 100 == 0:
                log.event("tg_feed_error", err=name + ": " + str(e)[:150],
                          errors=_stats["errors"], retry_sec=round(backoff, 1))
            if _stats["errors"] in (20, 500):
                await alerts.raise_alert(
                    "Телеграм-фід не підключається",
                    "Спроб поспіль: " + str(_stats["errors"]) + chr(10)
                    + name + ": " + str(e)[:150])
        await asyncio.sleep(backoff)
        backoff = min(120.0, backoff * 2)
