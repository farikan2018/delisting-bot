"""Парсер і ворота WebSocket-фіда cryptolisting.ws.

ЗВІДКИ ФІКСТУРИ. Це не здогадки: схема взята з документації постачальника
(https://cryptolisting.ws/docs/book/ і llms-full.txt, прочитано 2026-10-03) —
типи повідомлень `welcome`, `heartbeat`, `announcement`, `test_announcement`,
`error`, поля `publisher`, `listingType`, `detectedTimestampUs`,
`dispatchTimestampUs`, `abnormalDetectionLatency`, і повний перелік значень
listingType. Доти код фільтрував кадри за схемою, яку ніхто не звіряв із
джерелом, і жодного реального кадру в логах не було — за 34 доби.

НАЙВАЖЛИВІШІ ПЕРЕВІРКИ ТУТ — НЕГАТИВНІ:
  * `test_announcement` (DUMMYTOKEN) НЕ має відкривати позицію — інакше наша ж
    самоперевірка стане причиною реального ордера на неіснуючий токен;
  * подія не з Binance НЕ має торгуватись — серверний фільтр ?cex= задається
    в .env і може змінитись, а корейська економіка нами не підтверджена;
  * margin- і futures-делістинг НЕ мають торгуватись (заміряно: margin дає
    −0.24% за хвилину і НУЛЬ обвалів ≥10% із 48 пар);
  * будь-який дрейф схеми має бути ПОМІЧЕНИМ, а не тихо викинутим.

Запуск: python test_wsfeed.py
"""

# Тести НІКОЛИ не пишуть у бойовий events.jsonl: 2026-10-03 прогін на сервері
# залишив там 15 синтетичних подій ws_frame, тобто отруїв саме той файл, за
# яким ми судимо, чи фід віддав хоч один справжній кадр. Має стояти ДО будь-якого
# імпорту модулів бота, бо logbook читає цю змінну на імпорті.
import os
os.environ.setdefault("BOT_LOG_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs-test"))

import asyncio
import json
import sys
import time

import config
import wsfeed

FAILS = []


def check(name, cond, extra=""):
    print("  " + ("OK  " if cond else "FAIL") + "  " + name
          + ((" - " + str(extra)) if extra else ""))
    if not cond:
        FAILS.append(name)


# --- перехоплення побічних ефектів: тест нікуди нічого не шле ---
SENT, DRIFTS, SIGNALS = [], [], []


async def _fake_alert(key, text, cooldown_sec=0, detail=None):
    DRIFTS.append((key, text))
    return True


async def _fake_clear(key, text=""):
    return True


wsfeed.alerts.raise_alert = _fake_alert
wsfeed.alerts.clear_alert = _fake_clear


async def _handler(tickers, age, source):
    SIGNALS.append({"tickers": tickers, "age": age, "source": source})


wsfeed.set_handler(_handler)
wsfeed.set_notifier(lambda text: SENT.append(text))


class _OMIT:
    pass


def ann(**kw):
    """Анонс за документованою схемою постачальника."""
    now_us = int(time.time() * 1_000_000)
    base = {"type": "announcement",
            "title": "Binance Will Delist ICX, SCRT, STORJ on 2026-09-03",
            "ticker": "ICX,SCRT,STORJ",
            "publisher": "binance",
            "listingType": "spot_delisting",
            "detectedTimestampUs": now_us - 240_000,
            "dispatchTimestampUs": now_us,
            "abnormalDetectionLatency": False}
    base.update(kw)
    return json.dumps({k: v for k, v in base.items() if v is not _OMIT})


def feed(raw):
    SIGNALS.clear()
    SENT.clear()
    DRIFTS.clear()
    asyncio.run(wsfeed._on_frame(raw))


print("=== 1) Канонічний спот-делістинг Binance: єдине, що відкриває позицію ===")
p = wsfeed.parse(ann())
check("тип розпізнано", p["type"] == "announcement" and p["is_delist_type"])
check("publisher binance", p["publisher"] == "binance", p["publisher"])
check("тикери з коми", p["tickers"] == ["ICX", "SCRT", "STORJ"], p["tickers"])
check("категорія із заголовка SPOT_DELIST", p["category"] == "SPOT_DELIST", p["category"])
check("мікросекунди переведено в секунди", p["since_detect_sec"] is not None
      and 0 <= p["since_detect_sec"] < 2, p["since_detect_sec"])
feed(ann())
check("обробник викликано рівно раз", len(SIGNALS) == 1, len(SIGNALS))
check("джерело позначене", SIGNALS and SIGNALS[0]["source"] == "ws_cryptolisting")
check("вік ≈ константа детекту (їхній детект щойно)",
      SIGNALS and abs(SIGNALS[0]["age"] - config.FEED_DETECT_LAG_SEC) < 1.0,
      SIGNALS[0]["age"] if SIGNALS else None)
check("сповіщення пішло", len(SENT) == 1)

print()
print("=== 2) САМОПЕРЕВІРКА не сміє стати реальною угодою ===")
feed(json.dumps({"type": "test_announcement", "title": "Binance Will Delist DUMMYTOKEN",
                 "ticker": "DUMMYTOKEN", "publisher": "binance",
                 "listingType": "spot_delisting",
                 "detectedTimestampUs": int(time.time() * 1e6),
                 "dispatchTimestampUs": int(time.time() * 1e6)}))
check("синтетичний анонс НЕ торгується", len(SIGNALS) == 0, len(SIGNALS))
check("але зараховано як доказ життя фіда", wsfeed.stats()["selftests_ok"] >= 1)
check("дрейфу схеми не зафіксовано", len(DRIFTS) == 0)
# Навіть якби тип був звичайним announcement — DUMMYTOKEN не має пройти.
feed(ann(ticker="DUMMYTOKEN", title="Binance Will Delist DUMMYTOKEN"))
check("DUMMYTOKEN не торгується і під виглядом звичайного анонсу", len(SIGNALS) == 0)

print()
print("=== 3) НЕ Binance: економіку корейських бірж ми не підтверджували ===")
for pub in ("upbit", "bithumb", "robinhood"):
    feed(ann(publisher=pub, title="Upbit Will Delist ICX",
             listingType="spot_delisting"))
    check(pub + " НЕ торгується", len(SIGNALS) == 0, len(SIGNALS))

print()
print("=== 4) Тип події: з делістингів торгуємо лише спотовий ===")
feed(ann(listingType="futures_delisting",
         title="Binance Futures Will Delist USD-M AERGOUSDT Perpetual Contract"))
check("futures_delisting НЕ торгується", len(SIGNALS) == 0)
check("але сповіщення про нього є", len(SENT) == 1)
feed(ann(listingType="spot_listing", title="Binance Will List SOMECOIN"))
check("spot_listing НЕ торгується", len(SIGNALS) == 0)
check("spot_listing не дає сповіщення про делістинг", len(SENT) == 0)
for lt in ("hodler_airdrop", "monitoring_tag_extend", "caution_released", "not_listing"):
    feed(ann(listingType=lt, title="Binance announcement"))
    check(lt + " НЕ торгується", len(SIGNALS) == 0)

print()
print("=== 5) Категорія із ЗАГОЛОВКА: та сама класифікація, що в поллінгу ===")
feed(ann(title="Binance Margin And Loan Will Delist TST & IOTX on 2026-07-10",
         ticker="TST,IOTX"))
check("margin-делістинг НЕ торгується", len(SIGNALS) == 0, len(SIGNALS))
feed(ann(title="Notice of Removal of Spot Trading Pairs - 2026-10-02", ticker="ABC"))
check("прибирання пар НЕ торгується", len(SIGNALS) == 0)
feed(ann(title="Binance Will Delist COS, D, HIGH, MBOX on 2026-06-19",
         ticker="COS,D,HIGH,MBOX"))
check("однолітерний тикер D не губиться",
      SIGNALS and SIGNALS[0]["tickers"] == ["COS", "D", "HIGH", "MBOX"],
      SIGNALS[0]["tickers"] if SIGNALS else None)

print()
print("=== 6) Службові кадри: welcome і heartbeat ===")
feed(json.dumps({"type": "welcome", "tier": "FreeDelayed", "maxDistinctIps": 1,
                 "maxConnectionsPerIp": 3, "absoluteMaxConnections": 20,
                 "allowedCex": "binance", "expiresInSecs": 2592000}))
s = wsfeed.stats()
check("тариф зафіксовано", s["tier"] == "FreeDelayed", s["tier"])
check("термін ключа зафіксовано", s["key_expires_in_sec"] is not None,
      s["key_expires_in_sec"])
check("welcome не торгується", len(SIGNALS) == 0)
hb_before = wsfeed.stats()["heartbeats"]
feed(json.dumps({"type": "heartbeat", "timestampNs": 1710345030123456789,
                 "timeUtc": "2026-04-17T08:30:30.123456Z"}))
check("heartbeat порахований", wsfeed.stats()["heartbeats"] == hb_before + 1)
check("heartbeat — доказ живості фіда",
      wsfeed.stats()["last_heartbeat_age_sec"] is not None)

print()
print("=== 7) ДРЕЙФ СХЕМИ має бути помічений, а не тихо викинутий ===")
drift_cases = [
    ("перейменували listingType",
     json.dumps({"type": "announcement", "listing_type": "spot_delisting",
                 "ticker": "ICX", "publisher": "binance",
                 "title": "Binance Will Delist ICX"})),
    ("перейменували type",
     json.dumps({"event": "announcement", "listingType": "spot_delisting",
                 "ticker": "ICX", "title": "Binance Will Delist ICX"})),
    ("загорнули в конверт",
     json.dumps({"type": "message", "data": {"type": "announcement",
                 "listingType": "spot_delisting", "ticker": "ICX",
                 "title": "Binance Will Delist ICX"}})),
    ("нове значення listingType",
     json.dumps({"type": "announcement", "listingType": "spot_removal",
                 "ticker": "ICX", "publisher": "binance",
                 "title": "Binance Will Delist ICX"})),
]
for name, raw in drift_cases:
    feed(raw)
    check(name + ": зафіксовано", len(DRIFTS) == 1, len(DRIFTS))
    check(name + ": позиція НЕ відкрита", len(SIGNALS) == 0)

print()
print("=== 8) Сміття не валить підписку ===")
for name, raw in (("не JSON", "<html>502 Bad Gateway</html>"),
                  ("порожній рядок", ""),
                  ("масив замість об'єкта", "[{\"type\":\"announcement\"}]"),
                  ("число", "42"),
                  ("помилка від сервера",
                   "{\"type\":\"error\",\"code\":\"test_rate_limited\",\"retryAfterSecs\":42}")):
    try:
        feed(raw)
        check(name + ": не падає і не торгує", len(SIGNALS) == 0)
    except Exception as e:  # noqa: BLE001
        check(name + ": не падає", False, type(e).__name__ + ": " + str(e))
check("помилка сервера порахована", wsfeed.stats()["errors_from_server"] >= 1)

print()
print("=== 9) Ворота застарілості ===")
old_us = int((time.time() - config.MAX_SIGNAL_AGE_SEC - 30) * 1_000_000)
feed(ann(detectedTimestampUs=old_us, dispatchTimestampUs=old_us))
check("застарілий кадр НЕ торгується", len(SIGNALS) == 0, len(SIGNALS))
check("але сповіщення все одно пішло", len(SENT) == 1)
# Зниклі мітки часу: свідомо НЕ блокуємо угоду (пропустити делістинг гірше, ніж
# увійти без заміру), але слід має лишитись — інакше зміна схеми знову стане тихою.
feed(ann(detectedTimestampUs=_OMIT, dispatchTimestampUs=_OMIT))
check("без міток часу угода ВСЕ ОДНО відкривається", len(SIGNALS) == 1, len(SIGNALS))
check("вік при цьому невідомий", SIGNALS and SIGNALS[0]["age"] is None)
# Якщо є лише dispatch — міряємо від нього.
feed(ann(detectedTimestampUs=_OMIT))
check("фолбек на dispatch працює", len(SIGNALS) == 1)

print()
print("=== 10) Делістинг без тикерів — не вгадуємо ===")
feed(ann(ticker=""))
check("позиція не відкривається", len(SIGNALS) == 0)

print()
print("=== 11) healthy(): доказ — heartbeat або самоперевірка, не наявність ключа ===")
_real_key = config.CL_WS_KEY
config.CL_WS_KEY = ""
check("без ключа фід нездоровий навіть із кадрами", wsfeed.healthy() is False)
config.CL_WS_KEY = "x" * 10
check("з ключем і свіжим heartbeat — здоровий", wsfeed.healthy() is True)
wsfeed._stats["last_heartbeat_ms"] = 0
wsfeed._stats["last_selftest_ok_ms"] = 0
check("ключ є, кадрів нема — НЕздоровий (саме цей стан тривав 34 доби)",
      wsfeed.healthy() is False)
config.CL_WS_KEY = _real_key

s = wsfeed.stats()
check("лічильник дрейфу накопичився", s["schema_drift"] == 4, s["schema_drift"])
check("є підрахунок типів кадрів", bool(s["types"]), s["types"])

print()
print("ТЕСТ WS-ФІДА OK" if not FAILS else "ПРОВАЛЕНО: " + str(FAILS))
sys.exit(1 if FAILS else 0)
