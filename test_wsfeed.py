"""Парсер і ворота WebSocket-фіда cryptolisting.ws.

ЧОМУ ЦЕЙ ТЕСТ ВИГЛЯДАЄ ІНАКШЕ, НІЖ test_tgfeed.py. Там фікстури — дослівні пости,
зібрані спостерігачем за 7 діб. Тут дослівних кадрів НЕМА і це не недогляд: старий
код не лишав по кадру жодного сліду, тому за 34 доби живого з'єднання ми не маємо
ЖОДНОГО записаного кадру. Саме це й була проблема, яку лагодить wsfeed.py.

Тому фікстури нижче — схема, якої код очікував досі (поля type/listingType/ticker/
dispatchTimestampUs), плюс набір СПОТВОРЕНЬ цієї схеми. Перевіряємо не стільки
«ми вміємо читати правильний кадр», скільки головне:
    будь-яке відхилення від схеми має бути ПОМІЧЕНИМ, а не тихо викинутим.
Коли прилетять справжні кадри (їх тепер пише подія ws_frame), фікстури слід
замінити на дослівні — і тоді цей тест стане таким самим доказом, як у tgfeed.

Запуск: python test_wsfeed.py
"""
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


# --- перехоплення побічних ефектів: тест не має нікуди нічого слати ---
SENT = []
DRIFTS = []


async def _fake_alert(key, text, cooldown_sec=0, detail=None):
    DRIFTS.append((key, text))
    return True


async def _fake_clear(key, text=""):
    return True


wsfeed.alerts.raise_alert = _fake_alert
wsfeed.alerts.clear_alert = _fake_clear

SIGNALS = []


async def _handler(tickers, age, source):
    SIGNALS.append({"tickers": tickers, "age": age, "source": source})


wsfeed.set_handler(_handler)
wsfeed.set_notifier(lambda text: SENT.append(text))


def frame(**kw):
    now_us = int(time.time() * 1_000_000)
    base = {"type": "announcement", "listingType": "spot_delisting",
            "ticker": "ICX,SCRT,STORJ",
            "title": "Binance Will Delist ICX, SCRT, STORJ on 2026-09-03",
            "dispatchTimestampUs": now_us}
    base.update(kw)
    return json.dumps({k: v for k, v in base.items() if v is not _OMIT})


class _OMIT:
    pass


def feed(raw):
    SIGNALS.clear()
    SENT.clear()
    DRIFTS.clear()
    asyncio.run(wsfeed._on_frame(raw))


print("=== 1) Канонічний спот-делістинг: єдине, що відкриває позицію ===")
p = wsfeed.parse(frame())
check("tradeable", p["tradeable"])
check("тикери розібрані", p["tickers"] == ["ICX", "SCRT", "STORJ"], p["tickers"])
check("транспорт у секундах, не в мікро", p["transport_sec"] is not None
      and 0 <= p["transport_sec"] < 2, p["transport_sec"])
feed(frame())
check("обробник викликано рівно раз", len(SIGNALS) == 1, len(SIGNALS))
check("джерело позначене", SIGNALS and SIGNALS[0]["source"] == "ws_cryptolisting")
check("вік = транспорт + константа детекту",
      SIGNALS and abs(SIGNALS[0]["age"] - config.FEED_DETECT_LAG_SEC) < 1.0,
      SIGNALS[0]["age"] if SIGNALS else None)
check("сповіщення пішло", len(SENT) == 1)

print()
print("=== 2) Ф'ючерсний делістинг: сповіщаємо, але НЕ торгуємо ===")
feed(frame(listingType="futures_delisting",
           title="Binance Futures Will Delist USD-M AERGOUSDT Perpetual Contract"))
check("позиція НЕ відкривається", len(SIGNALS) == 0, len(SIGNALS))
check("але сповіщення є", len(SENT) == 1)
check("дрейфу схеми НЕ зафіксовано", len(DRIFTS) == 0)

print()
print("=== 3) Лістинг — не наша подія ===")
feed(frame(listingType="spot_listing", title="Binance Will List SOMECOIN"))
check("не торгуємо", len(SIGNALS) == 0)
check("не сповіщаємо", len(SENT) == 0)
check("дрейфу нема (у тексті нема delist)", len(DRIFTS) == 0)

print()
print("=== 4) ДРЕЙФ СХЕМИ — головна перевірка цього файлу ===")
# Саме цей клас кадру раніше зникав безслідно: щось про делістинг прилетіло,
# фільтр не впізнав, і 34 доби це виглядало як «делістингів не було».
drift_cases = [
    ("перейменували listingType",
     json.dumps({"type": "announcement", "listing_type": "spot_delisting",
                 "ticker": "ICX", "title": "Binance Will Delist ICX"})),
    ("перейменували type",
     json.dumps({"event": "announcement", "listingType": "spot_delisting",
                 "ticker": "ICX", "title": "Binance Will Delist ICX"})),
    ("загорнули в конверт",
     json.dumps({"type": "message", "data": {"type": "announcement",
                 "listingType": "spot_delisting", "ticker": "ICX",
                 "title": "Binance Will Delist ICX"}})),
    ("нове значення listingType",
     json.dumps({"type": "announcement", "listingType": "spot_removal",
                 "ticker": "ICX", "title": "Binance Will Delist ICX"})),
]
for name, raw in drift_cases:
    feed(raw)
    check(name + ": зафіксовано дрейф", len(DRIFTS) == 1, len(DRIFTS))
    check(name + ": позиція НЕ відкрита", len(SIGNALS) == 0)

print()
print("=== 5) Вітальний кадр і службові кадри не ламають і не торгують ===")
welcome = json.dumps({"message": "connected", "tier": "FreeDelayed",
                      "maxDistinctIps": 1, "heartbeatSec": 30})
p = wsfeed.parse(welcome)
check("розпізнано як службовий", p["is_ack"], p)
feed(welcome)
check("нічого не сталось", len(SIGNALS) == 0 and len(DRIFTS) == 0)

print()
print("=== 6) Сміття не валить підписку ===")
for name, raw in (("не JSON", "<html>502 Bad Gateway</html>"),
                  ("порожній рядок", ""),
                  ("масив замість об'єкта", "[{\"type\":\"announcement\"}]"),
                  ("число", "42")):
    try:
        feed(raw)
        check(name + ": не падає і не торгує", len(SIGNALS) == 0)
    except Exception as e:  # noqa: BLE001
        check(name + ": не падає", False, type(e).__name__ + ": " + e.__str__())
p = wsfeed.parse("[{\"type\":\"announcement\"}]")
check("масив помічено окремим типом", p["type"] == "_list", p["type"])

print()
print("=== 7) Мітка часу: ворота застарілості ===")
old_us = int((time.time() - config.MAX_SIGNAL_AGE_SEC - 30) * 1_000_000)
feed(frame(dispatchTimestampUs=old_us))
check("застарілий кадр НЕ торгується", len(SIGNALS) == 0, len(SIGNALS))
check("але сповіщення все одно пішло", len(SENT) == 1)

# Зникле поле часу: свідомо НЕ блокуємо угоду (пропустити делістинг гірше, ніж
# увійти без заміру), але це має лишати слід — інакше зміна схеми знову стане тихою.
feed(frame(dispatchTimestampUs=_OMIT))
check("без мітки часу угода ВСЕ ОДНО відкривається", len(SIGNALS) == 1, len(SIGNALS))
check("вік при цьому невідомий", SIGNALS and SIGNALS[0]["age"] is None)

print()
print("=== 8) Делістинг без тикерів — не вгадуємо ===")
feed(frame(ticker=""))
check("позиція не відкривається", len(SIGNALS) == 0)

print()
print("=== 9) healthy(): доказ — кадр, а не наявність ключа ===")
check("після отриманих кадрів фід вважається живим",
      wsfeed.healthy() == bool(config.CL_WS_KEY),
      "frames=" + str(wsfeed.stats()["frames"]) + " key=" + str(bool(config.CL_WS_KEY)))
s = wsfeed.stats()
check("лічильник дрейфу накопичився", s["schema_drift"] == 4, s["schema_drift"])
check("є підрахунок типів кадрів", bool(s["types"]), s["types"])

print()
print("ТЕСТ WS-ФІДА OK" if not FAILS else "ПРОВАЛЕНО: " + str(FAILS))
sys.exit(1 if FAILS else 0)
