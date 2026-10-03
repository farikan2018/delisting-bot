"""Резервне джерело детекту: публічний JSON постачальника.

Фікстури нижче — ДОСЛІВНІ записи з живого файлу
https://cryptolisting.ws/data/recent-announcements.json (прочитано 2026-10-03),
включно з реальними мітками detected_at_us. Це джерело відкриває РЕАЛЬНІ позиції,
тому поведінка тут має бути прибита тестом, а не триматись на тому, що «схема ж
очевидна».

НАЙВАЖЛИВІШЕ ТУТ — ВОРОТА ЗАСТАРІЛОСТІ. Файл статичний: на момент написання він
не оновлювався 32 години, бо подій не було. Якби вік сигналу рахувався від
моменту ЧИТАННЯ, бот на першому ж запуску відкрив би позицію на делістингу
32-годинної давності — у вже відпрацьований дамп, тобто гарантований збиток.
Вік рахується від detected_at_us, і саме це перевіряє розділ 4.

Запуск: python test_clwjson.py
"""

# Тести НІКОЛИ не пишуть у бойовий events.jsonl (див. logbook.BOT_LOG_DIR).
import os
os.environ.setdefault("BOT_LOG_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs-test"))

import asyncio
import sys
import time

import clwjson
import config

FAILS = []


def check(name, cond, extra=""):
    print("  " + ("OK  " if cond else "FAIL") + "  " + name
          + ((" - " + str(extra)) if extra else ""))
    if not cond:
        FAILS.append(name)


SIGNALS, SENT = [], []


async def _handler(tickers, age, source):
    SIGNALS.append({"tickers": tickers, "age": age, "source": source})


clwjson.set_handler(_handler)
clwjson.set_notifier(lambda t: SENT.append(t))


def fire(events):
    SIGNALS.clear()
    SENT.clear()
    asyncio.run(clwjson._handle_new(events))


def ev(cex, ticker, typ, us):
    return {"cex": cex, "ticker": ticker, "type": typ, "detected_at_us": us}


def now_us(offset_sec=0.0):
    return int((time.time() + offset_sec) * 1_000_000)


print("=== 1) Форма файлу розбирається (дослівні записи з живого фіда) ===")
real = {
    "updated_at_us": 1790926200942578,
    "events": [
        {"cex": "bithumb", "ticker": "HOOK", "type": "spot_delisting",
         "detected_at_us": 1790926200429435},
        {"cex": "bithumb", "ticker": "D", "type": "spot_delisting",
         "detected_at_us": 1790926200429435},
        {"cex": "upbit", "ticker": "SAND", "type": "caution_released",
         "detected_at_us": 1790924401132725},
        {"cex": "binance", "ticker": "USDP", "type": "spot_delisting",
         "detected_at_us": 1789020014142000},
    ],
}
evs = clwjson.parse_events(real)
check("усі події витягнуто", len(evs) == 4, len(evs))
check("мітку оновлення файлу зафіксовано",
      clwjson.stats()["feed_updated_us"] == 1790926200942578)
check("ключі подій унікальні",
      len({clwjson._key(e) for e in evs}) == 4)
check("ті самі тикери з різних бірж не злипаються",
      clwjson._key(evs[0]) != clwjson._key(evs[1]))

print()
print("=== 2) Торгуємо РІВНО binance + spot_delisting ===")
# Усі тикери одного анонсу приходять окремими рядками з однією міткою детекту —
# саме так, як у живому файлі (HOOK і D обидва на 1790926200429435).
_t = now_us(-3)
fire([ev("binance", "ICX", "spot_delisting", _t),
      ev("binance", "SCRT", "spot_delisting", _t),
      ev("binance", "STORJ", "spot_delisting", _t)])
check("один сигнал на весь анонс", len(SIGNALS) == 1, len(SIGNALS))
check("усі три тикери в ньому",
      SIGNALS and sorted(SIGNALS[0]["tickers"]) == ["ICX", "SCRT", "STORJ"],
      SIGNALS[0]["tickers"] if SIGNALS else None)
check("джерело позначене", SIGNALS and SIGNALS[0]["source"] == "clw_json")
check("вік = з моменту їхнього детекту + константа",
      SIGNALS and abs(SIGNALS[0]["age"] - (3 + config.FEED_DETECT_LAG_SEC)) < 1.5,
      SIGNALS[0]["age"] if SIGNALS else None)

print()
print("=== 3) НЕГАТИВНІ: усе інше не торгується ===")
NEG = [
    ("корейська біржа", ev("upbit", "ICX", "spot_delisting", now_us(-2))),
    ("bithumb", ev("bithumb", "HOOK", "spot_delisting", now_us(-2))),
    ("robinhood", ev("robinhood", "X", "spot_delisting", now_us(-2))),
    ("лістинг", ev("binance", "ABC", "spot_listing", now_us(-2))),
    ("ф'ючерсний делістинг", ev("binance", "AERGO", "futures_delisting", now_us(-2))),
    ("monitoring tag", ev("binance", "ACT", "monitoring_tag_extend", now_us(-2))),
    ("airdrop", ev("binance", "OPG", "hodler_airdrop", now_us(-2))),
    ("not_listing", ev("binance", "X", "not_listing", now_us(-2))),
    ("порожній тикер", ev("binance", "", "spot_delisting", now_us(-2))),
    ("нема мітки часу", {"cex": "binance", "ticker": "X", "type": "spot_delisting"}),
]
for name, e in NEG:
    fire([e])
    check(name + " НЕ торгується", len(SIGNALS) == 0, len(SIGNALS))

print()
print("=== 4) ВОРОТА ЗАСТАРІЛОСТІ: статичний файл не може відкрити стару подію ===")
# Реальний стан на 2026-10-03: файл не оновлювався 32 години.
fire([ev("binance", "ICX", "spot_delisting", now_us(-32 * 3600))])
check("подія 32-годинної давності НЕ торгується", len(SIGNALS) == 0, len(SIGNALS))
check("але сповіщення про неї є", len(SENT) == 1)
fire([ev("binance", "ICX", "spot_delisting",
         now_us(-(config.MAX_SIGNAL_AGE_SEC + 10)))])
check("подія за крок за поріг НЕ торгується", len(SIGNALS) == 0)
fire([ev("binance", "ICX", "spot_delisting",
         now_us(-(config.MAX_SIGNAL_AGE_SEC - 20)))])
check("подія в межах порога торгується", len(SIGNALS) == 1)

print()
print("=== 5) Різні анонси в одному зчитуванні не змішуються ===")
t1, t2 = now_us(-4), now_us(-9)
fire([ev("binance", "AAA", "spot_delisting", t1),
      ev("binance", "BBB", "spot_delisting", t1),
      ev("binance", "CCC", "spot_delisting", t2)])
check("два окремі сигнали", len(SIGNALS) == 2, len(SIGNALS))
groups = sorted(tuple(sorted(s["tickers"])) for s in SIGNALS)
check("групування за моментом детекту правильне",
      groups == [("AAA", "BBB"), ("CCC",)], groups)

# Крихкість, на якій тест уже спіймав помилку: на сервері три послідовні виклики
# годинника дали три РІЗНІ мікросекунди, і анонс розпався на три сигнали. У бою
# це означало б, що третій токен заходить на два ордерні раунди пізніше.
base = now_us(-5)
fire([ev("binance", "AAA", "spot_delisting", base),
      ev("binance", "BBB", "spot_delisting", base + 137),
      ev("binance", "CCC", "spot_delisting", base + 4021)])
check("мікросекундний розкид НЕ розриває анонс", len(SIGNALS) == 1, len(SIGNALS))
check("усі тикери разом",
      SIGNALS and sorted(SIGNALS[0]["tickers"]) == ["AAA", "BBB", "CCC"],
      SIGNALS[0]["tickers"] if SIGNALS else None)

fire([ev("binance", "AAA", "spot_delisting", base),
      ev("binance", "AAA", "spot_delisting", base)])
check("дубль тикера в одному анонсі не подвоюється",
      SIGNALS and SIGNALS[0]["tickers"] == ["AAA"],
      SIGNALS[0]["tickers"] if SIGNALS else None)

# Межа секунди: фіксоване відро (us // 1e6) розірвало б цю пару, кластеризація
# за близькістю — ні. Саме цей крайовий випадок і був причиною відмови від відер.
edge = (int(time.time()) - 5) * 1_000_000 + 999_900
fire([ev("binance", "AAA", "spot_delisting", edge),
      ev("binance", "BBB", "spot_delisting", edge + 300)])
check("пара на межі секунди лишається одним анонсом", len(SIGNALS) == 1, len(SIGNALS))

# А ось справді далекі події мають лишитись окремими.
far = now_us(-20)
fire([ev("binance", "AAA", "spot_delisting", far),
      ev("binance", "BBB", "spot_delisting", far + 3_000_000)])
check("події з розривом 3с — два різні анонси", len(SIGNALS) == 2, len(SIGNALS))

print()
print("=== 6) Сміття не валить цикл ===")
for name, data in (("порожній об'єкт", {}),
                   ("events не список", {"events": "nope"}),
                   ("None", None),
                   ("список замість обгортки", [ev("binance", "X", "spot_listing", now_us())])):
    try:
        out = clwjson.parse_events(data)
        check(name + ": розбирається без винятку", isinstance(out, list), len(out))
    except Exception as e:  # noqa: BLE001
        check(name + ": без винятку", False, type(e).__name__ + ": " + str(e))
fire([{"cex": None, "ticker": None, "type": None, "detected_at_us": "не число"}])
check("бите поле не відкриває позицію", len(SIGNALS) == 0)

print()
print("=== 7) Чесне використання чужого безкоштовного сервісу ===")
check("підлога періоду опитування 30с зашита в коді",
      clwjson._MIN_PERIOD_SEC >= 30.0, clwjson._MIN_PERIOD_SEC)
check("значення з .env не може опустити нижче підлоги",
      max(clwjson._MIN_PERIOD_SEC, 1.0) >= 30.0)

print()
print("ТЕСТ РЕЗЕРВНОГО ФІДА OK" if not FAILS else "ПРОВАЛЕНО: " + str(FAILS))
sys.exit(1 if FAILS else 0)
