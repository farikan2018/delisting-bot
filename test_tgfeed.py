"""Парсер каналу @CLWfeed на РЕАЛЬНИХ повідомленнях.

Фікстури нижче — дослівні пости, зібрані спостерігачем `probe.py` за 7 діб
(2026-08-14..21). Це не вигадані приклади: саме на них тепер тримається рішення
відкривати реальну позицію, тож будь-яка зміна формату каналу має валити цей
тест, а не тихо міняти поведінку бота.

Найважливіша перевірка — НЕГАТИВНА: канал шле також UPBIT і BITHUMB, а торгуємо
ми тільки BINANCE-делістинги. Одна помилка в цьому фільтрі означає шорт на
корейській події, економіку якої ми не підтверджували.

Запуск: python test_tgfeed.py
"""
import sys

import tgfeed

# --- дослівні пости каналу ---
M_BITHUMB_DELIST = ("BITHUMB Delisting Notice – $JASMY, $STORJ, $TT  "
                    "Detected (UTC): 2026-08-14T07:00:00.874644Z (µs precision)  "
                    "Telegram message delayed by 150ms. Contact @CLWebsocket for "
                    "websocket prices — free test key")
M_UPBIT_DELIST = ("UPBIT Delisting Notice – $TT  "
                  "Detected (UTC): 2026-08-14T07:00:06.330105Z (µs precision)  "
                  "Telegram message delayed by 150ms.")
M_BINANCE_DELIST = ("BINANCE Delisting Announcement – $ICX, $SCRT, $STORJ  "
                    "Binance Will Delist ICX, SCRT, STORJ on 2026-09-03  "
                    "Detected (UTC): 2026-08-20T06:00:08.885836Z (µs precision)  "
                    "Telegram message delayed by 150ms.")
M_BITHUMB_LIST = ("BITHUMB Listing Notice – $NEXO  "
                  "Detected (UTC): 2026-08-21T01:45:10.165578Z (µs precision)  "
                  "Telegram message delayed by 150ms.")
M_UPBIT_LIST = ("UPBIT Listing Notice – $CRV  "
                "Detected (UTC): 2026-08-21T02:22:41.714552Z (µs precision)  "
                "Telegram message delayed by 150ms.")
M_JUNK = "https://t.me/+vlM9pix9UgswMWM1"

FAILS = []


def check(name, cond, extra=""):
    print(f"  {'OK  ' if cond else 'FAIL'}  {name}{(' — ' + extra) if extra else ''}")
    if not cond:
        FAILS.append(name)


def tradeable(msg):
    """Точна копія умови з tgfeed._on_message: чи піде це в реальну угоду."""
    p = tgfeed.parse(msg)
    return (p["exchange"] == "BINANCE" and p["is_delist"] and bool(p["tickers"])), p


print("=== 1) BINANCE-делістинг: єдине, що торгуємо ===")
ok, p = tradeable(M_BINANCE_DELIST)
check("розпізнано як торговане", ok)
check("біржа BINANCE", p["exchange"] == "BINANCE", p["exchange"])
check("тикери рівно ICX, SCRT, STORJ", p["tickers"] == ["ICX", "SCRT", "STORJ"],
      str(p["tickers"]))
check("мітка детекту розібрана", p["feed_detected_ms"] is not None)
# 2026-08-20T06:00:08.885836Z -> ms
check("мітка детекту точна", p["feed_detected_ms"] == 1787205608885,
      str(p["feed_detected_ms"]))

print("\n=== 2) НЕГАТИВНІ: корейські біржі торгувати НЕ можна ===")
for name, msg in (("BITHUMB делістинг", M_BITHUMB_DELIST),
                  ("UPBIT делістинг", M_UPBIT_DELIST)):
    ok, p = tradeable(msg)
    check(f"{name} НЕ торгується", not ok, f"біржа={p['exchange']}")
    check(f"{name}: тикери все одно видно (для замірів)", bool(p["tickers"]),
          str(p["tickers"]))

print("\n=== 3) НЕГАТИВНІ: лістинги — не делістинги ===")
for name, msg in (("BITHUMB лістинг", M_BITHUMB_LIST), ("UPBIT лістинг", M_UPBIT_LIST)):
    ok, p = tradeable(msg)
    check(f"{name} НЕ торгується", not ok)
    check(f"{name}: is_delist=False", p["is_delist"] is False)

print("\n=== 4) СМІТТЯ не валить парсер ===")
for name, msg in (("гола URL", M_JUNK), ("порожній рядок", ""), ("None", None)):
    try:
        ok, p = tradeable(msg)
        check(f"{name}: не торгується і не падає", not ok)
    except Exception as e:  # noqa: BLE001
        check(f"{name}: не падає", False, f"{type(e).__name__}: {e}")

print("\n=== 5) ВІК СИГНАЛУ рахується від їхнього детекту ===")
p = tgfeed.parse(M_BINANCE_DELIST)
# У бою: transport = now - feed_detected. Тут перевіряємо саму арифметику на
# фіксованому «зараз» = детект + 2.23с (реальний транспорт тієї події).
now_ms = p["feed_detected_ms"] + 2230
transport = round((now_ms - p["feed_detected_ms"]) / 1000, 3)
check("транспорт 2.23с", abs(transport - 2.23) < 1e-6, str(transport))
import config  # noqa: E402
est = round(transport + config.FEED_DETECT_LAG_SEC, 3)
check("оцінка віку = транспорт + константа детекту", abs(est - 4.53) < 0.01,
      f"{est}с (константа {config.FEED_DETECT_LAG_SEC}с)")
check("оцінка віку в межах воріт застарілості", est <= config.MAX_SIGNAL_AGE_SEC,
      f"{est} <= {config.MAX_SIGNAL_AGE_SEC}")

print("\n=== 6) ТИКЕРИ беруться лише з $-префіксом ===")
p = tgfeed.parse("BINANCE Delisting Announcement – $ICX and USDT pairs on BINANCE")
check("службові слова не потрапили", p["tickers"] == ["ICX"], str(p["tickers"]))

print("\n" + ("ТЕСТ ПАРСЕРА OK" if not FAILS else f"ПРОВАЛЕНО: {FAILS}"))
sys.exit(1 if FAILS else 0)
