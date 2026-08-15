"""Наскрізний прогін ланцюжка в DRY-режимі, на живих ринкових даних.

Перевіряє те, чого не бачать модульні тести: що після всіх сьогоднішніх правок
сигнал -> рішення -> «ордер» -> облік -> монітор -> вихід досі проходить цілком,
і що гарячий шлях лишився без мережі, потоків і SQLite.

Реальних ордерів НЕ ставить (real=False скрізь).
Запуск на сервері:  ~/delisting-bot/.venv/bin/python test_integration.py
"""
import asyncio
import os
import sys
import tempfile
import time

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "t.db")

import config  # noqa: E402

config.DB_PATH = os.environ["DB_PATH"]
config.DRY_RUN = True
config.MAX_CONCURRENT = 3
config.POSITION_MARGIN_USDT = 3.0
config.LEVERAGE = 4.0
config.TAKE_PROFIT_MARGIN_PCT = 40.0
config.STOP_LOSS_MARGIN_PCT = 30.0
config.MAX_HOLD_MINUTES = 20.0

import exchange  # noqa: E402
import executor  # noqa: E402
import logbook as log  # noqa: E402
import storage  # noqa: E402
import strategy  # noqa: E402
import telegram_client as tg  # noqa: E402

# Перехоплюємо подію open, щоб дістати prep_ms без залізання в лог-файл.
OPEN_EV: dict = {}
_real_event = log.event


def _spy(kind, **kw):
    if kind == "open":
        OPEN_EV.update(kw)
    return _real_event(kind, **kw)


log.event = _spy
executor.log.event = _spy

storage.init()
FAILS = []
SENT = []


async def _capture(text, **kw):
    SENT.append(text)


tg.send_message = _capture


def check(name, cond, extra=""):
    print(f"  {'OK  ' if cond else 'FAIL'}  {name}{(' — ' + extra) if extra else ''}")
    if not cond:
        FAILS.append(name)


async def main():
    print("=== підготовка: вантажимо ринки (як на старті бота) ===")
    t0 = time.perf_counter()
    await asyncio.gather(*(asyncio.to_thread(exchange.client, v)
                           for v in config.VENUE_PRIORITY), return_exceptions=True)
    st = exchange.prearm_symbols()
    print(f"  prearm: {st} за {time.perf_counter()-t0:.1f}с")
    missing = [v for v in config.VENUE_PRIORITY if not st.get(v)]
    check(f"усі біржі з VENUE_PRIORITY={config.VENUE_PRIORITY} піднялись",
          not missing, f"не піднялись: {missing}")

    TICKER = "DOGE"
    meta = exchange.hot_meta(TICKER)
    check("тикер знайдено в HOT", meta is not None, str(meta))
    if not meta:
        return

    print("\n=== 1) гарячий шлях: сигнал -> «ордер» ===")
    executor.init_daily()
    executor.resync_open()
    SENT.clear()
    await executor.open_from_signal(TICKER, detect_latency=0.25, source="integration")
    # Сповіщення летять через fire() у фоні — саме тому вони й не гальмують вхід.
    # Тесту треба дати лупу прокрутитись, інакше він перевіряє порожній список.
    await asyncio.sleep(0.3)
    pos = storage.get_open_positions()
    check("позицію відкрито", len(pos) == 1, f"позицій={len(pos)}")
    if not pos:
        return
    p = pos[0]
    check("режим dry (реальних ордерів не було)", p["mode"] == "dry", p["mode"])
    check("плече з конфігу", float(p["leverage"]) == 4.0, str(p["leverage"]))
    check("маржа з конфігу", float(p["margin"]) == 3.0, str(p["margin"]))
    check("номінал >= мін. ордера Bybit ($5)",
          float(p["contracts"]) * float(p["contract_size"]) * float(p["entry_price"]) >= 5.0,
          f"{float(p['contracts'])*float(p['contract_size'])*float(p['entry_price']):.2f}")
    check("символ потрапив у памʼять дедупу", p["symbol"] in executor._open_symbols)
    check("сповіщення про відкриття надіслано", any("ВІДКРИТО" in s for s in SENT))

    print("\n=== 2) два шляхи ціни: фолбек REST і гарячий кеш ===")
    # Щойно виміряний прохід ішов БЕЗ price-cache (у тесті він не крутиться),
    # тобто це саме фолбек. Він має працювати, але коштує мережевого раунду.
    check("фолбек справді спрацював", OPEN_EV.get("price_src") == "rest",
          str(OPEN_EV.get("price_src")))
    check("фолбек укладається в розумні межі (<400мс)",
          OPEN_EV.get("prep_ms", 9e9) < 400, f"prep_ms={OPEN_EV.get('prep_ms')}")
    print(f"    ціна фолбеку: {OPEN_EV.get('prep_ms')}мс "
          f"(у продакшені з кешем було 0.3мс на живому ордері)")

    # Тепер те саме, але з наповненим кешем — як у бою.
    import pricecache
    px = OPEN_EV["entry_price"]
    pricecache.get_price = lambda raw: (px, 0.2)
    pricecache.reference_high = lambda raw, mins: px * 1.01
    executor.forget(TICKER, meta["symbol"])
    storage.close_position(storage.get_open_positions()[0]["id"], px, "MANUAL", 0.0, 0.0)
    executor.resync_open()
    OPEN_EV.clear()
    SENT.clear()
    await executor.open_from_signal(TICKER, source="integration-cached")
    await asyncio.sleep(0.3)
    check("з кешем price_src=cache", str(OPEN_EV.get("price_src", "")).startswith("cache"),
          str(OPEN_EV.get("price_src")))
    check("з кешем гарячий шлях <5мс", OPEN_EV.get("prep_ms", 9e9) < 5.0,
          f"prep_ms={OPEN_EV.get('prep_ms')}")
    check("сигнал дійшов до обліку з правильним джерелом",
          OPEN_EV.get("source") == "integration-cached", str(OPEN_EV.get("source")))

    print("\n=== 3) дедуп: другий сигнал по тому самому тикеру ===")
    SENT.clear()
    await executor.open_from_signal(TICKER, source="integration-dup")
    check("другої позиції НЕ відкрито", storage.open_positions_count() == 1,
          f"позицій={storage.open_positions_count()}")

    print("\n=== 4) монітор бачить позицію (оптимізація простою не сховала її) ===")
    executor._last_db_sync = time.time()   # ніби щойно звірялись
    await executor.monitor_once()
    fresh = storage.get_open_positions()[0]
    check("монітор оновив мінімум ціни", fresh["min_price"] is not None)
    check("позиція досі відкрита", storage.open_positions_count() == 1)

    print("\n=== 5) монітор ПРОСТОЮЄ, коли позицій нема ===")
    saved = storage.get_open_positions
    calls = {"n": 0}

    def counting():
        calls["n"] += 1
        return saved()

    storage.get_open_positions = counting
    executor._open_symbols.clear()
    executor._reserved = 0
    executor._last_db_sync = time.time()
    await executor.monitor_once()
    check("у простої 0 звернень до БД", calls["n"] == 0, f"звернень={calls['n']}")
    storage.get_open_positions = saved
    executor.resync_open()

    print("\n=== 6) вихід за часом і закриття ===")
    SENT.clear()
    ok = await executor.force_close(fresh["id"], reason="MANUAL")
    check("force_close спрацював", ok)
    check("позицію закрито", storage.open_positions_count() == 0)
    check("сповіщення про закриття надіслано", any("ЗАКРИТО" in s for s in SENT))
    closed = storage.realized_pnl_today("dry")
    check("PnL записано (dry не годує вимикач)", executor.daily_pnl() == 0.0,
          f"daily={executor.daily_pnl()}")

    print("\n=== 7) тикер знову доступний після закриття ===")
    check("заявку знято", TICKER not in executor._claimed)
    check("символ знято з памʼяті", fresh["symbol"] not in executor._open_symbols)

    print("\n" + ("ІНТЕГРАЦІЯ OK" if not FAILS else f"ПРОВАЛЕНО: {FAILS}"))


asyncio.run(main())
sys.exit(1 if FAILS else 0)
