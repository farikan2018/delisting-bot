"""Тести захисту реальних грошей. Без pytest і без мережі.

Ганяє рівно ті шляхи, які вперше вмикаються разом із DRY_RUN=0:
біржовий стоп, звірка з біржею, аварійний вимикач і баланс-гард.

Запуск:  python test_safety.py
"""
import asyncio
import os
import sys
import tempfile

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "t.db")

import config  # noqa: E402

config.DB_PATH = os.environ["DB_PATH"]
config.STOP_LOSS_MARGIN_PCT = 30.0
config.TAKE_PROFIT_MARGIN_PCT = 60.0
config.LEVERAGE = 3.0
config.MAX_CONCURRENT = 2
config.MAX_DAILY_LOSS_USDT = 4.0
config.BALANCE_GUARD = True
config.EXCHANGE_STOP = True

import exchange  # noqa: E402
import executor  # noqa: E402
import storage  # noqa: E402
import telegram_client as tg  # noqa: E402

# Зберігаємо СПРАВЖНЮ функцію до будь-яких підмін — інакше тест 8b перевіряв би стаб.
_ORIG_SET_STOP = exchange.set_position_stop

storage.init()

FAILS = []
CALLS = {"close_short": 0, "set_stop": [], "sent": []}


def check(name, cond, extra=""):
    print(f"  {'OK  ' if cond else 'FAIL'}  {name}{(' — ' + extra) if extra else ''}")
    if not cond:
        FAILS.append(name)


async def _noop_send(text, **kw):
    CALLS["sent"].append(text)


tg.send_message = _noop_send


def _mk_pos(pos_id_holder, mode="real", entry=1.0, contracts=10.0):
    pid = storage.insert_position({
        "ticker": "TEST", "symbol": "TEST/USDT:USDT", "venue": "bybit", "mode": mode,
        "margin": 3.0, "leverage": 3.0, "contracts": contracts, "contract_size": 1.0,
        "ref_price": entry, "entry_price": entry, "dropped_pct": 0.0,
    })
    pos_id_holder.append(pid)
    return next(p for p in storage.get_open_positions() if p["id"] == pid)


# ---------- 1) арифметика стопа для ШОРТА ----------
print("\n1) Ціни стопа й тейка для шорта")
captured = {}


def _fake_set_stop(venue, symbol, stop_price=None, take_price=None):
    captured["sl"], captured["tp"] = stop_price, take_price
    CALLS["set_stop"].append((stop_price, take_price))


exchange.set_position_stop = _fake_set_stop
asyncio.run(executor.arm_exchange_stop(1, "bybit", "X/USDT:USDT", 100.0, 3.0, "X"))
check("стоп ВИЩЕ входу (шорт)", captured["sl"] > 100.0, f"sl={captured['sl']}")
check("тейк НИЖЧЕ входу (шорт)", captured["tp"] < 100.0, f"tp={captured['tp']}")
check("стоп = −30% маржі при 3x → +10% ціни", abs(captured["sl"] - 110.0) < 1e-9)
check("тейк = +60% маржі при 3x → −20% ціни", abs(captured["tp"] - 80.0) < 1e-9)

# ---------- 2) фолбек «лише стоп», коли тейк відхилено ----------
print("\n2) Фолбек, коли біржа відхиляє тейк (ціна вже нижче нього)")
CALLS["set_stop"].clear()
state = {"n": 0}


def _reject_tp(venue, symbol, stop_price=None, take_price=None):
    state["n"] += 1
    if take_price is not None:
        raise RuntimeError("take profit price is invalid")
    CALLS["set_stop"].append((stop_price, take_price))


exchange.set_position_stop = _reject_tp
ok = asyncio.run(executor.arm_exchange_stop(2, "bybit", "X/USDT:USDT", 100.0, 3.0, "X"))
check("повернув успіх", ok is True)
check("стоп усе одно поставлено", len(CALLS["set_stop"]) == 1 and CALLS["set_stop"][0][1] is None)
check("тейк пробували 3 рази до фолбеку", state["n"] == 4, f"викликів={state['n']}")

# ---------- 3) невідома біржа не піднімає тривогу ----------
print("\n3) MEXC/Gate — біржового стопа нема, це не аварія")


def _unsupported(venue, symbol, stop_price=None, take_price=None):
    raise NotImplementedError("only bybit")


exchange.set_position_stop = _unsupported
CALLS["sent"].clear()
ok = asyncio.run(executor.arm_exchange_stop(3, "mexc", "X/USDT:USDT", 1.0, 3.0, "X"))
check("повертає False без ретраїв", ok is False)
check("не спамить у Telegram", not CALLS["sent"], f"надіслано={len(CALLS['sent'])}")

# ---------- 4) аварійний вимикач ----------
print("\n4) Аварійний вимикач по денному збитку")
executor.init_daily()
check("на старті вимикач не спрацював", not executor._kill_switch_hit())
executor._daily_pnl = -3.9
check("−3.9 при ліміті 4 — ще торгуємо", not executor._kill_switch_hit())
executor._daily_pnl = -4.0
check("−4.0 при ліміті 4 — стоп", executor._kill_switch_hit())
executor._daily_day = "1999-01-01"      # імітуємо перехід доби
check("нова доба обнуляє лічильник", not executor._kill_switch_hit())
check("_roll_day не ходить у БД", executor._daily_pnl == 0.0)

# ---------- 5) звірка: None ≠ 0.0 ----------
print("\n5) Звірка з біржею: None (збій запиту) НЕ можна плутати з 0.0 (позиції нема)")
ids = []
pos = _mk_pos(ids)
exchange.position_size = lambda v, s: None          # збій запиту
executor.current_price = lambda p: _acoro(1.0)


def _acoro(v):
    async def _c():
        return v
    return _c()


exchange.close_short = lambda *a, **k: CALLS.__setitem__("close_short", CALLS["close_short"] + 1)
exchange.closed_pnl = lambda v, s: {"exit_price": 0.9, "pnl": 0.3, "side": "Buy", "ts": 0}
asyncio.run(executor.reconcile_real())
check("None → позиція лишається відкритою",
      storage.open_positions_count() == 1, f"відкрито={storage.open_positions_count()}")

# Щойно відкрита позиція: Bybit ще не показує її в /v5/position/list, і 0.0 тут
# означає «не встиг проіндексувати», а не «закрито». Закривати заборонено.
exchange.position_size = lambda v, s: 0.0
config.RECONCILE_MIN_AGE_SEC = 3600
asyncio.run(executor.reconcile_real())
check("молода позиція + 0.0 → НЕ чіпаємо (захист від сироти)",
      storage.open_positions_count() == 1)

config.RECONCILE_MIN_AGE_SEC = 0
config.RECONCILE_CONFIRMS = 2
executor._flat_seen.clear()
asyncio.run(executor.reconcile_real())
check("перше 0.0 → ще не віримо (1 з 2 підтверджень)",
      storage.open_positions_count() == 1)
asyncio.run(executor.reconcile_real())
check("друге 0.0 поспіль → запис закрито", storage.open_positions_count() == 0)
check("ордер на закриття НЕ слався (позиції вже нема)", CALLS["close_short"] == 0)

# Ціна виходу мусить прийти З БІРЖІ, а не з поточного ринку
closed = [p for p in __import__("sqlite3").connect(config.DB_PATH)
          .execute("SELECT exit_price, exit_reason FROM positions WHERE status='closed'"
                   ).fetchall()]
check("ціна виходу взята з closed-pnl біржі, не вгадана",
      any(abs(r[0] - 0.9) < 1e-9 for r in closed), f"{closed}")

# Один виняток не має обривати весь прохід
ids2 = []
_p1 = _mk_pos(ids2)
_p2 = _mk_pos(ids2)


def _raise_for_first(v, s):
    raise RuntimeError("fetch_positions впав")


exchange.position_size = _raise_for_first
try:
    asyncio.run(executor.reconcile_real())
    check("виняток по одній позиції не валить прохід", True)
except Exception as e:
    check("виняток по одній позиції не валить прохід", False, str(e)[:60])

# ---------- 6) баланс-гард ----------
print("\n6) Баланс-гард")
exchange._balance_cache.clear()
check("порожній кеш → None (вхід НЕ блокуємо)", exchange.cached_free_balance("bybit") is None)
exchange._balance_cache["bybit"] = (9.9, __import__("time").time())
check("свіжий кеш повертає число", exchange.cached_free_balance("bybit") == 9.9)
exchange._balance_cache["bybit"] = (9.9, __import__("time").time() - 999)
check("протухлий кеш → None", exchange.cached_free_balance("bybit") is None)

# ---------- 7) дедуп слотів ----------
print("\n7) Резервація слотів (MAX_CONCURRENT=2)")
executor._claimed.clear()
executor._open_symbols.clear()
executor._reserved = 0
check("перший вхід дозволено", executor._reserve("AAA", "AAA/USDT:USDT") == "")
check("той самий тикер — дубль джерела",
      executor._reserve("AAA", "AAA/USDT:USDT") == "duplicate_source")
check("другий тикер дозволено", executor._reserve("BBB", "BBB/USDT:USDT") == "")
check("третій — ліміт", executor._reserve("CCC", "CCC/USDT:USDT") == "max_concurrent")

# ---------- 8) усиновлення осиротілої позиції ----------
print("\n8) Ордер відповів помилкою — але позиція на біржі МОЖЕ бути відкрита")


class _Dec:
    ref_price, dropped_pct = 1.0, 0.0


exchange.set_position_stop = _fake_set_stop
CALLS["sent"].clear()

before = storage.open_positions_count()
exchange.position_size = lambda v, s: 0.0            # ордер справді не пройшов
asyncio.run(executor._adopt_orphan("ORP", "bybit", "ORP/USDT:USDT", 1.0, 3.0, 1.0,
                                   _Dec(), "RequestTimeout"))
check("позиції нема → нічого не заводимо", storage.open_positions_count() == before)

CALLS["sent"].clear()
exchange.position_size = lambda v, s: None           # не змогли перевірити
asyncio.run(executor._adopt_orphan("ORP", "bybit", "ORP/USDT:USDT", 1.0, 3.0, 1.0,
                                   _Dec(), "RequestTimeout"))
check("None → запис НЕ створюємо", storage.open_positions_count() == before)
check("None → гучне попередження користувачу",
      any("ВРУЧНУ" in t for t in CALLS["sent"]), f"надіслано={len(CALLS['sent'])}")

CALLS["sent"].clear()
CALLS["set_stop"].clear()
executor._open_symbols.clear()
exchange.position_size = lambda v, s: 12.0           # позиція ВІДКРИТА попри помилку
asyncio.run(executor._adopt_orphan("ORP", "bybit", "ORP/USDT:USDT", 1.0, 3.0, 1.0,
                                   _Dec(), "RequestTimeout"))
check("позиція є → взято під облік", storage.open_positions_count() == before + 1)
check("розмір узято З БІРЖІ, не з нашої оцінки",
      any(p["contracts"] == 12.0 for p in storage.get_open_positions()))
check("символ повернуто в памʼять дедупу",
      "ORP/USDT:USDT" in executor._open_symbols)
check("стоп повішено", len(CALLS["set_stop"]) == 1)
check("користувача попереджено", any("ВІДКРИТА" in t for t in CALLS["sent"]))

# ---------- 8b) «not modified» — це успіх, а не збій ----------
print("\n8b) Bybit 34040 not modified = стоп уже там, куди ставимо")
CALLS["sent"].clear()
CALLS["set_stop"].clear()


# перевіряємо саме шар exchange: він має ковтати 34040 і не піднімати виняток
class _C:
    def market(self, s):
        return {"id": "X"}

    def price_to_precision(self, s, p):
        return str(p)

    def private_post_v5_position_trading_stop(self, body):
        raise RuntimeError('bybit {"retCode":34040,"retMsg":"not modified"}')


exchange.trade_client = lambda v: _C()
exchange.set_position_stop = _ORIG_SET_STOP
try:
    exchange.set_position_stop("bybit", "X/USDT:USDT", 1.1, 0.9)
    check("34040 не піднімає виняток", True)
except Exception as e:
    check("34040 не піднімає виняток", False, str(e)[:60])


class _C2(_C):
    def private_post_v5_position_trading_stop(self, body):
        raise RuntimeError('bybit {"retCode":10001,"retMsg":"invalid"}')


exchange.trade_client = lambda v: _C2()
try:
    exchange.set_position_stop("bybit", "X/USDT:USDT", 1.1, 0.9)
    check("справжня помилка ВСЕ Ж піднімається", False)
except Exception:
    check("справжня помилка ВСЕ Ж піднімається", True)

exchange.set_position_stop = _fake_set_stop

# ---------- 9) подвійне закриття ----------
print("\n9) Монітор і звірка не можуть закрити одну позицію двічі")
orp = next(p for p in storage.get_open_positions() if p["ticker"] == "ORP")
executor._closing.add(orp["id"])
n_before = storage.open_positions_count()
asyncio.run(executor._do_close(orp, 1.0, "MANUAL"))
check("закриття «в дорозі» ігнорується", storage.open_positions_count() == n_before)
executor._closing.discard(orp["id"])
asyncio.run(executor._do_close(orp, 1.0, "MANUAL", already_closed=True))
check("після зняття замка закривається", storage.open_positions_count() == n_before - 1)
check("замок звільнено", orp["id"] not in executor._closing)

# ---------- 10) заявка на тикер має TTL ----------
print("\n10) Заявка на тикер не вічна — інакше токен неторгований до рестарту")
import time as _t  # noqa: E402

config.CLAIM_TTL_SEC = 900
config.MAX_CONCURRENT = 2
executor._claimed.clear()
executor._open_symbols.clear()
executor._reserved = 0
executor._reserved_margin = 0.0
check("перша заявка проходить", executor._reserve("ZZZ", "ZZZ/USDT:USDT") == "")
executor._release("ZZZ", "ZZZ/USDT:USDT", False)   # вхід НЕ відбувся (спрацював фільтр)
check("одразу після — дубль джерела відсічено",
      executor._reserve("ZZZ", "ZZZ/USDT:USDT") == "duplicate_source")
executor._claimed["ZZZ"] = _t.time() - 901          # TTL минув
check("після TTL тикер знову торгований",
      executor._reserve("ZZZ", "ZZZ/USDT:USDT") == "")

# ---------- 11) баланс-гард бачить гроші «в дорозі» ----------
print("\n11) Паралельні входи не витрачають одні й ті самі гроші двічі")
executor._claimed.clear()
executor._open_symbols.clear()
executor._reserved = 0
executor._reserved_margin = 0.0
executor._reserve("AAA", "AAA/USDT:USDT", 3.0)
check("маржа в дорозі врахована", executor._reserved_margin == 3.0)
executor._reserve("BBB", "BBB/USDT:USDT", 3.0)
check("двоє в дорозі → $6", executor._reserved_margin == 6.0)
executor._release("AAA", "AAA/USDT:USDT", True, 3.0)
check("після відкриття маржа знята з «у дорозі»", executor._reserved_margin == 3.0)
executor._release("BBB", "BBB/USDT:USDT", False, 3.0)
check("після провалу теж знята", executor._reserved_margin == 0.0)

# ---------- 12) монітор не б'є диск, коли позицій нема ----------
print("\n12) Монітор у спокої не читає БД і не пише в лог")
executor._open_symbols.clear()
executor._reserved = 0
executor._last_db_sync = _t.time()          # щойно звірялись
db_calls = {"n": 0}
_real_get = storage.get_open_positions


def _counting():
    db_calls["n"] += 1
    return _real_get()


storage.get_open_positions = _counting
asyncio.run(executor.monitor_once())
asyncio.run(executor.monitor_once())
check("два проходи в спокої → 0 звернень до SQLite", db_calls["n"] == 0,
      f"звернень={db_calls['n']}")
executor._last_db_sync = 0.0                # час звірки настав
asyncio.run(executor.monitor_once())
check("періодична звірка з БД усе одно відбувається", db_calls["n"] == 1)
storage.get_open_positions = _real_get

print("\n" + ("ВСІ ТЕСТИ ПРОЙШЛИ" if not FAILS else f"ПРОВАЛЕНО: {FAILS}"))
sys.exit(1 if FAILS else 0)
