"""Мульти-біржовий шар виконання (ccxt).

Пріоритет бірж — config.VENUE_PRIORITY (за замовч. bybit → mexc).
resolve() шукає перший майданчик, де токен має активний перп.
Публічні методи (ціна, історія, наявність) працюють без ключів — тому dry-run
не потребує API-ключів жодної біржі.
"""
import time

import ccxt

import config

_clients: dict[str, "ccxt.Exchange"] = {}
# Окремий БОЙОВИЙ клієнт на біржу: enableRateLimit=False і власний конект-пул.
# Сенс: звичайний клієнт обслуговує моніторинг/keep-alive, і ccxt-тротлер може
# затримати виклик, щоб витримати паузу між запитами. Бойовий ордер не має ні за
# ким стояти в черзі — тому в нього окремий клієнт, який більше нічим не зайнятий.
_trade_clients: dict[str, "ccxt.Exchange"] = {}

_KEYS = {
    "mexc": lambda: (config.MEXC_API_KEY, config.MEXC_API_SECRET),
    "bybit": lambda: (config.BYBIT_API_KEY, config.BYBIT_API_SECRET),
}


def _new_client(venue: str, rate_limit: bool) -> "ccxt.Exchange":
    key, sec = _KEYS.get(venue, lambda: ("", ""))()
    return getattr(ccxt, venue)(
        {
            "apiKey": key,
            "secret": sec,
            "enableRateLimit": rate_limit,
            # Явний таймаут замість дефолтних 10с ccxt. Бойовий клієнт чекає менше:
            # ордер, який не відповів за 5с, усе одно вже поза стратегією (ринок
            # рухається на 1.5-3с), а от зайві 5с очікування — це 5с, протягом яких
            # ми не знаємо, чи висить на біржі позиція.
            "timeout": config.ORDER_TIMEOUT_MS if not rate_limit else config.HTTP_TIMEOUT_MS,
            "options": {"defaultType": "swap"},
        }
    )


def client(venue: str) -> "ccxt.Exchange":
    if venue not in _clients:
        c = _new_client(venue, True)
        c.load_markets()
        _clients[venue] = c
    return _clients[venue]


def trade_client(venue: str) -> "ccxt.Exchange":
    """Клієнт лише для ордерів/плеча. markets переносимо з основного (не тягнемо
    3200 ринків двічі — це 4с на старті)."""
    if venue not in _trade_clients:
        base = client(venue)
        c = _new_client(venue, False)
        c.markets = base.markets
        c.markets_by_id = base.markets_by_id
        c.symbols = base.symbols
        c.ids = base.ids
        c.currencies = base.currencies
        _trade_clients[venue] = c
    return _trade_clients[venue]


def warm_ping(venue: str) -> bool:
    """Тримає TLS-конект теплим, щоб перший ордер після простою не платив холодний
    TLS-старт. Гріємо ОБА клієнти, і бойовий — підписаним викликом, бо саме його
    конектом і auth-шляхом полетить create_order."""
    ok = False
    try:
        c = client(venue)
        if c.has.get("fetchTime"):
            c.fetch_time()
        else:
            c.fetch_ticker("BTC/USDT:USDT")
        ok = True
    except Exception:  # noqa: BLE001
        pass
    key, _sec = _KEYS.get(venue, lambda: ("", ""))()
    if key:
        try:
            # Підписаний прогрів бойового конекта. Заразом безкоштовно оновлюємо
            # кеш вільної маржі: гарячий шлях мусить знати баланс, але не має права
            # ходити по нього в мережу — тому бере його звідси, з памʼяті.
            b = trade_client(venue).fetch_balance()
            free = ((b.get("USDT") or {}).get("free"))
            if free is not None:
                _balance_cache[venue] = (float(free), time.time())
            ok = True
        except Exception:  # noqa: BLE001
            pass
    return ok


# ---- Вільна маржа: кеш, щоб гарячий шлях не платив за мережу ----
_balance_cache: dict[str, tuple[float, float]] = {}   # venue -> (free_usdt, ts)


def cached_free_balance(venue: str, max_age: float = 180.0) -> float | None:
    """Вільна USDT-маржа з останнього keep-alive. None = даних нема або застаріли;
    None НЕ означає «нуль» — на ньому вхід не блокуємо, щоб не пропустити подію."""
    v = _balance_cache.get(venue)
    if not v or (time.time() - v[1]) > max_age:
        return None
    return v[0]


def free_balance(venue: str) -> float | None:
    """Свіжий запит балансу (не для гарячого шляху)."""
    try:
        b = client(venue).fetch_balance()
        free = ((b.get("USDT") or {}).get("free"))
        if free is None:
            return None
        _balance_cache[venue] = (float(free), time.time())
        return float(free)
    except Exception:  # noqa: BLE001
        return None


def position_size(venue: str, symbol: str) -> float | None:
    """Фактичний розмір позиції на біржі, у контрактах.
    ВАЖЛИВО: None (запит не вдався) — це НЕ те саме, що 0.0 (позиції нема).
    Нулем ми закриваємо запис у БД, тому плутати їх не можна."""
    try:
        for p in client(venue).fetch_positions([symbol]):
            if p.get("symbol") == symbol:
                return abs(float(p.get("contracts") or 0))
        return 0.0
    except Exception:  # noqa: BLE001
        return None


def set_position_stop(venue: str, symbol: str, stop_price: float | None = None,
                      take_price: float | None = None) -> None:
    """Вішає SL/TP на САМУ ПОЗИЦІЮ (Bybit trading-stop), а не окремим ордером.

    Чому саме так: такий стоп живе на боці біржі — переживає падіння нашого процесу,
    втрату мережі й рестарт машини. І зникає РАЗОМ із позицією, тому не лишає
    осиротілого умовного ордера, який пізніше сам відкриє шорт на порожньому місці.
    Кидає виняток при невдачі — рішення, що з цим робити, приймає викликач."""
    if venue != "bybit":
        raise NotImplementedError(f"біржовий стоп реалізовано лише для bybit, не {venue}")
    c = trade_client(venue)
    body = {"category": "linear", "symbol": c.market(symbol)["id"],
            "tpslMode": "Full", "positionIdx": 0}
    if stop_price:
        body["stopLoss"] = c.price_to_precision(symbol, stop_price)
        body["slTriggerBy"] = "LastPrice"
    if take_price:
        body["takeProfit"] = c.price_to_precision(symbol, take_price)
        body["tpTriggerBy"] = "LastPrice"
    try:
        c.private_post_v5_position_trading_stop(body)
    except Exception as e:  # noqa: BLE001
        # 34040 «not modified» = стоп УЖЕ стоїть рівно там, куди ми його ставимо.
        # Це успіх, а не збій: саме так відповідає біржа, коли rearm_open_stops
        # переставляє стоп після рестарту бота. Без цієї гілки кожен перезапуск
        # із відкритою позицією слав користувачу фальшиву тривогу «стоп не став».
        msg = str(e).lower()
        if "34040" in msg or "not modified" in msg:
            return
        raise


def order_fill(venue: str, symbol: str, order_id: str) -> tuple:
    """Реальна середня ціна виконання + комісія ордера — для чесного net-PnL і заміру
    слиппеджу. Окремий запит ПІСЛЯ ордера (не на критичному шляху виконання).

    Три джерела по черзі, бо `fetch_order` на Bybit для ВИКОНАНОГО ордера не працює:
    /v5/order/realtime віддає лише активні, а заповнений одразу їде в історію.
    Живий тест 2026-08-15 показав саме це — `fill_price: null, fee: null`.
    Головним зробили `fetch_my_trades`: він єдиний коректно складає ЧАСТКОВІ філи
    (зважена середня ціна + сума комісій), а ринковий ордер по неліквідному токену
    під час обвалу — це майже завжди кілька філів.
    """
    c = client(venue)

    def _from_trades():
        trades = [t for t in c.fetch_my_trades(symbol, limit=20)
                  if t.get("order") == order_id]
        if not trades:
            return (None, None)
        qty = sum(float(t.get("amount") or 0) for t in trades)
        if qty <= 0:
            return (None, None)
        avg = sum(float(t["price"]) * float(t["amount"]) for t in trades) / qty
        fee = sum(float((t.get("fee") or {}).get("cost") or 0) for t in trades)
        return (avg, fee)

    def _from_closed():
        for o in c.fetch_closed_orders(symbol, limit=20):
            if o.get("id") == order_id:
                avg = o.get("average") or o.get("price")
                fee = (o.get("fee") or {}).get("cost")
                return (float(avg) if avg else None,
                        float(fee) if fee is not None else None)
        return (None, None)

    def _from_order():
        o = c.fetch_order(order_id, symbol)
        avg = o.get("average") or o.get("price")
        fee = (o.get("fee") or {}).get("cost")
        return (float(avg) if avg else None, float(fee) if fee is not None else None)

    for src in (_from_trades, _from_closed, _from_order):
        try:
            avg, fee = src()
            if avg:
                return (avg, fee)
        except Exception:  # noqa: BLE001
            continue
    return (None, None)


def resolve(ticker: str) -> tuple[str | None, str | None]:
    """(venue, symbol) для першої біржі з активним USDT-перпом, або (None, None)."""
    ticker = ticker.upper()
    symbol = f"{ticker}/USDT:USDT"
    for v in config.VENUE_PRIORITY:
        try:
            c = client(v)
        except Exception:  # noqa: BLE001
            continue
        m = c.markets.get(symbol)
        if m and m.get("swap") and m.get("active", True):
            return v, symbol
    return None, None


def raw_symbol_id(venue: str, symbol: str) -> str | None:
    """Сирий біржовий ID символу (напр. 'DOGEUSDT') з уже завантажених markets — без мережі."""
    try:
        return client(venue).market(symbol).get("id")
    except Exception:  # noqa: BLE001
        return None


def get_last_price(venue: str, symbol: str) -> float | None:
    return client(venue).fetch_ticker(symbol).get("last")


def reference_high(venue: str, symbol: str, lookback_min: int) -> float | None:
    """«До-дампова» ціна = максимум high за останні lookback_min хв (1m свічки)."""
    try:
        ohlcv = client(venue).fetch_ohlcv(symbol, "1m", limit=max(lookback_min, 1))
        return max(c[2] for c in ohlcv) if ohlcv else None
    except Exception:  # noqa: BLE001
        return None


def market_meta(venue: str, symbol: str) -> dict:
    m = client(venue).market(symbol)
    return {
        "contract_size": m.get("contractSize") or 1,
        "min_amount": (m.get("limits", {}).get("amount", {}) or {}).get("min"),
    }


def contracts_for(venue: str, symbol: str, notional_usdt: float, price: float) -> float:
    cs = market_meta(venue, symbol)["contract_size"] or 1
    raw = notional_usdt / (price * cs)
    try:
        return float(client(venue).amount_to_precision(symbol, raw))
    except Exception:  # noqa: BLE001
        return raw


# ---- Гарячий шлях: пре-обчислена мета символів ----
# Усе, що потрібно для відкриття, порахуємо ОДИН раз на старті. На сигналі — лише
# пошук у дикті: ні мережі, ні перебору бірж, ні перемикання в потік.
HOT: dict[str, dict] = {}      # "DOGE" -> {venue, symbol, raw_id, contract_size}
BY_RAW: dict[str, str] = {}    # "DOGEUSDT" -> "DOGE" (зворотний шлях для детектора обвалу)


def ticker_by_raw(raw: str) -> str | None:
    return BY_RAW.get(raw)


def prearm_symbols() -> dict:
    """Будує HOT по всіх активних USDT-перпах у порядку VENUE_PRIORITY.
    Перша біржа, де токен є, і виграє — та сама логіка, що в resolve(), але
    порахована заздалегідь."""
    HOT.clear()
    BY_RAW.clear()
    per_venue = {}
    for v in config.VENUE_PRIORITY:
        try:
            c = client(v)
        except Exception:  # noqa: BLE001
            continue
        n = 0
        for sym, m in c.markets.items():
            if not (m.get("swap") and m.get("active", True)):
                continue
            if m.get("quote") != "USDT" or m.get("settle") != "USDT":
                continue
            base = (m.get("base") or "").upper()
            if not base or base in HOT:  # пріоритет біржі — перша перемагає
                continue
            HOT[base] = {"venue": v, "symbol": sym, "raw_id": m.get("id"),
                         "contract_size": m.get("contractSize") or 1}
            if v == "bybit" and m.get("id"):  # детектор обвалу знає лише сирий bybit-ID
                BY_RAW[m["id"]] = base
            n += 1
        per_venue[v] = n
    return {"total": len(HOT), **per_venue}


def hot_meta(ticker: str) -> dict | None:
    """Мета для гарячого шляху (0 мережі, 0 потоків). None = нема перпа."""
    return HOT.get(ticker.upper())


# ---- РЕАЛЬНІ торгові методи (лише коли real-режим) ----
_leveraged: set = set()  # (venue,symbol) де плече вже виставлено — щоб не бити API двічі


def set_leverage_safe(venue: str, symbol: str, leverage: float) -> bool:
    """Виставляє плече. 'leverage not modified' = вже стоїть → вважаємо успіхом."""
    try:
        trade_client(venue).set_leverage(int(leverage), symbol)
        return True
    except Exception as e:  # noqa: BLE001
        msg = str(e).lower()
        if "not modified" in msg or "110043" in msg:
            return True
        return False


def ensure_leverage(venue: str, symbol: str, leverage: float) -> bool:
    """Пре-установка плеча з кешем у памʼяті. Використовується фоновим пре-армом
    і як фолбек, якщо ордер відхилили через нестачу маржі."""
    key = (venue, symbol)
    if key in _leveraged:
        return True
    if set_leverage_safe(venue, symbol, leverage):
        _leveraged.add(key)
        return True
    return False


def mark_leveraged(venue: str, symbol: str) -> None:
    """Позначити символ як уже озброєний (з БД, без мережевого виклику)."""
    _leveraged.add((venue, symbol))


def is_leveraged(venue: str, symbol: str) -> bool:
    return (venue, symbol) in _leveraged


# Bybit відхиляє ордер із нестачею маржі, якщо фактичне плече нижче за наше очікуване.
_MARGIN_ERR = ("insufficient", "110007", "110012", "not enough", "ab not enough")


def is_margin_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(m in msg for m in _MARGIN_ERR)


def open_short(venue: str, symbol: str, contracts: float, leverage: float | None = None) -> dict:
    """Лише ринковий ордер, бойовим клієнтом (без тротлера). Плече НЕ виставляємо
    тут: воно озброєне фоново на старті, а на розмір позиції не впливає — кількість
    контрактів ми задаємо самі."""
    return trade_client(venue).create_order(symbol, "market", "sell", contracts, None,
                                            {"marginMode": "isolated"})


def close_short(venue: str, symbol: str, contracts: float) -> dict:
    return trade_client(venue).create_order(symbol, "market", "buy", contracts, None,
                                            {"reduceOnly": True, "marginMode": "isolated"})
