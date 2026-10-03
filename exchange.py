"""Мульти-біржовий шар виконання (ccxt).

Пріоритет бірж — config.VENUE_PRIORITY (за замовч. bybit → mexc).
resolve() шукає перший майданчик, де токен має активний перп.
Публічні методи (ціна, історія, наявність) працюють без ключів — тому dry-run
не потребує API-ключів жодної біржі.
"""
import concurrent.futures as cf
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


def warm_ping(venue: str) -> dict:
    """Тримає TLS-конекти теплими і ЧЕСНО звітує, що саме вдалося.

    ЧОМУ ПОВЕРТАЄ dict, А НЕ bool (2026-10-03). Раніше функція ставила ok=True
    одразу після ПУБЛІЧНОГО fetch_time, а підписаний виклик робила нижче в
    try/except із голим pass. Публічний виклик проходить завжди — отже мертвий
    ключ Bybit (протермінований або відвʼязаний від IP після 49 діб аптайму)
    давав рівно той самий бадьорий `bybit:ok` у лозі. Наслідки були тихі й дорогі:
    кеш вільної маржі переставав оновлюватись, BALANCE_GUARD вимикався сам собою,
    а дізнались би ми про це лише на анонсі, коли create_order падає з auth.

    ГРІЄМО КІЛЬКА КОНЕКТІВ. ccxt тримає один requests.Session на клієнта, і один
    запит лишає в пулі рівно ОДНЕ тепле зʼєднання. Але анонс дає 1-6 токенів, і
    всі вони летять ПАРАЛЕЛЬНО: другий і третій ордери відкривали б TCP+TLS із
    нуля — сотні мілісекунд саме там, де кожна секунда коштує відсотків. Тому
    підігріваємо MAX_CONCURRENT конектів одночасно, і робимо це ПУБЛІЧНИМ
    fetch_time: той самий пул і той самі TLS, але без підпису — отже без
    одночасних nonce на бойовому клієнті.
    """
    res = {"public_ok": False, "signed_ok": False, "free": None, "warmed": 0}
    try:
        c = client(venue)
        if c.has.get("fetchTime"):
            c.fetch_time()
        else:
            c.fetch_ticker("BTC/USDT:USDT")
        res["public_ok"] = True
    except Exception:  # noqa: BLE001
        pass

    key, _sec = _KEYS.get(venue, lambda: ("", ""))()
    if not key:
        return res
    tc = trade_client(venue)
    n = max(1, int(config.MAX_CONCURRENT))
    if n > 1 and tc.has.get("fetchTime"):
        with cf.ThreadPoolExecutor(max_workers=n) as pool:
            for r in pool.map(lambda _i: _safe_fetch_time(tc), range(n)):
                res["warmed"] += 1 if r else 0
    try:
        # Підписаний прогрів бойового конекта. Заразом безкоштовно оновлюємо
        # кеш вільної маржі: гарячий шлях мусить знати баланс, але не має права
        # ходити по нього в мережу — тому бере його звідси, з памʼяті.
        b = tc.fetch_balance()
        free = ((b.get("USDT") or {}).get("free"))
        if free is not None:
            _balance_cache[venue] = (float(free), time.time())
            res["free"] = float(free)
        res["signed_ok"] = True
    except Exception as e:  # noqa: BLE001
        res["err"] = f"{type(e).__name__}: {e}"[:160]
    return res


def _safe_fetch_time(c) -> bool:
    try:
        c.fetch_time()
        return True
    except Exception:  # noqa: BLE001
        return False


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


def closed_pnl(venue: str, symbol: str) -> dict | None:
    """Реальний результат ОСТАННЬОГО закриття позиції з боку біржі.

    Потрібен, бо коли позицію закрив біржовий SL/TP, ми дізнаємось про це лише
    на звірці — до 30с пізніше. Брати тодішню ринкову ціну за ціну виходу не можна:
    після делістингового обвалу ціна за 30с ходить на відсотки, і «збиток» легко
    записався б прибутком. А цим числом живиться аварійний вимикач.

    Повертає {'exit_price', 'pnl', 'side', 'ts'} або None.
    """
    if venue != "bybit":
        return None
    try:
        c = client(venue)
        r = c.private_get_v5_position_closed_pnl(
            {"category": "linear", "symbol": c.market(symbol)["id"], "limit": 5})
        rows = ((r.get("result") or {}).get("list")) or []
        if not rows:
            return None
        row = rows[0]  # біржа віддає від найновішого
        return {"exit_price": float(row["avgExitPrice"]),
                "pnl": float(row["closedPnl"]),
                "side": row.get("side"),
                "ts": int(row.get("updatedTime") or row.get("createdTime") or 0)}
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
    порахована заздалегідь.

    ЗБИРАЄМО В НОВІ ДИКТИ І ПІДМІНЯЄМО ОДНИМ ПРИСВОЄННЯМ (2026-10-03). Раніше тут
    стояло HOT.clear() із подальшим заповненням. Поки карта будується (а це тисячі
    ринків), hot_meta() повертав би None — тобто сигнал, що влучив у це вікно, дав
    би «нема перпа» і тихий пропуск делістингу. На старті вікно нікого не чіпало,
    але відколи карта перебудовується ще й періодично (див. reload_markets),
    вікно стало реальним ризиком. Присвоєння імені атомарне, вікна нема зовсім.
    """
    global HOT, BY_RAW
    hot: dict[str, dict] = {}
    by_raw: dict[str, str] = {}
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
            if not base or base in hot:  # пріоритет біржі — перша перемагає
                continue
            hot[base] = {"venue": v, "symbol": sym, "raw_id": m.get("id"),
                         "contract_size": m.get("contractSize") or 1}
            if v == "bybit" and m.get("id"):  # детектор обвалу знає лише сирий bybit-ID
                by_raw[m["id"]] = base
            n += 1
        per_venue[v] = n
    if not hot:
        # Жоден ринок не зібрався (усі біржі лежать / ключі відвалились). Підмінити
        # робочу карту порожньою означало б осліпнути: кожен тикер дав би «нема
        # перпа». Краще лишити стару карту — вона застаріла, але робоча.
        return {"total": len(HOT), "kept_old": True, **per_venue}
    HOT, BY_RAW = hot, by_raw
    return {"total": len(HOT), **per_venue}


def reload_markets() -> dict:
    """Перетягує список ринків з бірж і перебудовує HOT.

    НАВІЩО. HOT будується один раз на старті, а процес живе місяцями (на момент
    написання — 49 діб без рестарту). Перп, доданий на Bybit після старту, для
    бота не існує: `hot_meta` поверне None і делістинг цього токена дасть подію
    `skip no_perp` — тобто тиху втрату саме тієї рідкісної події, заради якої все
    це працює. Блокуючі виклики ccxt, тому викликати лише через to_thread і НЕ на
    гарячому шляху.
    """
    reloaded = {}
    for v in config.VENUE_PRIORITY:
        try:
            client(v).load_markets(True)
            reloaded[v] = "ok"
        except Exception as e:  # noqa: BLE001
            reloaded[v] = type(e).__name__
    before = len(HOT)
    st = prearm_symbols()
    return {**st, "before": before, "added": len(HOT) - before, "reload": reloaded}


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
