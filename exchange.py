"""Мульти-біржовий шар виконання (ccxt).

Пріоритет бірж — config.VENUE_PRIORITY (за замовч. bybit → mexc).
resolve() шукає перший майданчик, де токен має активний перп.
Публічні методи (ціна, історія, наявність) працюють без ключів — тому dry-run
не потребує API-ключів жодної біржі.
"""
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
            trade_client(venue).fetch_balance()  # підписаний прогрів бойового конекта
            ok = True
        except Exception:  # noqa: BLE001
            pass
    return ok


def order_fill(venue: str, symbol: str, order_id: str) -> tuple:
    """Реальна середня ціна виконання + комісія ордера (avgPrice/fee) — для чесного
    логування входу/виходу зі слиппеджем. Окремий запит ПІСЛЯ ордера (не на критичному
    шляху виконання). Повертає (avg_price|None, fee_cost|None)."""
    try:
        o = client(venue).fetch_order(order_id, symbol)
        avg = o.get("average") or o.get("price")
        fee = (o.get("fee") or {}).get("cost")
        return (float(avg) if avg else None, float(fee) if fee is not None else None)
    except Exception:  # noqa: BLE001
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
