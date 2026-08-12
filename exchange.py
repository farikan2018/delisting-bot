"""Мульти-біржовий шар виконання (ccxt).

Пріоритет бірж — config.VENUE_PRIORITY (за замовч. bybit → mexc).
resolve() шукає перший майданчик, де токен має активний перп.
Публічні методи (ціна, історія, наявність) працюють без ключів — тому dry-run
не потребує API-ключів жодної біржі.
"""
import ccxt

import config

_clients: dict[str, "ccxt.Exchange"] = {}

_KEYS = {
    "mexc": lambda: (config.MEXC_API_KEY, config.MEXC_API_SECRET),
    "bybit": lambda: (config.BYBIT_API_KEY, config.BYBIT_API_SECRET),
}


def client(venue: str) -> "ccxt.Exchange":
    if venue not in _clients:
        key, sec = _KEYS.get(venue, lambda: ("", ""))()
        c = getattr(ccxt, venue)(
            {
                "apiKey": key,
                "secret": sec,
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
            }
        )
        c.load_markets()
        _clients[venue] = c
    return _clients[venue]


def warm_ping(venue: str) -> bool:
    """Тримає TLS-конект до біржі теплим (той самий requests.Session-пул, яким
    піде бойовий ордер), щоб перший ордер після простою не платив ~400мс на
    холодний TLS-handshake. Публічний пінг + (якщо є ключі) підписаний виклик,
    щоб теплим був і auth/POST-шлях бойового ордера."""
    try:
        c = client(venue)
        if c.has.get("fetchTime"):
            c.fetch_time()
        else:
            c.fetch_ticker("BTC/USDT:USDT")
        key, _sec = _KEYS.get(venue, lambda: ("", ""))()
        if key:  # підписаний прогрів (той самий конект, що й create_order)
            try:
                c.fetch_balance()
            except Exception:  # noqa: BLE001
                pass
        return True
    except Exception:  # noqa: BLE001
        return False


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


# ---- РЕАЛЬНІ торгові методи (лише коли real-режим) ----
_leveraged: set = set()  # (venue,symbol) де плече вже виставлено — щоб не бити API двічі


def set_leverage_safe(venue: str, symbol: str, leverage: float) -> bool:
    """Виставляє плече. 'leverage not modified' = вже стоїть → вважаємо успіхом."""
    try:
        client(venue).set_leverage(int(leverage), symbol)
        return True
    except Exception as e:  # noqa: BLE001
        msg = str(e).lower()
        if "not modified" in msg or "110043" in msg:
            return True
        return False


def ensure_leverage(venue: str, symbol: str, leverage: float) -> bool:
    """Важіль 1b: пре-установка плеча з кешем. Перший раз — мережевий виклик,
    далі миттєво (символ у _leveraged). Так бойовий ордер не платить за set_leverage."""
    key = (venue, symbol)
    if key in _leveraged:
        return True
    if set_leverage_safe(venue, symbol, leverage):
        _leveraged.add(key)
        return True
    return False


def open_short(venue: str, symbol: str, contracts: float, leverage: float | None = None) -> dict:
    """Лише ринковий ордер. Плече виставляється заздалегідь (set_leverage_safe),
    щоб не додавати мережевий раунд у момент відкриття."""
    return client(venue).create_order(symbol, "market", "sell", contracts, None,
                                      {"marginMode": "isolated"})


def close_short(venue: str, symbol: str, contracts: float) -> dict:
    c = client(venue)
    return c.create_order(symbol, "market", "buy", contracts, None,
                          {"reduceOnly": True, "marginMode": "isolated"})
