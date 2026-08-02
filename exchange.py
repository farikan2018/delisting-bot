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


# ---- РЕАЛЬНІ торгові методи (лише коли DRY_RUN=False) ----
def open_short(venue: str, symbol: str, contracts: float, leverage: float) -> dict:
    c = client(venue)
    try:
        c.set_leverage(int(leverage), symbol)
    except Exception:  # noqa: BLE001
        pass
    params = {"marginMode": "isolated"}
    return c.create_order(symbol, "market", "sell", contracts, None, params)


def close_short(venue: str, symbol: str, contracts: float) -> dict:
    c = client(venue)
    return c.create_order(symbol, "market", "buy", contracts, None,
                          {"reduceOnly": True, "marginMode": "isolated"})
