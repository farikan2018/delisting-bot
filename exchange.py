"""Обгортка над біржею виконання (MEXC через ccxt).

Абстрактно й тонко — щоб за потреби замінити біржу в одному місці.
У Фазі 3a використовуються лише публічні/read методи (resolve + ціна).
"""
import ccxt

import config

_client = None


def client() -> "ccxt.mexc":
    global _client
    if _client is None:
        c = ccxt.mexc(
            {
                "apiKey": config.MEXC_API_KEY,
                "secret": config.MEXC_API_SECRET,
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},  # ф'ючерси
            }
        )
        c.load_markets()
        _client = c
    return _client


def resolve_short_symbol(ticker: str) -> str | None:
    """Знайти активний USDT-перп для базового тикера.

    Повертає ccxt-символ (напр. 'ALPHA/USDT:USDT') або None, якщо перпа нема
    чи він не торгується.
    """
    ex = client()
    ticker = ticker.upper()
    candidate = f"{ticker}/USDT:USDT"
    m = ex.markets.get(candidate)
    if m and m.get("swap") and m.get("active", True):
        return candidate
    return None


def get_last_price(symbol: str) -> float | None:
    return client().fetch_ticker(symbol).get("last")


def market_meta(symbol: str) -> dict:
    """Дані ринку: contractSize, мін. розмір — для розрахунку кількості контрактів."""
    m = client().market(symbol)
    return {
        "contract_size": m.get("contractSize") or 1,
        "min_amount": (m.get("limits", {}).get("amount", {}) or {}).get("min"),
    }


def reference_high(symbol: str, lookback_min: int) -> float | None:
    """«До-дампова» ціна = максимум high за останні lookback_min хвилин (1m свічки)."""
    try:
        ohlcv = client().fetch_ohlcv(symbol, "1m", limit=max(lookback_min, 1))
        if not ohlcv:
            return None
        return max(c[2] for c in ohlcv)  # index 2 = high
    except Exception:  # noqa: BLE001
        return None


def contracts_for(symbol: str, notional_usdt: float, price: float) -> float:
    """Скільки контрактів дає задану notional-вартість за поточною ціною."""
    cs = market_meta(symbol)["contract_size"] or 1
    raw = notional_usdt / (price * cs)
    ex = client()
    try:
        return float(ex.amount_to_precision(symbol, raw))
    except Exception:  # noqa: BLE001
        return raw


# ---- РЕАЛЬНІ торгові методи (використовуються лише коли DRY_RUN=False) ----
def open_short(symbol: str, contracts: float, leverage: float) -> dict:
    """Ринковий шорт з ізольованою маржею. Повертає інфо про ордер."""
    ex = client()
    # плече + ізольована маржа (MEXC: openType=1 isolated)
    try:
        ex.set_leverage(int(leverage), symbol, {"openType": 1})
    except Exception:  # noqa: BLE001
        pass  # інколи плече задається у самому ордері
    params = {"marginMode": "isolated", "openType": 1, "leverage": int(leverage)}
    return ex.create_order(symbol, "market", "sell", contracts, None, params)


def close_short(symbol: str, contracts: float) -> dict:
    """Закрити шорт — ринкова купівля reduceOnly."""
    ex = client()
    params = {"reduceOnly": True, "marginMode": "isolated"}
    return ex.create_order(symbol, "market", "buy", contracts, None, params)


def fetch_position(symbol: str) -> dict | None:
    """Поточна відкрита позиція по символу (або None)."""
    ex = client()
    for p in ex.fetch_positions([symbol]):
        if p.get("contracts"):
            return p
    return None
