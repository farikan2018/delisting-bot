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
