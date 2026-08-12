"""Торгова логіка: фільтр входу (anti-late-entry) і умови виходу.

Чиста логіка без побічних ефектів — легко тестувати й міняти параметри.
"""
import datetime as dt
from dataclasses import dataclass

import config
import exchange


@dataclass
class EntryDecision:
    ok: bool
    reason: str
    ref_price: float | None
    entry_price: float | None
    dropped_pct: float | None  # на скільки вже впала від ref до входу


def decide_entry(ref_price: float | None, entry_price: float | None) -> EntryDecision:
    """ЧИСТЕ рішення входу (без I/O) — ціну й ref фетчимо ЗОВНІ (паралельно в executor).
    Дає змогу не робити мережеві виклики послідовно на гарячому шляху."""
    if not entry_price:
        return EntryDecision(False, "нема даних ціни", ref_price, entry_price, None)
    dropped_pct = ((ref_price - entry_price) / ref_price * 100.0) if ref_price else 0.0
    # Anti-late-entry: якщо вже впало більше за поріг — пропуск (0 = фільтр вимкнено).
    if config.MAX_ALREADY_DROP_PCT > 0 and dropped_pct > config.MAX_ALREADY_DROP_PCT:
        return EntryDecision(
            False,
            f"вже впала {dropped_pct:.1f}% (> {config.MAX_ALREADY_DROP_PCT:g}%)",
            ref_price, entry_price, dropped_pct,
        )
    return EntryDecision(True, "OK", ref_price, entry_price, dropped_pct)


def evaluate_entry(venue: str, symbol: str) -> EntryDecision:
    """I/O-обгортка (для тестів/сумісності): фетчить ціну+ref послідовно, тоді decide_entry.
    Гарячий шлях у executor фетчить ці два ЗНАЧЕННЯ паралельно й кличе decide_entry напряму."""
    entry_price = exchange.get_last_price(venue, symbol)
    ref_price = exchange.reference_high(venue, symbol, config.REF_LOOKBACK_MIN) if entry_price else None
    return decide_entry(ref_price, entry_price)


def margin_profit_pct(entry: float, price: float, leverage: float) -> float:
    """Нереалізований прибуток шорта у % від маржі (плюс = ціна впала)."""
    return (entry - price) / entry * 100.0 * leverage


def check_exit(position: dict, price: float, now: dt.datetime | None = None) -> tuple[bool, str]:
    """Strategy v2. Пороги у % від МАРЖІ. Пріоритет: стоп-лос → тейк → макс. час."""
    profit = margin_profit_pct(position["entry_price"], price, position["leverage"])

    # 1) Стоп-лос
    if profit <= -config.STOP_LOSS_MARGIN_PCT:
        return True, "STOP_LOSS"
    # 2) Тейк-профіт (фіксований)
    if profit >= config.TAKE_PROFIT_MARGIN_PCT:
        return True, "TAKE_PROFIT"
    # 3) Макс. час утримання
    now = now or dt.datetime.utcnow()
    opened = _parse_ts(position["opened_at"])
    if opened and (now - opened).total_seconds() >= config.MAX_HOLD_MINUTES * 60:
        return True, "MAX_HOLD"
    return False, ""


def _parse_ts(s: str) -> dt.datetime | None:
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None
