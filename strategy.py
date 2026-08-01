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


def evaluate_entry(symbol: str) -> EntryDecision:
    """Рахує «до-дампову» ціну (макс за REF_LOOKBACK_MIN хв) і поточну,
    визначає, чи не пізно входити."""
    ref_price = exchange.reference_high(symbol, config.REF_LOOKBACK_MIN)
    entry_price = exchange.get_last_price(symbol)

    if not ref_price or not entry_price:
        return EntryDecision(False, "нема даних ціни", ref_price, entry_price, None)

    dropped_pct = (ref_price - entry_price) / ref_price * 100.0

    if dropped_pct > config.MAX_ALREADY_DROP_PCT:
        return EntryDecision(
            False,
            f"вже впала {dropped_pct:.1f}% (> {config.MAX_ALREADY_DROP_PCT:g}%) — пізно/ризиково",
            ref_price, entry_price, dropped_pct,
        )
    return EntryDecision(True, "OK", ref_price, entry_price, dropped_pct)


def check_exit(position: dict, price: float, now: dt.datetime | None = None) -> tuple[bool, str]:
    """Чи закривати шорт. Повертає (закрити?, причина).

    Порядок пріоритету: стоп-лос → трейлінг-тейк → макс. час.
    """
    entry = position["entry_price"]
    min_price = min(position["min_price"], price)

    # % руху ціни від входу (для шорта плюс = ціна впала)
    move_pct = (entry - price) / entry * 100.0  # >0 у нашу користь

    # 1) Стоп-лос: ціна зросла на STOP_LOSS_PCT проти нас
    if -move_pct >= config.STOP_LOSS_PCT:
        return True, "STOP_LOSS"

    # 2) Trailing take-profit: якщо були достатньо в плюсі (по найнижчій ціні)
    #    і ціна відскочила від дна на TRAIL_PCT — фіксуємо.
    max_profit_pct = (entry - min_price) / entry * 100.0  # найкращий досягнутий плюс
    if max_profit_pct >= config.MIN_PROFIT_TO_TRAIL_PCT:
        bounce_pct = (price - min_price) / min_price * 100.0
        if bounce_pct >= config.TRAIL_PCT:
            return True, "TRAILING_TP"

    # 3) Макс. час утримання
    now = now or dt.datetime.utcnow()
    opened = _parse_ts(position["opened_at"])
    if opened and (now - opened).total_seconds() >= config.MAX_HOLD_HOURS * 3600:
        return True, "MAX_HOLD"

    return False, ""


def _parse_ts(s: str) -> dt.datetime | None:
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None
