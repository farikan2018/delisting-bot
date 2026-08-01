"""Executor — рішення й дії по сигналу делістингу.

Фаза 3a: тільки DRY-RUN. Для кожного тикера події:
  - шукає перп на MEXC,
  - рахує, який шорт відкрив би (розмір/плече/контракти),
  - формує людський опис плану.
Реальних ордерів НЕ ставить, поки config.DRY_RUN=True.
"""
from dataclasses import dataclass

import config
import exchange


@dataclass
class Plan:
    ticker: str
    symbol: str | None
    price: float | None
    contracts: float | None
    notional: float | None
    note: str


def build_plans(tickers: list[str]) -> list[Plan]:
    plans = []
    for ticker in tickers:
        try:
            symbol = exchange.resolve_short_symbol(ticker)
        except Exception as e:  # noqa: BLE001
            plans.append(Plan(ticker, None, None, None, None, f"помилка пошуку: {e}"))
            continue

        if not symbol:
            plans.append(Plan(ticker, None, None, None, None, "нема перпа на MEXC — пропуск"))
            continue

        try:
            price = exchange.get_last_price(symbol)
            meta = exchange.market_meta(symbol)
            notional = config.POSITION_MARGIN_USDT * config.LEVERAGE
            contracts = None
            if price and meta["contract_size"]:
                contracts = notional / (price * meta["contract_size"])
            plans.append(Plan(ticker, symbol, price, contracts, notional, "OK"))
        except Exception as e:  # noqa: BLE001
            plans.append(Plan(ticker, symbol, None, None, None, f"помилка ціни/ринку: {e}"))
    return plans


def format_plans(plans: list[Plan]) -> str:
    lines = ["🧪 <b>DRY-RUN — план дій</b> (реальних ордерів нема)"]
    for p in plans:
        if p.symbol and p.note == "OK":
            c = f"{p.contracts:.4f}" if p.contracts is not None else "?"
            lines.append(
                f"• <b>{p.ticker}</b> → шорт <code>{p.symbol}</code>\n"
                f"   маржа ${config.POSITION_MARGIN_USDT:g} × {config.LEVERAGE:g}x = "
                f"${p.notional:g} notional (~{c} контр.) @ ~{p.price}"
            )
        else:
            lines.append(f"• <b>{p.ticker}</b> — {p.note}")
    return "\n".join(lines)


def handle_signal_dryrun(tickers: list[str]) -> str:
    """Повертає готовий текст для Telegram по події делістингу (dry-run)."""
    plans = build_plans(tickers)
    return format_plans(plans)
