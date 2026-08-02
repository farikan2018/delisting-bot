"""Executor — відкриття, моніторинг і закриття шортів по сигналу делістингу.

DRY_RUN=True: усе симулюється (реальні ціни, віртуальні угоди), ордери не ставляться.
DRY_RUN=False: реальні ринкові ордери на MEXC (ізольована маржа).
"""
import asyncio
import datetime as dt

import config
import exchange
import storage
import strategy
import telegram_client as tg

_MODE = "dry" if config.DRY_RUN else "real"

_REASON_LABEL = {
    "STOP_LOSS": "🛑 Стоп-лос",
    "TAKE_PROFIT": "📉 Тейк-профіт",
    "MAX_HOLD": "⏰ Ліміт часу",
    "MANUAL": "🔧 Ручне закриття",
}


# ---------- ВІДКРИТТЯ ----------
async def open_from_signal(ticker: str) -> None:
    """Обробляє один тикер делістингу: перевірки → вхід → повідомлення."""
    venue, symbol = await asyncio.to_thread(exchange.resolve, ticker)
    if not symbol:
        vs = "/".join(config.VENUE_PRIORITY)
        await tg.send_message(f"ℹ️ <b>{ticker}</b>: нема перпа на {vs} — пропуск.")
        return

    if storage.has_open_position(symbol):
        await tg.send_message(f"ℹ️ <b>{ticker}</b>: позиція вже відкрита — пропуск.")
        return
    if storage.open_positions_count() >= config.MAX_CONCURRENT:
        await tg.send_message(
            f"⚠️ <b>{ticker}</b>: досягнуто ліміту одночасних позицій "
            f"({config.MAX_CONCURRENT}) — пропуск."
        )
        return

    decision = await asyncio.to_thread(strategy.evaluate_entry, venue, symbol)
    if not decision.ok:
        await tg.send_message(
            f"⏭️ <b>{ticker}</b> ({venue}): не входимо — {decision.reason}."
        )
        return

    entry_price = decision.entry_price
    notional = config.POSITION_MARGIN_USDT * config.LEVERAGE
    contracts = await asyncio.to_thread(
        exchange.contracts_for, venue, symbol, notional, entry_price
    )
    contract_size = (await asyncio.to_thread(exchange.market_meta, venue, symbol))["contract_size"]

    # Реальне відкриття (тільки не в dry-run)
    if not config.DRY_RUN:
        try:
            order = await asyncio.to_thread(
                exchange.open_short, venue, symbol, contracts, config.LEVERAGE
            )
            entry_price = order.get("average") or order.get("price") or entry_price
        except Exception as e:  # noqa: BLE001
            await tg.send_message(f"❌ <b>{ticker}</b>: помилка відкриття ордера ({venue}): {e}")
            return

    pos = {
        "ticker": ticker, "symbol": symbol, "venue": venue, "mode": _MODE,
        "margin": config.POSITION_MARGIN_USDT, "leverage": config.LEVERAGE,
        "contracts": contracts, "contract_size": contract_size,
        "ref_price": decision.ref_price, "entry_price": entry_price,
        "dropped_pct": decision.dropped_pct,
    }
    pos_id = storage.insert_position(pos)
    await tg.send_message(_open_message(pos_id, pos))


def _open_message(pos_id: int, p: dict) -> str:
    tag = "🧪 DRY-RUN" if config.DRY_RUN else "⚠️ РЕАЛ"
    notional = p["margin"] * p["leverage"]
    lev = p["leverage"]
    sl_price = p["entry_price"] * (1 + config.STOP_LOSS_MARGIN_PCT / lev / 100)   # стоп: ціна вгору
    tp_price = p["entry_price"] * (1 - config.TAKE_PROFIT_MARGIN_PCT / lev / 100)  # тейк: ціна вниз
    sl_loss = p["margin"] * config.STOP_LOSS_MARGIN_PCT / 100
    tp_gain = p["margin"] * config.TAKE_PROFIT_MARGIN_PCT / 100
    return (
        f"🟢 <b>ВІДКРИТО ШОРТ</b> [{tag}] #{pos_id}\n"
        f"Монета: <b>{p['ticker']}</b> (<code>{p['symbol']}</code>) на <b>{p.get('venue','?')}</b>\n"
        f"Ціна входу: <b>{_fmt(p['entry_price'])}</b>\n"
        f"Розмір: ${p['margin']:g} × {p['leverage']:g}x = ${notional:g} "
        f"(~{p['contracts']:g} контр.)\n"
        f"📉 Тейк: +{config.TAKE_PROFIT_MARGIN_PCT:g}% маржі (+${tp_gain:g}, ціна {_fmt(tp_price)})\n"
        f"🛑 Стоп: −{config.STOP_LOSS_MARGIN_PCT:g}% маржі (−${sl_loss:g}, ціна {_fmt(sl_price)})\n"
        f"⏰ Макс. утримання: {config.MAX_HOLD_MINUTES:g} хв"
    )


# ---------- МОНІТОРИНГ / ЗАКРИТТЯ ----------
async def monitor_once() -> None:
    """Один прохід по всіх відкритих позиціях: оновити мінімум, перевірити вихід."""
    positions = storage.get_open_positions()
    for pos in positions:
        try:
            price = await asyncio.to_thread(exchange.get_last_price, pos["venue"], pos["symbol"])
            if not price:
                continue
            if price < pos["min_price"]:
                storage.update_min_price(pos["id"], price)
                pos["min_price"] = price
            should_close, reason = strategy.check_exit(pos, price)
            if should_close:
                await _do_close(pos, price, reason)
        except Exception as e:  # noqa: BLE001
            print(f"[monitor] помилка по {pos.get('symbol')}: {e}")


async def force_close(pos_id: int, reason: str = "MANUAL") -> bool:
    """Ручне закриття позиції за id (для тестів/команд Telegram)."""
    for pos in storage.get_open_positions():
        if pos["id"] == pos_id:
            price = await asyncio.to_thread(exchange.get_last_price, pos["venue"], pos["symbol"])
            await _do_close(pos, price, reason)
            return True
    return False


async def _do_close(pos: dict, price: float, reason: str) -> None:
    exit_price = price
    if not config.DRY_RUN:
        try:
            order = await asyncio.to_thread(
                exchange.close_short, pos["venue"], pos["symbol"], pos["contracts"]
            )
            exit_price = order.get("average") or order.get("price") or price
        except Exception as e:  # noqa: BLE001
            await tg.send_message(
                f"❌ <b>{pos['ticker']}</b>: помилка закриття (#{pos['id']}): {e}\n"
                f"⚠️ Перевір позицію вручну на MEXC!"
            )
            return

    pnl_usdt = pos["contracts"] * pos["contract_size"] * (pos["entry_price"] - exit_price)
    pnl_pct = (pos["entry_price"] - exit_price) / pos["entry_price"] * 100 * pos["leverage"]
    storage.close_position(pos["id"], exit_price, reason, pnl_usdt, pnl_pct)
    await tg.send_message(_close_message(pos, exit_price, reason, pnl_usdt, pnl_pct))


def _close_message(p: dict, exit_price: float, reason: str,
                   pnl_usdt: float, pnl_pct: float) -> str:
    tag = "🧪 DRY-RUN" if config.DRY_RUN else "⚠️ РЕАЛ"
    emoji = "✅" if pnl_usdt >= 0 else "🔻"
    dur = _duration(p.get("opened_at"))
    return (
        f"{emoji} <b>ЗАКРИТО ШОРТ</b> [{tag}] #{p['id']}\n"
        f"Монета: <b>{p['ticker']}</b> (<code>{p['symbol']}</code>)\n"
        f"Причина: <b>{_REASON_LABEL.get(reason, reason)}</b>\n"
        f"Вхід: {_fmt(p['entry_price'])} → Вихід: {_fmt(exit_price)}\n"
        f"Прибуток: <b>{pnl_usdt:+.2f} USDT</b> ({pnl_pct:+.1f}% від маржі)\n"
        f"Тривалість: {dur}"
    )


# ---------- утиліти ----------
def _fmt(x: float) -> str:
    if x is None:
        return "?"
    if x >= 1:
        return f"{x:.4f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")


def _duration(opened_at: str | None) -> str:
    o = strategy._parse_ts(opened_at) if opened_at else None
    if not o:
        return "?"
    secs = int((dt.datetime.utcnow() - o).total_seconds())
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h}г {m}хв" if h else f"{m}хв {s}с"
