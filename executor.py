"""Executor — відкриття, моніторинг і закриття шортів по сигналу делістингу.

DRY_RUN=True: усе симулюється (реальні ціни, віртуальні угоди), ордери не ставляться.
DRY_RUN=False: реальні ринкові ордери на MEXC (ізольована маржа).
"""
import asyncio
import datetime as dt
import time

import config
import exchange
import logbook as log
import pricecache
import storage
import strategy
import telegram_client as tg

_MODE = "dry" if config.DRY_RUN else "real"

# Фонові задачі (Telegram-сповіщення) — щоб НЕ блокувати гарячий шлях відкриття.
_bg_tasks: set = set()


async def _safe(coro) -> None:
    try:
        await coro
    except Exception:  # noqa: BLE001
        log.exception("фонова задача впала")


def fire(coro) -> None:
    """Запустити корутину у фоні (не чекаючи) — для сповіщень/логів поза гарячим шляхом."""
    t = asyncio.create_task(_safe(coro))
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)

_REASON_LABEL = {
    "STOP_LOSS": "🛑 Стоп-лос",
    "TAKE_PROFIT": "📉 Тейк-профіт",
    "MAX_HOLD": "⏰ Ліміт часу",
    "MANUAL": "🔧 Ручне закриття",
}


# ---------- ВІДКРИТТЯ ----------
async def open_from_signal(ticker: str, detect_latency=None, real=None, margin=None) -> None:
    """Обробляє один тикер делістингу. ГАРЯЧИЙ ШЛЯХ оптимізовано:
    підготовка (ціна+ref+плече) — паралельно; ордер — якнайраніше;
    Telegram-сповіщення — у фоні (fire), не блокують відкриття.

    real:   None → за config.DRY_RUN; True → примусово РЕАЛЬНИЙ ордер (для /test_short
            навіть коли авто-режим у dry); False → симуляція.
    margin: None → config.POSITION_MARGIN_USDT; інакше — задана маржа (для тесту $)."""
    real = (not config.DRY_RUN) if real is None else real
    margin = config.POSITION_MARGIN_USDT if margin is None else margin
    mode = "real" if real else "dry"
    venue, symbol = await asyncio.to_thread(exchange.resolve, ticker)
    log.event("resolve", ticker=ticker, venue=venue, symbol=symbol)
    if not symbol:
        vs = "/".join(config.VENUE_PRIORITY)
        log.event("skip", ticker=ticker, reason="no_perp", venues=vs)
        fire(tg.send_message(f"ℹ️ <b>{ticker}</b>: нема перпа на {vs} — пропуск."))
        return

    if storage.has_open_position(symbol):
        log.event("skip", ticker=ticker, reason="already_open")
        fire(tg.send_message(f"ℹ️ <b>{ticker}</b>: позиція вже відкрита — пропуск."))
        return
    if storage.open_positions_count() >= config.MAX_CONCURRENT:
        log.event("skip", ticker=ticker, reason="max_concurrent", limit=config.MAX_CONCURRENT)
        fire(tg.send_message(
            f"⚠️ <b>{ticker}</b>: досягнуто ліміту одночасних позицій "
            f"({config.MAX_CONCURRENT}) — пропуск."))
        return

    # --- Ціна + ref: СПЕРШУ з кешу (в памʼяті, 0 мережі), інакше фолбек на REST ---
    entry_price = ref_price = None
    src = "rest"
    if config.PRICECACHE_POLL_SEC > 0 and venue == "bybit":
        raw = exchange.raw_symbol_id(venue, symbol)
        gp = pricecache.get_price(raw) if raw else None
        if gp and gp[1] <= config.PRICECACHE_MAX_AGE_SEC:
            entry_price, src = gp[0], f"cache({gp[1]:.1f}s)"
            ref_price = pricecache.reference_high(raw, config.REF_LOOKBACK_MIN)

    # Мережеві задачі: фетч ціни+ref лише якщо кеш-промах; плече — лише в реалі (лінива пре-установка).
    prep = []
    if entry_price is None:
        prep.append(asyncio.to_thread(exchange.get_last_price, venue, symbol))
        prep.append(asyncio.to_thread(exchange.reference_high, venue, symbol, config.REF_LOOKBACK_MIN))
    if real:
        prep.append(asyncio.to_thread(exchange.ensure_leverage, venue, symbol, config.LEVERAGE))
    prep_res = await asyncio.gather(*prep) if prep else []
    if entry_price is None:
        entry_price, ref_price = prep_res[0], prep_res[1]
    log.event("price_src", ticker=ticker, source=src, entry_price=entry_price, ref_price=ref_price)

    decision = strategy.decide_entry(ref_price, entry_price)
    log.event("entry_eval", ticker=ticker, venue=venue, symbol=symbol, ok=decision.ok,
              reason=decision.reason, ref_price=decision.ref_price,
              entry_price=decision.entry_price, dropped_pct=decision.dropped_pct,
              detect_latency_sec=detect_latency)
    if not decision.ok:
        log.event("skip", ticker=ticker, reason=f"entry:{decision.reason}")
        fire(tg.send_message(f"⏭️ <b>{ticker}</b> ({venue}): не входимо — {decision.reason}."))
        return

    entry_price = decision.entry_price
    notional = margin * config.LEVERAGE
    # Розмір позиції — з уже завантажених markets (в памʼяті, без мережі).
    contracts = exchange.contracts_for(venue, symbol, notional, entry_price)
    contract_size = exchange.market_meta(venue, symbol)["contract_size"]

    # --- ОРДЕР якнайраніше (плече вже виставлено вище) ---
    if real:
        t_ord = time.perf_counter()
        try:
            order = await asyncio.to_thread(exchange.open_short, venue, symbol, contracts)
            entry_price = order.get("average") or order.get("price") or entry_price
        except Exception:  # noqa: BLE001
            log.exception(f"open_short помилка {ticker} {venue}")
            fire(tg.send_message(f"❌ <b>{ticker}</b>: помилка відкриття ордера ({venue})."))
            return
        log.event("order_latency", ticker=ticker, order_ms=round((time.perf_counter() - t_ord) * 1000))

    pos = {
        "ticker": ticker, "symbol": symbol, "venue": venue, "mode": mode,
        "margin": margin, "leverage": config.LEVERAGE,
        "contracts": contracts, "contract_size": contract_size,
        "ref_price": decision.ref_price, "entry_price": entry_price,
        "dropped_pct": decision.dropped_pct,
    }
    pos_id = storage.insert_position(pos)
    log.event("open", pos_id=pos_id, ticker=ticker, venue=venue, symbol=symbol,
              mode=mode, entry_price=entry_price, contracts=contracts,
              margin=margin, leverage=config.LEVERAGE,
              dropped_pct=decision.dropped_pct, detect_latency_sec=detect_latency)
    fire(tg.send_message(_open_message(pos_id, pos)))  # сповіщення — у фоні


def _open_message(pos_id: int, p: dict) -> str:
    tag = "⚠️ РЕАЛ" if p.get("mode") == "real" else "🧪 DRY-RUN"
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
            profit_pct = strategy.margin_profit_pct(pos["entry_price"], price, pos["leverage"])
            log.event("tick", pos_id=pos["id"], symbol=pos["symbol"], price=price,
                      min_price=pos["min_price"], profit_pct=round(profit_pct, 1))
            should_close, reason = strategy.check_exit(pos, price)
            if should_close:
                await _do_close(pos, price, reason)
        except Exception:  # noqa: BLE001
            log.exception(f"monitor помилка по {pos.get('symbol')}")


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
    if pos.get("mode") == "real":  # закриваємо реально лише РЕАЛЬНІ позиції (не глоб. DRY_RUN)
        try:
            order = await asyncio.to_thread(
                exchange.close_short, pos["venue"], pos["symbol"], pos["contracts"]
            )
            exit_price = order.get("average") or order.get("price") or price
        except Exception:  # noqa: BLE001
            log.exception(f"close_short помилка #{pos['id']} {pos['symbol']}")
            await tg.send_message(
                f"❌ <b>{pos['ticker']}</b>: помилка закриття (#{pos['id']}).\n"
                f"⚠️ Перевір позицію вручну на біржі!"
            )
            return

    pnl_usdt = pos["contracts"] * pos["contract_size"] * (pos["entry_price"] - exit_price)
    pnl_pct = (pos["entry_price"] - exit_price) / pos["entry_price"] * 100 * pos["leverage"]
    storage.close_position(pos["id"], exit_price, reason, pnl_usdt, pnl_pct)
    log.event("close", pos_id=pos["id"], ticker=pos["ticker"], symbol=pos["symbol"],
              mode=pos.get("mode"), reason=reason, entry_price=pos["entry_price"],
              exit_price=exit_price, min_price=pos["min_price"],
              pnl_usdt=round(pnl_usdt, 2), pnl_pct=round(pnl_pct, 1))
    await tg.send_message(_close_message(pos, exit_price, reason, pnl_usdt, pnl_pct))


def _close_message(p: dict, exit_price: float, reason: str,
                   pnl_usdt: float, pnl_pct: float) -> str:
    tag = "⚠️ РЕАЛ" if p.get("mode") == "real" else "🧪 DRY-RUN"
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
