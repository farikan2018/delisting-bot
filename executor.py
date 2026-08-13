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
# Комісія входу по pos_id (у памʼяті процесу) — для net-PnL при закритті.
_entry_fee: dict = {}

# --- Дедуп між джерелами + резервація слотів. Усе в памʼяті й СИНХРОННО. ---
# Джерел детекту кілька (Odin / WS-фід / швидкий поллінг / Telegram) і на одну подію
# вони приходять із різницею мілісекунд. Якби перевірка «чи вже відкрито» йшла в БД
# через await, обидва дубли встигли б її пройти й відкрити дві позиції на один токен.
_claimed: set = set()       # тикери, по яких заявку вже взято в цьому процесі
_open_symbols: set = set()  # символи з відкритою позицією (люстро БД у памʼяті)
_reserved: int = 0          # відкриттів «у дорозі» — щоб не пробити MAX_CONCURRENT


def resync_open() -> None:
    """Синхронізує памʼять із БД (старт процесу / після закриття)."""
    _open_symbols.clear()
    for p in storage.get_open_positions():
        _open_symbols.add(p["symbol"])


def _reserve(ticker: str, symbol: str) -> str:
    """Синхронна заявка на відкриття: '' = можна. КРИТИЧНО: між перевіркою і
    заявкою не має бути жодного await, інакше дедуп нічого не гарантує."""
    global _reserved
    if ticker in _claimed:
        return "duplicate_source"
    if symbol in _open_symbols:
        return "already_open"
    if len(_open_symbols) + _reserved >= config.MAX_CONCURRENT:
        return "max_concurrent"
    _claimed.add(ticker)
    _reserved += 1
    return ""


def _release(ticker: str, symbol: str, opened: bool) -> None:
    global _reserved
    _reserved = max(0, _reserved - 1)
    if opened:
        _open_symbols.add(symbol)
    # _claimed НЕ знімаємо навмисно: якщо вхід не відбувся (фільтр або помилка ордера),
    # дубль з іншого джерела тим паче не має пробувати ще раз по тій самій події.


def busy() -> bool:
    """Чи є відкриття «в дорозі» — фонові масові задачі мають зачекати й не забивати конект."""
    return _reserved > 0


def hot_state() -> dict:
    return {"claimed": len(_claimed), "open": len(_open_symbols), "reserved": _reserved}


def forget(ticker: str, symbol: str) -> None:
    """Позиція закрита — знімаємо і заявку, і символ, щоб токен знову був доступний."""
    _claimed.discard(ticker.upper())
    _open_symbols.discard(symbol)


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
async def open_from_signal(ticker: str, detect_latency=None, real=None, margin=None,
                           dedup: bool = True, source: str = "?") -> None:
    """Обробляє один тикер делістингу.

    ГАРЯЧИЙ ШЛЯХ = все до create_order. Правило: до ордера — ЖОДНОЇ мережі, жодного
    потоку, жодного SQLite і жодного запису в лог-файл. Усе, що для цього потрібно,
    пре-обчислене на старті (exchange.HOT) або лежить у price-cache. Заміри показали,
    що сам ордер летить 165мс (фізика Франкфурт→матчер Bybit), тому будь-які наші
    власні мілісекунди — це чистий збиток.

    real:   None → за config.DRY_RUN; True → примусово РЕАЛЬНИЙ ордер (/test_short).
    margin: None → config.POSITION_MARGIN_USDT.
    dedup:  True → заявка з дедупом між джерелами (для сигналів). /test_short — False.
    """
    t_sig = time.perf_counter()
    real = (not config.DRY_RUN) if real is None else real
    margin = config.POSITION_MARGIN_USDT if margin is None else margin
    mode = "real" if real else "dry"
    ticker = ticker.upper()

    # 1) СИНХРОННО: мета символу з памʼяті — 0 мережі, 0 потоків.
    meta = exchange.hot_meta(ticker)
    if not meta:
        vs = "/".join(config.VENUE_PRIORITY)
        log.event("skip", ticker=ticker, reason="no_perp", venues=vs, source=source)
        fire(tg.send_message(f"ℹ️ <b>{ticker}</b>: нема перпа на {vs} — пропуск."))
        return
    venue, symbol, raw = meta["venue"], meta["symbol"], meta["raw_id"]

    # 2) СИНХРОННО: заявка (дедуп між джерелами + ліміт одночасних позицій).
    if dedup:
        why = _reserve(ticker, symbol)
        if why:
            log.event("skip", ticker=ticker, reason=why, source=source)
            if why != "duplicate_source":  # дубль джерела — нормальна робота, не спамимо
                fire(tg.send_message(f"ℹ️ <b>{ticker}</b>: пропуск — {why}."))
            return

    opened = False
    try:
        # 3) СИНХРОННО: ціна + ref із price-cache (в памʼяті). REST — лише як фолбек.
        entry_price = ref_price = None
        src = "rest"
        if config.PRICECACHE_POLL_SEC > 0 and venue == "bybit" and raw:
            gp = pricecache.get_price(raw)
            if gp and gp[1] <= config.PRICECACHE_MAX_AGE_SEC:
                entry_price, src = gp[0], f"cache({gp[1]:.1f}s)"
                ref_price = pricecache.reference_high(raw, config.REF_LOOKBACK_MIN)
        if entry_price is None:  # кеш-промах: два фетчі паралельно
            entry_price, ref_price = await asyncio.gather(
                asyncio.to_thread(exchange.get_last_price, venue, symbol),
                asyncio.to_thread(exchange.reference_high, venue, symbol,
                                  config.REF_LOOKBACK_MIN))

        # 4) СИНХРОННО: рішення + розмір (чиста арифметика по завантажених markets).
        decision = strategy.decide_entry(ref_price, entry_price)
        if not decision.ok:
            log.event("skip", ticker=ticker, reason=f"entry:{decision.reason}", source=source,
                      ref_price=decision.ref_price, entry_price=decision.entry_price,
                      dropped_pct=decision.dropped_pct, price_src=src)
            fire(tg.send_message(f"⏭️ <b>{ticker}</b> ({venue}): не входимо — {decision.reason}."))
            return
        entry_price = decision.entry_price
        contracts = exchange.contracts_for(venue, symbol, margin * config.LEVERAGE, entry_price)
        contract_size = meta["contract_size"]
        prep_ms = round((time.perf_counter() - t_sig) * 1000, 1)

        # 5) ОРДЕР. Плече вже озброєне фоново — тут його не торкаємось.
        order_ms = None
        order = None
        if real:
            t_ord = time.perf_counter()
            try:
                order = await asyncio.to_thread(exchange.open_short, venue, symbol, contracts)
            except Exception as e:  # noqa: BLE001
                # Єдина причина заплатити зайвий раунд: біржа відхилила через маржу,
                # бо фактичне плече нижче за наше. Виставляємо плече й пробуємо ще раз.
                if exchange.is_margin_error(e) and not exchange.is_leveraged(venue, symbol):
                    log.event("order_retry_leverage", ticker=ticker, err=str(e)[:120])
                    try:
                        await asyncio.to_thread(exchange.ensure_leverage, venue, symbol,
                                                config.LEVERAGE)
                        order = await asyncio.to_thread(exchange.open_short, venue, symbol,
                                                        contracts)
                    except Exception:  # noqa: BLE001
                        log.exception(f"open_short повторно впав {ticker} {venue}")
                        fire(tg.send_message(f"❌ <b>{ticker}</b>: помилка ордера ({venue})."))
                        return
                else:
                    log.exception(f"open_short помилка {ticker} {venue}")
                    fire(tg.send_message(f"❌ <b>{ticker}</b>: помилка ордера ({venue})."))
                    return
            order_ms = round((time.perf_counter() - t_ord) * 1000)

        # --- Далі гарячий шлях завершено: облік, логи, сповіщення. ---
        pos = {
            "ticker": ticker, "symbol": symbol, "venue": venue, "mode": mode,
            "margin": margin, "leverage": config.LEVERAGE,
            "contracts": contracts, "contract_size": contract_size,
            "ref_price": decision.ref_price, "entry_price": entry_price,
            "dropped_pct": decision.dropped_pct,
        }
        pos_id = storage.insert_position(pos)
        opened = True
        log.event("open", pos_id=pos_id, ticker=ticker, venue=venue, symbol=symbol,
                  mode=mode, entry_price=entry_price, contracts=contracts,
                  margin=margin, leverage=config.LEVERAGE, price_src=src,
                  dropped_pct=decision.dropped_pct, source=source,
                  detect_latency_sec=detect_latency,
                  prep_ms=prep_ms, order_ms=order_ms,
                  total_ms=round((time.perf_counter() - t_sig) * 1000, 1))
        fire(tg.send_message(_open_message(pos_id, pos)))
        if real and order is not None:  # реальний fill+комісія — уже поза критичним шляхом
            fire(_settle_fill(pos_id, venue, symbol, order))
    finally:
        if dedup:
            _release(ticker, symbol, opened)


async def _settle_fill(pos_id: int, venue: str, symbol: str, order: dict) -> None:
    """Довантажує реальну ціну виконання й комісію входу (окремий запит ПІСЛЯ ордера)
    і виправляє ними запис у БД. Потрібно для чесного net-PnL і заміру слиппеджу."""
    avg = order.get("average") or order.get("price")
    fee = (order.get("fee") or {}).get("cost")
    oid = order.get("id")
    if (avg is None or fee is None) and oid:
        f_avg, f_fee = await asyncio.to_thread(exchange.order_fill, venue, symbol, oid)
        avg = avg or f_avg
        fee = fee if fee is not None else f_fee
    if fee is not None:
        _entry_fee[pos_id] = fee
    if avg:
        storage.update_entry_price(pos_id, float(avg))
    log.event("fill", pos_id=pos_id, symbol=symbol, fill_price=avg, fee=fee)


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
async def current_price(pos: dict) -> float | None:
    """Ціна для перевірки виходу: спершу price-cache (WS, реал-тайм, 0 мережі),
    REST — лише як фолбек. Раніше кожна позиція раз на тік їла REST-запит 165мс,
    через що стоп реагував на ціну, якій уже чверть секунди."""
    meta = exchange.hot_meta(pos["ticker"])
    if meta and meta["venue"] == "bybit" and meta["raw_id"]:
        gp = pricecache.get_price(meta["raw_id"])
        if gp and gp[1] <= config.PRICECACHE_MAX_AGE_SEC:
            return gp[0]
    return await asyncio.to_thread(exchange.get_last_price, pos["venue"], pos["symbol"])


async def monitor_once() -> None:
    """Один прохід по всіх відкритих позиціях: оновити мінімум, перевірити вихід."""
    positions = storage.get_open_positions()
    for pos in positions:
        try:
            price = await current_price(pos)
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
            await _do_close(pos, await current_price(pos), reason)
            return True
    return False


async def _do_close(pos: dict, price: float, reason: str) -> None:
    exit_price = price
    exit_fee = None
    if pos.get("mode") == "real":  # закриваємо реально лише РЕАЛЬНІ позиції (не глоб. DRY_RUN)
        try:
            order = await asyncio.to_thread(
                exchange.close_short, pos["venue"], pos["symbol"], pos["contracts"]
            )
        except Exception:  # noqa: BLE001
            log.exception(f"close_short помилка #{pos['id']} {pos['symbol']}")
            await tg.send_message(
                f"❌ <b>{pos['ticker']}</b>: помилка закриття (#{pos['id']}).\n"
                f"⚠️ Перевір позицію вручну на біржі!"
            )
            return
        avg = order.get("average") or order.get("price")
        fee = (order.get("fee") or {}).get("cost")
        oid = order.get("id")
        if (avg is None or fee is None) and oid:
            f_avg, f_fee = await asyncio.to_thread(
                exchange.order_fill, pos["venue"], pos["symbol"], oid)
            avg = avg or f_avg
            fee = fee if fee is not None else f_fee
        exit_price = avg or price
        exit_fee = fee

    price_pnl = pos["contracts"] * pos["contract_size"] * (pos["entry_price"] - exit_price)
    entry_fee = _entry_fee.pop(pos["id"], exit_fee)  # памʼять процесу; фолбек ~exit_fee
    fees = (entry_fee or 0.0) + (exit_fee or 0.0)
    pnl_usdt = price_pnl - fees
    pnl_pct = pnl_usdt / pos["margin"] * 100 if pos.get("margin") else 0.0
    storage.close_position(pos["id"], exit_price, reason, pnl_usdt, pnl_pct)
    forget(pos["ticker"], pos["symbol"])  # токен знову доступний для наступного сигналу
    log.event("close", pos_id=pos["id"], ticker=pos["ticker"], symbol=pos["symbol"],
              mode=pos.get("mode"), reason=reason, entry_price=pos["entry_price"],
              exit_price=exit_price, min_price=pos["min_price"],
              price_pnl=round(price_pnl, 4), fees=round(fees, 4),
              pnl_usdt=round(pnl_usdt, 4), pnl_pct=round(pnl_pct, 1))
    await tg.send_message(_close_message(pos, exit_price, reason, pnl_usdt, pnl_pct, fees))


def _close_message(p: dict, exit_price: float, reason: str,
                   pnl_usdt: float, pnl_pct: float, fees: float = 0.0) -> str:
    tag = "⚠️ РЕАЛ" if p.get("mode") == "real" else "🧪 DRY-RUN"
    emoji = "✅" if pnl_usdt >= 0 else "🔻"
    dur = _duration(p.get("opened_at"))
    fee_line = f"Комісії: −{fees:.4f} USDT\n" if fees else ""
    return (
        f"{emoji} <b>ЗАКРИТО ШОРТ</b> [{tag}] #{p['id']}\n"
        f"Монета: <b>{p['ticker']}</b> (<code>{p['symbol']}</code>)\n"
        f"Причина: <b>{_REASON_LABEL.get(reason, reason)}</b>\n"
        f"Вхід: {_fmt(p['entry_price'])} → Вихід: {_fmt(exit_price)}\n"
        f"{fee_line}"
        f"Прибуток (net): <b>{pnl_usdt:+.4f} USDT</b> ({pnl_pct:+.1f}% від маржі)\n"
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
