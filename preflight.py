"""Самоперевірка бойового шляху: прогін усіх кроків відкриття позиції БЕЗ ордера.

ЧОМУ ЦЕ ІСНУЄ (2026-10-03). Між делістингами проходить 3-6 тижнів. Весь цей час
гарячий шлях — пошук символу, розрахунок кількості контрактів, округлення до
кроку біржі, ціни стопів — НЕ ВИКОНУЄТЬСЯ ЖОДНОГО РАЗУ. Перший його реальний
прогін за місяць відбувається рівно тоді, коли на нього дивитись ніколи: у
перші секунди дампа, за живі гроші.

Так уже було. `NameError` прожив тиждень у гілці, яка виконується лише під
`fire()`. `RuntimeError: take profit price is invalid` знайшовся на біржі, а не
в нас. Те, що ніколи не виконується, не можна вважати робочим.

ЩО САМЕ ПЕРЕВІРЯЄМО. Кожен крок від тикера до готового ордера, крім самого
`create_order`. Жодного мережевого виклику на біржу тут нема: ccxt-ринки вже в
пам'яті з `prearm_symbols()`, ціна — з price-cache, округлення й точність — чиста
локальна арифметика. Тобто перевірка дешева, і її можна ганяти регулярно.

ЧОГО ЦЕ НЕ ПЕРЕВІРЯЄ І ЧОМУ. Саме виставлення ордера й біржовий стоп на РЕАЛЬНІЙ
позиції. Перевірити їх можна лише реальним ордером, а це гроші. Тому тут
перевіряється все, що можна перевірити без грошей, а для решти лишається
`/test_short` — його тисне користувач свідомо.
"""
import asyncio
import time

import alerts
import config
import exchange
import logbook as log
import pricecache

_last: dict = {"ts": None, "ok": None, "checks": [], "runs": 0, "fails": 0}


def last() -> dict:
    d = dict(_last)
    d["age_sec"] = round(time.time() - d["ts"], 1) if d["ts"] else None
    return d


def _step(name: str, ok, detail: str) -> dict:
    return {"step": name, "ok": ok, "detail": detail}


def _check_ticker(ticker: str) -> list[dict]:
    """Повний шлях для одного тикера. Повертає крок за кроком."""
    out = []
    meta = exchange.hot_meta(ticker)
    if not meta:
        # Для тестових тикерів це вже збій: вони свідомо вибрані ліквідними.
        out.append(_step(ticker + ":hot_meta", False,
                         "тикера нема в HOT — prearm_symbols не побудував карту "
                         "або біржа не піднялась"))
        return out
    venue, symbol = meta["venue"], meta["symbol"]
    out.append(_step(ticker + ":hot_meta", True, venue + " " + symbol))

    price = None
    pc = pricecache.get_price(meta.get("raw_id") or "")
    if pc:
        price, age = pc
        out.append(_step(ticker + ":price", age < 300,
                         "$" + str(price) + " з price-cache, вік " + str(round(age, 1)) + "с"))
    else:
        out.append(_step(ticker + ":price", False,
                         "ціни нема в price-cache (raw_id=" + str(meta.get("raw_id")) + ")"))
        return out
    if not price or price <= 0:
        out.append(_step(ticker + ":price_sane", False, "ціна " + str(price)))
        return out

    notional = config.POSITION_MARGIN_USDT * config.LEVERAGE
    try:
        contracts = exchange.contracts_for(venue, symbol, notional, price)
    except Exception as e:  # noqa: BLE001
        out.append(_step(ticker + ":size", False,
                         "contracts_for впав: " + type(e).__name__ + ": " + str(e)[:120]))
        return out

    # Округлення до кроку біржі може перетворити розмір на НУЛЬ — на дорогій монеті
    # при номіналі $12 це цілком реально, і ордер просто не піде.
    out.append(_step(ticker + ":size", contracts > 0,
                     str(contracts) + " контрактів на номінал $" + str(round(notional, 2))
                     + ("" if contracts > 0 else " — округлення з'їло розмір у нуль")))
    if contracts <= 0:
        return out

    try:
        mm = exchange.market_meta(venue, symbol)
        cs = mm.get("contract_size") or 1
        min_amt = mm.get("min_amount")
        real_notional = contracts * cs * price
        # Bybit відхиляє ордери, менші за мінімальний номінал (зазвичай $5).
        out.append(_step(ticker + ":notional", real_notional >= 5.0,
                         "фактичний номінал $" + str(round(real_notional, 2))
                         + ("" if real_notional >= 5.0
                            else " — нижче мінімуму біржі ~$5, ордер відхилять")))
        if min_amt:
            out.append(_step(ticker + ":min_amount", contracts >= float(min_amt),
                             str(contracts) + " проти мінімуму " + str(min_amt)))
    except Exception as e:  # noqa: BLE001
        out.append(_step(ticker + ":notional", False,
                         "market_meta впав: " + type(e).__name__))

    # Плече має бути озброєне ЗАЗДАЛЕГІДЬ: інакше перший ордер по символу платить
    # зайвий мережевий виклик set_leverage рівно тоді, коли кожна мілісекунда
    # коштує відсотків. Делістинг — це завжди «новий» символ.
    if venue == "bybit" and config.ARM_LEVERAGE:
        out.append(_step(ticker + ":leverage", exchange.is_leveraged(venue, symbol),
                         "плече " + str(config.LEVERAGE) + "x "
                         + ("озброєно" if exchange.is_leveraged(venue, symbol)
                            else "НЕ озброєно — ордер заплатить зайвий виклик")))

    # Ціни стопів — та сама арифметика, що в executor.arm_exchange_stop.
    lev = config.LEVERAGE
    sl = price * (1 + config.STOP_LOSS_MARGIN_PCT / lev / 100)
    tp = price * (1 - config.TAKE_PROFIT_MARGIN_PCT / lev / 100)
    ok_side = sl > price > tp > 0
    out.append(_step(ticker + ":stop_math", ok_side,
                     "вхід " + str(round(price, 8)) + " → SL " + str(round(sl, 8))
                     + " / TP " + str(round(tp, 8))
                     + ("" if ok_side else " — сторони переплутані або TP<=0")))
    if venue == "bybit":
        try:
            c = exchange.trade_client(venue)
            sl_p = float(c.price_to_precision(symbol, sl))
            tp_p = float(c.price_to_precision(symbol, tp))
            # Після округлення до кроку ціни стоп може СХЛОПНУТИСЬ у ціну входу —
            # на монетах із грубим кроком це робить захист фіктивним.
            collapsed = sl_p <= price or tp_p >= price
            out.append(_step(ticker + ":stop_precision", not collapsed,
                             "після округлення SL " + str(sl_p) + " / TP " + str(tp_p)
                             + ("" if not collapsed
                                else " — крок ціни з'їв дистанцію, стоп фіктивний")))
        except Exception as e:  # noqa: BLE001
            out.append(_step(ticker + ":stop_precision", False,
                             "price_to_precision впав: " + type(e).__name__))
    return out


def _check_global() -> list[dict]:
    out = []
    out.append(_step("hot_map", len(exchange.HOT) > 100,
                     str(len(exchange.HOT)) + " символів у гарячій карті"))
    if config.BYBIT_API_KEY:
        free = exchange.cached_free_balance("bybit",
                                            max_age=max(300.0, config.KEEPALIVE_SEC * 6))
        need = config.POSITION_MARGIN_USDT
        out.append(_step("balance", free is not None and free >= need,
                         ("$" + str(round(free, 2)) + " вільно, треба $"
                          + str(round(need, 2)) + " на позицію")
                         if free is not None
                         else "підписаний виклик до Bybit не проходить"))
    # Ворота застарілості мають пропускати реальні сигнали. Поллінг дає медіану
    # 46с — тобто при MAX_SIGNAL_AGE_SEC=60 половина поллінг-сигналів проходить
    # за крок до порога. Це не поломка, але про це має бути видно.
    out.append(_step("stale_gate", config.MAX_SIGNAL_AGE_SEC >= 10,
                     "MAX_SIGNAL_AGE_SEC=" + str(config.MAX_SIGNAL_AGE_SEC)
                     + "с (швидкий тригер ~4.5с, поллінг медіана ~46с)"))
    return out


async def run_once(alert_on_fail: bool = True) -> dict:
    """Один прогін. Усе важке — у потоці: ccxt-округлення тримає GIL."""
    def _work():
        checks = _check_global()
        for tk in config.PREFLIGHT_TICKERS:
            checks += _check_ticker(tk)
        return checks

    checks = await asyncio.to_thread(_work)
    bad = [c for c in checks if c["ok"] is False]
    _last.update({"ts": time.time(), "ok": not bad, "checks": checks,
                  "runs": _last["runs"] + 1,
                  "fails": _last["fails"] + (1 if bad else 0)})
    log.event("preflight", ok=not bad, steps=len(checks), failed=len(bad),
              failing=[c["step"] for c in bad][:12])
    if bad and alert_on_fail:
        await alerts.raise_alert(
            "Самоперевірка бойового шляху провалилась",
            "Не пройшли " + str(len(bad)) + " з " + str(len(checks)) + " кроків:"
            + chr(10)
            + chr(10).join("• " + c["step"] + ": " + c["detail"] for c in bad[:8])
            + chr(10) + "Це шлях, яким піде РЕАЛЬНИЙ ордер на делістингу.",
            cooldown_sec=12 * 3600)
    elif not bad:
        await alerts.clear_alert("Самоперевірка бойового шляху провалилась")
    return _last


def report_text() -> str:
    d = last()
    if not d["ts"]:
        return "➖ Самоперевірка ще не виконувалась"
    bad = [c for c in d["checks"] if c["ok"] is False]
    head = ("✅ Самоперевірка пройдена" if not bad
            else "❌ Самоперевірка: " + str(len(bad)) + " збоїв")
    head += " (" + str(len(d["checks"])) + " кроків, " + str(d["age_sec"]) + "с тому)"
    if not bad:
        return head
    return head + chr(10) + chr(10).join("• " + c["step"] + ": " + c["detail"]
                                         for c in bad[:8])


async def run() -> None:
    """Цикл: перший прогін після прогріву, далі раз на PREFLIGHT_SEC."""
    # Чекаємо, поки наповниться price-cache і кеш балансу — інакше перший прогін
    # завалиться не на справжній поломці, а на порожніх кешах.
    await asyncio.sleep(max(60.0, config.KEEPALIVE_SEC * 2))
    while True:
        try:
            await run_once()
        except Exception:  # noqa: BLE001
            log.exception("preflight: прогін впав")
        if config.PREFLIGHT_SEC <= 0:
            return
        await asyncio.sleep(config.PREFLIGHT_SEC)
