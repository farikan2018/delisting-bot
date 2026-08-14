"""Delisting-бот: watcher делістингів Binance + executor шортів на MEXC.

Два паралельні цикли: _watch_loop (ловить делістинги, відкриває шорти)
і _monitor_loop (стежить за позиціями, закриває за стратегією).
Режим торгівлі керується config.DRY_RUN (симуляція vs реальні ордери).
"""
import asyncio
import datetime as dt
import sys
import time

import aiohttp

# Windows-консоль інколи cp1252 — примусово UTF-8, щоб кирилиця не ламала вивід.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

import binance_watcher as bw
import config
import dumpwatch
import exchange
import executor
import fastcms
import fastjson
import logbook as log
import pricecache
import runtime
import storage
import telegram_client as tg


_LOOP = "asyncio"  # перезаписується в __main__ на "uvloop", якщо він доступний

_CAT_LABEL = {
    bw.SPOT_DELIST: "🔴 ПОВНИЙ ДЕЛІСТИНГ ТОКЕНА (сигнал для шорта)",
    bw.MARGIN_DELIST: "🔵 Делістинг лише з margin/loan (спот лишається — НЕ торгуємо)",
    bw.FUTURES_DELIST: "🟠 Делістинг ф'ючерсного контракту",
    bw.PAIR_REMOVAL: "🟡 Прибирання торгових пар",
    bw.OTHER: "⚪ Інше",
}


def _fmt_event(ev: bw.DelistingEvent) -> str:
    tickers = ", ".join(ev.tickers) if ev.tickers else "— (дивись у тілі анонсу)"
    lines = [
        f"<b>{_CAT_LABEL.get(ev.category, ev.category)}</b>",
        f"<b>Токени:</b> {tickers}",
        f"<b>Заголовок:</b> {ev.title}",
    ]
    if ev.url:
        lines.append(f'<a href="{ev.url}">Анонс</a>')
    if not ev.actionable:
        lines.append("<i>(не торгуємо: не повний спот-делістинг)</i>")
    return "\n".join(lines)


async def _fire_tickers(tickers: list[str], latency, source: str) -> None:
    """Усі токени з одного анонсу — ПАРАЛЕЛЬНО. Послідовно другий токен чекав би,
    поки перший відпрацює свій ордер (165мс до матчера Bybit), третій — двічі стільки:
    на типовому анонсі з трьох токенів останній заходив на пів секунди пізніше."""
    async def one(tk: str) -> None:
        try:
            await executor.open_from_signal(tk, detect_latency=latency, source=source)
        except Exception:  # noqa: BLE001
            log.exception(f"executor помилка по {tk}")
    await asyncio.gather(*(one(tk) for tk in tickers))


async def _on_fastcms(ev: bw.DelistingEvent, latency, host: str) -> None:
    """Подія від ВЛАСНОГО швидкого детектора (поллінг некешованого origin, ~0.4с).
    Це той самий сигнал, що й WS-фід, але швидший — і без залежності від третьої сторони."""
    executor.fire(tg.send_message(
        f"⚡ <b>Власний детектор (+{latency}с, {host.split('.')[0]})</b>\n"
        f"{_fmt_event(ev)}"
    ))
    if not (ev.actionable and ev.tickers):
        return
    if not config.FASTCMS_TRADE:
        log.event("fastcms_no_trade", tickers=ev.tickers, reason="FASTCMS_TRADE=0")
        return
    if latency is not None and latency > config.MAX_SIGNAL_AGE_SEC:
        log.event("fastcms_stale_no_trade", tickers=ev.tickers, latency_sec=latency)
        return
    await _fire_tickers(ev.tickers, latency, f"fastcms:{host.split('.')[0]}")


async def _watch_loop() -> None:
    storage.init()
    print(f"[{dt.datetime.now():%H:%M:%S}] Старт. Уже бачених анонсів: {storage.seen_count()}")

    # Прайм: маркуємо наявні анонси як бачені, щоб не спамити старими при першому запуску.
    first_run = storage.seen_count() == 0
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                events = await bw.fetch_new_events(session)
                for ev in events:
                    # Дедуп СПІЛЬНИЙ із fastcms і синхронний: обидва джерела читають той
                    # самий ендпоінт, тож без спільної заявки був би дубль угоди.
                    if not fastcms.claim(ev.article_id):
                        continue
                    storage.mark_seen(ev.article_id, ev.title)
                    now_ms = int(time.time() * 1000)
                    # затримка детекту: скільки минуло від публікації до нашого виявлення
                    latency = round((now_ms - ev.release_ms) / 1000, 1) if ev.release_ms else None
                    if first_run:
                        log.info(f"(прайм, без сповіщення) {ev.title}")
                        continue
                    log.event("delisting_detected", article_id=ev.article_id,
                              category=ev.category, tickers=ev.tickers, title=ev.title,
                              release_ms=ev.release_ms, detected_ms=now_ms,
                              detect_latency_sec=latency, actionable=ev.actionable)
                    await tg.send_message(_fmt_event(ev))

                    # Поллінг — лише СТОРОЖ. Торгуємо тільки якщо сигнал свіжий
                    # (зазвичай це WS; поллінг ~126с → лише попередження).
                    if ev.actionable and ev.tickers:
                        fresh = latency is not None and latency <= config.MAX_SIGNAL_AGE_SEC
                        if fresh:
                            await _fire_tickers(ev.tickers, latency, "poll_apex")
                        else:
                            log.event("poll_stale_no_trade", tickers=ev.tickers,
                                      latency_sec=latency)
                            await tg.send_message(
                                f"⏱️ <b>Делістинг помічено ПІЗНО через поллінг</b> "
                                f"(+{latency}с) — угоду НЕ відкриваю (застаріло).\n"
                                f"Токени: {', '.join(ev.tickers)}\n"
                                f"<i>Якщо WS працює — він мав відпрацювати раніше.</i>"
                            )
                if first_run:
                    log.info("Первинні анонси позначені як бачені. Далі — тільки нові.")
                    first_run = False
            except Exception:  # noqa: BLE001
                log.exception("watcher помилка")
            await asyncio.sleep(config.POLL_INTERVAL)


async def _handle_ws_delisting(d: dict) -> None:
    """Обробка делістинг-події з WebSocket-фіда (основний, швидкий тригер)."""
    now_ms = int(time.time() * 1000)
    disp = d.get("dispatchTimestampUs")
    age = round((now_ms - disp / 1000) / 1000, 2) if disp else None  # транспортна затримка від фіда
    listing_type = d.get("listingType")
    tickers = [t.strip().upper() for t in (d.get("ticker") or "").split(",") if t.strip()]
    log.event("ws_delisting", listing_type=listing_type, ticker=d.get("ticker"),
              title=d.get("title"), tickers=tickers, transport_age_sec=age)
    # Сповіщення про сигнал — у фоні, щоб НЕ затримувати відкриття угоди.
    executor.fire(tg.send_message(
        f"⚡ <b>WS-сигнал: {listing_type}</b>\n"
        f"Токени: {', '.join(tickers) or '—'}\n"
        f"<i>{d.get('title', '')}</i>"
    ))
    # Торгуємо лише повний спот-делістинг (як і раніше).
    if listing_type != "spot_delisting":
        return
    await _fire_tickers(tickers, age, "ws_cryptolisting")


async def _ws_loop() -> None:
    """Основний тригер: слухає WebSocket-фід cryptolisting.ws (push, ~3-4с)."""
    if not config.CL_WS_KEY:
        log.info("WS: CL_WS_KEY не заданий — WebSocket-тригер вимкнено (працює лише поллінг-сторож)")
        return
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(config.CL_WS_URL,
                                        headers={"X-API-Key": config.CL_WS_KEY},
                                        heartbeat=15, timeout=25) as ws:
                    log.event("ws_connected", url=config.CL_WS_URL)
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        try:
                            d = fastjson.loads(msg.data)
                        except Exception:  # noqa: BLE001
                            continue
                        if d.get("type") == "announcement" and \
                                d.get("listingType") in ("spot_delisting", "futures_delisting"):
                            await _handle_ws_delisting(d)
        except Exception:  # noqa: BLE001
            log.exception("WS помилка зʼєднання")
        await asyncio.sleep(5)


async def _keepalive_loop() -> None:
    """Тримає конекти до бірж теплими, щоб бойовий ордер не платив холодний
    TLS-старт (~400мс). Прогрів на старті + пінг кожні KEEPALIVE_SEC."""
    if config.KEEPALIVE_SEC <= 0:
        log.info("keepalive вимкнено (KEEPALIVE_SEC=0)")
        return
    # первинний прогрів (створює клієнтів + TLS-конекти)
    warmed = []
    for v in config.VENUE_PRIORITY:
        ok = await asyncio.to_thread(exchange.warm_ping, v)
        warmed.append(f"{v}:{'ok' if ok else 'fail'}")
    log.event("keepalive_start", venues=warmed, interval_sec=config.KEEPALIVE_SEC)
    while True:
        await asyncio.sleep(config.KEEPALIVE_SEC)
        for v in config.VENUE_PRIORITY:
            try:
                await asyncio.to_thread(exchange.warm_ping, v)
            except Exception:  # noqa: BLE001
                log.exception(f"keepalive помилка {v}")
            # Розводимо біржі в часі: підряд це 6 підписаних викликів, чий ccxt-парсинг
            # тримає GIL і давав сплески лагу лупу до ~50мс (видно в loop_lag_high).
            await asyncio.sleep(1)


def _on_dump(sym: str, drop: float, top: float, price: float, span_ms: int) -> None:
    """Колбек детектора обвалу. Летить на WS-гарячому шляху → все важке у фон (fire).
    Торгуємо лише якщо DUMPWATCH_TRADE=1; інакше це чистий замір + сповіщення."""
    ticker = exchange.ticker_by_raw(sym)
    # Сповіщення — лише за явним DUMPWATCH_ALERT=1. Детект пише в лог завжди.
    if config.DUMPWATCH_ALERT:
        act = "відкриваю шорт" if (config.DUMPWATCH_TRADE and ticker) else "лише сповіщення (тінь)"
        executor.fire(tg.send_message(
            f"📉 <b>ОБВАЛ: {ticker or sym}</b>\n"
            f"−{drop:.1f}% за {span_ms / 1000:.1f}с ({top:g} → {price:g})\n"
            f"<i>{act}</i>"
        ))
    if config.DUMPWATCH_TRADE and ticker:
        executor.fire(executor.open_from_signal(ticker, detect_latency=span_ms / 1000,
                                                source="dumpwatch"))


async def _arm_leverage_loop() -> None:
    """Озброює плече по ВСІХ символах заздалегідь. Перший ордер по «новому» символу
    інакше платить +165мс за set_leverage — а делістинг це завжди новий символ.
    Bybit тримає плече у себе назавжди, тому робимо це один раз і пишемо в БД, щоб
    рестарт не бив API 800 разів. Раз на ARM_REFRESH_SEC — щоб озброїти нові листинги."""
    if not config.ARM_LEVERAGE or not config.BYBIT_API_KEY:
        log.info("arm: пре-озброєння плеча вимкнено (нема ключів або ARM_LEVERAGE=0)")
        return
    await asyncio.sleep(5)  # даємо старту вгамуватися
    while True:
        try:
            done = await asyncio.to_thread(storage.armed_symbols, "bybit", config.LEVERAGE)
            for s in done:
                exchange.mark_leveraged("bybit", s)
            todo = sorted({m["symbol"] for m in exchange.HOT.values()
                           if m["venue"] == "bybit"} - done)
            if todo:
                log.event("arm_start", leverage=config.LEVERAGE, armed=len(done), todo=len(todo))
                ok = 0
                for i, sym in enumerate(todo, 1):
                    while executor.busy():  # сигнал у роботі — не забиваємо конект
                        await asyncio.sleep(0.2)
                    if await asyncio.to_thread(exchange.ensure_leverage, "bybit", sym,
                                               config.LEVERAGE):
                        await asyncio.to_thread(storage.mark_armed, "bybit", sym, config.LEVERAGE)
                        ok += 1
                    if i % 200 == 0:
                        log.event("arm_progress", done=i, total=len(todo), ok=ok)
                    await asyncio.sleep(config.ARM_SLEEP_SEC)
                log.event("arm_done", armed_ok=ok, total=len(todo))
        except Exception:  # noqa: BLE001
            log.exception("arm помилка")
        await asyncio.sleep(config.ARM_REFRESH_SEC)


_HELP = (
    "🎛 <b>Команди</b>\n"
    "/status — стан бота\n"
    "/positions — відкриті позиції\n"
    "/test_short СИМВОЛ — <b>РЕАЛЬНИЙ</b> тест-шорт на ${margin:g} (напр. /test_short DOGE)\n"
    "/close ID — закрити позицію за id\n"
    "/panic — 🛑 закрити ВСІ позиції\n"
    "/help — ця довідка"
)


async def _handle_command(text: str) -> None:
    parts = text.split()
    cmd = parts[0].lower().lstrip("/").split("@")[0]
    log.event("tg_command", cmd=cmd, text=text)

    if cmd in ("help", "start"):
        await tg.send_message(_HELP.format(margin=config.TEST_MARGIN_USDT))

    elif cmd == "status":
        pcs = pricecache.stats()
        dw = dumpwatch.stats()
        hs = executor.hot_state()
        fc = fastcms.stats()
        armed = len(await asyncio.to_thread(storage.armed_symbols, "bybit", config.LEVERAGE))
        mode = "🧪 DRY (авто)" if config.DRY_RUN else "⚠️ РЕАЛ (авто)"
        await tg.send_message(
            "📊 <b>Стан</b>\n"
            f"Авто-режим: {mode}\n"
            f"Тригер: {'⚡ WS' if config.CL_WS_KEY else '🐌 поллінг'}\n"
            f"Власний детектор: {fc['polls']} опитів / {fc['errors']} збоїв, "
            f"{fc['hosts']} хости, нових {fc['new']}, "
            f"торгівля {'✅' if config.FASTCMS_TRADE else '⛔'}\n"
            f"Price-cache: {pcs['symbols']} симв., WS-оновлень {pcs['ws_msgs']}\n"
            f"Гарячих символів: {len(exchange.HOT)} | плече озброєно: {armed}\n"
            f"Луп: {_LOOP} | json: {fastjson.NAME}\n"
            + (f"Обвал-детектор: {dw['tracked']} симв., спрацювань {dw['alerts']}, "
               f"сповіщення {'✅' if config.DUMPWATCH_ALERT else '⛔ тільки в лог'}, "
               f"торгівля {'✅' if config.DUMPWATCH_TRADE else '⛔ тінь'}\n"
               if config.DUMPWATCH else "Обвал-детектор: ⛔ вимкнено\n")
            + f"Відкритих позицій: {hs['open']} (у роботі {hs['reserved']})\n"
            f"Тест-маржа: ${config.TEST_MARGIN_USDT:g} × {config.LEVERAGE:g}x"
        )

    elif cmd == "positions":
        rows = storage.get_open_positions()
        if not rows:
            await tg.send_message("Відкритих позицій немає.")
        else:
            lines = [f"#{p['id']} {p['ticker']} @{p.get('venue')} "
                     f"[{p.get('mode')}] вхід {p['entry_price']}" for p in rows]
            await tg.send_message("<b>Відкриті позиції:</b>\n" + "\n".join(lines))

    elif cmd in ("test_short", "testshort"):
        sym = parts[1].upper() if len(parts) > 1 else "DOGE"
        await tg.send_message(
            f"⏳ Відкриваю <b>РЕАЛЬНИЙ</b> тест-шорт <b>{sym}</b> "
            f"на ${config.TEST_MARGIN_USDT:g} × {config.LEVERAGE:g}x…"
        )
        await executor.open_from_signal(sym, real=True, margin=config.TEST_MARGIN_USDT,
                                       dedup=False, source="test_short")

    elif cmd == "close":
        if len(parts) < 2 or not parts[1].isdigit():
            await tg.send_message("Вкажи id: /close 12")
        else:
            ok = await executor.force_close(int(parts[1]))
            await tg.send_message("✅ закрито" if ok else "❌ не знайдено такої відкритої позиції")

    elif cmd == "panic":
        rows = storage.get_open_positions()
        if not rows:
            await tg.send_message("Немає що закривати.")
        else:
            await tg.send_message(f"🛑 Закриваю ВСІ позиції ({len(rows)})…")
            for p in rows:
                await executor.force_close(p["id"], reason="MANUAL")
    else:
        await tg.send_message("Невідома команда. /help")


async def _command_loop() -> None:
    """Приймання команд Telegram (long-poll). Реагує лише на повідомлення з нашого chat_id."""
    if not (config.TELEGRAM_BOT_TOKEN and config.TELEGRAM_CHAT_ID):
        return
    # прайм: пропускаємо старий backlog, щоб не виконати застарілі команди
    offset = None
    try:
        old = await tg.get_updates(timeout=0)
        if old:
            offset = old[-1]["update_id"] + 1
    except Exception:  # noqa: BLE001
        pass
    log.event("command_loop_start")
    while True:
        try:
            updates = await tg.get_updates(offset=offset, timeout=25)
            for u in updates:
                offset = u["update_id"] + 1
                msg = u.get("message") or u.get("edited_message")
                if not msg:
                    continue
                if str(msg.get("chat", {}).get("id")) != str(config.TELEGRAM_CHAT_ID):
                    continue  # чужий чат — ігноруємо
                text = (msg.get("text") or "").strip()
                if text.startswith("/"):
                    await _handle_command(text)
        except Exception:  # noqa: BLE001
            log.exception("command loop помилка")
            await asyncio.sleep(3)


async def _monitor_loop() -> None:
    """Паралельний цикл: стежить за відкритими позиціями й закриває за стратегією."""
    while True:
        try:
            await executor.monitor_once()
        except Exception:  # noqa: BLE001
            log.exception("monitor помилка")
        await asyncio.sleep(config.EXIT_CHECK_SEC)


async def main() -> None:
    storage.init()
    executor.resync_open()  # люстро відкритих позицій у памʼять (дедуп на гарячому шляху)
    # Пре-обчислення всього, що потрібно для входу: venue+symbol+raw_id+contract_size по
    # кожному тикеру. Тягне ccxt-markets — свідомо на СТАРТІ, а не на сигналі. Біржі
    # вантажимо паралельно: послідовно це 23с, коли бот ще нічого не чує.
    t0 = time.perf_counter()
    loaded = await asyncio.gather(*(asyncio.to_thread(exchange.client, v)
                                    for v in config.VENUE_PRIORITY), return_exceptions=True)
    # return_exceptions тут потрібен (одна мертва біржа не має валити старт), але БЕЗ
    # цього логу він глитав падіння молча. Реальний випадок: ключ Bybit був привʼязаний
    # до старого IP, ccxt робить підписаний виклик уже в load_markets → Bybit не піднявся
    # взагалі, символи забрав MEXC, і єдиним слідом було відсутнє поле bybit= нижче.
    # Бот при цьому «працював» — і симулював би на не тій біржі.
    for venue, res in zip(config.VENUE_PRIORITY, loaded):
        if isinstance(res, BaseException):
            log.event("venue_load_failed", venue=venue,
                      err=f"{type(res).__name__}: {res}"[:250])
    st = exchange.prearm_symbols()  # уже лише памʼять
    log.event("prearm_symbols", load_ms=round((time.perf_counter() - t0) * 1000), **st)
    missing = [v for v in config.VENUE_PRIORITY if not st.get(v)]
    if missing:
        log.event("venues_missing", missing=missing,
                  hint="перевір ключі та IP-привʼязку API-ключа на біржі")
    # Символи, де плече вже виставлено раніше — щоб ордер не бив set_leverage вдруге.
    for s in await asyncio.to_thread(storage.armed_symbols, "bybit", config.LEVERAGE):
        exchange.mark_leveraged("bybit", s)
    dumpwatch.set_handler(_on_dump)
    dumpwatch.install()
    # Дедуп анонсів живе в fastcms і спільний із поллінг-сторожем. Праймимо ДО gather:
    # інакше сторож на першому ж проході вважав би всі 20 наявних статей новими.
    fastcms.set_handler(_on_fastcms)
    log.event("fastcms_primed", seen=fastcms.prime())
    gcinfo = runtime.tune_gc()
    log.event("runtime", loop=_LOOP, json=fastjson.NAME, **gcinfo)
    log.event("startup", dry_run=config.DRY_RUN, venues=config.VENUE_PRIORITY,
              margin=config.POSITION_MARGIN_USDT, leverage=config.LEVERAGE,
              tp=config.TAKE_PROFIT_MARGIN_PCT, sl=config.STOP_LOSS_MARGIN_PCT,
              max_hold_min=config.MAX_HOLD_MINUTES, poll=config.POLL_INTERVAL,
              open_positions=storage.open_positions_count())
    if config.TELEGRAM_CHAT_ID:
        mode = "🧪 DRY-RUN (без реальних ордерів)" if config.DRY_RUN else "⚠️ РЕАЛЬНА ТОРГІВЛЯ"
        trigger = "⚡ WebSocket (швидкий)" if config.CL_WS_KEY else "🐌 лише поллінг"
        open_n = storage.open_positions_count()
        await tg.send_message(
            "🟢 <b>Delisting-бот запущено</b>\n"
            f"Режим: {mode}\n"
            f"Тригер: {trigger} | поллінг-сторож {config.POLL_INTERVAL:g}с\n"
            f"Біржі: {' → '.join(config.VENUE_PRIORITY)}\n"
            + (f"⚠️ <b>НЕ піднялись: {', '.join(missing)}</b> — перевір ключі "
               f"та IP-привʼязку!\n" if missing else "")
            + f"Маржа ${config.POSITION_MARGIN_USDT:g} × {config.LEVERAGE:g}x\n"
            f"Відкритих позицій: {open_n}"
        )
    else:
        print("[!] TELEGRAM_CHAT_ID не заданий — сповіщення підуть у консоль. "
              "Запусти get_chat_id.py, щоб його дізнатися.")
    # WS-тригер, поллінг-сторож, monitor, keep-alive, price-cache, Telegram-команди,
    # пре-озброєння плеча і монітор лагу лупу — паралельно.
    await asyncio.gather(fastcms.run(), _ws_loop(), _watch_loop(), _monitor_loop(),
                         _keepalive_loop(), pricecache.run(), pricecache.ws_run(),
                         _command_loop(), _arm_leverage_loop(),
                         runtime.loop_lag_monitor())


if __name__ == "__main__":
    _LOOP = runtime.install_loop()  # uvloop, якщо є — ДО створення лупу
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nЗупинено.")
