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

import alerts
import binance_watcher as bw
import config
import dumpwatch
import exchange
import executor
import fastcms
import fastjson
import logbook as log
import preflight
import pricecache
import runtime
import storage
import telegram_client as tg
import tgfeed
import watchdog
import wsfeed


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
        except Exception as e:  # noqa: BLE001
            # Тихо втратити сигнал тут не можна. Весь шлях від ціни до ордера не
            # захищений власним except, тож будь-який мережевий збій (fetch_ticker
            # без try/except, таймаут, 429) прилітає саме сюди — і раніше йшов
            # ЛИШЕ в лог-файл. Користувач при цьому бачив сповіщення «детектор
            # зловив делістинг» і робив висновок, що угода відкрита. Делістинг
            # буває раз на 3-6 тижнів: мовчазна втрата = втрачений місяць.
            log.exception(f"executor помилка по {tk}")
            log.event("entry_failed", ticker=tk, source=source,
                      err=f"{type(e).__name__}: {e}"[:200], detect_latency_sec=latency)
            executor.fire(tg.send_message(
                "🚨 <b>" + tk + "</b>: вхід ЗІРВАВСЯ — угоди НЕМА." + chr(10)
                + "<code>" + f"{type(e).__name__}: {e}"[:160] + "</code>" + chr(10)
                + "Джерело: " + source))
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
    # Вік сигналу НЕВІДОМИЙ (у статті нема releaseDate) — на шляху ПОЛЛІНГА це
    # привід не торгувати, а не привід торгувати. Заміряна медіана детекту
    # поллінгом 46с при порозі 60с: сигнал невідомого віку з цього джерела майже
    # напевно вже мертвий. Раніше latency=None повністю обходив ворота — тобто
    # найменш надійний випадок проходив найлегше.
    # Для пуш-джерел (WS, телеграм-фід) логіка протилежна і свідомо інша: там
    # відсутність мітки означає лише зміну схеми, а сам пуш за побудовою свіжий.
    if latency is None:
        log.event("fastcms_no_release_no_trade", tickers=ev.tickers,
                  article_id=ev.article_id, title=ev.title[:120])
        executor.fire(tg.send_message(
            "⏱️ <b>" + ", ".join(ev.tickers) + "</b>: у статті нема дати публікації, "
            "вік сигналу невідомий — поллінгом НЕ торгую."))
        return
    if latency > config.MAX_SIGNAL_AGE_SEC:
        log.event("fastcms_stale_no_trade", tickers=ev.tickers, latency_sec=latency)
        return
    await _fire_tickers(ev.tickers, latency, f"fastcms:{host.split('.')[0]}")


async def _on_tg_feed(tickers: list, age, source: str) -> None:
    """Сигнал із @CLWfeed. Веде в ТОЙ САМИЙ _fire_tickers, що й fastcms:
    дедуплікація вже є в executor через заявку на тикер (CLAIM_TTL_SEC), тож
    повторне спрацювання того ж делістингу з поллінга через ~11с буде
    відкинуте — і саме воно дасть нам парний замір затримки."""
    executor.fire(tg.send_message(
        "📡 <b>Телеграм-фід</b> (вік ~" + str(age) + "с)" + chr(10)
        + "Тикери: " + ", ".join(tickers)))
    await _fire_tickers(tickers, age, source)


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
                    # Сповіщення — у ФОН. Раніше тут стояв await: ордер чекав на
                    # відповідь api.telegram.org (сотні мс, інколи секунди) ПЕРЕД
                    # власним відкриттям. На кривій входу це чистий збиток, і саме
                    # цей шлях працює тоді, коли швидкий тригер мертвий — тобто
                    # зараз. Швидкий шлях (_on_fastcms) так робив уже давно.
                    executor.fire(tg.send_message(_fmt_event(ev)))

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


async def _on_ws_feed(tickers: list, age, source: str) -> None:
    """Сигнал із WebSocket-фіда. Веде в ТОЙ САМИЙ _fire_tickers, що й решта джерел:
    дедуплікація живе в executor через заявку на тикер, тож повторне спрацювання
    того ж делістингу з поллінга через десятки секунд буде відкинуте — і саме воно
    дасть нам парний замір затримки."""
    await _fire_tickers(tickers, age, source)


def _ws_notify(text: str) -> None:
    """Сповіщення про БУДЬ-ЯКИЙ анонс із фіда — у фон, щоб не затримувати ордер."""
    executor.fire(tg.send_message(text))


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


def _days_left() -> int | None:
    """Скільки днів до кінця безкоштовного періоду машини."""
    if not config.TRIAL_END_DATE:
        return None
    try:
        end = dt.datetime.strptime(config.TRIAL_END_DATE, "%Y-%m-%d").date()
    except ValueError:
        return None
    return (end - dt.datetime.now(dt.timezone.utc).date()).days


async def _daily_text() -> str:
    fc = fastcms.stats()
    left = _days_left()
    lines = ["📅 <b>Щоденне зведення</b>"]
    if left is None:
        lines.append("Безкоштовний період: дата не задана (<code>TRIAL_END_DATE</code>)")
    elif left > 0:
        lines.append(f"🗓 Безкоштовний сервер: залишилось <b>{left} дн.</b> "
                     f"(до {config.TRIAL_END_DATE})")
    elif left == 0:
        lines.append(f"🔴 <b>Безкоштовний період закінчується СЬОГОДНІ</b> "
                     f"({config.TRIAL_END_DATE})")
    else:
        lines.append(f"🔴 <b>Безкоштовний період скінчився {-left} дн. тому</b> — "
                     f"машина вже або платна, або зупинена")
    lines.append(f"⚡ Детект: {fc['polls']} опитів, помилок {fc['errors']}, "
                 f"нових анонсів {fc['new']}")
    lines.append(f"📊 Позицій відкрито: {storage.open_positions_count()} | "
                 f"анонсів бачено: {fc['seen']}")
    # ЗДОРОВ'Я — у щоденне зведення (2026-10-03). Це єдина регулярна поверхня,
    # яка реально доставлялась користувачу всі 34 доби, поки швидкий тригер був
    # мертвий. Вона звітувала про поллінг і дні тріалу — і жодним словом про те,
    # що tg_feed не отримав ЖОДНОГО повідомлення, а WS — жодного кадру.
    # Тепер стан тригерів і готовність до делістингу тут є завжди.
    lines.append("")
    lines.append(_trigger_line())
    s = watchdog.summary()
    lines.append("✅ Готовий до делістингу" if s["ok"]
                 else "❌ <b>НЕ готовий: " + ", ".join(s["failing"]) + "</b>")
    if not s["ok"]:
        lines.append(watchdog.report_text())
    lines.append(preflight.report_text())
    ts, lb = tg.stats(), log.stats()
    if ts["failed"] or lb["errors"]:
        lines.append("⚠️ Збоїв Telegram: " + str(ts["failed"])
                     + " | помилок у логу: " + str(lb["errors"])
                     + (" | остання: " + str(lb["last_error_where"])[:80]
                        if lb["last_error_where"] else ""))
    act = alerts.active_keys()
    if act:
        lines.append("🚨 Активні аварії: " + ", ".join(act))
    if left is not None and left <= 7:
        # Нагадування саме тоді, коли ще є час діяти, а не після факту.
        lines.append("\n⚠️ <b>Час вирішувати, куди переїжджати:</b>\n"
                     "• Lightsail $7–12/міс у <code>ap-southeast-1</code> — дешевше "
                     "за GCP і ще ближче до матчера Bybit\n"
                     "• лишитись у GCP — ~$18/міс\n"
                     "• назад у Франкфурт — безкоштовно, але ордер ~660мс замість ~200мс")
    return "\n".join(lines)


async def _daily_report_loop() -> None:
    """ОДНЕ повідомлення на добу. Дата останньої відправки — у БД, щоб рестарт
    (а їх буває багато) не сипав зведенням повторно."""
    if not (config.DAILY_REPORT and config.TELEGRAM_BOT_TOKEN and config.TELEGRAM_CHAT_ID):
        log.info("щоденне зведення вимкнено (DAILY_REPORT=0 або немає Telegram)")
        return
    hour = max(0, min(23, config.DAILY_REPORT_HOUR_UTC))
    log.event("daily_report_armed", hour_utc=hour, trial_end=config.TRIAL_END_DATE or None)
    while True:
        now = dt.datetime.now(dt.timezone.utc)
        target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if target <= now:
            target += dt.timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        today = dt.datetime.now(dt.timezone.utc).date().isoformat()
        try:
            if await asyncio.to_thread(storage.meta_get, "daily_report_sent") == today:
                continue
            await tg.send_message(await _daily_text())
            await asyncio.to_thread(storage.meta_set, "daily_report_sent", today)
            log.event("daily_report_sent", date=today, days_left=_days_left())
        except Exception:  # noqa: BLE001
            log.exception("щоденне зведення не надіслалось")


_HELP = (
    "🎛 <b>Команди</b>\n"
    "/status — стан бота\n"
    "/positions — відкриті позиції\n"
    "/test_short СИМВОЛ — <b>РЕАЛЬНИЙ</b> тест-шорт на ${margin:g} (напр. /test_short DOGE)\n"
    "/health — чи здатен бот відпрацювати делістинг ПРЯМО ЗАРАЗ\n"
    "/preflight — прогнати самоперевірку бойового шляху негайно\n"
    "/daily — щоденне зведення зараз (скільки лишилось безкоштовного сервера)\n"
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
            + _trigger_line() + "\n"
            + f"Власний детектор: {fc['polls']} опитів / {fc['errors']} збоїв, "
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

    elif cmd == "health":
        # /status показує КОНФІГ і лічильники. Це різні питання, і плутати їх
        # дорого: 34 доби /status чесно писав про WS, поки кадрів було нуль.
        hc = watchdog.summary()
        head = ("✅ <b>Готовий до делістингу</b>" if hc["ok"]
                else "❌ <b>НЕ готовий: " + ", ".join(hc["failing"]) + "</b>")
        await tg.send_message(head + chr(10) + watchdog.report_text()
                              + chr(10) + chr(10) + preflight.report_text())

    elif cmd == "preflight":
        await tg.send_message("⏳ Ганяю бойовий шлях без ордера...")
        await preflight.run_once(alert_on_fail=False)
        await tg.send_message(preflight.report_text())

    elif cmd == "daily":
        await tg.send_message(await _daily_text())

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
            # Кожна позиція окремо: /panic — це аварійна кнопка, і одна помилка
            # НЕ має лишати решту позицій відкритими без жодного повідомлення.
            ok, bad = 0, []
            for p in rows:
                try:
                    await executor.force_close(p["id"], reason="MANUAL")
                    ok += 1
                except Exception:  # noqa: BLE001
                    log.exception(f"panic: не закрилась #{p['id']}")
                    bad.append(f"#{p['id']} {p['ticker']}")
            await tg.send_message(
                f"🛑 Закрито {ok} з {len(rows)}."
                + (f"\n⚠️ НЕ закрились: {', '.join(bad)} — перевір вручну на біржі!"
                   if bad else ""))
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


def _capabilities() -> dict:
    """Прапорці КОЖНОЇ можливості — у структуровану подію startup.

    Причина існування: за тиждень ЧОТИРИ рази можливість тихо вимикалась через
    відсутню змінну оточення (CL_WS_KEY, TG_*, TELEGRAM_BOT_TOKEN), і знімок
    здоровʼя цього не показував. Бот шість днів торгував без швидкого тригера —
    детект 15с замість 4с, тобто впʼятеро гірший результат на угоду — і жоден
    лог не кричав. Тепер стан кожної можливості видно одним grep.
    """
    return {
        "cap_fastcms": config.FASTCMS,
        "cap_fastcms_trade": config.FASTCMS_TRADE,
        "cap_ws": bool(config.CL_WS_KEY),
        "cap_tg_feed": bool(config.TG_API_ID and config.TG_API_HASH
                            and config.TG_SESSION),
        "cap_tg_notify": bool(config.TELEGRAM_BOT_TOKEN and config.TELEGRAM_CHAT_ID),
        "cap_bybit_keys": bool(config.BYBIT_API_KEY and config.BYBIT_API_SECRET),
        "cap_pricecache_ws": config.PRICECACHE_WS,
        "cap_dumpwatch": config.DUMPWATCH,
        "cap_daily_report": config.DAILY_REPORT,
        "cap_balance_guard": config.BALANCE_GUARD,
        "cap_exchange_stop": config.EXCHANGE_STOP,
    }


def _fast_triggers() -> list:
    """Швидкі тригери, у яких є ДОКАЗ роботи. Поллінг сюди НЕ входить: він дає
    медіану 46с, а на заміряній кривій це +5% на угоду проти +26% при вході за 1-2с.

    ЧОМУ САМЕ ДОКАЗ, А НЕ КОНФІГ (2026-10-03). Раніше цей список будувався з
    наявності змінних оточення. Наслідок: телеграм-фід помер 2026-08-30 і лежав
    34 доби, а список усі ці 34 доби чесно писав "tg_feed" — бо креди ж на місці.
    Рівно так само "ws" стояв у списку при нулі отриманих кадрів. Тобто єдиний
    сигнал, який мав попередити про втрату швидкого детекту, сам і брехав.
    """
    out = []
    if wsfeed.healthy():
        out.append("ws")
    if tgfeed.healthy():
        out.append("tg_feed")
    return out


def _trigger_line() -> str:
    """Один рядок про стан швидкого детекту: для /status і добового зведення.

    Показує ДВА набори навмисно: що налаштовано і що реально дало дані. Саме
    розбіжність між ними і була аварією, якої ніхто не бачив 34 доби.
    """
    live, conf = _fast_triggers(), _configured_triggers()
    ws, tgs = wsfeed.stats(), tgfeed.stats()
    if not conf:
        return "Тригер: 🐌 лише поллінг (медіана 46с), швидкого НЕМА"
    icon = "⚡" if live else "🐌"
    body = (icon + " Тригер: налаштовано [" + ", ".join(conf) + "], "
            + ("живі [" + ", ".join(live) + "]" if live else "ЖИВИХ НЕМА"))
    body += (chr(10) + "   WS: кадрів " + str(ws["frames"]) + ", останній "
             + (str(ws["last_frame_age_sec"]) + "с тому"
                if ws["last_frame_age_sec"] is not None else "ніколи")
             + ", підключень " + str(ws["connects"]))
    body += (chr(10) + "   TG-фід: повідомлень " + str(tgs["msgs"])
             + (", СЕСІЮ ВІДКЛИКАНО (" + str(tgs["fatal"]) + ")" if tgs["fatal"] else ""))
    return body


def _configured_triggers() -> list:
    """Тригери, яким ЗАДАНО конфіг — незалежно від того, чи вони працюють.
    Різниця з _fast_triggers() і є тією самою тихою аварією."""
    out = []
    if config.CL_WS_KEY:
        out.append("ws")
    if config.TG_API_ID and config.TG_API_HASH and config.TG_SESSION:
        out.append("tg_feed")
    return out


async def _supervise(factory, name: str, delay: float = 3.0,
                     optional: bool = False) -> None:
    """Тримає цикл живим. asyncio.gather без return_exceptions пробиває перший же
    виняток нагору й кладе процес — а це може статись із відкритою реальною
    позицією. Тут впалий цикл просто перезапускається, а факт падіння йде в лог."""
    while True:
        try:
            await factory()
            if optional:
                log.event("loop_exited", loop=name, optional=True)
            else:
                # Штатний вихід НЕ-опційного циклу — це втрачена можливість,
                # а не дрібниця. Рівно так зник WS-тригер: один рядок INFO,
                # подія без алерту, і шість днів торгівлі на поллінгу.
                log.event("loop_exited", loop=name, optional=False, alert=True)
                log.error("ЦИКЛ " + name + " ЗАВЕРШИВСЯ — можливість втрачено")
                try:
                    await tg.send_message(
                        "⚠️ Цикл <b>" + name + "</b> завершився штатно. "
                        "Це втрата можливості — перевір конфіг.")
                except Exception:  # noqa: BLE001
                    pass
            return
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception(f"цикл {name} впав — перезапускаю через {delay:g}с")
            await asyncio.sleep(delay)


async def _markets_refresh_loop() -> None:
    """Періодично перетягує ринки бірж і перебудовує карту символів.

    Делістингу підлягають і нові токени: лістинг у березні, делістинг у жовтні —
    звичайна історія. Без оновлення такий токен для бота не існує, і подія
    завершується рядком `skip no_perp` у лозі, якого ніхто не читає. Робимо це
    рідко (раз на MARKETS_REFRESH_SEC), у потоці, і НІКОЛИ поки є сигнал у роботі:
    load_markets тримає GIL на тисячах ринків, а гарячий шлях цього не пробачає.
    """
    if config.MARKETS_REFRESH_SEC <= 0:
        log.info("оновлення ринків вимкнено (MARKETS_REFRESH_SEC=0)")
        return
    while True:
        await asyncio.sleep(config.MARKETS_REFRESH_SEC)
        while executor.busy():
            await asyncio.sleep(0.5)
        try:
            st = await asyncio.to_thread(exchange.reload_markets)
            log.event("markets_refreshed", **st)
            if st.get("added"):
                # Нові перпи треба ще й озброїти плечем, інакше перший ордер по
                # такому символу заплатить зайвий мережевий виклик у найгірший
                # момент. Фоновий арм підхопить їх на наступному колі сам.
                log.event("markets_new_symbols", added=st["added"])
        except Exception:  # noqa: BLE001
            log.exception("оновлення ринків впало")


async def _reconcile_loop() -> None:
    """Звірка з біржею: біржовий SL/TP може закрити позицію без нас, і тоді запис
    у БД треба закрити самим — інакше слот MAX_CONCURRENT зайнятий назавжди."""
    if config.DRY_RUN and not config.EXCHANGE_STOP:
        return
    while True:
        await asyncio.sleep(config.RECONCILE_SEC)
        try:
            await executor.reconcile_real()
        except Exception:  # noqa: BLE001
            log.exception("reconcile помилка")


async def main() -> None:
    storage.init()
    executor.resync_open()  # люстро відкритих позицій у памʼять (дедуп на гарячому шляху)
    executor.init_daily()   # денний лічильник збитку — щоб рестарт не обнуляв ліміт
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
    tgfeed.set_handler(_on_tg_feed)
    wsfeed.set_handler(_on_ws_feed)
    wsfeed.set_notifier(_ws_notify)
    log.event("fastcms_primed", seen=fastcms.prime())
    gcinfo = runtime.tune_gc()
    log.event("runtime", loop=_LOOP, json=fastjson.NAME, **gcinfo)
    log.event("startup", dry_run=config.DRY_RUN, venues=config.VENUE_PRIORITY,
              margin=config.POSITION_MARGIN_USDT, leverage=config.LEVERAGE,
              tp=config.TAKE_PROFIT_MARGIN_PCT, sl=config.STOP_LOSS_MARGIN_PCT,
              max_hold_min=config.MAX_HOLD_MINUTES, poll=config.POLL_INTERVAL,
              max_concurrent=config.MAX_CONCURRENT, exchange_stop=config.EXCHANGE_STOP,
              daily_loss_limit=config.MAX_DAILY_LOSS_USDT,
              daily_pnl=round(executor.daily_pnl(), 4),
              open_positions=storage.open_positions_count(),
              fast_triggers=_fast_triggers(),
              configured_triggers=_configured_triggers(),
              tg_auth_ok=await tg.verify(), **_capabilities())
    # На СТАРТІ живість ще не доведена нічим (кадр не прийшов, повідомлення не
    # прийшло) — тому тут питаємо про КОНФІГ. Живість візьме на себе watchdog,
    # який дасть першу оцінку через WATCHDOG_GRACE_SEC і буде кричати, поки не
    # полагодять. Якби тут стояв _fast_triggers(), кожен рестарт слав би фальшиву
    # тривогу, а фальшива тривога швидко вчить ігнорувати справжню.
    if not _configured_triggers():
        log.event("degraded_detection", fast_triggers=[],
                  falls_back_to="fastcms_polling",
                  measured_cost="детект медіана 46с замість ~4.5с: "
                                "+5% замість +26% на угоду")
        log.error("УВАГА: швидкого тригера НЕМА — лише поллінг (медіана 46с). "
                  "Перевір CL_WS_KEY або TG_API_ID+TG_API_HASH+TG_SESSION.")
    if config.TELEGRAM_CHAT_ID:
        mode = "🧪 DRY-RUN (без реальних ордерів)" if config.DRY_RUN else "⚠️ РЕАЛЬНА ТОРГІВЛЯ"
        conf = _configured_triggers()
        # Свідомо кажемо «налаштовано», а не «працює»: на старті доказу роботи ще
        # нема. Рівно це формулювання 34 доби вводило в оману — писало «⚡ WS»,
        # хоча кадрів було нуль.
        trigger = ("⚡ налаштовано: " + ", ".join(conf) + " (живість перевірить сторож)"
                   if conf else "🐌 лише поллінг — швидкого тригера НЕМА")
        open_n = storage.open_positions_count()
        await tg.send_message(
            "🟢 <b>Delisting-бот запущено</b>\n"
            f"Режим: {mode}\n"
            f"Тригер: {trigger} | поллінг-сторож {config.POLL_INTERVAL:g}с\n"
            f"Біржі: {' → '.join(config.VENUE_PRIORITY)}\n"
            + (f"⚠️ <b>НЕ піднялись: {', '.join(missing)}</b> — перевір ключі "
               f"та IP-привʼязку!\n" if missing else "")
            + f"Маржа ${config.POSITION_MARGIN_USDT:g} × {config.LEVERAGE:g}x "
            f"(до {config.MAX_CONCURRENT} позицій)\n"
            f"Вихід: TP +{config.TAKE_PROFIT_MARGIN_PCT:g}% / SL −{config.STOP_LOSS_MARGIN_PCT:g}% "
            f"маржі, макс {config.MAX_HOLD_MINUTES:g} хв\n"
            + ("🛡 Стоп дублюється на біржі\n" if config.EXCHANGE_STOP and not config.DRY_RUN
               else "")
            + (f"🛑 Денний ліміт збитку: ${config.MAX_DAILY_LOSS_USDT:g}\n"
               if config.MAX_DAILY_LOSS_USDT > 0 else "")
            + f"Відкритих позицій: {open_n}"
        )
    else:
        print("[!] TELEGRAM_CHAT_ID не заданий — сповіщення підуть у консоль. "
              "Запусти get_chat_id.py, щоб його дізнатися.")
    # WS-тригер, поллінг-сторож, monitor, keep-alive, price-cache, Telegram-команди,
    # пре-озброєння плеча і монітор лагу лупу — паралельно.
    # Кожен цикл під наглядом: без цього перший же виняток у будь-якому з них
    # пробивав би gather і клав ВЕСЬ процес — можливо, з відкритою реальною
    # позицією. Перезапускати впалий цикл безпечніше, ніж падати цілком.
    await asyncio.gather(
        _supervise(fastcms.run, "fastcms"),
        _supervise(tgfeed.run, "tg_feed", optional=True),
        _supervise(wsfeed.run, "ws", optional=True),
        _supervise(_watch_loop, "watch"),
        _supervise(_monitor_loop, "monitor"),
        _supervise(_keepalive_loop, "keepalive"),
        _supervise(pricecache.run, "pricecache"),
        _supervise(pricecache.ws_run, "pricecache_ws"),
        _supervise(_command_loop, "command"),
        _supervise(_arm_leverage_loop, "arm_leverage", optional=True),
        _supervise(_reconcile_loop, "reconcile"),
        _supervise(_daily_report_loop, "daily_report"),
        _supervise(runtime.loop_lag_monitor, "loop_lag"),
        # Сторож живості і самоперевірка бойового шляху. Делістинг буває раз на
        # 3-6 тижнів, тому між подіями ніщо інше не виконує гарячий шлях і не
        # перевіряє, чи ми взагалі здатні відпрацювати. Див. watchdog.py/preflight.py.
        _supervise(watchdog.run, "watchdog"),
        _supervise(preflight.run, "preflight", optional=True),
        _supervise(_markets_refresh_loop, "markets_refresh", optional=True),
        # Реальні позиції могли лишитись від попереднього запуску. Раніше це
        # чекали ПЕРЕД gather — а якщо Bybit гальмує (часта причина рестарту),
        # бот стояв глухий десятки секунд, не чуючи анонсів.
        # Під наглядом, як і решта. Раніше це був ЄДИНИЙ член gather без
        # _supervise — і виняток тут кладе процес рівно тоді, коли є
        # незакриті реальні позиції, бо саме для них ця функція й існує.
        # optional=True: штатне завершення тут нормальне (одноразова дія).
        _supervise(executor.rearm_open_stops, "rearm", optional=True),
    )


if __name__ == "__main__":
    _LOOP = runtime.install_loop()  # uvloop, якщо є — ДО створення лупу
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nЗупинено.")
