"""Delisting-бот: watcher делістингів Binance + executor шортів на MEXC.

Два паралельні цикли: _watch_loop (ловить делістинги, відкриває шорти)
і _monitor_loop (стежить за позиціями, закриває за стратегією).
Режим торгівлі керується config.DRY_RUN (симуляція vs реальні ордери).
"""
import asyncio
import datetime as dt
import json
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
import executor
import logbook as log
import storage
import telegram_client as tg


_CAT_LABEL = {
    bw.SPOT_DELIST: "🔴 ПОВНИЙ ДЕЛІСТИНГ ТОКЕНА (сигнал для шорта)",
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
    lines.append("<i>(Фаза 1: сповіщення, без ордерів)</i>")
    return "\n".join(lines)


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
                    if storage.is_seen(ev.article_id):
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
                            for ticker in ev.tickers:
                                try:
                                    await executor.open_from_signal(ticker, detect_latency=latency)
                                except Exception:  # noqa: BLE001
                                    log.exception(f"executor помилка по {ticker}")
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
    await tg.send_message(
        f"⚡ <b>WS-сигнал: {listing_type}</b>\n"
        f"Токени: {', '.join(tickers) or '—'}\n"
        f"<i>{d.get('title', '')}</i>"
    )
    # Торгуємо лише повний спот-делістинг (як і раніше).
    if listing_type != "spot_delisting":
        return
    for tk in tickers:
        try:
            await executor.open_from_signal(tk, detect_latency=age)
        except Exception:  # noqa: BLE001
            log.exception(f"WS executor помилка по {tk}")


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
                            d = json.loads(msg.data)
                        except Exception:  # noqa: BLE001
                            continue
                        if d.get("type") == "announcement" and \
                                d.get("listingType") in ("spot_delisting", "futures_delisting"):
                            await _handle_ws_delisting(d)
        except Exception:  # noqa: BLE001
            log.exception("WS помилка зʼєднання")
        await asyncio.sleep(5)


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
            f"Маржа ${config.POSITION_MARGIN_USDT:g} × {config.LEVERAGE:g}x\n"
            f"Відкритих позицій: {open_n}"
        )
    else:
        print("[!] TELEGRAM_CHAT_ID не заданий — сповіщення підуть у консоль. "
              "Запусти get_chat_id.py, щоб його дізнатися.")
    # WS-тригер (основний), поллінг-сторож і monitor працюють паралельно
    await asyncio.gather(_ws_loop(), _watch_loop(), _monitor_loop())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nЗупинено.")
