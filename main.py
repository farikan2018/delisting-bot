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

                    # Тільки для повного делістингу токена — відкриваємо шорт(и).
                    if ev.actionable and ev.tickers:
                        for ticker in ev.tickers:
                            try:
                                await executor.open_from_signal(ticker, detect_latency=latency)
                            except Exception:  # noqa: BLE001
                                log.exception(f"executor помилка по {ticker}")
                if first_run:
                    log.info("Первинні анонси позначені як бачені. Далі — тільки нові.")
                    first_run = False
            except Exception:  # noqa: BLE001
                log.exception("watcher помилка")
            await asyncio.sleep(config.POLL_INTERVAL)


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
        open_n = storage.open_positions_count()
        await tg.send_message(
            "🟢 <b>Delisting-бот запущено</b>\n"
            f"Режим: {mode}\n"
            f"Біржі: {' → '.join(config.VENUE_PRIORITY)}\n"
            f"Маржа ${config.POSITION_MARGIN_USDT:g} × {config.LEVERAGE:g}x | poll {config.POLL_INTERVAL:g}с\n"
            f"Відкритих позицій: {open_n}"
        )
    else:
        print("[!] TELEGRAM_CHAT_ID не заданий — сповіщення підуть у консоль. "
              "Запусти get_chat_id.py, щоб його дізнатися.")
    # watcher і monitor працюють паралельно
    await asyncio.gather(_watch_loop(), _monitor_loop())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nЗупинено.")
