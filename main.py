"""Delisting-бот: watcher делістингів Binance + executor шортів на MEXC.

Два паралельні цикли: _watch_loop (ловить делістинги, відкриває шорти)
і _monitor_loop (стежить за позиціями, закриває за стратегією).
Режим торгівлі керується config.DRY_RUN (симуляція vs реальні ордери).
"""
import asyncio
import datetime as dt
import sys

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
                    stamp = f"{dt.datetime.now():%H:%M:%S}"
                    if first_run:
                        print(f"[{stamp}] (прайм, без сповіщення) {ev.title}")
                        continue
                    print(f"[{stamp}] НОВИЙ ДЕЛІСТИНГ: {ev.tickers} | {ev.title}")
                    await tg.send_message(_fmt_event(ev))

                    # Тільки для повного делістингу токена — відкриваємо шорт(и).
                    if ev.actionable and ev.tickers:
                        for ticker in ev.tickers:
                            try:
                                await executor.open_from_signal(ticker)
                            except Exception as e:  # noqa: BLE001
                                print(f"[{stamp}] помилка executor {ticker}: {e}")
                if first_run:
                    print("[i] Первинні анонси позначені як бачені. Далі — тільки нові.")
                    first_run = False
            except Exception as e:  # noqa: BLE001
                print(f"[{dt.datetime.now():%H:%M:%S}] помилка watcher: {e}")
            await asyncio.sleep(config.POLL_INTERVAL)


async def _monitor_loop() -> None:
    """Паралельний цикл: стежить за відкритими позиціями й закриває за стратегією."""
    while True:
        try:
            await executor.monitor_once()
        except Exception as e:  # noqa: BLE001
            print(f"[{dt.datetime.now():%H:%M:%S}] помилка monitor: {e}")
        await asyncio.sleep(config.EXIT_CHECK_SEC)


async def main() -> None:
    storage.init()
    if config.TELEGRAM_CHAT_ID:
        mode = "🧪 DRY-RUN (без реальних ордерів)" if config.DRY_RUN else "⚠️ РЕАЛЬНА ТОРГІВЛЯ"
        open_n = storage.open_positions_count()
        await tg.send_message(
            "🟢 <b>Delisting-бот запущено</b>\n"
            f"Режим: {mode}\n"
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
