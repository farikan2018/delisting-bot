"""Фаза 1: стежимо за делістингами Binance і шлемо сповіщення в Telegram.

Реальних ордерів НЕМАЄ. Це безпечний режим для перевірки якості й швидкості сигналів.
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

                    # Тільки для повного делістингу токена — план шорта.
                    if ev.actionable and ev.tickers:
                        try:
                            plan_text = await asyncio.to_thread(
                                executor.handle_signal_dryrun, ev.tickers
                            )
                            await tg.send_message(plan_text)
                            print(f"[{stamp}] {'DRY-RUN план' if config.DRY_RUN else 'ВИКОНАННЯ'}: {ev.tickers}")
                        except Exception as e:  # noqa: BLE001
                            print(f"[{stamp}] помилка executor: {e}")
                if first_run:
                    print("[i] Первинні анонси позначені як бачені. Далі — тільки нові.")
                    first_run = False
            except Exception as e:  # noqa: BLE001
                print(f"[{dt.datetime.now():%H:%M:%S}] помилка watcher: {e}")
            await asyncio.sleep(config.POLL_INTERVAL)


async def main() -> None:
    if config.TELEGRAM_CHAT_ID:
        mode = "🧪 DRY-RUN (без реальних ордерів)" if config.DRY_RUN else "⚠️ РЕАЛЬНА ТОРГІВЛЯ"
        await tg.send_message(
            "🟢 <b>Delisting-бот запущено</b>\n"
            f"Режим: {mode}\n"
            f"Маржа ${config.POSITION_MARGIN_USDT:g} × {config.LEVERAGE:g}x | poll {config.POLL_INTERVAL:g}с."
        )
    else:
        print("[!] TELEGRAM_CHAT_ID не заданий — сповіщення підуть у консоль. "
              "Запусти get_chat_id.py, щоб його дізнатися.")
    await _watch_loop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nЗупинено.")
