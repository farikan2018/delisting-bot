"""Ручна перевірка dry-run: рахує план шорта для заданих тикерів і шле в Telegram.

Приклад на сервері:
    ./.venv/bin/python dryrun_test.py DOGE GALA 1000SATS
"""
import asyncio
import sys

import executor
import telegram_client as tg


async def main() -> None:
    tickers = [t.upper() for t in sys.argv[1:]] or ["DOGE"]
    text = "🔧 <b>Ручний тест dry-run</b>\n" + executor.handle_signal_dryrun(tickers)
    print(text)
    ok = await tg.send_message(text)
    print(f"\nнадіслано в Telegram: {ok}")


if __name__ == "__main__":
    asyncio.run(main())
