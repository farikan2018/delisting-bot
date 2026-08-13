"""Службова CLI для тестів і ручного керування.

  python admin.py open TICKER [--confirm]   # відкрити позицію (тест повного циклу)
  python admin.py list                       # показати відкриті позиції
  python admin.py close ID                    # закрити позицію ID за поточною ціною

У DRY_RUN=1 усе симулюється. У реальному режимі 'open' вимагає --confirm.
"""
import asyncio
import sys

import config
import exchange
import executor
import storage


async def main() -> None:
    storage.init()
    args = sys.argv[1:]
    cmd = args[0] if args else "list"
    if cmd in ("open", "close"):
        # Гарячий шлях читає пре-обчислену мету символів — в окремому процесі її треба
        # побудувати самому (у бота це робить main.py на старті).
        await asyncio.to_thread(exchange.prearm_symbols)
        executor.resync_open()

    if cmd == "open":
        if len(args) < 2:
            print("usage: admin.py open TICKER [--confirm]")
            return
        ticker = args[1].upper()
        if not config.DRY_RUN and "--confirm" not in args:
            print("⚠️ РЕАЛЬНИЙ режим. Для реального ордера додай --confirm.")
            return
        await executor.open_from_signal(ticker)

    elif cmd == "list":
        rows = storage.get_open_positions()
        if not rows:
            print("Відкритих позицій немає.")
        for p in rows:
            print(f"#{p['id']} {p['ticker']} {p['symbol']} @{p.get('venue')} mode={p['mode']} "
                  f"entry={p['entry_price']} min={p['min_price']} "
                  f"dropped={p['dropped_pct']}")

    elif cmd == "close":
        if len(args) < 2:
            print("usage: admin.py close ID")
            return
        ok = await executor.force_close(int(args[1]))
        print("закрито:" if ok else "не знайдено позицію:", ok)

    else:
        print(__doc__)


if __name__ == "__main__":
    asyncio.run(main())
