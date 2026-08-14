"""Регресійний тест розбору заголовків. Запуск: python test_watcher.py

Навмисно без pytest і без мережі — це найкрихкіша частина грошового шляху
(класифікація вирішує, чи взагалі відкривати угоду), тому вона має перевірятись
одною командою. Заголовки — реальні, з CMS Binance за 2022-2026.
"""
import sys
import types

# aiohttp потрібен binance_watcher лише для анотацій — для чистих функцій його заглушаємо,
# щоб тест бігав будь-де, у тому числі на машині без venv.
if "aiohttp" not in sys.modules:
    _stub = types.ModuleType("aiohttp")
    _stub.ClientSession = _stub.ClientTimeout = object
    sys.modules["aiohttp"] = _stub

import binance_watcher as bw  # noqa: E402

# (заголовок, категорія, тикери)
CASES = [
    # --- повний спот-делістинг: ЄДИНЕ, що торгуємо ---
    ("Binance Will Delist ACX, HFT, PIVX, PYR, VANRY, VIC on 2026-08-17",
     bw.SPOT_DELIST, ["ACX", "HFT", "PIVX", "PYR", "VANRY", "VIC"]),
    # однолітерний тикер: старий код вимагав >=2 символів і губив D
    ("Binance Will Delist COS, D, HIGH, MBOX on 2026-06-19",
     bw.SPOT_DELIST, ["COS", "D", "HIGH", "MBOX"]),
    ("Binance Will Delist SNM, SRM and YFII on 2023-08-22",
     bw.SPOT_DELIST, ["SNM", "SRM", "YFII"]),
    ("Binance Announced the First Batch of Vote to Delist Results and Will Delist "
     "BADGER, BAL, BETA, CREAM, CTXC, ELF, FIRO, HARD, NULS, PROS, SNT, TROY, UFT, "
     "VIDT on 2025-04-16",
     bw.SPOT_DELIST, ["BADGER", "BAL", "BETA", "CREAM", "CTXC", "ELF", "FIRO", "HARD",
                      "NULS", "PROS", "SNT", "TROY", "UFT", "VIDT"]),

    # --- margin/loan: спот лишається торгуватись, тому НЕ торгуємо ---
    # Бектест: 48 пар, середня зміна ціни -0.24% за хвилину, обвал >=10% у 0 пар.
    ("Binance Margin And Loan Will Delist BTTC & POWR on 2026-08-14",
     bw.MARGIN_DELIST, ["BTTC", "POWR"]),
    # THE — реальний токен, який раніше зникав, бо лежав у стоп-словах
    ("Binance Margin And Loan Will Delist HOT, THE on 2026-07-03",
     bw.MARGIN_DELIST, ["HOT", "THE"]),
    ("Binance Margin Will Delist TUSD from Cross and Isolated Margin - 2024-07-24",
     bw.MARGIN_DELIST, ["TUSD"]),
    ("Binance Will Delist SRM and RAY Margin Pairs and Binance Earn Products",
     bw.MARGIN_DELIST, ["SRM", "RAY"]),
    ("Binance VIP Loan Will Delist TUSD and ALCX from Eligible Collateral Asset List "
     "- 2026-03-30",
     bw.MARGIN_DELIST, ["TUSD", "ALCX"]),

    # --- інші категорії ---
    ("Binance Futures Will Delist USDS-M AERGOUSDT Perpetual Contract (2026-07-24)",
     bw.FUTURES_DELIST, None),
    ("Notice of Removal of Spot Trading Pairs - 2026-08-14", bw.PAIR_REMOVAL, []),
    # не токен — тикерів бути не має, інакше бот шукав би символ-привид
    ("Binance Will Delist All American-Style Daily Options", bw.SPOT_DELIST, []),
]


def main() -> int:
    fails = []
    for title, want_cat, want_tk in CASES:
        got_cat = bw.classify(title)
        if got_cat != want_cat:
            fails.append(f"КАТЕГОРІЯ: {title[:60]!r}\n    чекали {want_cat}, отримали {got_cat}")
        if want_tk is not None:
            got_tk = bw.extract_tickers(title)
            if got_tk != want_tk:
                fails.append(f"ТИКЕРИ: {title[:60]!r}\n    чекали {want_tk}\n    отримали {got_tk}")

    # Торгуємо ВИКЛЮЧНО повний спот-делістинг.
    for cat, actionable in ((bw.SPOT_DELIST, True), (bw.MARGIN_DELIST, False),
                            (bw.FUTURES_DELIST, False), (bw.PAIR_REMOVAL, False),
                            (bw.OTHER, False)):
        ev = bw.DelistingEvent(article_id="x", title="t", category=cat)
        if ev.actionable is not actionable:
            fails.append(f"ACTIONABLE: {cat} має бути {actionable}")

    if fails:
        print(f"❌ провалено {len(fails)}:")
        for f in fails:
            print("  " + f)
        return 1
    print(f"✅ усі {len(CASES)} заголовків + перевірка actionable — ОК")
    return 0


if __name__ == "__main__":
    sys.exit(main())
