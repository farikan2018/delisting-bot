"""Замір API-плеча затримки на Bybit БЕЗ ризику для коштів.

Ставить лімітний шорт сильно ВИЩЕ ринку (не виконається ніколи) → засікає час
create→ack матчинг-двигуна → одразу скасовує. Позиція не відкривається, гроші
не рухаються. Проганяє N разів, друкує медіану/мін/макс.

Запуск на сервері (де ключ IP-прив'язаний):
    python3 measure_bybit_latency.py [SYMBOL] [N]
Приклад:
    python3 measure_bybit_latency.py BTC/USDT:USDT 10
"""
import statistics
import sys
import time

import ccxt

import config

SYMBOL = sys.argv[1] if len(sys.argv) > 1 else "BTC/USDT:USDT"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 10


def _client():
    ex = ccxt.bybit({
        "apiKey": config.BYBIT_API_KEY,
        "secret": config.BYBIT_API_SECRET,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })
    return ex


def main():
    if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
        print("НЕМА BYBIT_API_KEY/SECRET у .env"); return
    ex = _client()

    # разова синхронізація часу + ринки (не входить у замір)
    t0 = time.perf_counter()
    ex.load_markets()
    print(f"load_markets: {(time.perf_counter()-t0)*1000:.0f} ms")

    m = ex.market(SYMBOL)
    last = ex.fetch_ticker(SYMBOL)["last"]
    # ціна лімітки — сильно ВИЩЕ ринку (+50%), щоб шорт-лімітка НЕ виконалась
    price = ex.price_to_precision(SYMBOL, last * 1.5)
    # мінімальний обсяг контракту
    amount = m.get("limits", {}).get("amount", {}).get("min") or 1
    amount = ex.amount_to_precision(SYMBOL, amount)
    print(f"symbol={SYMBOL} last={last} limit_price={price} (не виконається) amount={amount}\n")

    place_ms, cancel_ms = [], []
    for i in range(1, N + 1):
        try:
            t = time.perf_counter()
            order = ex.create_order(SYMBOL, "limit", "sell", amount, price,
                                    {"reduceOnly": False, "timeInForce": "GTC"})
            dt_place = (time.perf_counter() - t) * 1000
            oid = order["id"]

            t = time.perf_counter()
            ex.cancel_order(oid, SYMBOL)
            dt_cancel = (time.perf_counter() - t) * 1000

            place_ms.append(dt_place)
            cancel_ms.append(dt_cancel)
            print(f"[{i:2}] place={dt_place:6.0f} ms  cancel={dt_cancel:6.0f} ms  id={oid}")
        except Exception as e:  # noqa: BLE001
            print(f"[{i:2}] ПОМИЛКА: {type(e).__name__}: {str(e)[:160]}")
        time.sleep(0.3)  # гентельно до rate-limit

    if place_ms:
        print("\n=== ПОСТАНОВКА ОРДЕРА (create→ack) ===")
        print(f"медіана {statistics.median(place_ms):.0f} ms | "
              f"мін {min(place_ms):.0f} | макс {max(place_ms):.0f} | n={len(place_ms)}")
        print("=== СКАСУВАННЯ ===")
        print(f"медіана {statistics.median(cancel_ms):.0f} ms | "
              f"мін {min(cancel_ms):.0f} | макс {max(cancel_ms):.0f}")
        print("\nАPI-плече затримки бота ≈ медіана постановки ордера вище.")
    else:
        print("\nЖодного успішного заміру — дивись помилки вище.")


if __name__ == "__main__":
    main()
