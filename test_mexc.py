"""Перевірка з'єднання з MEXC (Фаза 2, крок 1) — READ-ONLY.

Нічого не купує і не продає. Лише:
  1) завантажує ринки ф'ючерсів,
  2) читає публічну ціну,
  3) читає баланс ф'ючерсів (перевірка, що ключ авторизується).

Запуск на сервері:  ./.venv/bin/python test_mexc.py
"""
import sys

try:
    import ccxt
except ImportError:
    print("ccxt не встановлено. Виконай: ./.venv/bin/pip install -r requirements.txt")
    sys.exit(1)

import config

SAMPLE = "BTC/USDT:USDT"  # безпечний приклад для перевірки


def make_client():
    return ccxt.mexc(
        {
            "apiKey": config.MEXC_API_KEY,
            "secret": config.MEXC_API_SECRET,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},  # ф'ючерси
        }
    )


def main() -> None:
    if not config.MEXC_API_KEY or not config.MEXC_API_SECRET:
        print("!! MEXC_API_KEY / MEXC_API_SECRET не задані в .env")
        return

    ex = make_client()
    print(f"ccxt {ccxt.__version__} | біржа: mexc | режим: swap (ф'ючерси)\n")

    # 1) Ринки
    print("== 1) Завантаження ринків ==")
    try:
        markets = ex.load_markets()
        swaps = [m for m in markets.values() if m.get("swap")]
        print(f"   OK. усього ринків: {len(markets)}, з них ф'ючерсних (swap): {len(swaps)}")
        print(f"   приклад '{SAMPLE}' доступний: {SAMPLE in markets}")
    except Exception as e:  # noqa: BLE001
        print(f"   ПОМИЛКА: {type(e).__name__}: {e}")
        return

    # 2) Публічна ціна
    print("== 2) Публічна ціна (тікер) ==")
    try:
        t = ex.fetch_ticker(SAMPLE)
        print(f"   OK. {SAMPLE} last = {t.get('last')}")
    except Exception as e:  # noqa: BLE001
        print(f"   ПОМИЛКА: {type(e).__name__}: {e}")

    # 3) Баланс ф'ючерсів — головна перевірка авторизації ключа
    print("== 3) Баланс ф'ючерсів (авторизація ключа) ==")
    try:
        bal = ex.fetch_balance()
        usdt = bal.get("USDT", {}) or {}
        print(f"   ✅ АВТОРИЗАЦІЯ ОК. USDT: free={usdt.get('free')} total={usdt.get('total')}")
        print("   => ключ робочий, ф'ючерсний акаунт читається.")
    except ccxt.AuthenticationError as e:
        print(f"   ❌ AuthenticationError: {e}")
        print("   => неправильний ключ/секрет, або немає прав на ф'ючерси, або потрібна прив'язка IP.")
        return
    except ccxt.PermissionDenied as e:
        print(f"   ❌ PermissionDenied: {e}")
        print("   => ключу бракує потрібних прав (перевір галочки ф'ючерсів).")
        return
    except Exception as e:  # noqa: BLE001
        print(f"   ❌ {type(e).__name__}: {e}")
        return

    # 4) Проба розміщення ордера — ТІЛЬКИ з прапорцем --place
    if "--place" not in sys.argv:
        print("\n(Проба ордера пропущена. Щоб перевірити розміщення — запусти з --place)")
        return
    probe_order(ex, t)


def probe_order(ex, ticker) -> None:
    """Безпечна проба: лімітний buy на 40% нижче ринку (не виконається) + скасування."""
    print("\n== 4) ПРОБА РОЗМІЩЕННЯ ОРДЕРА (buy -40% від ринку, потім скасування) ==")
    market = ex.market(SAMPLE)
    last = ticker.get("last")
    price = float(ex.price_to_precision(SAMPLE, last * 0.6))  # 40% нижче — не заповниться
    min_amt = (market.get("limits", {}).get("amount", {}) or {}).get("min") or 1
    amount = float(ex.amount_to_precision(SAMPLE, min_amt))
    print(f"   символ={SAMPLE} side=buy amount={amount} price={price} (ринок={last})")

    order = None
    try:
        # isolated margin, мінімальне плече — щоб маржа була мінімальна
        params = {"marginMode": "isolated", "leverage": 1}
        order = ex.create_order(SAMPLE, "limit", "buy", amount, price, params)
        oid = order.get("id")
        print(f"   ✅✅ ОРДЕР ПРИЙНЯТО! id={oid}")
        print("   => 🟢 API-ТОРГІВЛЯ ПРАЦЮЄ. Головний ризик знято.")
    except ccxt.NotSupported as e:
        print(f"   🔴 NotSupported: {e}")
        print("   => ccxt каже, що MEXC не підтримує розміщення контрактних ордерів через API.")
    except Exception as e:  # noqa: BLE001
        print(f"   ⚠️ {type(e).__name__}: {e}")
        print("   => дивимось на текст: якщо про 'interface/contract not open' — API-торгівля закрита;")
        print("      якщо про параметр (leverage/openType/precision) — підправимо виклик і повторимо.")
    finally:
        if order and order.get("id"):
            try:
                ex.cancel_order(order["id"], SAMPLE)
                print("   🧹 тест-ордер скасовано.")
            except Exception as e:  # noqa: BLE001
                print(f"   ⚠️ не вдалось скасувати автоматично ({e}) — перевір вручну на MEXC!")


if __name__ == "__main__":
    main()
