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
    except ccxt.PermissionDenied as e:
        print(f"   ❌ PermissionDenied: {e}")
        print("   => ключу бракує потрібних прав (перевір галочки ф'ючерсів).")
    except Exception as e:  # noqa: BLE001
        print(f"   ❌ {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
