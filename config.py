"""Конфіг: читає .env (без зовнішніх залежностей)."""
from pathlib import Path

_BASE = Path(__file__).parent


def _load_env(path: Path) -> dict:
    env = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        env[key.strip()] = val.strip()
    return env


_ENV = _load_env(_BASE / ".env")

TELEGRAM_BOT_TOKEN = _ENV.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = _ENV.get("TELEGRAM_CHAT_ID", "").strip()
POLL_INTERVAL = float(_ENV.get("POLL_INTERVAL", "3") or "3")

# Біржа виконання (MEXC). Ключі задаються на сервері у .env, у git не потрапляють.
MEXC_API_KEY = _ENV.get("MEXC_API_KEY", "").strip()
MEXC_API_SECRET = _ENV.get("MEXC_API_SECRET", "").strip()

# --- Торгові параметри (Фаза 3) ---
# DRY_RUN=1 → бот лише симулює угоди (реальних ордерів НЕ ставить).
DRY_RUN = (_ENV.get("DRY_RUN", "1").strip() != "0")
POSITION_MARGIN_USDT = float(_ENV.get("POSITION_MARGIN_USDT", "15") or "15")
LEVERAGE = float(_ENV.get("LEVERAGE", "3") or "3")

# --- Стратегія входу/виходу (усе в %, легко міняти) ---
REF_LOOKBACK_MIN = int(_ENV.get("REF_LOOKBACK_MIN", "60") or "60")        # вікно для «до-дампової» ціни
MAX_ALREADY_DROP_PCT = float(_ENV.get("MAX_ALREADY_DROP_PCT", "30") or "30")  # >цього вже впало → не входимо
STOP_LOSS_PCT = float(_ENV.get("STOP_LOSS_PCT", "10") or "10")            # ціна +% від входу → стоп
TRAIL_PCT = float(_ENV.get("TRAIL_PCT", "8") or "8")                      # відскок від дна → тейк
MIN_PROFIT_TO_TRAIL_PCT = float(_ENV.get("MIN_PROFIT_TO_TRAIL_PCT", "3") or "3")  # трейл лише після цього плюса
MAX_HOLD_HOURS = float(_ENV.get("MAX_HOLD_HOURS", "12") or "12")          # примусове закриття
EXIT_CHECK_SEC = float(_ENV.get("EXIT_CHECK_SEC", "5") or "5")            # як часто перевіряти відкриті позиції

# Risk-ліміти
MAX_CONCURRENT = int(_ENV.get("MAX_CONCURRENT", "3") or "3")             # макс. одночасних позицій

# Binance Announcements CMS API. catalogId=161 = розділ "Delisting".
BINANCE_CMS_URL = (
    "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
    "?type=1&catalogId=161&pageNo=1&pageSize=20"
)

DB_PATH = str(_BASE / "state.db")
