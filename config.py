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

# Біржі виконання. Пріоритет: шортимо на першій, де є перп. Ключі — на сервері у .env.
VENUE_PRIORITY = [v.strip() for v in
                  (_ENV.get("VENUE_PRIORITY", "bybit,mexc") or "bybit,mexc").split(",") if v.strip()]
MEXC_API_KEY = _ENV.get("MEXC_API_KEY", "").strip()
MEXC_API_SECRET = _ENV.get("MEXC_API_SECRET", "").strip()
BYBIT_API_KEY = _ENV.get("BYBIT_API_KEY", "").strip()
BYBIT_API_SECRET = _ENV.get("BYBIT_API_SECRET", "").strip()

# --- Торгові параметри (Фаза 3) ---
# DRY_RUN=1 → бот лише симулює угоди (реальних ордерів НЕ ставить).
DRY_RUN = (_ENV.get("DRY_RUN", "1").strip() != "0")
POSITION_MARGIN_USDT = float(_ENV.get("POSITION_MARGIN_USDT", "100") or "100")
LEVERAGE = float(_ENV.get("LEVERAGE", "7") or "7")

# --- Вхід: anti-late-entry ---
# Якщо за останні REF_LOOKBACK_MIN хв ціна вже впала більше ніж на MAX_ALREADY_DROP_PCT — не входимо.
REF_LOOKBACK_MIN = int(_ENV.get("REF_LOOKBACK_MIN", "5") or "5")
MAX_ALREADY_DROP_PCT = float(_ENV.get("MAX_ALREADY_DROP_PCT", "10") or "10")

# --- Вихід (усе у % від МАРЖІ — інтуїтивно в грошах) ---
STOP_LOSS_MARGIN_PCT = float(_ENV.get("STOP_LOSS_MARGIN_PCT", "25") or "25")        # збиток -25% маржі → стоп
TRAIL_ARM_MARGIN_PCT = float(_ENV.get("TRAIL_ARM_MARGIN_PCT", "30") or "30")        # трейл вмикається після +30% маржі
TRAIL_GIVEBACK_MARGIN_PCT = float(_ENV.get("TRAIL_GIVEBACK_MARGIN_PCT", "15") or "15")  # віддали 15% маржі від піку → вихід
MAX_HOLD_MINUTES = float(_ENV.get("MAX_HOLD_MINUTES", "30") or "30")                # примусове закриття
EXIT_CHECK_SEC = float(_ENV.get("EXIT_CHECK_SEC", "5") or "5")                      # частота перевірки позицій

# Risk-ліміти
MAX_CONCURRENT = int(_ENV.get("MAX_CONCURRENT", "3") or "3")             # макс. одночасних позицій

# Binance Announcements CMS API. catalogId=161 = розділ "Delisting".
BINANCE_CMS_URL = (
    "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
    "?type=1&catalogId=161&pageNo=1&pageSize=20"
)

DB_PATH = str(_BASE / "state.db")
