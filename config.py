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

# cryptolisting.ws — швидкий WebSocket-фід анонсів (тест-ключ FreeDelayed).
CL_WS_KEY = _ENV.get("CL_WS_KEY", "").strip()
CL_WS_URL = _ENV.get("CL_WS_URL", "wss://cryptolisting.ws?cex=binance").strip()

# --- Торгові параметри (Фаза 3) ---
# DRY_RUN=1 → бот лише симулює угоди (реальних ордерів НЕ ставить).
DRY_RUN = (_ENV.get("DRY_RUN", "1").strip() != "0")
POSITION_MARGIN_USDT = float(_ENV.get("POSITION_MARGIN_USDT", "100") or "100")
LEVERAGE = float(_ENV.get("LEVERAGE", "3") or "3")

# --- Вхід (Strategy v2) ---
# Головний дамп стається В МОМЕНТ анонсу (до появи в API), тому backward-фільтр «вже впало»
# ріже все. Ловимо CONTINUATION: входимо на детекті. REF_LOOKBACK_MIN лишаємо тільки для
# інформації в повідомленні (на скільки вже впало). MAX_ALREADY_DROP_PCT=0 → фільтр вимкнено.
REF_LOOKBACK_MIN = int(_ENV.get("REF_LOOKBACK_MIN", "5") or "5")
MAX_ALREADY_DROP_PCT = float(_ENV.get("MAX_ALREADY_DROP_PCT", "0") or "0")  # 0 = не відсіювати

# --- Вихід (усе у % від МАРЖІ) ---
STOP_LOSS_MARGIN_PCT = float(_ENV.get("STOP_LOSS_MARGIN_PCT", "30") or "30")        # збиток -30% маржі → стоп
TAKE_PROFIT_MARGIN_PCT = float(_ENV.get("TAKE_PROFIT_MARGIN_PCT", "30") or "30")    # прибуток +30% маржі → тейк
MAX_HOLD_MINUTES = float(_ENV.get("MAX_HOLD_MINUTES", "45") or "45")                # примусове закриття
EXIT_CHECK_SEC = float(_ENV.get("EXIT_CHECK_SEC", "5") or "5")                      # частота перевірки позицій

# Risk-ліміти
MAX_CONCURRENT = int(_ENV.get("MAX_CONCURRENT", "3") or "3")             # макс. одночасних позицій
# Свіжість сигналу: угоду відкриваємо лише якщо від публікації минуло <= стільки секунд.
# WS-фід дає ~3-4с → торгує. Поллінг ~126с → лише попереджає, не торгує.
MAX_SIGNAL_AGE_SEC = float(_ENV.get("MAX_SIGNAL_AGE_SEC", "60") or "60")

# Binance Announcements API. catalogId=161 = розділ "Delisting".
# apex-ендпоінт стійкіший до rate-limit (429), ніж composite; структура ідентична.
BINANCE_CMS_URL = (
    "https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query"
    "?type=1&catalogId=161&pageNo=1&pageSize=20"
)

DB_PATH = str(_BASE / "state.db")
