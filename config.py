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

# Telegram userbot (Telethon) — слухає канал @CLWfeed як окреме джерело анонсів.
# Поки лише для probe (заміри WS vs Telegram-канал). Сесія = доступ до акаунта!
TG_API_ID = int(_ENV.get("TG_API_ID", "0") or "0")
TG_API_HASH = _ENV.get("TG_API_HASH", "").strip()
TG_SESSION = _ENV.get("TG_SESSION", "").strip()
TG_FEED_CHANNEL = _ENV.get("TG_FEED_CHANNEL", "CLWfeed").strip()

# --- Торгові параметри (Фаза 3) ---
# DRY_RUN=1 → бот лише симулює угоди (реальних ордерів НЕ ставить).
DRY_RUN = (_ENV.get("DRY_RUN", "1").strip() != "0")
POSITION_MARGIN_USDT = float(_ENV.get("POSITION_MARGIN_USDT", "100") or "100")
LEVERAGE = float(_ENV.get("LEVERAGE", "3") or "3")
# Маржа для РЕАЛЬНОГО тест-шорта через /test_short (незалежно від авто-DRY_RUN).
# $2 × 3x = $6 позиції — щоб пройти можливий мін. ордер Bybit (~$5). Ризик при стопі ~$0.60.
TEST_MARGIN_USDT = float(_ENV.get("TEST_MARGIN_USDT", "2") or "2")

# --- Вхід (Strategy v2) ---
# Anti-late-entry: якщо за останні REF_LOOKBACK_MIN хв ціна вже впала більше ніж на
# MAX_ALREADY_DROP_PCT — НЕ входимо (обвал уже стався, ризик відскоку). З WS-детектом (~3-4с)
# цей фільтр адекватний: відсіює лише блискавичні обвали. 0 = вимкнути фільтр.
REF_LOOKBACK_MIN = int(_ENV.get("REF_LOOKBACK_MIN", "5") or "5")
MAX_ALREADY_DROP_PCT = float(_ENV.get("MAX_ALREADY_DROP_PCT", "8") or "8")

# --- Вихід (усе у % від МАРЖІ) ---
STOP_LOSS_MARGIN_PCT = float(_ENV.get("STOP_LOSS_MARGIN_PCT", "30") or "30")        # збиток -30% маржі → стоп
TAKE_PROFIT_MARGIN_PCT = float(_ENV.get("TAKE_PROFIT_MARGIN_PCT", "30") or "30")    # прибуток +30% маржі → тейк
MAX_HOLD_MINUTES = float(_ENV.get("MAX_HOLD_MINUTES", "45") or "45")                # примусове закриття
EXIT_CHECK_SEC = float(_ENV.get("EXIT_CHECK_SEC", "5") or "5")                      # частота перевірки позицій

# Keep-alive: пінг бірж, щоб TLS-конект був теплим і бойовий ордер летів ~165мс,
# а не ~566мс (холодний старт). 0 = вимкнути. Замір показав різницю ~400мс.
KEEPALIVE_SEC = float(_ENV.get("KEEPALIVE_SEC", "30") or "30")

# Price-cache: фоновий знімок цін усіх Bybit-перпів (1 HTTP/усі символи) раз на
# PRICECACHE_POLL_SEC — щоб на сигналі мати ціну+ref У ПАМʼЯТІ (без мережі на гарячому
# шляху). Якщо ціна старіша за MAX_AGE — не довіряємо, фолбек на REST. 0 = вимкнути кеш.
PRICECACHE_POLL_SEC = float(_ENV.get("PRICECACHE_POLL_SEC", "2") or "2")
PRICECACHE_MAX_AGE_SEC = float(_ENV.get("PRICECACHE_MAX_AGE_SEC", "10") or "10")
# WS реал-тайм шар: стрім tickers усіх перпів (ціна оновлюється в реальному часі).
# 1=увімк (максимальна свіжість, основа для детектора обвалу). Знімок лишається сідом/страховкою.
PRICECACHE_WS = (_ENV.get("PRICECACHE_WS", "1").strip() != "0")

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
