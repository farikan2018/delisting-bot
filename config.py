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
# Перевірка виходу тепер читає ціну з price-cache (0 мережі), тому частіше = безкоштовно.
EXIT_CHECK_SEC = float(_ENV.get("EXIT_CHECK_SEC", "2") or "2")                      # частота перевірки позицій

# Пре-озброєння плеча: виставити LEVERAGE по ВСІХ символах ЗАЗДАЛЕГІДЬ, щоб бойовий
# ордер не платив +165мс за set_leverage (делістинг — це завжди «новий» символ).
# Робиться один раз (стан у БД), потім лише для нових листингів раз на ARM_REFRESH_SEC.
# ВИМКНЕНО за замовчуванням свідомо: це масова зміна налаштувань акаунта на біржі
# (~700 підписаних викликів). Поки вимкнено, ордер по неозброєному символу спершу
# виставляє плече сам (+165мс) — бо інакше при дефолтних 10x стоп і ліквідація
# майже збігаються. ARM_LEVERAGE=1 повертає ці 165мс.
ARM_LEVERAGE = (_ENV.get("ARM_LEVERAGE", "0").strip() != "0")
ARM_SLEEP_SEC = float(_ENV.get("ARM_SLEEP_SEC", "0.15") or "0.15")   # пауза між викликами (rate-limit)
ARM_REFRESH_SEC = float(_ENV.get("ARM_REFRESH_SEC", "21600") or "21600")  # 6г: догнати нові символи

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

# --- Детектор обвалу (dumpwatch) — ВИКЛЮЧЕНИЙ, і ось чому ---
# Він зʼявився як обхід «стіни CDN»: анонси на www.binance.com кешовані з TTL ~120с,
# тому найшвидшим доступним сигналом здавався сам рух ціни (Bybit WS tickers, 81мс).
# Обидві підстави відпали:
#   1) стіну пробито — некешовані хости origin дають анонс за ~0.4с (див. fastcms.py),
#      тобто новина тепер ШВИДША за реакцію ринку, а не повільніша;
#   2) бектест на тік-даних показав, що поріг −4% за 20с спрацьовує вже ЗА обривом
#      кривої PnL: вхід на 5-10с дає +11…+4% маржі проти +28% при вході до 2с.
# Тобто це строго слабший сигнал, який ще й їв CPU (826 символів, ~400 перевірок/с на
# 2 vCPU — а CPU тут прямо конвертується в джитер event-loop і в затримку ордера).
# Код лишено: DUMPWATCH=1 повертає детект, DUMPWATCH_ALERT=1 — сповіщення,
# DUMPWATCH_TRADE=1 — угоди. Але за замовчуванням усе три вимкнено.
DUMPWATCH = (_ENV.get("DUMPWATCH", "0").strip() != "0")
DUMPWATCH_TRADE = (_ENV.get("DUMPWATCH_TRADE", "0").strip() != "0")
# Сповіщення в Telegram про кожен обвал. За замовчуванням ВИКЛЮЧЕНО: при DUMP_PCT=4
# за 20с по 826 символах це ~7 повідомлень на годину рівно на порозі (4.0-4.5%) —
# звичайна волатильність альтів, не делістинги. Детект лишається й пише в лог
# (`dump_detected`), тож дані для оцінки стратегії не втрачаються.
DUMPWATCH_ALERT = (_ENV.get("DUMPWATCH_ALERT", "0").strip() != "0")
DUMP_PCT = float(_ENV.get("DUMP_PCT", "4") or "4")               # просадка у % → тригер
DUMP_WINDOW_SEC = float(_ENV.get("DUMP_WINDOW_SEC", "20") or "20")  # за який час
DUMP_MAXLEN = int(_ENV.get("DUMP_MAXLEN", "64") or "64")         # довжина вікна на символ
DUMP_COOLDOWN_SEC = float(_ENV.get("DUMP_COOLDOWN_SEC", "900") or "900")  # антиспам на символ
# Якщо одночасно просіло стільки ж інших символів — це обвал РИНКУ, а не делістинг.
DUMP_MARKET_WIDE_N = int(_ENV.get("DUMP_MARKET_WIDE_N", "5") or "5")
DUMP_MARKET_SEC = float(_ENV.get("DUMP_MARKET_SEC", "60") or "60")

# Risk-ліміти
MAX_CONCURRENT = int(_ENV.get("MAX_CONCURRENT", "3") or "3")             # макс. одночасних позицій
# Свіжість сигналу: угоду відкриваємо лише якщо від публікації минуло <= стільки секунд.
# WS-фід дає ~3-4с → торгує. Поллінг ~126с → лише попереджає, не торгує.
MAX_SIGNAL_AGE_SEC = float(_ENV.get("MAX_SIGNAL_AGE_SEC", "60") or "60")

# --- Власний швидкий детектор анонсів (fastcms) ---
# www.binance.com віддає CMS через CloudFront із TTL ~120с (виміряно по Age), тому
# поллінг там бачить анонс із запізненням до 2хв. Ті самі дані на accounts/p2p/
# launchpad.binance.com віддаються БЕЗ кешу (X-Cache: Miss завжди), RTT ~260мс з
# Франкфурта. Крутимо хости по колу зі зсувом фази: затримка = POLL/(2·N) + RTT.
# 0.6с × 3 хости => ефективний інтервал 200мс => детект ~360мс.
# Rate-limit перевірено до 8 запитів/с з одного IP — усі 200. Тут виходить ~5/с.
FASTCMS = (_ENV.get("FASTCMS", "1").strip() != "0")
FASTCMS_POLL_SEC = float(_ENV.get("FASTCMS_POLL_SEC", "0.6") or "0.6")
FASTCMS_HOSTS = int(_ENV.get("FASTCMS_HOSTS", "3") or "3")
FASTCMS_TIMEOUT_SEC = float(_ENV.get("FASTCMS_TIMEOUT_SEC", "5") or "5")
# Торгувати за сигналом fastcms (а не лише сповіщати). Це ТОЙ САМИЙ тип сигналу, що й
# WS-фід (новина про делістинг), тільки швидший — тому за замовчуванням увімкнено.
FASTCMS_TRADE = (_ENV.get("FASTCMS_TRADE", "1").strip() != "0")

# Binance Announcements API. catalogId=161 = розділ "Delisting".
# apex-ендпоінт стійкіший до rate-limit (429), ніж composite; структура ідентична.
# Хост — некешований (див. вище), тому навіть сторож-поллінг бачить анонси одразу,
# а не з віком до 120с.
BINANCE_CMS_URL = (
    "https://accounts.binance.com/bapi/apex/v1/public/apex/cms/article/list/query"
    "?type=1&catalogId=161&pageNo=1&pageSize=20"
)

DB_PATH = str(_BASE / "state.db")
