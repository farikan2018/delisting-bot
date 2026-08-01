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

# Binance Announcements CMS API. catalogId=161 = розділ "Delisting".
BINANCE_CMS_URL = (
    "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
    "?type=1&catalogId=161&pageNo=1&pageSize=20"
)

DB_PATH = str(_BASE / "state.db")
