"""Стежить за анонсами делістингу на Binance (CMS API).

Фаза 1: тільки детект + подія. Ордерів немає.
"""
import re
from dataclasses import dataclass, field

import aiohttp

import config

# Слова, які виглядають як тикери, але ними не є — відсіюємо.
_STOPWORDS = {
    "WILL", "AND", "THE", "ON", "FOR", "USDT", "BUSD", "USDC", "FDUSD", "BTC",
    "ETH", "BNB", "TRY", "EUR", "USD", "NOTICE", "BINANCE", "SPOT", "MARGIN",
    "FUTURES", "TRADING", "PAIRS", "PAIR", "OF", "TO", "UTC", "CEASE", "REMOVE",
    "REMOVAL", "DELIST", "DELISTS", "DELISTING", "CONVERT", "EARN", "OR",
}

# Заголовки, що реально означають делістинг спот-токена.
_DELIST_HINT = re.compile(r"\b(delist|removal|remove|will delist)\b", re.IGNORECASE)

# Категорії за силою сигналу для стратегії.
SPOT_DELIST = "SPOT_DELIST"        # головний сигнал: повний делістинг токена
FUTURES_DELIST = "FUTURES_DELIST"  # делістинг ф'ючерсного контракту
PAIR_REMOVAL = "PAIR_REMOVAL"      # прибирання окремих пар (тикери в тілі)
OTHER = "OTHER"


def classify(title: str) -> str:
    t = title.lower()
    if "futures will delist" in t or ("futures" in t and "delist" in t):
        return FUTURES_DELIST
    if "will delist" in t:
        return SPOT_DELIST
    if "removal of" in t and ("trading pair" in t or "margin" in t):
        return PAIR_REMOVAL
    return OTHER


@dataclass
class DelistingEvent:
    article_id: str
    title: str
    tickers: list[str] = field(default_factory=list)
    url: str = ""
    category: str = OTHER
    release_ms: int | None = None  # releaseDate анонсу (для заміру затримки детекту)

    @property
    def actionable(self) -> bool:
        """Тип, на який стратегія (у майбутніх фазах) реально відкриватиме шорт."""
        return self.category == SPOT_DELIST


def extract_tickers(title: str) -> list[str]:
    """Груба евристика: беремо UPPERCASE-токени 2-10 символів, чистимо стопслова."""
    # Кандидати: слідом за 'Delist'/'Removal' зазвичай перелік тикерів.
    candidates = re.findall(r"\b[A-Z0-9]{2,10}\b", title)
    tickers, seen = [], set()
    for c in candidates:
        if c in _STOPWORDS or c.isdigit() or c in seen:
            continue
        seen.add(c)
        tickers.append(c)
    return tickers


def _iter_articles(data: dict):
    """CMS-відповідь буває у двох формах — обробляємо обидві."""
    d = data.get("data") or {}
    if isinstance(d.get("articles"), list):
        yield from d["articles"]
    for cat in d.get("catalogs", []) or []:
        yield from cat.get("articles", []) or []


async def fetch_new_events(session: aiohttp.ClientSession) -> list[DelistingEvent]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "lang": "en",
    }
    async with session.get(
        config.BINANCE_CMS_URL,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=15),
    ) as r:
        if r.status != 200:
            raise RuntimeError(f"CMS API HTTP {r.status}")
        data = await r.json()

    events = []
    for art in _iter_articles(data):
        title = art.get("title", "")
        art_id = str(art.get("id") or art.get("code") or title)
        code = art.get("code", "")
        url = f"https://www.binance.com/en/support/announcement/{code}" if code else ""
        if not _DELIST_HINT.search(title):
            continue
        events.append(
            DelistingEvent(
                article_id=art_id,
                title=title,
                tickers=extract_tickers(title),
                url=url,
                category=classify(title),
                release_ms=art.get("releaseDate"),
            )
        )
    return events
