"""Стежить за анонсами делістингу на Binance (CMS API).

Фаза 1: тільки детект + подія. Ордерів немає.
"""
import re
from dataclasses import dataclass, field

import aiohttp

import config

# Слова, які виглядають як тикери, але ними не є — відсіюємо.
# УВАГА: сюди НЕ можна класти короткі англійські слова, які є й тикерами (THE, ON,
# FOR, ID, AI…) — бо тоді реальний токен зникає. Тому список тримаємо мінімальним і
# застосовуємо його лише до сегмента заголовка з перелічними тикерами (див. нижче).
_STOPWORDS = {
    "USDT", "BUSD", "USDC", "FDUSD", "BTC", "ETH", "BNB", "TRY", "EUR", "USD",
    "AND", "OR", "ON", "TO", "FROM", "UTC", "AM", "PM",
}
# Ці ж слова, але для «широкого» режиму (коли сегмент не вдалось вирізати).
_STOPWORDS_WIDE = _STOPWORDS | {
    "WILL", "THE", "FOR", "NOTICE", "BINANCE", "SPOT", "MARGIN", "FUTURES",
    "TRADING", "PAIRS", "PAIR", "OF", "CEASE", "REMOVE", "REMOVAL", "DELIST",
    "DELISTS", "DELISTING", "CONVERT", "EARN", "LOAN", "LOANS", "CROSS",
    "ISOLATED", "PERPETUAL", "CONTRACT", "SUPPORT", "PLAN",
}

# Заголовки, що реально означають делістинг спот-токена.
_DELIST_HINT = re.compile(r"\b(delist|removal|remove|will delist)\b", re.IGNORECASE)

# Категорії за силою сигналу для стратегії.
SPOT_DELIST = "SPOT_DELIST"          # головний сигнал: повний делістинг токена
MARGIN_DELIST = "MARGIN_DELIST"      # делістинг лише з margin/loan — спот лишається
FUTURES_DELIST = "FUTURES_DELIST"    # делістинг ф'ючерсного контракту
PAIR_REMOVAL = "PAIR_REMOVAL"        # прибирання окремих пар (тикери в тілі)
OTHER = "OTHER"

# «Суб'єктом» делістингу є margin/loan, а не спот: «Binance Margin And Loan Will
# Delist…», «…Will Delist TUSD from Cross and Isolated Margin». Спот при цьому
# лишається торгуватись.
# Бектест на тік-даних Bybit: 48 пар «margin-анонс+символ» — середня зміна ціни
# −0.24% за хвилину, −0.20% за пів години, обвал ≥10% у 0 (НУЛЬ) пар. Для порівняння
# чистий спот-делістинг: −8.5% за хвилину, −12.6% за пів години, ≥10% у 69% пар.
# Тому такі анонси НЕ торгуємо: це були б угоди на шум, які платять комісію і
# випадково ловлять стоп.
_MARGIN_SUBJ = re.compile(
    r"(margin|loans?)\s+(and\s+(binance\s+)?loans?\s+)?will\s+delist"
    r"|will\s+delist\b.*\b(from|on)\b.*\b(cross|isolated|margin|loans?)\b"
    r"|will\s+delist\b.*\b(margin|earn)\s+(pairs?|products?)\b"
    r"|margin\s+and\s+(binance\s+)?loans?\b",
    re.IGNORECASE,
)


def classify(title: str) -> str:
    t = title.lower()
    if "futures will delist" in t or ("futures" in t and "delist" in t):
        return FUTURES_DELIST
    if "will delist" in t:
        return MARGIN_DELIST if _MARGIN_SUBJ.search(title) else SPOT_DELIST
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


# Перелік тикерів стоїть ПІСЛЯ дієслова і ДО хвоста («on 2026-08-17», «from Cross…»).
# Вирізаємо саме цей сегмент — тоді можна не боятись коротких слів у решті заголовка
# і дозволити однолітерні тикери (реальний приклад: «Will Delist COS, D, HIGH, MBOX»,
# де старий код втрачав D через мінімум 2 символи, а THE — через стоп-слово).
_SEG = re.compile(
    r"\b(?:will\s+delist|delists?|delisting|removal\s+of|remove\s+of)\b\s*(.+)",
    re.IGNORECASE,
)
_SEG_TAIL = re.compile(
    r"\s+\b(?:on|from|at|effective|and\s+support|will\s+be)\b\s|\s+[-–—(]|\s*\(",
    re.IGNORECASE,
)


def _ticker_segment(title: str) -> str | None:
    m = _SEG.search(title)
    if not m:
        return None
    seg = m.group(1)
    cut = _SEG_TAIL.search(seg)
    if cut:
        seg = seg[:cut.start()]
    seg = seg.strip(" .,:-")
    # Сегмент має виглядати як перелік тикерів, а не як фраза: інакше краще широкий режим.
    words = re.findall(r"[A-Za-z0-9]+", seg)
    if not words or len(words) > 12:
        return None
    lower = sum(1 for w in words if w.islower() or (w[:1].isupper() and w[1:].islower()))
    if lower > max(1, len(words) // 3):
        return None
    return seg


def extract_tickers(title: str) -> list[str]:
    """Тикери з заголовка. Спершу пробуємо вирізати сегмент-перелік (точний режим),
    інакше — старий широкий пошук по всьому заголовку з довшим списком стоп-слів."""
    seg = _ticker_segment(title)
    if seg is not None:
        candidates = re.findall(r"\b[A-Z0-9]{1,12}\b", seg)
        stop = _STOPWORDS
    else:
        candidates = re.findall(r"\b[A-Z0-9]{2,10}\b", title)
        stop = _STOPWORDS_WIDE
    tickers, seen = [], set()
    for c in candidates:
        if c in stop or c.isdigit() or c in seen:
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
