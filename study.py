"""ДЕТАЛЬНЕ дослідження життєздатності стратегії.

Бере справжні спот-делістинги Binance за багато сторінок анонсів і по кожному токену:
  - чи є шортабельний перп на MEXC / Bybit / Gate / Bitget,
  - яку просадку дав (1г / 24г) — ціна з Binance spot, або з перпа, де є.
Наприкінці — зведена статистика: скільки % делістингів реально шортабельні й на скільки падали.
"""
import asyncio
import datetime as dt
import statistics

import aiohttp
import ccxt

import binance_watcher as bw

CMS = ("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
       "?type=1&catalogId=161&pageNo={pg}&pageSize=50")
VENUES = ["mexc", "bybit", "gate", "bitget"]
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", "lang": "en"}
PAGES = 40


def is_true_spot_delist(title: str) -> bool:
    """Тільки повний спот-делістинг: 'Binance Will Delist ...'.
    Виключає margin/loan, futures, alpha, pair removal."""
    t = title.strip().lower()
    return t.startswith("binance will delist")


async def fetch_articles(session) -> list[dict]:
    seen, arts = set(), []
    for pg in range(1, PAGES + 1):
        try:
            async with session.get(CMS.format(pg=pg), headers=HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=20)) as r:
                data = await r.json()
        except Exception:
            continue
        d = data.get("data") or {}
        chunk = list(d.get("articles") or [])
        for cat in d.get("catalogs", []) or []:
            chunk += cat.get("articles", []) or []
        if not chunk:
            break
        for a in chunk:
            aid = a.get("id") or a.get("code")
            if aid in seen:
                continue
            seen.add(aid)
            arts.append(a)
        await asyncio.sleep(0.25)
    return arts


def load_venues() -> dict:
    exs = {}
    for v in VENUES:
        try:
            e = getattr(ccxt, v)({"enableRateLimit": True, "options": {"defaultType": "swap"}})
            e.load_markets()
            exs[v] = e
        except Exception as ex:  # noqa: BLE001
            print(f"  (біржа {v} не завантажилась: {ex})")
    return exs


def perp_venues(exs: dict, ticker: str) -> list[str]:
    return [v for v, e in exs.items() if f"{ticker}/USDT:USDT" in e.markets]


def _drop_from(ohlcv, rel):
    if not ohlcv:
        return None, None
    ref = next((c[1] for c in ohlcv if c[0] >= rel), ohlcv[0][4])
    def low(h):
        end = rel + h * 3600000
        ls = [c[3] for c in ohlcv if rel <= c[0] <= end]
        return min(ls) if ls else None
    d = lambda l: round((ref - l) / ref * 100, 1) if l else None
    return d(low(1)), d(low(24))


async def binance_drop(session, ticker, rel):
    url = ("https://api.binance.com/api/v3/klines"
           f"?symbol={ticker}USDT&interval=15m&startTime={rel-3600000}"
           f"&endTime={rel+24*3600000}&limit=200")
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            kl = await r.json()
        if not isinstance(kl, list) or not kl:
            return None, None
        kl = [[k[0], float(k[1]), float(k[2]), float(k[3]), float(k[4])] for k in kl]
        return _drop_from(kl, rel)
    except Exception:  # noqa: BLE001
        return None, None


def perp_drop(exs, venues, ticker, rel):
    for v in venues:
        try:
            oh = exs[v].fetch_ohlcv(f"{ticker}/USDT:USDT", "15m", since=rel - 3600000, limit=200)
            d1, d24 = _drop_from(oh, rel)
            if d24 is not None:
                return d1, d24, v
        except Exception:  # noqa: BLE001
            continue
    return None, None, None


async def main():
    print("Завантажую анонси Binance (може зайняти ~15с)...")
    async with aiohttp.ClientSession() as s:
        arts = await fetch_articles(s)
        events = [a for a in arts if is_true_spot_delist(a.get("title", ""))]
        print(f"Усього анонсів: {len(arts)} | справжніх спот-делістингів: {len(events)}")
        if not events:
            print("Немає подій для аналізу."); return

        print("Завантажую ринки бірж (MEXC/Bybit/Gate/Bitget)...")
        exs = load_venues()

        rows = []  # (date, ticker, venues, drop1, drop24, src)
        for a in events:
            rel = a.get("releaseDate")
            title = a.get("title", "")
            if not rel:
                continue
            for t in bw.extract_tickers(title):
                venues = perp_venues(exs, t)
                d1, d24 = await binance_drop(s, t, rel)
                src = "binance"
                if d24 is None and venues:
                    d1, d24, src = perp_drop(exs, venues, t, rel)
                rows.append((rel, t, venues, d1, d24, src))

    # ---- звіт ----
    rows.sort(key=lambda r: r[0], reverse=True)
    print("\n================= ПО ПОДІЯХ =================")
    for rel, t, venues, d1, d24, src in rows:
        when = dt.datetime.fromtimestamp(rel / 1000, dt.UTC).strftime("%Y-%m-%d")
        vtxt = ",".join(venues) if venues else "—"
        dtxt = f"1г −{d1}% 24г −{d24}% [{src}]" if d24 is not None else "просадка: н/д"
        flag = "🟢шорт" if venues else "🔴нема"
        print(f"[{when}] {t:9} {flag} перп:{vtxt:20} {dtxt}")

    total = len(rows)
    shortable = [r for r in rows if r[2]]
    with_drop = [r for r in rows if r[4] is not None]
    big = [r for r in rows if r[4] is not None and r[4] >= 15]
    big_short = [r for r in big if r[2]]

    def med(xs): return round(statistics.median(xs), 1) if xs else None

    print("\n================= ЗВЕДЕННЯ =================")
    if rows:
        dr = dt.datetime.fromtimestamp(min(r[0] for r in rows)/1000, dt.UTC).strftime("%Y-%m-%d")
        to = dt.datetime.fromtimestamp(max(r[0] for r in rows)/1000, dt.UTC).strftime("%Y-%m-%d")
        print(f"Період: {dr} … {to}")
    print(f"Токенів у делістингах: {total}")
    print(f"Шортабельних (є перп ≥1 біржа): {len(shortable)} ({100*len(shortable)//max(total,1)}%)")
    print(f"Вдалося виміряти просадку: {len(with_drop)}")
    if with_drop:
        print(f"  медіана просадки 24г: −{med([r[4] for r in with_drop])}%")
        print(f"  макс просадка 24г: −{max(r[4] for r in with_drop)}%")
    print(f"Сильних дампів (≥15% за 24г): {len(big)}, з них шортабельних: {len(big_short)}")
    if shortable:
        sd = [r[4] for r in shortable if r[4] is not None]
        print(f"Серед ШОРТАБЕЛЬНИХ медіана просадки: −{med(sd)}%" if sd else
              "Серед шортабельних просадку виміряти не вдалось")


if __name__ == "__main__":
    asyncio.run(main())
