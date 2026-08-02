"""Point-in-time шортабельність: чи існував перп на МОМЕНТ анонсу.

Перевіряємо напряму історію ф'ючерсних свічок (raw API), а не поточні лістинги —
тому ловимо й ті перпи, які згодом прибрали. Джерело подій — Binance CMS.
"""
import asyncio
import datetime as dt
import statistics

import aiohttp

import binance_watcher as bw

CMS = ("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
       "?type=1&catalogId=161&pageNo={pg}&pageSize=50")
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", "lang": "en"}
PAGES = 40


def is_true_spot_delist(title: str) -> bool:
    return title.strip().lower().startswith("binance will delist")


async def fetch_events(session):
    seen, events = set(), []
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
            if is_true_spot_delist(a.get("title", "")) and a.get("releaseDate"):
                events.append(a)
        await asyncio.sleep(0.2)
    return events


# ---- point-in-time перевірки перпа (вікно: тиждень ДО анонсу) ----
async def had_perp_binance(s, t, rel):
    url = (f"https://fapi.binance.com/fapi/v1/klines?symbol={t}USDT&interval=1h"
           f"&startTime={rel-7*86400000}&endTime={rel}&limit=10")
    try:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            j = await r.json()
        return isinstance(j, list) and len(j) > 0
    except Exception:
        return False


async def had_perp_bybit(s, t, rel):
    url = (f"https://api.bybit.com/v5/market/kline?category=linear&symbol={t}USDT"
           f"&interval=60&start={rel-7*86400000}&end={rel}&limit=10")
    try:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            j = await r.json()
        return bool(((j or {}).get("result") or {}).get("list"))
    except Exception:
        return False


async def had_perp_mexc(s, t, rel):
    url = (f"https://contract.mexc.com/api/v1/contract/kline/{t}_USDT?interval=Min60"
           f"&start={(rel-7*86400000)//1000}&end={rel//1000}")
    try:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            j = await r.json()
        data = (j or {}).get("data") or {}
        return bool(data.get("time"))
    except Exception:
        return False


async def had_perp_gate(s, t, rel):
    url = (f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={t}_USDT"
           f"&interval=1h&from={(rel-7*86400000)//1000}&to={rel//1000}&limit=10")
    try:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            j = await r.json()
        return isinstance(j, list) and len(j) > 0
    except Exception:
        return False


async def shortable_then(s, t, rel):
    checks = {
        "binance": had_perp_binance, "bybit": had_perp_bybit,
        "mexc": had_perp_mexc, "gate": had_perp_gate,
    }
    res = await asyncio.gather(*[f(s, t, rel) for f in checks.values()])
    return [name for name, ok in zip(checks, res) if ok]


async def binance_spot_drop(s, t, rel):
    url = ("https://api.binance.com/api/v3/klines"
           f"?symbol={t}USDT&interval=15m&startTime={rel-3600000}"
           f"&endTime={rel+24*3600000}&limit=200")
    try:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            kl = await r.json()
        if not isinstance(kl, list) or not kl:
            return None
        ref = next((float(k[1]) for k in kl if k[0] >= rel), float(kl[0][4]))
        end = rel + 24 * 3600000
        lows = [float(k[3]) for k in kl if rel <= k[0] <= end]
        return round((ref - min(lows)) / ref * 100, 1) if lows else None
    except Exception:
        return None


async def main():
    async with aiohttp.ClientSession() as s:
        print("Збираю події...")
        events = await fetch_events(s)
        tokens = []
        for a in events:
            for t in bw.extract_tickers(a.get("title", "")):
                tokens.append((t, a["releaseDate"]))
        print(f"Подій: {len(events)} | токенів: {len(tokens)}. Перевіряю point-in-time перпи...\n")

        rows = []
        for t, rel in tokens:
            venues = await shortable_then(s, t, rel)
            drop = await binance_spot_drop(s, t, rel)
            rows.append((rel, t, venues, drop))

    rows.sort(key=lambda r: r[0], reverse=True)
    print("========== ШОРТАБЕЛЬНІ НА МОМЕНТ АНОНСУ ==========")
    for rel, t, venues, drop in rows:
        if not venues:
            continue
        when = dt.datetime.fromtimestamp(rel/1000, dt.UTC).strftime("%Y-%m-%d")
        dtxt = f"−{drop}%" if drop is not None else "н/д"
        print(f"[{when}] {t:9} просадка 24г {dtxt:7} | перп тоді: {','.join(venues)}")

    total = len(rows)
    short = [r for r in rows if r[2]]
    short_drops = [r[3] for r in short if r[3] is not None]
    big_short = [r for r in short if r[3] is not None and r[3] >= 15]

    def med(x): return round(statistics.median(x), 1) if x else None
    # «Чисто» шортабельні — на незалежних біржах (не тільки Binance, який робить reduce-only)
    INDEP = {"bybit", "mexc", "gate"}
    clean = [r for r in short if INDEP.intersection(r[2])]
    clean_drops = [r[3] for r in clean if r[3] is not None]
    clean_big = [r for r in clean if r[3] is not None and r[3] >= 15]
    # покриття по біржах
    from collections import Counter
    cov = Counter()
    for r in short:
        for v in r[2]:
            cov[v] += 1

    print("\n================= ЗВЕДЕННЯ (point-in-time) =================")
    print(f"Токенів у делістингах: {total}")
    print(f"Шортабельних (будь-де, вкл. Binance): {len(short)} ({100*len(short)//max(total,1)}%)")
    print(f"Шортабельних на НЕЗАЛЕЖНИХ (Bybit/MEXC/Gate — без reduce-only): "
          f"{len(clean)} ({100*len(clean)//max(total,1)}%)")
    if clean_drops:
        print(f"  медіана просадки (незалежні): −{med(clean_drops)}% | сильних ≥15%: {len(clean_big)}")
    print(f"Покриття по біржах: {dict(cov)}")
    yrs = (max(r[0] for r in rows) - min(r[0] for r in rows)) / (365.25*86400000)
    print(f"Період ~{yrs:.1f} р → на незалежних: ~{len(clean)/max(yrs,0.1):.1f} можливостей/рік")


if __name__ == "__main__":
    asyncio.run(main())
