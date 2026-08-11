"""Латентність-проба джерел анонсів Binance.

Одночасно стежить за кількома HTTP-джерелами. На КОЖЕН новий делістинг пише в
logs/probe.jsonl: яке джерело, коли побачило, releaseDate і затримку від публікації.
Мета — емпірично зʼясувати, звідки анонс приходить швидше (і потім порівняти з
WebSocket-фідом, коли буде тест-ключ).
"""
import asyncio
import datetime as dt
import json
import time
from pathlib import Path

import aiohttp

_LOGDIR = Path(__file__).parent / "logs"
_LOGDIR.mkdir(exist_ok=True)
_OUT = _LOGDIR / "probe.jsonl"
POLL = 5.0  # гентельно, щоб не 429-ити composite (яким користується сам бот)
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", "lang": "en"}

# Джерела для порівняння. composite (як у бота) vs apex (окремий, стійкіший сервіс).
# WebSocket-фід додамо сюди, коли буде тест-ключ.
SOURCES = [
    {"name": "cms_delisting",
     "url": "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
            "?type=1&catalogId=161&pageNo=1&pageSize=20"},
    {"name": "apex_delisting",
     "url": "https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query"
            "?type=1&catalogId=161&pageNo=1&pageSize=20"},
]


def _articles(data):
    d = (data or {}).get("data") or {}
    arts = list(d.get("articles") or [])
    for c in d.get("catalogs", []) or []:
        arts += c.get("articles", []) or []
    out = []
    for a in arts:
        t = a.get("title", "")
        if "will delist" in t.lower():
            out.append((str(a.get("id") or a.get("code") or t), t, a.get("releaseDate")))
    return out


def _log(rec):
    try:
        with open(_OUT, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:  # noqa: BLE001
        pass
    print(rec["ts"], rec["source"], "lat=%ss" % rec["latency_sec"], rec["title"][:55], flush=True)


async def watch(session, src):
    seen, primed = set(), False
    while True:
        try:
            async with session.get(src["url"], headers=HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    for aid, title, rel in _articles(json.loads(await r.text())):
                        if aid in seen:
                            continue
                        seen.add(aid)
                        if not primed:
                            continue  # прайм: наявні не логуємо
                        now_ms = int(time.time() * 1000)
                        lat = round((now_ms - rel) / 1000, 1) if rel else None
                        _log({"ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                              "source": src["name"], "article_id": aid, "title": title,
                              "release_ms": rel, "detected_ms": now_ms, "latency_sec": lat})
                    primed = True
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(POLL)


async def main():
    print("probe: стежу за", [s["name"] for s in SOURCES], flush=True)
    async with aiohttp.ClientSession() as s:
        await asyncio.gather(*[watch(s, src) for src in SOURCES])


if __name__ == "__main__":
    asyncio.run(main())
