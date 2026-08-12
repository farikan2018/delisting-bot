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

import config

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


async def watch_ws():
    """WebSocket-фід cryptolisting.ws (push). Логує делістинги в мить отримання."""
    if not config.CL_WS_KEY:
        print("ws: нема CL_WS_KEY — пропускаю", flush=True)
        return
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.ws_connect(config.CL_WS_URL,
                                        headers={"X-API-Key": config.CL_WS_KEY},
                                        heartbeat=15, timeout=25) as ws:
                    print("ws: підключено до", config.CL_WS_URL, flush=True)
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        try:
                            d = json.loads(msg.data)
                        except Exception:  # noqa: BLE001
                            continue
                        if d.get("type") == "announcement" and \
                                d.get("listingType") in ("spot_delisting", "futures_delisting"):
                            now_ms = int(time.time() * 1000)
                            _log({"ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                                  "source": "ws_cryptolisting", "title": d.get("title", ""),
                                  "ticker": d.get("ticker"), "listingType": d.get("listingType"),
                                  "ws_detected_us": d.get("detectedTimestampUs"),
                                  "ws_dispatch_us": d.get("dispatchTimestampUs"),
                                  "detected_ms": now_ms, "release_ms": None, "latency_sec": None})
        except Exception as e:  # noqa: BLE001
            print("ws: помилка", type(e).__name__, str(e)[:120], flush=True)
        await asyncio.sleep(5)


async def watch_telegram():
    """Userbot-слухач каналу @CLWfeed (Telethon). Логує кожен пост у мить отримання.
    Порівнюємо його receipt-time з WS та CMS. Тільки заміри, не торгує."""
    if not (config.TG_API_ID and config.TG_API_HASH and config.TG_SESSION):
        print("tg: нема TG_API_ID/HASH/SESSION — пропускаю", flush=True)
        return
    try:
        from telethon import TelegramClient, events
        from telethon.sessions import StringSession
        from telethon.tl.functions.channels import JoinChannelRequest
    except Exception as e:  # noqa: BLE001
        print("tg: telethon не встановлено:", e, flush=True)
        return

    client = TelegramClient(StringSession(config.TG_SESSION),
                            config.TG_API_ID, config.TG_API_HASH)

    @client.on(events.NewMessage(chats=config.TG_FEED_CHANNEL))
    async def _handler(event):  # noqa: ANN001
        now_ms = int(time.time() * 1000)
        text = (event.message.message or "").replace("\n", " ")
        low = text.lower()
        is_binance = "binance" in low
        is_delist = "delist" in low
        _log({"ts": dt.datetime.now(dt.timezone.utc).isoformat(),
              "source": "tg_clwfeed", "title": text[:200],
              "is_binance": is_binance, "is_delisting": is_delist,
              "msg_date": event.message.date.isoformat() if event.message.date else None,
              "detected_ms": now_ms, "release_ms": None, "latency_sec": None})

    while True:
        try:
            await client.start()
            try:
                await client(JoinChannelRequest(config.TG_FEED_CHANNEL))
            except Exception:  # noqa: BLE001
                pass  # вже підписані / приватний — ігноруємо
            me = await client.get_me()
            print(f"tg: userbot підключено як @{me.username} → слухаю @{config.TG_FEED_CHANNEL}",
                  flush=True)
            await client.run_until_disconnected()
        except Exception as e:  # noqa: BLE001
            print("tg: помилка", type(e).__name__, str(e)[:120], flush=True)
        await asyncio.sleep(5)


async def main():
    print("probe: стежу за", [s["name"] for s in SOURCES], "+ ws + tg", flush=True)
    async with aiohttp.ClientSession() as s:
        await asyncio.gather(watch_ws(), watch_telegram(),
                             *[watch(s, src) for src in SOURCES])


if __name__ == "__main__":
    asyncio.run(main())
