"""Латентність-проба джерел анонсів Binance.

Одночасно стежить за кількома HTTP-джерелами. На КОЖЕН новий делістинг пише в
logs/probe.jsonl: яке джерело, коли побачило, releaseDate і затримку від публікації.
Мета — емпірично зʼясувати, звідки анонс приходить швидше (і потім порівняти з
WebSocket-фідом, коли буде тест-ключ).
"""
import asyncio
import base64
import datetime as dt
import json
import os
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

import aiohttp

import config

_LOGDIR = Path(__file__).parent / "logs"
_LOGDIR.mkdir(exist_ok=True)
_OUT = _LOGDIR / "probe.jsonl"
POLL = 5.0  # дефолтний інтервал
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", "lang": "en"}

# Джерела CMS — тепер це ПРЯМА дуель кешованого хоста проти некешованого.
#
# Виміряно 2026-08-14, і попереднє твердження в цьому файлі («обійти кеш не вдалось»)
# СПРОСТОВАНО: обійти не вдалось лише на www.binance.com. Там CloudFront тримає обʼєкт
# із TTL ~120с (заголовок Age росте 0→120 і скидається), а трюки з query-параметрами
# дають 400, а Cache-Control/Pragma: no-cache CDN ігнорує.
# Але ТОЙ САМИЙ ендпоінт на інших хостах Binance віддається без кешу взагалі:
#   accounts.binance.com / p2p.binance.com / launchpad.binance.com
#   → X-Cache: Miss from cloudfront на КОЖНОМУ запиті, заголовка Age немає, RTT ~260мс.
# (www.binance.info має власний кеш із TTL ~30с — не годиться.)
#
# Ендпоінт без catalogId віддає всі 140 останніх статей по 7 розділах, тому дуель
# ловить будь-який анонс, а не лише рідкі делістинги. Різниця detected_ms між двома
# джерелами на одному article_id і є доказом виграшу.
#
# poll=3с, а не 1с: проба — дослідницька, а кожен HTTPS-запит на 2 vCPU коштує джитеру
# event-loop у БОТА (перевірено: разом із fastcms 7 запитів/с підняли хвіст лагу з
# p99 1.7-4.2мс / max 15мс до p99 16мс / max 52мс). Очікувана різниця між хостами —
# десятки секунд, тож роздільної здатності 3с вистачає з головою.
_CMS_ALL = ("/bapi/apex/v1/public/apex/cms/article/list/query"
            "?type=1&pageNo=1&pageSize=20")
SOURCES = [
    {"name": "cms_www_cached", "poll": 3.0, "url": f"https://www.binance.com{_CMS_ALL}"},
    {"name": "cms_origin_fast", "poll": 3.0, "url": f"https://accounts.binance.com{_CMS_ALL}"},
]


def _articles(data):
    """ВСІ статті (не лише делістинги): щоб було з чим зіставляти WS-події по титулу.
    Делістингів мало, а міряти затримку джерела можна на будь-якому анонсі."""
    d = (data or {}).get("data") or {}
    arts = list(d.get("articles") or [])
    for c in d.get("catalogs", []) or []:
        arts += c.get("articles", []) or []
    return [(str(a.get("id") or a.get("code") or a.get("title", "")),
             a.get("title", ""), a.get("releaseDate")) for a in arts]


def _log(rec):
    try:
        with open(_OUT, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:  # noqa: BLE001
        pass
    print(rec["ts"], rec["source"], "lat=%ss" % rec["latency_sec"], rec["title"][:55], flush=True)


async def watch(session, src):
    seen, primed = set(), False
    poll = src.get("poll", POLL)
    n429, last429log = 0, time.time()
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
                elif r.status == 429:
                    n429 += 1  # стежимо за rate-limit при швидкому полінгу
        except Exception:  # noqa: BLE001
            pass
        now = time.time()
        if now - last429log > 300:  # heartbeat раз на 5хв: скільки 429 і поточний темп
            print(f"{src['name']}: poll={poll}s 429/5хв={n429}", flush=True)
            n429, last429log = 0, now
        await asyncio.sleep(poll)


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
                        # Логуємо ВСІ анонси, не лише делістинги: делістинги рідкі, а щоб
                        # зміряти затримку самого джерела, згодиться будь-яка подія. Титул
                        # потім зіставляємо зі статтею з CMS, щоб дістати releaseDate.
                        if d.get("type") == "announcement":
                            now_ms = int(time.time() * 1000)
                            det = d.get("detectedTimestampUs")
                            disp = d.get("dispatchTimestampUs")
                            _log({"ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                                  "source": "ws_cryptolisting", "title": d.get("title", ""),
                                  "ticker": d.get("ticker"), "listingType": d.get("listingType"),
                                  "ws_detected_us": det, "ws_dispatch_us": disp,
                                  # їх детект→відправка, і відправка→ми: два плеча окремо
                                  "their_hold_ms": round((disp - det) / 1000, 1)
                                  if (det and disp) else None,
                                  "transport_ms": round(now_ms - disp / 1000, 1)
                                  if disp else None,
                                  "detected_ms": now_ms, "release_ms": None,
                                  "latency_sec": None})
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


# --- Odin: нативний real-time push Binance (NATS-over-WS) ---
_ODIN_REG = "https://www.binance.com/bapi/fe/message/immed/web/register"


def _odin_register() -> dict:
    """Анонімна headless-реєстрація у push-сервісі Binance. Повертає addrs+appId+creds."""
    u = str(uuid.uuid4())
    req = urllib.request.Request(_ODIN_REG, headers={
        "User-Agent": "Mozilla/5.0", "clienttype": "web",
        "bnc-uuid": u, "cookie": f"bnc-uuid={u}"})
    d = json.loads(urllib.request.urlopen(req, timeout=15).read())
    return d.get("data") or {}


def _jwt_subjects(creds: str) -> list:
    jwt = creds.split("BEGIN NATS USER JWT-----")[1].split("-----")[0].strip()
    payload = json.loads(base64.urlsafe_b64decode(jwt.split(".")[1] + "=="))
    return payload.get("nats", {}).get("sub", {}).get("allow", [])


async def watch_odin():
    """Слухає нативний push-канал Binance (Odin/NATS). Логує КОЖНЕ повідомлення.

    РЕЗУЛЬТАТ РОЗБОРУ (важливо, щоб не витрачати час удруге): анонімна реєстрація
    видає JWT, у якому sub.allow — це ЛИШЕ приватні subjects власної сесії:
      push.inbox.sys.<appId>, push.inbox.<appId>.*,
      push.inbox.<uuid>.<appId>.>, push.inbox.immed.<uuid>.<appId>
    Публічних broadcast-топіків немає. Зате є pub.allow на push.outbox.<uuid>.<appId>.>,
    тобто клієнт спершу ПУБЛІКУЄ запит на підписку, і лише тоді сервер кладе події в
    inbox. Ми такого запиту не надсилаємо → канал законно молчить (0 повідомлень).
    Формат outbox-запиту невідомий; поки тримаємо слухача як дешевий монітор.
    Creds живуть 300с (exp-iat) → пере-реєстрація кожні ~250с."""
    try:
        import nats
    except Exception as e:  # noqa: BLE001
        print("odin: nats-py не встановлено:", e, flush=True)
        return
    while True:
        credfile = None
        nc = None
        try:
            data = await asyncio.to_thread(_odin_register)
            if not data.get("creds"):
                print("odin: реєстрація не вдалась", flush=True)
                await asyncio.sleep(10)
                continue
            subs = _jwt_subjects(data["creds"])
            f = tempfile.NamedTemporaryFile("w", suffix=".creds", delete=False)
            f.write(data["creds"]); f.close()
            credfile = f.name
            # ping_interval — інакше сервер рвав конект приблизно щоп70с (UnexpectedEOF).
            nc = await nats.connect(servers=data["addrs"], user_credentials=credfile,
                                    connect_timeout=10, max_reconnect_attempts=3,
                                    ping_interval=20, max_outstanding_pings=5)
            print(f"odin: підключено -> {nc.connected_url.netloc if nc.connected_url else '?'}, "
                  f"subjects={len(subs)}", flush=True)

            async def handler(msg):  # noqa: ANN001
                now_ms = int(time.time() * 1000)
                try:
                    body = msg.data.decode("utf-8", "replace")
                except Exception:  # noqa: BLE001
                    body = str(msg.data)
                _log({"ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                      "source": "odin_push", "subject": msg.subject,
                      "title": body[:300], "detected_ms": now_ms,
                      "release_ms": None, "latency_sec": None})

            for s in subs:
                await nc.subscribe(s, cb=handler)
            await asyncio.sleep(250)  # тримаємо ~4хв, поки creds свіжі
        except Exception as e:  # noqa: BLE001
            print("odin: помилка", type(e).__name__, str(e)[:120], flush=True)
            await asyncio.sleep(5)
        finally:
            try:
                if nc:
                    await nc.close()
            except Exception:  # noqa: BLE001
                pass
            if credfile and os.path.exists(credfile):
                os.unlink(credfile)


async def main():
    print("probe: стежу за", [s["name"] for s in SOURCES], "+ ws + tg + odin", flush=True)
    async with aiohttp.ClientSession() as s:
        await asyncio.gather(watch_ws(), watch_telegram(), watch_odin(),
                             *[watch(s, src) for src in SOURCES])


if __name__ == "__main__":
    asyncio.run(main())
