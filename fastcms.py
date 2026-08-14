"""Швидкий ВЛАСНИЙ детектор анонсів: поллінг НЕкешованих origin-хостів Binance.

ЧОМУ ЦЕ ІСНУЄ
Той самий CMS-ендпоінт на www.binance.com віддається через CloudFront із TTL ~120с
(виміряно по заголовку Age: він росте 0→120 і скидається). Тобто анонс можна
побачити із запізненням до двох хвилин — саме тому поллінг у боті був лише сторожем,
а тригером служив сторонній WebSocket.

Але той самий ендпоінт на кількох інших хостах Binance віддається БЕЗ кешу —
X-Cache: Miss from cloudfront на КОЖНОМУ запиті, заголовка Age немає взагалі:
    accounts.binance.com    RTT ~260мс з Франкфурта
    p2p.binance.com         RTT ~260мс
    launchpad.binance.com   RTT ~260мс
(www.binance.info має власний кеш із TTL ~30с — тому його тут НЕМА.)
Заголовки Cache-Control/Pragma: no-cache CDN ігнорує — перевірено, не працює.

ЯК ЦЕ ДАЄ ШВИДКІСТЬ
Крутимо хости по колу зі зсувом фази: N хостів × POLL сек = ефективний інтервал
POLL/N. Затримка детекту = POLL/(2N) + RTT. При POLL=0.6с і 3 хостах це
100мс + 260мс ≈ 360мс проти ~2.2с у сторонього фіда.

Домінує тут RTT 260мс — це плече edge→origin (origin Binance, найпевніше Токіо:
CloudFront-PoP у Франкфурті ходить по дані саме стільки). Із машини в Токіо/Сінгапурі
це плече впало б до ~10-40мс.

Бектест (100 пар «анонс+символ» на тік-даних Bybit) показав, чому цього достатньо:
ринок починає рух аж на +1.5с (p10) / +2.9с (медіана) від releaseDate, а середній
PnL плоский на Δ≤2с і завалюється після 3с. Тобто ~0.4с ставить нас перед ринком.
"""
import asyncio
import time

import aiohttp

import binance_watcher as bw
import config
import fastjson
import logbook as log
import storage

# НЕкешовані хости (перевірено: X-Cache=Miss завжди, Age відсутній).
HOSTS = ["accounts.binance.com", "p2p.binance.com", "launchpad.binance.com"]
_PATH = ("/bapi/apex/v1/public/apex/cms/article/list/query"
         "?type=1&catalogId=161&pageNo=1&pageSize=20")
_HDRS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"),
    "Accept": "application/json",
    "clienttype": "web",
    "lang": "en",
}

_seen: set[str] = set()          # заявлені article_id (синхронний дедуп)
# (article_id, host) — щоб зафіксувати, коли КОЖЕН хост уперше побачив статтю, а не лише
# той, хто виграв заявку. Без цього парне порівняння двох машин ламається: якщо в
# Сінгапурі першим спрацював accounts, а у Франкфурті p2p, ми порівнювали б різні хости
# з різними RTT. Подій 8-9 на рік, тому кожна має дати однозначні дані.
_sighted: set[tuple[str, str]] = set()
_on_event = None
_stats = {"polls": 0, "errors": 0, "new": 0, "by_host": {}, "polls_by_host": {}}


def set_handler(cb) -> None:
    """cb(ev: bw.DelistingEvent, latency_sec: float | None, host: str) — може бути async."""
    global _on_event
    _on_event = cb


def claim(article_id: str) -> bool:
    """СИНХРОННО: True якщо цей анонс бачимо вперше. Між перевіркою і заявкою немає
    await — інакше два джерела (fastcms і поллінг-сторож) відкрили б угоду двічі."""
    if article_id in _seen:
        return False
    _seen.add(article_id)
    return True


def seen_count() -> int:
    return len(_seen)


def stats() -> dict:
    return dict(_stats, seen=len(_seen), hosts=len(HOSTS))


def prime() -> int:
    """Підтягує вже бачені id з БД, щоб після рестарту не сипати старими анонсами."""
    try:
        for aid in storage.seen_ids():
            _seen.add(aid)
    except Exception:  # noqa: BLE001
        log.exception("fastcms: prime не вдався")
    return len(_seen)


def _articles(data: dict):
    d = data.get("data") or {}
    if isinstance(d.get("articles"), list):
        yield from d["articles"]
    for cat in d.get("catalogs") or []:
        yield from cat.get("articles") or []


async def _handle(art: dict, host: str) -> None:
    title = art.get("title", "")
    if not bw._DELIST_HINT.search(title):
        return
    aid = str(art.get("id") or art.get("code") or title)
    now_ms = int(time.time() * 1000)
    release_ms = art.get("releaseDate")
    latency = round((now_ms - release_ms) / 1000, 2) if release_ms else None
    # Замір по кожному хосту окремо — лише для СВІЖИХ статей (10хв), щоб прайм і
    # старий архів не сипали в лог. Ключ до порівняння двох машин на одній новині.
    sight = (aid, host)
    if sight not in _sighted and release_ms and (now_ms - release_ms) < 600_000:
        if len(_sighted) > 10_000:
            _sighted.clear()
        _sighted.add(sight)
        log.event("fastcms_sighting", article_id=aid, host=host, release_ms=release_ms,
                  detected_ms=now_ms, detect_latency_sec=latency, title=title[:80])
    if not claim(aid):
        return
    code = art.get("code", "")
    ev = bw.DelistingEvent(
        article_id=aid, title=title, tickers=bw.extract_tickers(title),
        url=f"https://www.binance.com/en/support/announcement/{code}" if code else "",
        category=bw.classify(title), release_ms=release_ms,
    )
    _stats["new"] += 1
    _stats["by_host"][host] = _stats["by_host"].get(host, 0) + 1
    log.event("fastcms_new", article_id=aid, host=host, category=ev.category,
              tickers=ev.tickers, title=title, release_ms=release_ms,
              detected_ms=now_ms, detect_latency_sec=latency,
              actionable=ev.actionable)
    # У БД — у фоні: гарячий шлях не чекає на SQLite.
    asyncio.get_running_loop().run_in_executor(None, storage.mark_seen, aid, title)
    if _on_event:
        res = _on_event(ev, latency, host)
        if asyncio.iscoroutine(res):
            await res


async def _poll_host(host: str, phase: float) -> None:
    url = f"https://{host}{_PATH}"
    timeout = aiohttp.ClientTimeout(total=config.FASTCMS_TIMEOUT_SEC)
    conn = aiohttp.TCPConnector(limit=2, ttl_dns_cache=600, force_close=False,
                                enable_cleanup_closed=True)
    await asyncio.sleep(phase)
    backoff = 0.0
    async with aiohttp.ClientSession(connector=conn, timeout=timeout,
                                     headers=_HDRS) as sess:
        while True:
            try:
                async with sess.get(url) as r:
                    body = await r.read()
                    if r.status != 200:
                        raise RuntimeError(f"HTTP {r.status}")
                data = fastjson.loads(body)
                _stats["polls"] += 1
                _stats["polls_by_host"][host] = _stats["polls_by_host"].get(host, 0) + 1
                backoff = 0.0
                for art in _articles(data):
                    await _handle(art, host)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                _stats["errors"] += 1
                # Не спамимо лог на кожному збою — лише кожен 20-й.
                if _stats["errors"] % 20 == 1:
                    log.event("fastcms_error", host=host, err=f"{type(e).__name__}: {e}",
                              errors=_stats["errors"], polls=_stats["polls"])
                backoff = min(30.0, (backoff or config.FASTCMS_POLL_SEC) * 2)
            if backoff:
                await asyncio.sleep(backoff)
                continue
            # Фазу тримаємо по СТІННОМУ годиннику (UTC), а не від моменту старту процесу.
            # Дає дві речі:
            #  1) ефективний інтервал строго POLL/N і не «поїде» після повільного запиту
            #     (у Франкфурті так набігало 218мс замість 200мс: p2p/launchpad інколи
            #     перевищували 550мс і цикл розтягувався);
            #  2) дві машини опитують у ТІ САМІ моменти, тому на живому анонсі різниця
            #     детекту дорівнює різниці RTT, а не лотереї «кому пощастило з циклом».
            #     Без цього шум ±POLL змазав би замір, а подій усього 8-9 на рік.
            now = time.time()
            nxt = (now // config.FASTCMS_POLL_SEC) * config.FASTCMS_POLL_SEC + phase
            while nxt <= now:
                nxt += config.FASTCMS_POLL_SEC
            await asyncio.sleep(nxt - now)


async def _prime_fetch() -> int:
    """ПЕРШИЙ запуск із порожньою БД: позначаємо наявні анонси як бачені БЕЗ сповіщень
    і без угод — інакше бот вистрелив би по всіх 20 старих статтях одразу."""
    n = 0
    timeout = aiohttp.ClientTimeout(total=config.FASTCMS_TIMEOUT_SEC * 3)
    # Кілька спроб: якщо прайм не вдасться, перший же опит вважатиме всі 20 наявних
    # статей новими і насипле 20 сповіщень (угоди відсіє MAX_SIGNAL_AGE_SEC, але шум лишиться).
    for attempt, host in enumerate((HOSTS * 3)[:3], 1):
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=_HDRS) as s:
                async with s.get(f"https://{host}{_PATH}") as r:
                    if r.status != 200:
                        raise RuntimeError(f"HTTP {r.status}")
                    data = fastjson.loads(await r.read())
            for art in _articles(data):
                aid = str(art.get("id") or art.get("code") or art.get("title", ""))
                if claim(aid):
                    storage.mark_seen(aid, art.get("title", ""))
                    n += 1
            return n
        except Exception as e:  # noqa: BLE001
            log.event("fastcms_prime_retry", attempt=attempt, host=host,
                      err=f"{type(e).__name__}: {e}")
            await asyncio.sleep(1.5 * attempt)
    log.info("fastcms: прайм не вдався 3 рази — можливі сповіщення по старих анонсах")
    return n


async def _heartbeat(period: float = 300.0) -> None:
    """Без цього неможливо відрізнити «поллінг іде 5/с» від «задача тихо померла»:
    детектор молчить тижнями, бо делістингів 8-9 на рік. Логуємо ФАКТИЧНИЙ темп."""
    last, t_last = 0, time.perf_counter()
    while True:
        await asyncio.sleep(period)
        now = time.perf_counter()
        rate = (_stats["polls"] - last) / (now - t_last) if now > t_last else 0.0
        log.event("fastcms_heartbeat", polls=_stats["polls"], errors=_stats["errors"],
                  new=_stats["new"], rate_per_sec=round(rate, 2), seen=len(_seen),
                  by_host=_stats["polls_by_host"])
        last, t_last = _stats["polls"], now


async def run() -> None:
    """Піднімає по задачі на хост зі зсувом фази (рівномірне покриття інтервалу)."""
    if not config.FASTCMS:
        log.info("fastcms вимкнено (FASTCMS=0)")
        return
    hosts = HOSTS[:max(1, config.FASTCMS_HOSTS)]
    primed = prime()
    if primed == 0:
        # Поллінг-сторож праймиться паралельно і читає ТОЙ САМИЙ ендпоінт, тож він може
        # заявити всі статті раніше за нас — тоді claimed=0 при 20 бачених. Це нормально
        # (жоден із двох шляхів не сповіщає під час прайму), тому логуємо обидва числа,
        # інакше «0» виглядає як зламаний прайм.
        claimed = await _prime_fetch()
        primed = seen_count()
        log.info(f"fastcms: перший запуск — заявлено {claimed}, всього бачених {primed} "
                 f"(решту міг заявити поллінг-сторож)")
    step = config.FASTCMS_POLL_SEC / len(hosts)
    log.event("fastcms_start", hosts=hosts, poll_sec=config.FASTCMS_POLL_SEC,
              effective_gap_ms=round(step * 1000), primed_seen=primed,
              trade=config.FASTCMS_TRADE)
    await asyncio.gather(_heartbeat(),
                         *(_poll_host(h, i * step) for i, h in enumerate(hosts)))
