"""Сторож живості: перевіряє, що бот справді здатен відпрацювати делістинг.

ЧОМУ ЦЕ ІСНУЄ (2026-10-03). Повний спот-делістинг Binance буває РІДКО — за
історією анонсів приблизно раз на 3-6 тижнів (2026-09-10 USDP, 2026-08-20
ICX/SCRT/STORJ, 2026-08-03, 2026-06-26, 2026-06-05, 2026-05-13). Між подіями
бот місяцями нічого не робить. Саме тому тиха аварія тут коштує не хвилин, а
МІСЯЦЯ: наступний шанс буде нескоро, і дізнатись про поломку рівно в момент
анонсу — означає дізнатись запізно.

Реальний випадок, заради якого це написано: телеграм-фід помер 2026-08-30 і
лежав 34 доби. Усе виглядало здоровим — процес живий, `cap_tg_feed` true,
`fast_triggers` містив "tg_feed", у стартовому повідомленні писало «швидкий
тригер є». Насправді швидкого тригера не було жодного, і детект упав би на
поллінг із заміряною медіаною 46с, тобто з +26% на угоду до +5%.

ПРИНЦИП. Прапорці можливостей (`cap_*`) кажуть лише те, що КОНФІГ на місці.
Сторож питає інше: чи є ДОКАЗ, що шлях працює — лічильник, який зріс, кадр,
який прийшов, баланс, який біржа підтвердила підписаним викликом. Конфіг без
доказу тут вважається непрацюючим.

Усі перевірки читають ПАМ'ЯТЬ, уже оновлену іншими циклами. Сторож навмисно не
ходить сам у мережу: він не має права стати ще одним джерелом затримки на
машині, де рахують мілісекунди.
"""
import asyncio
import shutil
import time
from pathlib import Path

import alerts
import config
import exchange
import fastcms
import logbook as log
import pricecache
import storage
import telegram_client as tg
import tgfeed
import wsfeed

_START_TS = time.time()
_stats = {"rounds": 0, "failing": 0, "last_round_ts": None}
_META_LAST_NEW = "watchdog_last_new_ms"
_last_new_ms = 0


def _ok(name: str, ok, text: str, critical: bool = False) -> dict:
    return {"name": name, "ok": ok, "text": text, "critical": critical}


def _fmt_age(sec) -> str:
    """Тривалість словами. None -> порожньо: формулювання добирає викликач."""
    if sec is None:
        return ""
    if sec < 90:
        return str(int(sec)) + "с"
    if sec < 5400:
        return str(int(sec // 60)) + " хв"
    if sec < 172800:
        return str(round(sec / 3600, 1)) + " год"
    return str(round(sec / 86400, 1)) + " діб"


def _ago(sec) -> str:
    """«3 хв тому» або «ЖОДНОГО РАЗУ» — щоб у повідомленні не виникало
    «останній ніколи тому»."""
    return (_fmt_age(sec) + " тому") if sec is not None else "ЖОДНОГО РАЗУ"


def _check_fast_trigger() -> dict:
    ws_ok, tg_ok = wsfeed.healthy(), tgfeed.healthy()
    ws_s, tg_s = wsfeed.stats(), tgfeed.stats()
    if ws_ok or tg_ok:
        parts = []
        if ws_ok:
            parts.append("WS: " + str(ws_s["frames"]) + " кадрів, останній "
                         + _ago(ws_s["last_frame_age_sec"]))
        if tg_ok:
            parts.append("TG: " + str(tg_s["msgs"]) + " повідомлень, останнє "
                         + _ago(tg_s["last_msg_age_sec"]))
        return _ok("fast_trigger", True, "; ".join(parts), critical=True)
    why = []
    if not config.CL_WS_KEY:
        why.append("WS: ключа нема")
    elif ws_s["frames"] == 0:
        why.append("WS: підключень " + str(ws_s["connects"])
                   + ", але жодного кадру за весь час процесу")
    if tg_s["fatal"]:
        why.append("TG: сесію відкликано (" + str(tg_s["fatal"]) + ")")
    elif not (config.TG_API_ID and config.TG_API_HASH and config.TG_SESSION):
        why.append("TG: кредів нема")
    elif tg_s["msgs"] == 0:
        why.append("TG: підключень " + str(tg_s["connects"]) + ", повідомлень 0")
    return _ok("fast_trigger", False,
               "ЖОДНОГО швидкого тригера. " + "; ".join(why)
               + ". Детект впаде на поллінг (медіана 46с): ~+5% на угоду замість ~+26%.",
               critical=True)


def _check_fastcms() -> list[dict]:
    s = fastcms.stats()
    out = []
    if not config.FASTCMS:
        out.append(_ok("fastcms", None, "вимкнено конфігом"))
        return out
    poll_age = s["last_poll_age_sec"]
    out.append(_ok("fastcms_polling", poll_age is not None and poll_age < 60,
                   "опитів " + str(s["polls"]) + ", останній " + _ago(poll_age)
                   + ", збоїв " + str(s["errors"]), critical=True))
    # Головне тут — не «HTTP 200», а «у відповіді справді є статті». Якщо Binance
    # змінить формат, polls і далі ростимуть, а ми розбиратимемо порожнечу.
    art_age = s["last_article_age_sec"]
    out.append(_ok("cms_schema", art_age is not None and art_age < 300,
                   "останню статтю розібрано " + _ago(art_age)
                   + ("" if art_age is not None and art_age < 300
                      else " — ендпоінт віддає відповідь, але статей у ній нема"),
                   critical=True))
    return out


def _check_announcement_flow() -> dict:
    """Чи бачив ланцюг детекту хоч один НОВИЙ анонс останнім часом.

    Переживає рестарт через storage.meta: інакше лічильник обнулявся б при кожному
    перезапуску і сторож ніколи б не нагромадив достатньо тиші, щоб забити на сполох.
    """
    global _last_new_ms
    s = fastcms.stats()
    if s["last_new_ms"] and s["last_new_ms"] > _last_new_ms:
        _last_new_ms = s["last_new_ms"]
        try:
            storage.meta_set(_META_LAST_NEW, str(_last_new_ms))
        except Exception:  # noqa: BLE001
            pass
    if not _last_new_ms:
        return _ok("announcement_flow", None, "ще не бачили жодного нового анонсу")
    age = time.time() - _last_new_ms / 1000
    return _ok("announcement_flow", age < config.ANNOUNCE_SILENCE_SEC,
               "останній новий анонс " + _fmt_age(age) + " тому"
               + ("" if age < config.ANNOUNCE_SILENCE_SEC
                  else " — каталог делістингів зазвичай поповнюється щотижня, "
                       "така тиша означає поломку детекту, а не затишшя на ринку"))


def _check_money() -> list[dict]:
    out = []
    if not (config.BYBIT_API_KEY and config.BYBIT_API_SECRET):
        return [_ok("bybit_keys", None, "ключів нема — торгівля неможлива")]
    # Кеш наповнює keepalive ПІДПИСАНИМ fetch_balance раз на KEEPALIVE_SEC. Якщо
    # значення нема або воно застаріле — підписаний шлях мертвий, тобто ключ або
    # прострочений, або відв'язаний від IP. Публічний пінг цього не показав би.
    free = exchange.cached_free_balance("bybit", max_age=max(300.0,
                                                             config.KEEPALIVE_SEC * 6))
    out.append(_ok("bybit_keys", free is not None,
                   "вільна маржа $" + str(round(free, 2)) if free is not None
                   else "підписаний виклик до Bybit не проходить — ключ мертвий "
                        "або відв'язаний від IP (BALANCE_GUARD тихо не працює)",
                   critical=True))
    if free is not None:
        need = config.POSITION_MARGIN_USDT
        slots = int(free // need) if need > 0 else 0
        out.append(_ok("balance", free >= need,
                       "$" + str(round(free, 2)) + " вільної маржі = " + str(slots)
                       + " з " + str(config.MAX_CONCURRENT) + " позицій по $"
                       + str(round(need, 2))
                       + ("" if free >= need else " — не вистачить НАВІТЬ НА ОДНУ"),
                       critical=True))
    return out


def _check_plumbing() -> list[dict]:
    out = []
    ts = tg.stats()
    out.append(_ok("telegram", ts["auth_ok"] is not False,
                   "надіслано " + str(ts["sent"]) + ", збоїв " + str(ts["failed"])
                   + (("; остання: " + str(ts["last_error"])[:80]) if ts["failed"] else "")))
    pc = pricecache.stats()
    age = pc["last_update_age_sec"]
    out.append(_ok("pricecache", age is not None and age < 120,
                   str(pc["symbols"]) + " символів, оновлено " + _ago(age) + ("" if age is not None and age < 120
                                else " — вихід за стратегією осліп")))
    lb = log.stats()
    out.append(_ok("log_writes", lb["write_fails"] == 0,
                   "подій " + str(lb["events"]) + ", помилок " + str(lb["errors"])
                   + ", збоїв запису " + str(lb["write_fails"])))
    try:
        du = shutil.disk_usage(str(Path(__file__).parent))
        free_mb = du.free // (1024 * 1024)
        out.append(_ok("disk", free_mb > config.DISK_FREE_MIN_MB,
                       str(free_mb) + " МБ вільно"))
    except Exception as e:  # noqa: BLE001
        out.append(_ok("disk", None, "не вдалось перевірити: " + type(e).__name__))
    return out


def health() -> list[dict]:
    """Усі перевірки одним знімком. Чисто з пам'яті, без мережі."""
    checks = [_check_fast_trigger()]
    checks += _check_fastcms()
    checks.append(_check_announcement_flow())
    checks += _check_money()
    checks += _check_plumbing()
    return checks


def summary() -> dict:
    checks = health()
    bad = [c for c in checks if c["ok"] is False]
    return {
        "ok": not bad,
        "failing": [c["name"] for c in bad],
        "critical_failing": [c["name"] for c in bad if c["critical"]],
        "checks": {c["name"]: c["ok"] for c in checks},
    }


def report_text() -> str:
    """Людський рядок для /status і добового зведення."""
    icon = {True: "✅", False: "❌", None: "➖"}
    lines = []
    for c in health():
        lines.append(icon[c["ok"]] + " <b>" + c["name"] + "</b>: " + c["text"])
    return chr(10).join(lines)


async def run() -> None:
    """Цикл сторожа. Перші WATCHDOG_GRACE_SEC лише спостерігає: на старті кеші
    порожні, і без паузи кожен рестарт сипав би хибними тривогами."""
    global _last_new_ms
    try:
        _last_new_ms = int(storage.meta_get(_META_LAST_NEW) or 0)
    except Exception:  # noqa: BLE001
        _last_new_ms = 0
    while True:
        await asyncio.sleep(config.WATCHDOG_SEC)
        try:
            checks = health()
            _stats["rounds"] += 1
            _stats["last_round_ts"] = time.time()
            bad = [c for c in checks if c["ok"] is False]
            _stats["failing"] = len(bad)
            log.event("watchdog", rounds=_stats["rounds"], failing=len(bad),
                      **{c["name"]: c["ok"] for c in checks})
            if time.time() - _START_TS < config.WATCHDOG_GRACE_SEC:
                continue
            for c in checks:
                key = "Сторож: " + c["name"]
                if c["ok"] is False:
                    await alerts.raise_alert(
                        key, c["text"],
                        cooldown_sec=(6 * 3600 if c["critical"] else 24 * 3600))
                elif c["ok"] is True:
                    await alerts.clear_alert(key, c["text"])
        except Exception:  # noqa: BLE001
            log.exception("watchdog: раунд впав")
