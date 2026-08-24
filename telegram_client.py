"""Мінімальний Telegram-клієнт: sendMessage + getUpdates (для пошуку chat_id).

ЧОМУ ТУТ ТЕПЕР ЛІЧИЛЬНИКИ І ПОДІЇ, А НЕ print (2026-08-24). Токен бота був
відкликаний, у `.env` лишився мертвий, і бот перестав мати можливість надіслати
хоч що-небудь — включно з «🟢 ВІДКРИТО ШОРТ» і «🚨 ПЕРЕВІР ВРУЧНУ». У логах при
цьому було НУЛЬ рядків про проблему: збої йшли лише в print(), тобто в bot.log і
нікуди більше, а `events.jsonl` — єдина поверхня, на якій ми щось рахуємо, —
про них не знала взагалі. Канал сповіщень мовчить саме тоді, коли він потрібен.
Тепер кожен збій це подія `tg_send_failed` плюс лічильники в stats().
"""
import datetime as dt

import aiohttp

import config
import logbook as log

_API = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"
_session: aiohttp.ClientSession | None = None

_stats = {"sent": 0, "failed": 0, "last_error": None, "last_error_ts": None,
          "auth_ok": None}


def stats() -> dict:
    """Знімок для /status і добового зведення."""
    return dict(_stats)


def _fail(reason: str, **extra) -> None:
    _stats["failed"] += 1
    _stats["last_error"] = reason[:200]
    _stats["last_error_ts"] = dt.datetime.now(dt.timezone.utc).isoformat()
    # Перші кілька і далі рідше: канал може лежати годинами, і забивати
    # events.jsonl однаковими рядками сенсу немає.
    if _stats["failed"] <= 5 or _stats["failed"] % 50 == 0:
        log.event("tg_send_failed", reason=reason[:200], n=_stats["failed"], **extra)


def _sess() -> aiohttp.ClientSession:
    """Одна персистентна сесія на процес. Раніше кожне сповіщення підіймало новий
    TLS-конект до api.telegram.org — а сповіщення летять одразу після ордера, тобто
    саме тоді, коли лупу треба займатися позицією, а не рукостисканнями."""
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
    return _session


async def verify() -> bool:
    """getMe на старті: живий токен чи ні. Без цього мертвий токен помітно лише
    тоді, коли не прийшло сповіщення про вже відкриту реальну позицію."""
    if not config.TELEGRAM_BOT_TOKEN:
        _stats["auth_ok"] = False
        return False
    try:
        async with _sess().get(f"{_API}/getMe") as r:
            data = await r.json()
        ok = bool(data.get("ok"))
        _stats["auth_ok"] = ok
        if ok:
            log.event("tg_auth_ok", bot=(data.get("result") or {}).get("username"))
        else:
            log.error("Telegram: токен НЕ ПРАЦЮЄ (%s %s) — сповіщень НЕ БУДЕ"
                      % (data.get("error_code"), data.get("description")))
            log.event("tg_auth_failed", code=data.get("error_code"),
                      desc=str(data.get("description"))[:160])
        return ok
    except Exception as e:  # noqa: BLE001
        _stats["auth_ok"] = False
        log.event("tg_auth_failed", err=f"{type(e).__name__}: {str(e)[:120]}")
        return False


async def send_message(text: str, chat_id: str | None = None) -> bool:
    chat_id = chat_id or config.TELEGRAM_CHAT_ID
    if not config.TELEGRAM_BOT_TOKEN or not chat_id:
        _fail("token/chat_id не задані", have_token=bool(config.TELEGRAM_BOT_TOKEN),
              have_chat=bool(chat_id))
        return False
    try:
        async with _sess().post(
            f"{_API}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
        ) as r:
            data = await r.json()
            if not data.get("ok"):
                _fail(f"api: {data.get('error_code')} {data.get('description')}",
                      code=data.get("error_code"))
                return False
            _stats["sent"] += 1
            _stats["auth_ok"] = True
            return True
    except Exception as e:  # noqa: BLE001
        _fail(f"{type(e).__name__}: {str(e)[:150]}")
        return False


async def get_updates(offset: int | None = None, timeout: int = 0) -> list[dict]:
    """Апдейти. offset — з якого update_id читати; timeout>0 → long-poll (сек)."""
    params: dict = {}
    if offset is not None:
        params["offset"] = offset
    if timeout:
        params["timeout"] = timeout
    # Спільна сесія, а не нова на кожен опит: довгий поллінг раз на 25с інакше
    # щоразу підіймав новий TLS-конект, і кожен обрив давав ERROR-трейсбек.
    async with _sess().get(
        f"{_API}/getUpdates", params=params,
        timeout=aiohttp.ClientTimeout(total=timeout + 15),
    ) as r:
        data = await r.json()
        return data.get("result", []) if data.get("ok") else []
