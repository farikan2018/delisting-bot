"""Мінімальний Telegram-клієнт: sendMessage + getUpdates (для пошуку chat_id)."""
import aiohttp

import config

_API = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"
_session: aiohttp.ClientSession | None = None


def _sess() -> aiohttp.ClientSession:
    """Одна персистентна сесія на процес. Раніше кожне сповіщення підіймало новий
    TLS-конект до api.telegram.org — а сповіщення летять одразу після ордера, тобто
    саме тоді, коли лупу треба займатися позицією, а не рукостисканнями."""
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
    return _session


async def send_message(text: str, chat_id: str | None = None) -> bool:
    chat_id = chat_id or config.TELEGRAM_CHAT_ID
    if not config.TELEGRAM_BOT_TOKEN or not chat_id:
        print(f"[telegram] chat_id/token не задані, повідомлення не надіслано:\n{text}")
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
                print(f"[telegram] помилка: {data}")
            return bool(data.get("ok"))
    except Exception as e:  # noqa: BLE001
        print(f"[telegram] виняток: {e}")
        return False


async def get_updates(offset: int | None = None, timeout: int = 0) -> list[dict]:
    """Апдейти. offset — з якого update_id читати; timeout>0 → long-poll (сек)."""
    params: dict = {}
    if offset is not None:
        params["offset"] = offset
    if timeout:
        params["timeout"] = timeout
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"{_API}/getUpdates", params=params,
            timeout=aiohttp.ClientTimeout(total=timeout + 15),
        ) as r:
            data = await r.json()
            return data.get("result", []) if data.get("ok") else []
