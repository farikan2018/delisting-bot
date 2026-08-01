"""Мінімальний Telegram-клієнт: sendMessage + getUpdates (для пошуку chat_id)."""
import aiohttp

import config

_API = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"


async def send_message(text: str, chat_id: str | None = None) -> bool:
    chat_id = chat_id or config.TELEGRAM_CHAT_ID
    if not config.TELEGRAM_BOT_TOKEN or not chat_id:
        print(f"[telegram] chat_id/token не задані, повідомлення не надіслано:\n{text}")
        return False
    async with aiohttp.ClientSession() as s:
        try:
            async with s.post(
                f"{_API}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                data = await r.json()
                if not data.get("ok"):
                    print(f"[telegram] помилка: {data}")
                return bool(data.get("ok"))
        except Exception as e:  # noqa: BLE001
            print(f"[telegram] виняток: {e}")
            return False


async def get_updates() -> list[dict]:
    """Повертає останні апдейти — використовуємо, щоб дізнатися chat_id."""
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"{_API}/getUpdates", timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            data = await r.json()
            return data.get("result", []) if data.get("ok") else []
