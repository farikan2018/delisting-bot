"""Дізнатися свій chat_id: спочатку напиши боту будь-що в Telegram, потім запусти це."""
import asyncio

import telegram_client as tg


async def main() -> None:
    updates = await tg.get_updates()
    if not updates:
        print("Апдейтів немає. Напиши боту в Telegram будь-яке повідомлення і запусти ще раз.")
        return
    seen = {}
    for u in updates:
        msg = u.get("message") or u.get("edited_message") or {}
        chat = msg.get("chat") or {}
        if chat.get("id") is not None:
            seen[chat["id"]] = chat.get("username") or chat.get("first_name") or "?"
    print("Знайдені chat_id:")
    for cid, who in seen.items():
        print(f"  chat_id = {cid}   (від: {who})")
    print("\nВізьми потрібний і встав у .env → TELEGRAM_CHAT_ID=")


if __name__ == "__main__":
    asyncio.run(main())
