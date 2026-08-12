"""ОДНОРАЗОВИЙ локальний вхід для userbot (Telethon). Запускати У СЕБЕ НА КОМПІ.

Друкує StringSession — це «ключ» від акаунта. НЕ заливай сам скрипт із кодом/2FA
на сервер; на сервер піде ЛИШЕ готовий session-рядок (у .env як TG_SESSION).

Крок 0: pip install telethon
Крок 1: візьми api_id/api_hash тут → https://my.telegram.org (API development tools)
Крок 2: python tg_login.py  → введи api_id, api_hash, далі телефон, код із Telegram, 2FA
Крок 3: скопіюй надрукований рядок StringSession
"""
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = int(input("api_id: ").strip())
api_hash = input("api_hash: ").strip()

with TelegramClient(StringSession(), api_id, api_hash) as client:
    me = client.get_me()
    print(f"\nУвійшли як: {me.first_name} (@{me.username}) id={me.id}")
    print("\n=== ТВОЯ SESSION STRING (тримай у секреті!) ===")
    print(client.session.save())
    print("=== кінець ===")
