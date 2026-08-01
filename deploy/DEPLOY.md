# Розгортання на Oracle Cloud (Ubuntu 22.04, ARM)

## Передумови
- Створений інстанс, є його **Public IP** і завантажений **приватний SSH-ключ** (напр. `ssh-key.key`).
- На Windows зручно підключатися через вбудований `ssh` (PowerShell) або PuTTY.

## 1. Підключення по SSH
```powershell
# з папки, де лежить приватний ключ:
icacls .\ssh-key.key /inheritance:r ; icacls .\ssh-key.key /grant:r "$($env:USERNAME):(R)"
ssh -i .\ssh-key.key ubuntu@ПУБЛІЧНИЙ_IP
```

## 2. Залити код бота на сервер
Варіант А — через `scp` з локальної машини (з папки delisting-bot):
```powershell
scp -i .\ssh-key.key -r . ubuntu@ПУБЛІЧНИЙ_IP:~/delisting-bot
```
(Каталоги `.venv/` і `state.db` можна не копіювати — створяться на сервері.)

Варіант Б — через git, якщо покладеш код у приватний репозиторій.

## 3. Створити .env на сервері
```bash
cd ~/delisting-bot
cp .env.example .env
nano .env   # встав TELEGRAM_BOT_TOKEN і TELEGRAM_CHAT_ID
```

## 4. Запустити setup (venv + залежності + автозапуск)
```bash
bash deploy/setup.sh
```

## Керування сервісом
```bash
sudo systemctl status delisting-bot      # статус
sudo systemctl restart delisting-bot     # перезапуск
sudo systemctl stop delisting-bot        # зупинити
journalctl -u delisting-bot -f           # логи в реальному часі
```

Після перезавантаження сервера бот піднімається сам (enable вже зроблено в setup.sh).
