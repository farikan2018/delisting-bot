# === Google Cloud (Ubuntu 22.04) — через браузерний SSH + GitHub ===
#
# Відкрий SSH до інстансу (кнопка SSH у Compute Engine → VM instances)
# і встав ЦЕЙ блок повністю:
#
# ```bash
# sudo apt-get update -y && sudo apt-get install -y git python3 python3-venv python3-pip
# git clone https://github.com/farikan2018/delisting-bot.git ~/delisting-bot
# cd ~/delisting-bot
# cat > .env <<'EOF'
# TELEGRAM_BOT_TOKEN=8694704608:AAEk7EXoMx5RwUaf_ScZRHW0rUK0Tv1f8k4
# TELEGRAM_CHAT_ID=641324432
# POLL_INTERVAL=3
# EOF
# bash deploy/setup.sh
# ```
#
# setup.sh поставить venv, залежності і автозапуск через systemd.
# Перевірка логів:  journalctl -u delisting-bot -f
#
# Оновлення коду згодом:
# ```bash
# cd ~/delisting-bot && git pull && sudo systemctl restart delisting-bot
# ```
#
# ---------------------------------------------------------------------

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
