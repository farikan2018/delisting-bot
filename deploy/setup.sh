#!/usr/bin/env bash
# Розгортання Delisting-бота на свіжій Ubuntu 22.04 (Oracle Cloud ARM).
# Запускати НА СЕРВЕРІ з каталогу ~/delisting-bot:  bash deploy/setup.sh
set -euo pipefail

APP_DIR="$HOME/delisting-bot"
cd "$APP_DIR"

echo "==> Оновлення системи та встановлення Python..."
sudo apt-get update -y
sudo apt-get install -y python3 python3-venv python3-pip

echo "==> Віртуальне оточення + залежності..."
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip
./.venv/bin/pip install -r requirements.txt

if [ ! -f .env ]; then
  echo "!! .env не знайдено. Створи його з токеном і chat_id (див. .env.example)."
  exit 1
fi

echo "==> Встановлення systemd-сервісу (автозапуск 24/7)..."
sudo cp deploy/delisting-bot.service /etc/systemd/system/delisting-bot.service
sudo systemctl daemon-reload
sudo systemctl enable delisting-bot
sudo systemctl restart delisting-bot

echo "==> Готово. Статус:"
sudo systemctl --no-pager status delisting-bot || true
echo
echo "Логи в реальному часі:  journalctl -u delisting-bot -f"
