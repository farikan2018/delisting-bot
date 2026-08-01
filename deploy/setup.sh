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

echo "==> Генерація systemd-сервісу під поточного користувача ($USER)..."
# Не використовуємо статичний файл із хардкодом 'ubuntu' — на GCP користувач інший.
sudo tee /etc/systemd/system/delisting-bot.service >/dev/null <<EOF
[Unit]
Description=Delisting Short Bot (Phase 1: notifications)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$APP_DIR
Environment=PYTHONUTF8=1
Environment=PYTHONUNBUFFERED=1
ExecStart=$APP_DIR/.venv/bin/python $APP_DIR/main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable delisting-bot
sudo systemctl restart delisting-bot

echo "==> Готово. Статус:"
sudo systemctl --no-pager status delisting-bot || true
echo
echo "Логи в реальному часі:  journalctl -u delisting-bot -f"
