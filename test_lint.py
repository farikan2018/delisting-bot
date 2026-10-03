"""Статична перевірка на невизначені імена.

Причина існування: 2026-08-15 я виніс спільну `_fill_details` з `_settle_fill`,
переніс туди `oid = order.get("id")`, а посилання на `oid` лишив у старій
функції. Вийшов гарантований NameError рівно на тій гілці, яка спрацьовує,
коли Bybit не встиг проіндексувати угоду — тобто на реальному сценарії, який
уже траплявся в бою. Ані 54 модульні тести, ані наскрізний прогін цього не
побачили: гілка виконується лише у фоновій задачі, а `fire()` ковтає винятки.

Такий клас багів (перенесли код — лишили посилання) статичний аналіз ловить
миттєво, тому тепер він у тестах, а не в моїй памʼяті.

Запуск: python test_lint.py
"""
import sys
from pathlib import Path

try:
    from pyflakes import api, reporter
except ImportError:
    print("pyflakes не встановлений: pip install pyflakes")
    sys.exit(2)

# Модулі бота. Скрипти-дослідження (study*.py) навмисно поза перевіркою —
# вони одноразові й їхні невживані змінні нікого не цікавлять.
TARGETS = ["main.py", "executor.py", "exchange.py", "strategy.py", "storage.py",
           "config.py", "fastcms.py", "binance_watcher.py", "pricecache.py",
           "dumpwatch.py", "runtime.py", "logbook.py", "telegram_client.py",
           "admin.py", "probe.py",
           # Додано 2026-10-03. tgfeed.py не був у списку — а це рівно той модуль,
           # який мовчки лежав 34 доби. Модуль поза лінтом ризикує повторити
           # історію з NameError заради якої цей тест і писався.
           "tgfeed.py", "wsfeed.py", "watchdog.py", "alerts.py", "preflight.py"]

# Що вважаємо фатальним. Невживані імпорти й змінні — шум, не помилка.
FATAL = ("undefined name", "undefined local", "syntax error",
         "redefinition of unused", "f-string is missing placeholders")


class Collect:
    def __init__(self):
        self.lines = []

    def unexpectedError(self, filename, msg):
        self.lines.append(f"{filename}: {msg}")

    def syntaxError(self, filename, msg, lineno, offset, text):
        self.lines.append(f"{filename}:{lineno}: syntax error: {msg}")

    def flake(self, message):
        self.lines.append(str(message))


here = Path(__file__).parent
rep = Collect()
checked = 0
for t in TARGETS:
    p = here / t
    if not p.exists():
        continue
    checked += 1
    api.checkPath(str(p), rep)

fatal = [ln for ln in rep.lines if any(f in ln.lower() for f in FATAL)]
noise = len(rep.lines) - len(fatal)

print(f"=== pyflakes: {checked} модулів, {len(rep.lines)} зауважень "
      f"({len(fatal)} фатальних, {noise} шуму) ===")
for ln in fatal:
    print(f"  ФАТАЛЬНО  {ln}")

if fatal:
    print("\nПРОВАЛЕНО: є невизначені імена або синтаксичні помилки")
    sys.exit(1)
print("\nЛІНТ OK — невизначених імен нема")
