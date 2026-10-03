"""Централізоване логування: людський лог (bot.log) + структурований JSONL (events.jsonl).

events.jsonl — по одному JSON на рядок, зручно аналізувати потім:
  grep / jq / pandas. Кожен запис має ts (UTC ISO) і kind.

ЧОМУ error/exception ТЕПЕР ТЕЖ ПИШУТЬ У events.jsonl (2026-08-24). Раніше вони
йшли ЛИШЕ в logger, тобто в bot.log і stdout. events.jsonl — єдина поверхня, на
якій ми рахуємо латентність і PnL, — не могла відповісти навіть на питання «чи
щось падало». А падати є чому: `executor._safe()` це ЄДИНИЙ запис про смерть
фонової задачі (а всі `fire()` крутяться саме там: _settle_and_arm, _adopt_orphan,
arm_exchange_stop, надсилання в Telegram), і `main._supervise` це єдиний запис про
цикл, який перезапускається щотри секунди хоч і тиждень. Саме так тиждень тому
вижив NameError на гілці, яка виконується лише у fire().
"""
import datetime as dt
import json
import logging
import os
import sys
import traceback
from logging.handlers import RotatingFileHandler
from pathlib import Path

_BASE = Path(__file__).parent
# BOT_LOG_DIR дає тестам власну теку логів. Причина конкретна: 2026-10-03
# test_wsfeed.py, запущений на БОЙОВІЙ машині, записав 15 синтетичних подій
# ws_frame прямо в events.jsonl. А саме цей файл відповідає на питання «чи фід
# коли-небудь віддав хоч один кадр» — тобто тест отруїв єдине джерело правди
# про те, що він перевіряє. Події мають бути або з бою, або з тесту, і ніколи
# в одному файлі.
_LOGDIR = Path(os.environ.get("BOT_LOG_DIR") or (_BASE / "logs"))
_LOGDIR.mkdir(parents=True, exist_ok=True)
_EVENTS = _LOGDIR / "events.jsonl"
NEWLINE = chr(10)

logger = logging.getLogger("delisting")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = RotatingFileHandler(_LOGDIR / "bot.log", maxBytes=5_000_000,
                             backupCount=5, encoding="utf-8")
    fh.setFormatter(_fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(_fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)

# Лічильники здоров'я — щоб «чи все гаразд» був запитом до даних, а не до відчуттів.
_stats = {"events": 0, "errors": 0, "write_fails": 0,
          "last_error_ts": None, "last_error_where": None}
# bot.log ротується через RotatingFileHandler, а events.jsonl не ротувався взагалі.
_EVENTS_MAX_BYTES = 32 * 1024 * 1024
_SIZE_CHECK_EVERY = 500     # stat() на кожній події — зайва сисколка на гарячому шляху


def stats() -> dict:
    """Знімок для /status і добового зведення."""
    return dict(_stats)


def _rotate_if_big() -> None:
    try:
        if _EVENTS.exists() and _EVENTS.stat().st_size > _EVENTS_MAX_BYTES:
            _EVENTS.replace(_EVENTS.with_suffix(".jsonl.1"))
    except Exception:  # noqa: BLE001
        pass


def _append(rec: dict) -> None:
    try:
        with open(_EVENTS, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + NEWLINE)
    except Exception as e:  # noqa: BLE001
        # Тихий pass тут означав би, що вся аналітика протікає безслідно.
        _stats["write_fails"] += 1
        if _stats["write_fails"] in (1, 10, 100):
            logger.error("events.jsonl не пишеться (%d-й раз): %s: %s",
                         _stats["write_fails"], type(e).__name__, str(e)[:120])


def event(kind: str, **fields) -> None:
    """Структурована подія → events.jsonl + короткий рядок у людський лог."""
    _stats["events"] += 1
    if _stats["events"] % _SIZE_CHECK_EVERY == 1:
        _rotate_if_big()
    rec = {"ts": dt.datetime.now(dt.timezone.utc).isoformat(), "kind": kind}
    rec.update(fields)
    _append(rec)
    logger.info("%s | %s", kind, " ".join(f"{k}={v}" for k, v in fields.items()))


def info(msg: str) -> None:
    logger.info(msg)


def _record_error(kind: str, msg: str, exc: str | None = None) -> None:
    _stats["errors"] += 1
    _stats["last_error_ts"] = dt.datetime.now(dt.timezone.utc).isoformat()
    _stats["last_error_where"] = msg[:160]
    rec = {"ts": _stats["last_error_ts"], "kind": kind,
           "where": msg[:300], "n": _stats["errors"]}
    if exc:
        rec["exc"] = exc[:2000]
    _append(rec)


def error(msg: str) -> None:
    logger.error(msg)
    _record_error("error", msg)


def exception(msg: str) -> None:
    logger.exception(msg)
    _record_error("exception", msg, traceback.format_exc())
