"""Централізоване логування: людський лог (bot.log) + структурований JSONL (events.jsonl).

events.jsonl — по одному JSON на рядок, зручно аналізувати потім:
  grep / jq / pandas. Кожен запис має ts (UTC ISO) і kind.
"""
import datetime as dt
import json
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

_BASE = Path(__file__).parent
_LOGDIR = _BASE / "logs"
_LOGDIR.mkdir(exist_ok=True)
_EVENTS = _LOGDIR / "events.jsonl"

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


def event(kind: str, **fields) -> None:
    """Структурована подія → events.jsonl + короткий рядок у людський лог."""
    rec = {"ts": dt.datetime.now(dt.timezone.utc).isoformat(), "kind": kind}
    rec.update(fields)
    try:
        with open(_EVENTS, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:  # noqa: BLE001
        pass
    logger.info("%s | %s", kind, " ".join(f"{k}={v}" for k, v in fields.items()))


def info(msg: str) -> None:
    logger.info(msg)


def error(msg: str) -> None:
    logger.error(msg)


def exception(msg: str) -> None:
    logger.exception(msg)
