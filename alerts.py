"""Гучні сповіщення з придушенням повторів: лог + Telegram, але без спаму.

ЧОМУ ЦЕ ІСНУЄ (2026-10-03). За історію проєкту ЩОНАЙМЕНШЕ п'ять разів можливість
тихо вимикалась і ніхто не знав:
    NameError у гілці, яка виконується лише під fire();
    відсутній TELEGRAM_BOT_TOKEN — канал сповіщень мертвий, і сказати про це нікому;
    відсутній CL_WS_KEY — шість днів торгівлі без швидкого тригера;
    відсутні TG_*;
    і найдовша — tgfeed помер 2026-08-30 о 14:21 з AuthKeyDuplicatedError і лежав
    34 доби, насмітивши 24411 однакових подій tg_feed_error, при цьому НЕ надіславши
    жодного сповіщення.
Спільне в усіх п'яти: подія в лог ішла (або й не йшла), але нікому не ставало боляче.
Лог ніхто не читає між делістингами — а делістинг буває раз на 3-6 тижнів.

ПРАВИЛО: аварія, яка коштує сигналу або грошей, зобов'язана прилетіти в Telegram
ОДИН раз, а не нуль і не 24411. Саме це робить цей модуль.

Чому власний cooldown, а не просто «логувати рідше»: аварії тут довгі (доба, тиждень).
Потрібне перше повідомлення негайно, повторне нагадування раз на кілька годин, і
ОДНЕ повідомлення про одужання — інакше незрозуміло, чи полагодилось.
"""
import time

import logbook as log
import telegram_client as tg

DEFAULT_COOLDOWN_SEC = 6 * 3600

# key -> {"first_ts", "last_sent_ts", "count", "active"}
_state: dict[str, dict] = {}
_stats = {"raised": 0, "sent": 0, "suppressed": 0, "cleared": 0}


def stats() -> dict:
    d = dict(_stats)
    d["active"] = sorted(k for k, v in _state.items() if v["active"])
    return d


def active_keys() -> list[str]:
    return sorted(k for k, v in _state.items() if v["active"])


def _age(ts: float) -> str:
    sec = max(0, int(time.time() - ts))
    if sec < 3600:
        return str(sec // 60) + " хв"
    if sec < 86400:
        return str(sec // 3600) + " год"
    return str(sec // 86400) + " діб"


async def raise_alert(key: str, text: str, cooldown_sec: float = DEFAULT_COOLDOWN_SEC,
                      detail: dict | None = None) -> bool:
    """Повідомити про аварію `key`. Повертає True, якщо повідомлення реально пішло.

    Перший раз — негайно. Далі — не частіше ніж раз на cooldown_sec, але з
    приміткою, скільки аварія вже триває: інакше друге повідомлення не відрізнити
    від першого, і незрозуміло, чи це нова поломка чи стара.
    """
    now = time.time()
    st = _state.get(key)
    if st is None or not st["active"]:
        st = {"first_ts": now, "last_sent_ts": 0.0, "count": 0, "active": True}
        _state[key] = st
    st["count"] += 1
    _stats["raised"] += 1

    if now - st["last_sent_ts"] < cooldown_sec:
        _stats["suppressed"] += 1
        return False

    repeat = st["last_sent_ts"] > 0
    st["last_sent_ts"] = now
    log.event("alert", key=key, text=text[:300], repeat=repeat,
              since_sec=round(now - st["first_ts"], 1), raised=st["count"],
              **(detail or {}))
    body = "🚨 <b>" + key + "</b>" + chr(10) + text
    if repeat:
        body += chr(10) + "<i>триває вже " + _age(st["first_ts"]) + "</i>"
    ok = await tg.send_message(body)
    if ok:
        _stats["sent"] += 1
    else:
        # Канал сповіщень мертвий — це САМЕ ПО СОБІ аварія, але сказати про неї
        # нема куди. Лишається лог: хоч одна поверхня, на якій це видно.
        log.error("alert '" + key + "' НЕ доставлено в Telegram: " + text[:160])
    return ok


async def clear_alert(key: str, text: str = "") -> bool:
    """Аварія минула. Повідомляємо ОДИН раз і лише якщо про неї справді говорили:
    інакше після кожного рестарту прилітав би потік «усе гаразд» ні про що."""
    st = _state.get(key)
    if not st or not st["active"]:
        return False
    st["active"] = False
    _stats["cleared"] += 1
    if st["last_sent_ts"] <= 0:
        return False  # про аварію не встигли сказати — мовчимо й про одужання
    log.event("alert_cleared", key=key, lasted_sec=round(time.time() - st["first_ts"], 1),
              raised=st["count"])
    return await tg.send_message(
        "✅ <b>" + key + "</b> — відновлено" + chr(10)
        + (text or "Аварія тривала " + _age(st["first_ts"]) + "."))
