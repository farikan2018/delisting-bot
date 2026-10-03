"""Executor — відкриття, моніторинг і закриття шортів по сигналу делістингу.

DRY_RUN=True: усе симулюється (реальні ціни, віртуальні угоди), ордери не ставляться.
DRY_RUN=False: реальні ринкові ордери на MEXC (ізольована маржа).
"""
import asyncio
import datetime as dt
import time

import config
import exchange
import logbook as log
import pricecache
import storage
import strategy
import telegram_client as tg

_MODE = "dry" if config.DRY_RUN else "real"

# Фонові задачі (Telegram-сповіщення) — щоб НЕ блокувати гарячий шлях відкриття.
_bg_tasks: set = set()
# Комісія входу по pos_id (у памʼяті процесу) — для net-PnL при закритті.
_entry_fee: dict = {}

# --- Дедуп між джерелами + резервація слотів. Усе в памʼяті й СИНХРОННО. ---
# Джерел детекту кілька (Odin / WS-фід / швидкий поллінг / Telegram) і на одну подію
# вони приходять із різницею мілісекунд. Якби перевірка «чи вже відкрито» йшла в БД
# через await, обидва дубли встигли б її пройти й відкрити дві позиції на один токен.
_claimed: dict = {}         # тикер -> час заявки (TTL, див. _reserve)
_open_symbols: set = set()  # символи з відкритою позицією (люстро БД у памʼяті)
_reserved: int = 0          # відкриттів «у дорозі» — щоб не пробити MAX_CONCURRENT
_reserved_margin: float = 0.0  # маржа відкриттів «у дорозі» (баланс-гард)
_closing: dict = {}         # pos_id -> задача, що закриває (реентрантно, див. _do_close)
_flat_seen: dict = {}       # pos_id -> скільки разів поспіль біржа показала «нема позиції»
_close_alerted: set = set()  # pos_id, по яких уже кричали про невдале закриття

# --- Стан монітора в памʼяті: щоб не бити диск щодві секунди (див. monitor_once) ---
_min_price: dict = {}       # pos_id -> найнижча бачена ціна
_min_saved: dict = {}       # pos_id -> коли востаннє зберегли мінімум у БД
_tick_logged: dict = {}     # pos_id -> коли востаннє писали tick у лог
_last_db_sync: float = 0.0

# --- Аварійний вимикач по денному збитку. У памʼяті, бо гарячий шлях не ходить у SQLite. ---
_daily_pnl: float = 0.0     # реалізований PnL реальних угод за поточну добу UTC
_daily_day: str = ""


def init_daily() -> None:
    """Старт процесу: підняти денний лічильник із БД. Рестарт посеред доби не має
    обнуляти вже понесений збиток, інакше ліміт нічого не обмежує."""
    global _daily_day, _daily_pnl
    _daily_day = dt.datetime.utcnow().strftime("%Y-%m-%d")
    _daily_pnl = storage.realized_pnl_today("real")


def _roll_day() -> None:
    """Перекидає лічильник на нову добу UTC. БЕЗ I/O — його кличе гарячий шлях.
    Нова доба завжди починається з нуля, бо закритих угод у ній ще не було."""
    global _daily_day, _daily_pnl
    d = dt.datetime.utcnow().strftime("%Y-%m-%d")
    if d != _daily_day:
        _daily_day, _daily_pnl = d, 0.0


def daily_pnl() -> float:
    _roll_day()
    return _daily_pnl


def _risk_in_flight() -> float:
    """Найгірший ще НЕ реалізований збиток по позиціях, що відкриті або в дорозі.

    Кожна позиція обмежена стопом у STOP_LOSS_MARGIN_PCT% маржі, тож верхня межа
    рахується точно, без звернень до біржі.
    """
    n = len(_open_symbols) + _reserved
    return n * config.POSITION_MARGIN_USDT * config.STOP_LOSS_MARGIN_PCT / 100.0


def _kill_switch_hit() -> bool:
    """True = денний ліміт збитку вичерпано, нових позицій не відкриваємо.

    РАХУЄМО Й ЗБИТОК У ДОРОЗІ (2026-10-03). Раніше тут був лише реалізований PnL,
    тобто лічильник, який оновлюється ТІЛЬКИ при закритті позиції. Наслідок: один
    анонс відкриває три позиції паралельно, усі три проходять перевірку, бо жодна
    ще не закрилась, і ліміт фізично не може спрацювати всередині події. На живих
    числах (.env: маржа $3, стоп -30% маржі, ліміт $4) це давало перевищення в
    1.7-2 рази: лічильник на -$3.0 ліміт не пробив, а три нові позиції додали ще
    до -$2.7 зверху.

    Верхня межа збитку позиції відома точно — це її стоп, — тому резерв не
    здогадка, а арифметика.
    """
    lim = config.MAX_DAILY_LOSS_USDT
    return lim > 0 and (daily_pnl() - _risk_in_flight()) <= -lim


def resync_open() -> None:
    """Синхронізує памʼять із БД (старт процесу / після закриття)."""
    _open_symbols.clear()
    for p in storage.get_open_positions():
        _open_symbols.add(p["symbol"])


def _reserve(ticker: str, symbol: str, margin: float = 0.0) -> str:
    """Синхронна заявка на відкриття: '' = можна. КРИТИЧНО: між перевіркою і
    заявкою не має бути жодного await, інакше дедуп нічого не гарантує."""
    global _reserved, _reserved_margin
    claimed_at = _claimed.get(ticker)
    # Заявка з TTL. Її призначення — відсікти дублі ТІЄЇ САМОЇ події з чотирьох
    # джерел, які приходять із різницею мілісекунд. Раніше вона жила вічно, тож
    # тикер, по якому вхід не відбувся (спрацював фільтр, збій ордера), ставав
    # неторгованим до рестарту — і мовчки: наступний делістинг того ж токена
    # через місяць так само пропускався б.
    if claimed_at is not None and (time.time() - claimed_at) < config.CLAIM_TTL_SEC:
        return "duplicate_source"
    if symbol in _open_symbols:
        return "already_open"
    if len(_open_symbols) + _reserved >= config.MAX_CONCURRENT:
        return "max_concurrent"
    _claimed[ticker] = time.time()
    _reserved += 1
    _reserved_margin += margin
    return ""


def _release(ticker: str, symbol: str, opened: bool, margin: float = 0.0,
             drop_claim: bool = False) -> None:
    """drop_claim=True — знімаємо заявку, бо ТОЧНО відомо, що позиції не виникло.

    Навіщо (2026-10-03). Заявка живе CLAIM_TTL_SEC=900с. Це правильно, поки
    позиція жива. Але якщо вхід ЗІРВАВСЯ — наприклад, Bybit відповів 503 на
    найшвидшому джерелі, — заявка все одно блокувала тикер на 15 хвилин. Той
    самий анонс від повільнішого джерела приходить через 10-40с і отримував
    `duplicate_source`. Тобто одна транзієнтна помилка вбивала угоду, заради
    якої чекали місяць, і робила це мовчки.

    Знімаємо заявку ЛИШЕ коли невизначеності нема. Якщо ордер відповів помилкою,
    але позиція могла відкритись (загублена відповідь), заявку тримаємо — інакше
    повторний сигнал відкрив би ДРУГУ позицію поверх першої. Для цього випадку є
    _adopt_orphan: він питає біржу і сам знімає заявку, коли доведено, що чисто.
    """
    global _reserved, _reserved_margin
    _reserved = max(0, _reserved - 1)
    _reserved_margin = max(0.0, _reserved_margin - margin)
    if opened:
        _open_symbols.add(symbol)
        _claimed[ticker] = time.time()  # поки позиція жива, дублі не потрібні
    elif drop_claim:
        _claimed.pop(ticker, None)


def busy() -> bool:
    """Чи є відкриття «в дорозі» — фонові масові задачі мають зачекати й не забивати конект."""
    return _reserved > 0


def hot_state() -> dict:
    return {"claimed": len(_claimed), "open": len(_open_symbols), "reserved": _reserved}


def forget(ticker: str, symbol: str) -> None:
    """Позиція закрита — знімаємо і заявку, і символ, щоб токен знову був доступний."""
    _claimed.pop(ticker.upper(), None)
    _open_symbols.discard(symbol)


async def _safe(coro) -> None:
    try:
        await coro
    except Exception:  # noqa: BLE001
        log.exception("фонова задача впала")


def fire(coro) -> None:
    """Запустити корутину у фоні (не чекаючи) — для сповіщень/логів поза гарячим шляхом."""
    t = asyncio.create_task(_safe(coro))
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)

_REASON_LABEL = {
    "STOP_LOSS": "🛑 Стоп-лос",
    "TAKE_PROFIT": "📉 Тейк-профіт",
    "MAX_HOLD": "⏰ Ліміт часу",
    "MANUAL": "🔧 Ручне закриття",
    "EXCH_SL": "🛑 Стоп-лос (біржовий)",
    "EXCH_TP": "📉 Тейк-профіт (біржовий)",
}


# ---------- ВІДКРИТТЯ ----------
async def open_from_signal(ticker: str, detect_latency=None, real=None, margin=None,
                           dedup: bool = True, source: str = "?") -> None:
    """Обробляє один тикер делістингу.

    ГАРЯЧИЙ ШЛЯХ = все до create_order. Правило: до ордера — ЖОДНОЇ мережі, жодного
    потоку, жодного SQLite і жодного запису в лог-файл. Усе, що для цього потрібно,
    пре-обчислене на старті (exchange.HOT) або лежить у price-cache. Заміри показали,
    що сам ордер летить 165мс (фізика Франкфурт→матчер Bybit), тому будь-які наші
    власні мілісекунди — це чистий збиток.

    real:   None → за config.DRY_RUN; True → примусово РЕАЛЬНИЙ ордер (/test_short).
    margin: None → config.POSITION_MARGIN_USDT.
    dedup:  True → заявка з дедупом між джерелами (для сигналів). /test_short — False.
    """
    t_sig = time.perf_counter()
    real = (not config.DRY_RUN) if real is None else real
    margin = config.POSITION_MARGIN_USDT if margin is None else margin
    mode = "real" if real else "dry"
    ticker = ticker.upper()

    # 1) СИНХРОННО: мета символу з памʼяті — 0 мережі, 0 потоків.
    meta = exchange.hot_meta(ticker)
    if not meta:
        vs = "/".join(config.VENUE_PRIORITY)
        log.event("skip", ticker=ticker, reason="no_perp", venues=vs, source=source)
        fire(tg.send_message(f"ℹ️ <b>{ticker}</b>: нема перпа на {vs} — пропуск."))
        return
    venue, symbol, raw = meta["venue"], meta["symbol"], meta["raw_id"]

    # 1b) СИНХРОННО: захист реальних грошей. Обидві перевірки читають ПАМʼЯТЬ
    # (лічильник доби і кеш балансу з keep-alive) — 0 мережі, 0 SQLite.
    if real:
        if _kill_switch_hit():
            log.event("skip", ticker=ticker, reason="daily_loss_limit",
                      daily_pnl=round(daily_pnl(), 4), limit=config.MAX_DAILY_LOSS_USDT,
                      source=source)
            fire(tg.send_message(
                f"🛑 <b>{ticker}</b>: пропуск — денний ліміт збитку вичерпано "
                f"({daily_pnl():+.2f} USDT). Нових позицій не відкриваю."))
            return
        if config.BALANCE_GUARD:
            free = exchange.cached_free_balance(venue)
            # None = даних нема; на здогадці вхід НЕ блокуємо, бо пропущена подія
            # дорожча за відхилений ордер.
            # Віднімаємо маржу відкриттів «у дорозі»: один анонс дає кілька токенів,
            # вони летять ПАРАЛЕЛЬНО, і всі бачили б той самий знімок балансу —
            # тобто кожен вважав би, що гроші вільні, хоча вони вже розписані.
            if free is not None and (free - _reserved_margin) < margin * 1.1:
                log.event("skip", ticker=ticker, reason="low_balance", free=round(free, 4),
                          in_flight=round(_reserved_margin, 4), need=margin,
                          venue=venue, source=source)
                fire(tg.send_message(
                    f"💸 <b>{ticker}</b>: пропуск — вільної маржі ${free:.2f}"
                    + (f" (з них ${_reserved_margin:g} вже в дорозі)"
                       if _reserved_margin else "")
                    + f", потрібно ${margin:g}."))
                return

    # 2) СИНХРОННО: заявка (дедуп між джерелами + ліміт одночасних позицій).
    if dedup:
        why = _reserve(ticker, symbol, margin if real else 0.0)
        if why:
            log.event("skip", ticker=ticker, reason=why, source=source)
            if why != "duplicate_source":  # дубль джерела — нормальна робота, не спамимо
                fire(tg.send_message(f"ℹ️ <b>{ticker}</b>: пропуск — {why}."))
            return

    opened = False
    # True, щойно ми відправили ордер і НЕ знаємо напевно його долю. Поки це так,
    # заявку на тикер знімати НЕ можна: повторний сигнал відкрив би другу позицію
    # поверх можливо вже відкритої. Долю з'ясовує _adopt_orphan.
    ambiguous = False
    try:
        # 3) СИНХРОННО: ціна + ref із price-cache (в памʼяті). REST — лише як фолбек.
        entry_price = ref_price = None
        src = "rest"
        if config.PRICECACHE_POLL_SEC > 0 and venue == "bybit" and raw:
            gp = pricecache.get_price(raw)
            if gp and gp[1] <= config.PRICECACHE_MAX_AGE_SEC:
                entry_price, src = gp[0], f"cache({gp[1]:.1f}s)"
                ref_price = pricecache.reference_high(raw, config.REF_LOOKBACK_MIN)
        if entry_price is None:  # кеш-промах: два фетчі паралельно
            entry_price, ref_price = await asyncio.gather(
                asyncio.to_thread(exchange.get_last_price, venue, symbol),
                asyncio.to_thread(exchange.reference_high, venue, symbol,
                                  config.REF_LOOKBACK_MIN))

        # 4) СИНХРОННО: рішення + розмір (чиста арифметика по завантажених markets).
        decision = strategy.decide_entry(ref_price, entry_price)
        if not decision.ok:
            log.event("skip", ticker=ticker, reason=f"entry:{decision.reason}", source=source,
                      ref_price=decision.ref_price, entry_price=decision.entry_price,
                      dropped_pct=decision.dropped_pct, price_src=src)
            fire(tg.send_message(f"⏭️ <b>{ticker}</b> ({venue}): не входимо — {decision.reason}."))
            return
        entry_price = decision.entry_price
        contracts = exchange.contracts_for(venue, symbol, margin * config.LEVERAGE, entry_price)
        contract_size = meta["contract_size"]
        prep_ms = round((time.perf_counter() - t_sig) * 1000, 1)

        # 5) ОРДЕР. Якщо плече по символу вже озброєне (фоново) — летимо одразу.
        # Якщо ні — спершу виставляємо його (+165мс): при біржовому дефолті 10x наш
        # стоп −30% маржі і ліквідація опиняються майже в одній точці, і це гірше за
        # втрачені мілісекунди. ARM_LEVERAGE=1 прибирає цей крок назовсім.
        order_ms = None
        order = None
        lev_ok = True
        if real:
            if not exchange.is_leveraged(venue, symbol):
                # Результат ПЕРЕВІРЯЄМО: якщо плече не стало (rate-limit біржі),
                # ордер полетить на дефолті акаунта — а це може бути 10x, де
                # ліквідація на 10% руху проти нас, тобто ближче за наш стоп-намір.
                lev_ok = await asyncio.to_thread(exchange.ensure_leverage, venue, symbol,
                                                 config.LEVERAGE)
            t_ord = time.perf_counter()
            try:
                order = await asyncio.to_thread(exchange.open_short, venue, symbol, contracts)
            except Exception as e:  # noqa: BLE001
                # Єдина причина заплатити зайвий раунд: біржа відхилила через маржу,
                # бо фактичне плече нижче за наше. Виставляємо плече й пробуємо ще раз.
                if exchange.is_margin_error(e) and not exchange.is_leveraged(venue, symbol):
                    log.event("order_retry_leverage", ticker=ticker, err=str(e)[:120])
                    try:
                        await asyncio.to_thread(exchange.ensure_leverage, venue, symbol,
                                                config.LEVERAGE)
                        order = await asyncio.to_thread(exchange.open_short, venue, symbol,
                                                        contracts)
                    except Exception as e2:  # noqa: BLE001
                        log.exception(f"open_short повторно впав {ticker} {venue}")
                        ambiguous = True
                        fire(_adopt_orphan(ticker, venue, symbol, entry_price, margin,
                                           contract_size, decision, str(e2)))
                        return
                else:
                    log.exception(f"open_short помилка {ticker} {venue}")
                    ambiguous = True
                    fire(_adopt_orphan(ticker, venue, symbol, entry_price, margin,
                                       contract_size, decision, str(e)))
                    return
            order_ms = round((time.perf_counter() - t_ord) * 1000)

        # --- Далі гарячий шлях завершено: облік, логи, сповіщення. ---
        pos = {
            "ticker": ticker, "symbol": symbol, "venue": venue, "mode": mode,
            "margin": margin, "leverage": config.LEVERAGE,
            "contracts": contracts, "contract_size": contract_size,
            "ref_price": decision.ref_price, "entry_price": entry_price,
            "dropped_pct": decision.dropped_pct,
        }
        try:
            pos_id = storage.insert_position(pos)
        except Exception as e:  # noqa: BLE001
            # Ордер уже долетів, а запис не став (БД заблокована / диск повний).
            # Мовчки вийти не можна: на біржі висітиме позиція, якої нема в обліку.
            log.exception(f"insert_position впав {ticker}")
            if real:
                ambiguous = True
                fire(_adopt_orphan(ticker, venue, symbol, entry_price, margin,
                                   contract_size, decision, f"insert_position: {e}"))
            return
        opened = True
        if not dedup:
            # /test_short іде повз резервацію, але позиція від цього не менш реальна.
            # Без цього рядка наступний сигнал по ТОМУ САМОМУ символу побачив би
            # порожній _open_symbols і відкрив би другу позицію поверх першої.
            _open_symbols.add(symbol)
        if real and not lev_ok:
            log.event("leverage_not_set", ticker=ticker, symbol=symbol, venue=venue)
            fire(tg.send_message(
                f"⚠️ <b>{ticker}</b> #{pos_id}: плече {config.LEVERAGE:g}x НЕ виставилось, "
                f"ордер пішов на дефолті акаунта. Перевір позицію на біржі."))
        log.event("open", pos_id=pos_id, ticker=ticker, venue=venue, symbol=symbol,
                  mode=mode, entry_price=entry_price, contracts=contracts,
                  margin=margin, leverage=config.LEVERAGE, price_src=src,
                  dropped_pct=decision.dropped_pct, source=source,
                  detect_latency_sec=detect_latency,
                  prep_ms=prep_ms, order_ms=order_ms,
                  total_ms=round((time.perf_counter() - t_sig) * 1000, 1))
        fire(tg.send_message(_open_message(pos_id, pos)))
        if real and order is not None:  # реальний fill+комісія — уже поза критичним шляхом
            fire(_settle_and_arm(pos_id, pos, order))
    finally:
        if dedup:
            # Вхід не відбувся і невизначеності нема (виняток ДО ордера, відмова
            # стратегії, промах ціни) — знімаємо заявку, щоб повільніше джерело
            # мало право на другу спробу по цій самій події. Раніше тикер лишався
            # заблокованим 15 хвилин, тобто на весь дамп.
            _release(ticker, symbol, opened, margin if real else 0.0,
                     drop_claim=not opened and not ambiguous)


async def _adopt_orphan(ticker: str, venue: str, symbol: str, entry_price: float,
                        margin: float, contract_size: float, decision, err: str) -> None:
    """Ордер відповів помилкою — але це НЕ означає, що його не виконали.

    Найгірший реальний сценарій: Bybit прийняв ринковий продаж, а відповідь
    загубилась (ccxt кидає RequestTimeout). Тоді на біржі висить ГОЛА позиція,
    якої нема в БД — а її не побачить ні monitor_once, ні reconcile_real, бо обидва
    ходять лише по БД. Без цієї функції така позиція жила б до ліквідації, і єдиним
    слідом було б повідомлення «помилка ордера», яке активно вводить в оману.

    Тому: питаємо біржу, що там насправді, і якщо позиція є — беремо її під облік
    і вішаємо стоп.
    """
    size = None
    for _ in range(3):
        size = await asyncio.to_thread(exchange.position_size, venue, symbol)
        if size is not None:
            break
        await asyncio.sleep(1.0)

    if size is None:  # None — це «не знаю», а не «нема». Мовчати тут не можна.
        log.event("orphan_check_failed", ticker=ticker, symbol=symbol, venue=venue,
                  err=err[:200])
        await tg.send_message(
            f"🚨 <b>{ticker}</b>: ордер впав, і перевірити позицію на {venue} НЕ вдалося.\n"
            f"Помилка: <code>{err[:120]}</code>\n"
            f"⚠️ ПЕРЕВІР <code>{symbol}</code> ВРУЧНУ на біржі!")
        return

    if size <= 0:  # ордер справді не пройшов — усе чисто
        # Доведено, що позиції нема → знімаємо заявку. Без цього тикер лишався б
        # заблокованим CLAIM_TTL_SEC=900с, і повторний сигнал від повільнішого
        # джерела (поллінг на +33с) відсікався б як duplicate_source. Тобто одна
        # транзієнтна відмова біржі коштувала б усієї події.
        _claimed.pop(ticker.upper(), None)
        log.event("orphan_none", ticker=ticker, symbol=symbol, err=err[:200],
                  claim_released=True)
        await tg.send_message(f"❌ <b>{ticker}</b>: помилка ордера ({venue}), "
                              f"позиції на біржі нема — чисто. "
                              f"Заявку знято: повторний сигнал матиме другий шанс.")
        return

    pos = {"ticker": ticker, "symbol": symbol, "venue": venue, "mode": "real",
           "margin": margin, "leverage": config.LEVERAGE, "contracts": size,
           "contract_size": contract_size, "ref_price": decision.ref_price,
           "entry_price": entry_price, "dropped_pct": decision.dropped_pct}
    pos_id = await asyncio.to_thread(storage.insert_position, pos)
    _open_symbols.add(symbol)  # _release уже відпрацював із opened=False
    log.event("orphan_adopted", pos_id=pos_id, ticker=ticker, symbol=symbol,
              venue=venue, contracts=size, err=err[:200])
    await tg.send_message(
        f"🚨 <b>{ticker}</b>: ордер відповів помилкою, але позиція на {venue} "
        f"ВІДКРИТА ({size:g} контр.).\nВзяв під облік #{pos_id}, вішаю стоп.")
    if config.EXCHANGE_STOP:
        await arm_exchange_stop(pos_id, venue, symbol, entry_price,
                                config.LEVERAGE, ticker)


async def _settle_and_arm(pos_id: int, pos: dict, order: dict) -> None:
    """Післяордерний хвіст: стоп на біржу, тоді зʼясування реальної ціни виконання.

    Порядок саме такий — ЗАХИСТ ПЕРЕД ТОЧНІСТЮ. Спокусливо спершу дізнатись фактичний
    fill і повісити стоп рівно від нього, але зʼясування коштує 1-3 запити з паузами
    (Bybit індексує угоду не миттєво), і всі ці секунди позиція стояла б гола.
    Тому вішаємо від оцінки з кешу одразу, а коли фактична ціна приходить — за
    потреби переставляємо. Оцінка з price-cache відрізняється від fill на частки
    відсотка, тож проміжний стоп усе одно на своєму місці.
    """
    venue, symbol = pos["venue"], pos["symbol"]
    est = pos["entry_price"]
    if config.EXCHANGE_STOP:
        await arm_exchange_stop(pos_id, venue, symbol, est, pos["leverage"], pos["ticker"])

    fill = await _settle_fill(pos_id, venue, symbol, order)

    # Переставляємо, лише якщо проковзування реально зсунуло поріг.
    if config.EXCHANGE_STOP and fill and abs(fill - est) / est > 0.0005:
        log.event("stop_readjust", pos_id=pos_id, symbol=symbol, est=est, fill=fill,
                  slip_pct=round((fill - est) / est * 100, 4))
        await arm_exchange_stop(pos_id, venue, symbol, fill, pos["leverage"], pos["ticker"])


async def _fill_details(venue: str, symbol: str, order: dict) -> tuple:
    """(ціна виконання, комісія) ордера, з ретраями.

    Bybit індексує угоду в історії не миттєво, тому одразу після ордера запит
    часто повертає порожньо. Живий тест це й показав: закриття записалось із
    `fees: 0.0`, хоча комісія була — тобто PnL систематично завищувався б на
    ~0.4% маржі за угоду, і цим завищеним числом живився б аварійний вимикач."""
    avg = order.get("average") or order.get("price")
    fee = (order.get("fee") or {}).get("cost")
    oid = order.get("id")
    if (avg is None or fee is None) and oid:
        for delay in (0.0, 0.5, 1.5):
            if delay:
                await asyncio.sleep(delay)
            f_avg, f_fee = await asyncio.to_thread(exchange.order_fill, venue, symbol, oid)
            avg = avg if avg is not None else f_avg
            fee = fee if fee is not None else f_fee
            if avg is not None and fee is not None:
                break
    return (float(avg) if avg else None, float(fee) if fee is not None else None)


async def _settle_fill(pos_id: int, venue: str, symbol: str, order: dict) -> float | None:
    """Довантажує реальну ціну виконання й комісію входу (окремий запит ПІСЛЯ ордера)
    і виправляє ними запис у БД. Потрібно для чесного net-PnL і заміру слиппеджу.
    Повертає фактичну ціну входу (або None)."""
    # Ретраї безпечні: ми вже поза гарячим шляхом, і стоп на біржі вже стоїть.
    avg, fee = await _fill_details(venue, symbol, order)
    if fee is not None:
        _entry_fee[pos_id] = fee
        # Дублюємо в БД: памʼять процесу не переживає рестарт, а позиція живе до
        # 20 хвилин. Без цього будь-який деплой посеред угоди губив комісію входу,
        # і закриття записувало б завищений PnL.
        await asyncio.to_thread(storage.meta_set, f"entry_fee:{pos_id}", repr(fee))
    if avg:
        storage.update_entry_price(pos_id, float(avg))
    else:
        # Не мовчимо: без реальної ціни входу PnL і слиппедж будуть оцінкою.
        # order.get("id"), а НЕ oid: цей рядок лишився від часів, коли id діставався
        # тут же, а після виносу _fill_details змінна поїхала в іншу функцію.
        log.event("fill_unknown", pos_id=pos_id, symbol=symbol, order_id=order.get("id"))
    log.event("fill", pos_id=pos_id, symbol=symbol, fill_price=avg, fee=fee)
    return float(avg) if avg else None


async def arm_exchange_stop(pos_id: int, venue: str, symbol: str, entry: float,
                            leverage: float, ticker: str) -> bool:
    """Вішає SL і TP на позицію на боці біржі. Три спроби: мережевий збій — не привід
    лишати реальні гроші без захисту.

    Якщо всі спроби впали, позицію НЕ закриваємо: програмний стоп у monitor_once
    живий і перевіряє раз на EXIT_CHECK_SEC. Закрити хорошу угоду через збій API —
    гарантований збиток проти малоймовірного; краще гучно попередити."""
    sl = entry * (1 + config.STOP_LOSS_MARGIN_PCT / leverage / 100)   # шорт: стоп ВИЩЕ входу
    tp = entry * (1 - config.TAKE_PROFIT_MARGIN_PCT / leverage / 100)  # тейк НИЖЧЕ входу

    # Спершу пробуємо повісити обидва одним запитом, тоді — САМ СТОП.
    # Чому потрібен фолбек: Bybit валідує тейк проти поточної ціни, а ми входимо
    # рівно в обвал. Якщо за ті мілісекунди, що минули від філа, ціна встигла
    # провалитись нижче нашого тейка, біржа відхилить ЗАПИТ ЦІЛКОМ — і разом із
    # непотрібним уже тейком ми втратили б і стоп, тобто єдиний реальний захист.
    for label, kw in (("sl+tp", {"stop_price": sl, "take_price": tp}),
                      ("sl", {"stop_price": sl})):
        for attempt in (1, 2, 3):
            try:
                await asyncio.to_thread(exchange.set_position_stop, venue, symbol, **kw)
                log.event("stop_armed", pos_id=pos_id, symbol=symbol, venue=venue,
                          entry=entry, sl_price=sl,
                          tp_price=tp if "take_price" in kw else None,
                          armed=label, attempt=attempt)
                if label == "sl":  # тейк не став — його добере програмний монітор
                    fire(tg.send_message(
                        f"ℹ️ <b>{ticker}</b> #{pos_id}: біржовий стоп поставлено, "
                        f"а тейк ні (ціна вже нижче нього). Тейк відпрацює бот."))
                return True
            except NotImplementedError:
                # MEXC/Gate — біржового стопу поки нема. Це відомий стан, не аварія:
                # лишається програмний стоп. Спамити попередженнями не треба.
                log.event("stop_arm_unsupported", pos_id=pos_id, venue=venue, symbol=symbol)
                return False
            except Exception as e:  # noqa: BLE001
                log.event("stop_arm_failed", pos_id=pos_id, symbol=symbol, mode=label,
                          attempt=attempt, err=f"{type(e).__name__}: {e}"[:200])
                if attempt < 3:
                    await asyncio.sleep(1.0 * attempt)
    fire(tg.send_message(
        f"⚠️ <b>{ticker}</b> #{pos_id}: НЕ вдалось повісити стоп на біржі після 6 спроб.\n"
        f"Позиція жива, захищена лише програмним стопом (потребує живого бота).\n"
        f"Перевір вручну на Bybit!"))
    return False


async def rearm_open_stops() -> None:
    """Старт процесу: переконатись, що на кожній реальній позиції висить біржовий стоп.
    Сценарій, заради якого це існує: бот упав між ордером і встановленням стопа —
    після рестарту позиція була б голою, і ніхто б про це не дізнався.

    Спершу питаємо біржу, чи позиція взагалі жива. Найчастіший випадок рестарту —
    запис у БД є, а позицію біржа вже закрила своїм стопом; без цієї перевірки
    кожен такий запис коштував би 6 приречених спроб із паузами (~9с) і фальшиву
    тривогу «позиція без захисту»."""
    if not config.EXCHANGE_STOP:
        return
    for pos in storage.get_open_positions():
        if pos.get("mode") != "real":
            continue
        size = await asyncio.to_thread(exchange.position_size, pos["venue"], pos["symbol"])
        if size == 0:
            log.event("rearm_skip_flat", pos_id=pos["id"], symbol=pos["symbol"])
            continue  # звірка закриє запис коректно, з реальною ціною виходу
        await arm_exchange_stop(pos["id"], pos["venue"], pos["symbol"],
                                pos["entry_price"], pos["leverage"], pos["ticker"])


def _open_message(pos_id: int, p: dict) -> str:
    tag = "⚠️ РЕАЛ" if p.get("mode") == "real" else "🧪 DRY-RUN"
    notional = p["margin"] * p["leverage"]
    lev = p["leverage"]
    sl_price = p["entry_price"] * (1 + config.STOP_LOSS_MARGIN_PCT / lev / 100)   # стоп: ціна вгору
    tp_price = p["entry_price"] * (1 - config.TAKE_PROFIT_MARGIN_PCT / lev / 100)  # тейк: ціна вниз
    sl_loss = p["margin"] * config.STOP_LOSS_MARGIN_PCT / 100
    tp_gain = p["margin"] * config.TAKE_PROFIT_MARGIN_PCT / 100
    return (
        f"🟢 <b>ВІДКРИТО ШОРТ</b> [{tag}] #{pos_id}\n"
        f"Монета: <b>{p['ticker']}</b> (<code>{p['symbol']}</code>) на <b>{p.get('venue','?')}</b>\n"
        f"Ціна входу: <b>{_fmt(p['entry_price'])}</b>\n"
        f"Розмір: ${p['margin']:g} × {p['leverage']:g}x = ${notional:g} "
        f"(~{p['contracts']:g} контр.)\n"
        f"📉 Тейк: +{config.TAKE_PROFIT_MARGIN_PCT:g}% маржі (+${tp_gain:g}, ціна {_fmt(tp_price)})\n"
        f"🛑 Стоп: −{config.STOP_LOSS_MARGIN_PCT:g}% маржі (−${sl_loss:g}, ціна {_fmt(sl_price)})\n"
        f"⏰ Макс. утримання: {config.MAX_HOLD_MINUTES:g} хв"
    )


# ---------- МОНІТОРИНГ / ЗАКРИТТЯ ----------
async def current_price(pos: dict) -> float | None:
    """Ціна для перевірки виходу: спершу price-cache (WS, реал-тайм, 0 мережі),
    REST — лише як фолбек. Раніше кожна позиція раз на тік їла REST-запит 165мс,
    через що стоп реагував на ціну, якій уже чверть секунди."""
    meta = exchange.hot_meta(pos["ticker"])
    if meta and meta["venue"] == "bybit" and meta["raw_id"]:
        gp = pricecache.get_price(meta["raw_id"])
        if gp and gp[1] <= config.PRICECACHE_MAX_AGE_SEC:
            return gp[0]
    return await asyncio.to_thread(exchange.get_last_price, pos["venue"], pos["symbol"])


async def monitor_once() -> None:
    """Один прохід по всіх відкритих позиціях: оновити мінімум, перевірити вихід.

    Коли позицій нема (а це 99.9% часу), прохід не робить НІЧОГО: ні читання
    SQLite, ні запису в лог. Раніше монітор щодві секунди ходив у базу й писав
    рядок на кожну позицію — синхронні операції з диском просто в event-loop,
    тобто рівно в тому лупі, який має бути вільним на момент сигналу.
    Раз на MEMORY_RESYNC_SEC усе одно звіряємось із БД — щоб памʼять, яка з
    якоїсь причини розійшлась із базою, не сховала позицію назавжди.
    """
    global _last_db_sync
    now = time.time()
    idle = not _open_symbols and _reserved == 0
    if idle and (now - _last_db_sync) < 60.0:
        return
    _last_db_sync = now

    positions = storage.get_open_positions()
    if idle and positions:  # памʼять розійшлась із БД — відновлюємо
        log.event("monitor_resync", found=len(positions))
        for p in positions:
            _open_symbols.add(p["symbol"])
    for pos in positions:
        try:
            price = await current_price(pos)
            if not price:
                # Ціни нема (перп зняли з торгів / REST мовчить). Раніше тут стояв
                # голий continue — і позиція зависала НАЗАВЖДИ, бо MAX_HOLD теж не
                # перевірявся, а слот із MAX_CONCURRENT лишався зайнятим.
                # Часовий вихід не потребує ціни, тому перевіряємо його окремо.
                opened = strategy._parse_ts(pos.get("opened_at"))
                if opened and (dt.datetime.utcnow() - opened).total_seconds() \
                        >= config.MAX_HOLD_MINUTES * 60:
                    log.event("exit_no_price", pos_id=pos["id"], symbol=pos["symbol"])
                    await _do_close(pos, pos["entry_price"], "MAX_HOLD")
                continue
            pid = pos["id"]
            # Мінімум тримаємо в памʼяті, у SQLite скидаємо рідко: це поле потрібне
            # лише для звітності, а запис у базу на кожному тіку — це fsync у лупі.
            mem_min = _min_price.get(pid)
            if mem_min is None or mem_min > pos["min_price"]:
                mem_min = pos["min_price"]
            if price < mem_min:
                mem_min = price
            _min_price[pid] = mem_min
            pos["min_price"] = mem_min
            if now - _min_saved.get(pid, 0.0) >= config.MIN_PRICE_PERSIST_SEC:
                _min_saved[pid] = now
                storage.update_min_price(pid, mem_min)

            profit_pct = strategy.margin_profit_pct(pos["entry_price"], price, pos["leverage"])
            if now - _tick_logged.get(pid, 0.0) >= config.TICK_LOG_SEC:
                _tick_logged[pid] = now
                log.event("tick", pos_id=pid, symbol=pos["symbol"], price=price,
                          min_price=mem_min, profit_pct=round(profit_pct, 1))
            should_close, reason = strategy.check_exit(pos, price)
            if should_close:
                storage.update_min_price(pid, mem_min)  # зафіксувати мінімум перед закриттям
                await _do_close(pos, price, reason)
        except Exception:  # noqa: BLE001
            log.exception(f"monitor помилка по {pos.get('symbol')}")


async def force_close(pos_id: int, reason: str = "MANUAL") -> bool:
    """Ручне закриття позиції за id (для тестів/команд Telegram).

    Ціна тут потрібна лише для обліку, а не для рішення — тож її відсутність
    (REST мовчить) не має заважати закрити позицію. Раніше None доїжджав до
    арифметики PnL і кидав TypeError уже ПІСЛЯ того, як ордер пішов на біржу:
    позиція закрита, а запис лишався відкритим, і /panic мовчки обривався."""
    for pos in storage.get_open_positions():
        if pos["id"] == pos_id:
            price = await current_price(pos) or pos["entry_price"]
            await _do_close(pos, price, reason)
            return True
    return False


async def reconcile_real() -> None:
    """Звірка з біржею для РЕАЛЬНИХ позицій.

    Навіщо: SL/TP висять на боці Bybit, тож позиція може закритись БЕЗ нашої участі.
    У БД вона при цьому лишиться відкритою — слот із MAX_CONCURRENT буде зайнятий
    назавжди, monitor даремно довбатиме ціну, а PnL ніколи не запишеться.

    Кожна позиція — у власному try: інакше один виняток (наприклад `fetch_ticker`
    по знятому з торгів контракту) обривав би ВЕСЬ прохід, і решта позицій не
    звірялась би ніколи.
    """
    for pos in storage.get_open_positions():
        if pos.get("mode") != "real":
            continue
        try:
            await _reconcile_one(pos)
        except Exception:  # noqa: BLE001
            log.exception(f"reconcile помилка по #{pos.get('id')} {pos.get('symbol')}")


async def _reconcile_one(pos: dict) -> None:
    pid = pos["id"]

    # 1) Занадто молода позиція — не чіпаємо. Bybit показує щойно відкриту позицію
    # в /v5/position/list із затримкою, і без цього вікна звірка закрила б запис
    # ЖИВОЇ позиції: у БД «закрито», на біржі відкрито, стоп є, але ніхто не стежить.
    opened = strategy._parse_ts(pos.get("opened_at"))
    if opened:
        age = (dt.datetime.utcnow() - opened).total_seconds()
        if age < config.RECONCILE_MIN_AGE_SEC:
            return

    size = await asyncio.to_thread(exchange.position_size, pos["venue"], pos["symbol"])
    if size is None:  # запит не вдався — це «не знаю», а не «нема»
        return
    if size > 0:
        _flat_seen.pop(pid, None)
        return

    # 2) Одного нульового читання замало: порожній список може прийти й через
    # тимчасовий збій на боці біржі. Вимагаємо N підтверджень поспіль.
    n = _flat_seen.get(pid, 0) + 1
    _flat_seen[pid] = n
    if n < config.RECONCILE_CONFIRMS:
        log.event("reconcile_flat_once", pos_id=pid, symbol=pos["symbol"], seen=n)
        return
    _flat_seen.pop(pid, None)

    # 3) Ціну виходу беремо З БІРЖІ, а не з поточного ринку. Між спрацюванням
    # стопа і цією звіркою минуло до RECONCILE_SEC, і після делістингового обвалу
    # ціна за цей час ходить на відсотки — вгадування записало б збиток прибутком,
    # а саме цим числом живиться аварійний вимикач.
    cp = await asyncio.to_thread(exchange.closed_pnl, pos["venue"], pos["symbol"])
    if cp and cp.get("exit_price"):
        price, exact = cp["exit_price"], True
    else:
        price, exact = (await current_price(pos) or pos["entry_price"]), False
    reason = "EXCH_TP" if price < pos["entry_price"] else "EXCH_SL"
    log.event("reconcile_closed", pos_id=pid, symbol=pos["symbol"], reason=reason,
              exit_price=price, exact_price=exact,
              exchange_pnl=(cp or {}).get("pnl"))
    await _do_close(pos, price, reason, already_closed=True, exact_exit=exact)


async def _do_close(pos: dict, price: float, reason: str,
                    already_closed: bool = False, exact_exit: bool = False) -> None:
    """already_closed=True — позиції на біржі вже НЕМА (спрацював біржовий SL/TP),
    тому ордер на закриття слати не можна: reduce-only без позиції буде відхилено,
    а без reduce-only ми б відкрили нову позицію в протилежний бік.

    Закриття НЕ ідемпотентне саме по собі: monitor_once і reconcile_real — два
    незалежні цикли, і між читанням позиції та її закриттям є await. Без замка
    обидва встигли б послати ордер на закриття однієї позиції.

    ЗАМОК РЕЕНТРАНТНИЙ ДЛЯ СВОЄЇ Ж ЗАДАЧІ (2026-10-03). Раніше це був простий
    set, і він блокував сам себе на ШТАТНОМУ шляху: біржовий стоп спрацював →
    monitor кличе _do_close → _closing.add(pid) → close_short відхилено, бо
    позиції вже нема → гілка size==0 кличе _reconcile_one → той кличе _do_close
    із already_closed=True → pid уже в _closing → `close_skipped_inflight` і
    вихід. Запис у БД лишався ВІДКРИТИМ, монітор довбив close_short раз на дві
    секунди, а слот із трьох зайнятий, поки окремий цикл звірки не добереться.
    У логах це рівно 7 подій close_already_flat і рівно 7 close_skipped_inflight.

    Справжня мета замка — не пустити ДРУГУ задачу (monitor проти reconcile), а не
    заборонити тій самій задачі довести своє ж закриття до кінця. Тому ключем
    тепер є задача-власник. Зовнішній виклик після вкладеного одразу робить
    return, тож подвійного закриття запису не виникає."""
    pid = pos["id"]
    cur = asyncio.current_task()
    owner = _closing.get(pid)
    if owner is not None and owner is not cur:
        log.event("close_skipped_inflight", pos_id=pid, reason=reason)
        return
    reentrant = owner is cur
    if not reentrant:
        _closing[pid] = cur
    try:
        await _do_close_inner(pos, price, reason, already_closed, exact_exit)
    finally:
        if not reentrant:
            _closing.pop(pid, None)


async def _do_close_inner(pos: dict, price: float, reason: str,
                          already_closed: bool = False,
                          exact_exit: bool = False) -> None:
    exit_price = price
    exit_fee = None
    if pos.get("mode") == "real" and not already_closed:
        try:
            order = await asyncio.to_thread(
                exchange.close_short, pos["venue"], pos["symbol"], pos["contracts"]
            )
        except Exception:  # noqa: BLE001
            log.exception(f"close_short помилка #{pos['id']} {pos['symbol']}")
            # Найчастіша причина відмови — позиції на біржі ВЖЕ НЕМА (спрацював
            # біржовий стоп), і reduce-only відхиляється. Якщо просто вийти, запис
            # лишиться відкритим, monitor повторить ордер через 2с — і так вічно,
            # із новим повідомленням у Telegram щоразу, поки чат не впреться в ліміт.
            size = await asyncio.to_thread(exchange.position_size,
                                           pos["venue"], pos["symbol"])
            if size == 0:
                log.event("close_already_flat", pos_id=pos["id"], symbol=pos["symbol"])
                await _reconcile_one(pos)  # закриє запис за реальною ціною з біржі
                return
            if pos["id"] not in _close_alerted:  # кричимо ОДИН раз, не щодві секунди
                _close_alerted.add(pos["id"])
                await tg.send_message(
                    f"❌ <b>{pos['ticker']}</b>: помилка закриття (#{pos['id']}).\n"
                    f"⚠️ Перевір позицію вручну на біржі! (далі мовчу, щоб не спамити)"
                )
            return
        avg, fee = await _fill_details(pos["venue"], pos["symbol"], order)
        exit_price = avg or price
        exit_fee = fee
        if fee is None:
            log.event("exit_fee_unknown", pos_id=pos["id"], symbol=pos["symbol"])

    price_pnl = pos["contracts"] * pos["contract_size"] * (pos["entry_price"] - exit_price)
    entry_fee = _entry_fee.pop(pos["id"], None)
    if entry_fee is None:  # памʼять процесу порожня — тягнемо з БД (пережило рестарт)
        saved = storage.meta_get(f"entry_fee:{pos['id']}")
        try:
            entry_fee = float(saved) if saved is not None else None
        except ValueError:
            entry_fee = None
    if entry_fee is None:  # останній фолбек: комісія виходу ≈ комісія входу
        entry_fee = exit_fee
    fees = (entry_fee or 0.0) + (exit_fee or 0.0)
    pnl_usdt = price_pnl - fees
    pnl_pct = pnl_usdt / pos["margin"] * 100 if pos.get("margin") else 0.0
    storage.close_position(pos["id"], exit_price, reason, pnl_usdt, pnl_pct)
    forget(pos["ticker"], pos["symbol"])  # токен знову доступний для наступного сигналу
    _close_alerted.discard(pos["id"])
    for d in (_flat_seen, _min_price, _min_saved, _tick_logged):
        d.pop(pos["id"], None)
    storage.meta_set(f"entry_fee:{pos['id']}", "")  # прибираємо за собою
    if pos.get("mode") == "real":  # живлення аварійного вимикача
        global _daily_pnl
        _roll_day()
        _daily_pnl += pnl_usdt
    log.event("close", pos_id=pos["id"], ticker=pos["ticker"], symbol=pos["symbol"],
              mode=pos.get("mode"), reason=reason, entry_price=pos["entry_price"],
              exit_price=exit_price, min_price=pos["min_price"],
              price_pnl=round(price_pnl, 4), fees=round(fees, 4),
              pnl_usdt=round(pnl_usdt, 4), pnl_pct=round(pnl_pct, 1))
    await tg.send_message(_close_message(pos, exit_price, reason, pnl_usdt, pnl_pct,
                                         fees, exact_exit or not already_closed))


def _close_message(p: dict, exit_price: float, reason: str,
                   pnl_usdt: float, pnl_pct: float, fees: float = 0.0,
                   exact: bool = True) -> str:
    tag = "⚠️ РЕАЛ" if p.get("mode") == "real" else "🧪 DRY-RUN"
    emoji = "✅" if pnl_usdt >= 0 else "🔻"
    dur = _duration(p.get("opened_at"))
    fee_line = f"Комісії: −{fees:.4f} USDT\n" if fees else ""
    # Позначаємо приблизний PnL, лише коли ціну виходу справді НЕ вдалось дістати
    # з біржі (closed-pnl не відповів) — інакше число точне навіть для біржового стопа.
    approx = "" if exact else " (≈)"
    return (
        f"{emoji} <b>ЗАКРИТО ШОРТ</b> [{tag}] #{p['id']}\n"
        f"Монета: <b>{p['ticker']}</b> (<code>{p['symbol']}</code>)\n"
        f"Причина: <b>{_REASON_LABEL.get(reason, reason)}</b>\n"
        f"Вхід: {_fmt(p['entry_price'])} → Вихід{approx}: {_fmt(exit_price)}\n"
        f"{fee_line}"
        f"Прибуток (net): <b>{pnl_usdt:+.4f} USDT</b> ({pnl_pct:+.1f}% від маржі)\n"
        f"Тривалість: {dur}"
    )


# ---------- утиліти ----------
def _fmt(x: float) -> str:
    if x is None:
        return "?"
    if x >= 1:
        return f"{x:.4f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")


def _duration(opened_at: str | None) -> str:
    o = strategy._parse_ts(opened_at) if opened_at else None
    if not o:
        return "?"
    secs = int((dt.datetime.utcnow() - o).total_seconds())
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h}г {m}хв" if h else f"{m}хв {s}с"
