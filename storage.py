"""SQLite-стан: дедуп анонсів + облік позицій."""
import sqlite3
from contextlib import closing

import config


def _conn():
    return closing(sqlite3.connect(config.DB_PATH))


def init() -> None:
    with _conn() as db:
        db.execute(
            """CREATE TABLE IF NOT EXISTS seen_articles (
                article_id TEXT PRIMARY KEY,
                title      TEXT,
                seen_at    TEXT DEFAULT (datetime('now'))
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS positions (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker        TEXT,
                symbol        TEXT,
                venue         TEXT,     -- біржа виконання (bybit/mexc)
                mode          TEXT,     -- dry / real
                margin        REAL,
                leverage      REAL,
                contracts     REAL,
                contract_size REAL,
                ref_price     REAL,     -- «до-дампова» ціна
                entry_price   REAL,
                dropped_pct   REAL,     -- на скільки вже впала на момент входу
                min_price     REAL,     -- найнижча ціна з моменту входу (для трейлінгу)
                opened_at     TEXT DEFAULT (datetime('now')),
                status        TEXT DEFAULT 'open',  -- open / closed
                exit_price    REAL,
                exit_reason   TEXT,
                pnl_usdt      REAL,
                pnl_pct       REAL,
                closed_at     TEXT
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS armed_leverage (
                venue    TEXT,
                symbol   TEXT,
                leverage REAL,
                armed_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (venue, symbol)
            )"""
        )
        # міграція: додати venue, якщо БД створювалась до мульти-біржі
        cols = [r[1] for r in db.execute("PRAGMA table_info(positions)").fetchall()]
        if "venue" not in cols:
            db.execute("ALTER TABLE positions ADD COLUMN venue TEXT")
        db.commit()


# ---- seen_articles (дедуп анонсів) ----
def is_seen(article_id: str) -> bool:
    with _conn() as db:
        return db.execute(
            "SELECT 1 FROM seen_articles WHERE article_id = ?", (str(article_id),)
        ).fetchone() is not None


def mark_seen(article_id: str, title: str) -> None:
    with _conn() as db:
        db.execute(
            "INSERT OR IGNORE INTO seen_articles (article_id, title) VALUES (?, ?)",
            (str(article_id), title),
        )
        db.commit()


def seen_count() -> int:
    with _conn() as db:
        return db.execute("SELECT COUNT(*) FROM seen_articles").fetchone()[0]


# ---- armed_leverage (плече, виставлене заздалегідь) ----
# Bybit тримає плече на своїй стороні назавжди, тому виставити його достатньо ОДИН раз.
# Записуємо в БД, щоб рестарт бота не бив API 800 разів заново.
def armed_symbols(venue: str, leverage: float) -> set[str]:
    with _conn() as db:
        rows = db.execute(
            "SELECT symbol FROM armed_leverage WHERE venue = ? AND leverage = ?",
            (venue, leverage),
        ).fetchall()
        return {r[0] for r in rows}


def mark_armed(venue: str, symbol: str, leverage: float) -> None:
    with _conn() as db:
        db.execute(
            "INSERT OR REPLACE INTO armed_leverage (venue, symbol, leverage) VALUES (?,?,?)",
            (venue, symbol, leverage),
        )
        db.commit()


# ---- positions ----
def has_open_position(symbol: str) -> bool:
    with _conn() as db:
        return db.execute(
            "SELECT 1 FROM positions WHERE symbol = ? AND status = 'open'", (symbol,)
        ).fetchone() is not None


def open_positions_count() -> int:
    with _conn() as db:
        return db.execute(
            "SELECT COUNT(*) FROM positions WHERE status = 'open'"
        ).fetchone()[0]


def insert_position(p: dict) -> int:
    with _conn() as db:
        cur = db.execute(
            """INSERT INTO positions
               (ticker, symbol, venue, mode, margin, leverage, contracts, contract_size,
                ref_price, entry_price, dropped_pct, min_price, status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'open')""",
            (
                p["ticker"], p["symbol"], p["venue"], p["mode"], p["margin"], p["leverage"],
                p["contracts"], p["contract_size"], p["ref_price"], p["entry_price"],
                p["dropped_pct"], p["entry_price"],
            ),
        )
        db.commit()
        return cur.lastrowid


def get_open_positions() -> list[dict]:
    with _conn() as db:
        db.row_factory = sqlite3.Row
        rows = db.execute("SELECT * FROM positions WHERE status = 'open'").fetchall()
        return [dict(r) for r in rows]


def update_entry_price(pos_id: int, entry_price: float) -> None:
    """Виправляє ціну входу на РЕАЛЬНУ ціну виконання (довантажується після ордера,
    щоб не тримати гарячий шлях). min_price підтягуємо, якщо fill був нижчий."""
    with _conn() as db:
        db.execute(
            "UPDATE positions SET entry_price = ?, min_price = MIN(min_price, ?) WHERE id = ?",
            (entry_price, entry_price, pos_id),
        )
        db.commit()


def update_min_price(pos_id: int, min_price: float) -> None:
    with _conn() as db:
        db.execute(
            "UPDATE positions SET min_price = ? WHERE id = ?", (min_price, pos_id)
        )
        db.commit()


def close_position(pos_id: int, exit_price: float, reason: str,
                   pnl_usdt: float, pnl_pct: float) -> None:
    with _conn() as db:
        db.execute(
            """UPDATE positions
               SET status='closed', exit_price=?, exit_reason=?, pnl_usdt=?, pnl_pct=?,
                   closed_at=datetime('now')
               WHERE id = ?""",
            (exit_price, reason, pnl_usdt, pnl_pct, pos_id),
        )
        db.commit()
