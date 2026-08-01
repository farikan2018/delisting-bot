"""SQLite-стан: дедуп анонсів (щоб не спрацьовувати двічі)."""
import sqlite3
from contextlib import closing

import config


def init() -> None:
    with closing(sqlite3.connect(config.DB_PATH)) as db:
        db.execute(
            """CREATE TABLE IF NOT EXISTS seen_articles (
                article_id TEXT PRIMARY KEY,
                title      TEXT,
                seen_at    TEXT DEFAULT (datetime('now'))
            )"""
        )
        db.commit()


def is_seen(article_id: str) -> bool:
    with closing(sqlite3.connect(config.DB_PATH)) as db:
        cur = db.execute(
            "SELECT 1 FROM seen_articles WHERE article_id = ?", (str(article_id),)
        )
        return cur.fetchone() is not None


def mark_seen(article_id: str, title: str) -> None:
    with closing(sqlite3.connect(config.DB_PATH)) as db:
        db.execute(
            "INSERT OR IGNORE INTO seen_articles (article_id, title) VALUES (?, ?)",
            (str(article_id), title),
        )
        db.commit()


def seen_count() -> int:
    with closing(sqlite3.connect(config.DB_PATH)) as db:
        return db.execute("SELECT COUNT(*) FROM seen_articles").fetchone()[0]
