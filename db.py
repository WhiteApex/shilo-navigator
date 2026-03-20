import os
from typing import Optional, List, Tuple, Set, Callable, Awaitable
from datetime import datetime
from pathlib import Path
from datetime import datetime

import aiosqlite

# --------- SETTINGS ---------
BASE_DIR = Path(__file__).resolve().parent.parent

# Папка data на уровне проекта
DB_PATH = os.getenv("DB_PATH", "/data/users.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


print(f"[DB] Используется база данных: {DB_PATH}")

# --------- BASE SCHEMA (only creates; changes go via migrations) ---------
CREATE_USERS_SQL = """
CREATE TABLE IF NOT EXISTS users(
    user_id     INTEGER PRIMARY KEY,
    username    TEXT,
    first_name  TEXT,
    last_name   TEXT,
    language    TEXT,
    created_at  TEXT,
    last_seen   TEXT
);
"""

CREATE_EVENTS_SQL = """
CREATE TABLE IF NOT EXISTS events(
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    title           TEXT NOT NULL,
    description     TEXT,
    photo_path      TEXT,
    starts_at       TEXT,
    ends_at         TEXT
);
"""

CREATE_REGISTRATIONS_SQL = """
CREATE TABLE IF NOT EXISTS registrations(
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id      INTEGER NOT NULL,
    user_id       INTEGER NOT NULL,
    registered_at TEXT NOT NULL,
    UNIQUE(event_id, user_id),
    FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE
);
"""

# --------- QUERIES ---------
INSERT_OR_IGNORE_USER_SQL = """
INSERT OR IGNORE INTO users(user_id, username, first_name, last_name, language, created_at, last_seen)
VALUES(?, ?, ?, ?, ?, ?, ?);
"""

UPDATE_LAST_SEEN_SQL = """
UPDATE users SET
    username   = ?,
    first_name = ?,
    last_name  = ?,
    language   = ?,
    last_seen  = ?
WHERE user_id = ?;
"""

SELECT_USER_SQL = "SELECT * FROM users WHERE user_id = ?;"

INSERT_EVENT_SQL = """
INSERT INTO events(title, description, photo_path, starts_at, ends_at, is_public)
VALUES(?, ?, ?, ?, ?, ?);
"""

UPSERT_EVENT_SQL = """
INSERT INTO events(id, title, description, photo_path, starts_at, ends_at, is_public)
VALUES(?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    title=excluded.title,
    description=excluded.description,
    photo_path=excluded.photo_path,
    starts_at=excluded.starts_at,
    ends_at=excluded.ends_at,
    is_public=excluded.is_public;
"""


SELECT_EVENTS_SQL = (
    "SELECT id, title, description, photo_path, starts_at, ends_at "
    "FROM events WHERE is_public = 1 ORDER BY id ASC;"
)


SELECT_EVENT_SQL = (
    "SELECT id, title, description, photo_path, starts_at, ends_at, "
    "COALESCE(photo_file_id, NULL) AS photo_file_id "
    "FROM events WHERE id = ?;"
)


INSERT_REG_SQL = """
INSERT OR IGNORE INTO registrations(event_id, user_id, registered_at)
VALUES(?, ?, ?);
"""

COUNT_REGS_SQL = "SELECT COUNT(*) FROM registrations WHERE event_id = ?;"

# ---------- MIGRATIONS FRAMEWORK ----------
async def _ensure_schema_migrations_table(conn: aiosqlite.Connection) -> None:
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations(
            version     TEXT PRIMARY KEY,
            applied_at  TEXT NOT NULL
        );
    """)

async def _applied_versions(conn: aiosqlite.Connection) -> Set[str]:
    cur = await conn.execute("SELECT version FROM schema_migrations;")
    rows = await cur.fetchall()
    await cur.close()
    return {r[0] for r in rows}

async def _record_applied(conn: aiosqlite.Connection, version: str) -> None:
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    await conn.execute(
        "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES(?, ?);",
        (version, now),
    )

async def _ensure_column(conn: aiosqlite.Connection, table: str, column: str, coltype: str) -> None:
    cur = await conn.execute(f"PRAGMA table_info({table});")
    cols = [row[1] for row in await cur.fetchall()]
    await cur.close()
    if column not in cols:
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype};")

async def _ensure_index(conn: aiosqlite.Connection, index_name: str, table: str, columns: str, unique: bool=False) -> None:
    cur = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name = ?;",
        (index_name,)
    )
    row = await cur.fetchone()
    await cur.close()
    if not row:
        sql = f"CREATE {'UNIQUE ' if unique else ''}INDEX {index_name} ON {table}({columns});"
        await conn.execute(sql)

# --- concrete migrations (idempotent) ---
async def m_001_add_users_phone(conn: aiosqlite.Connection) -> None:
    await _ensure_column(conn, "users", "phone", "TEXT")

async def m_002_add_events_photo_file_id(conn: aiosqlite.Connection) -> None:
    await _ensure_column(conn, "events", "photo_file_id", "TEXT")

async def m_003_add_basic_indexes(conn: aiosqlite.Connection) -> None:
    await _ensure_index(conn, "idx_regs_event", "registrations", "event_id")
    await _ensure_index(conn, "idx_regs_user",  "registrations", "user_id")

async def m_004_create_confirmations(conn: aiosqlite.Connection) -> None:
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS confirmations(
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id    INTEGER NOT NULL,
            user_id     INTEGER NOT NULL,
            status      TEXT NOT NULL CHECK(status IN ('yes','no')),
            updated_at  TEXT NOT NULL,
            UNIQUE(event_id, user_id),
            FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE
        );
    """)
    await _ensure_index(conn, "idx_conf_event", "confirmations", "event_id")
    await _ensure_index(conn, "idx_conf_user",  "confirmations", "user_id")

async def m_005_add_users_referral(conn: aiosqlite.Connection) -> None:
    await _ensure_column(conn, "users", "ref_code", "TEXT")
    await _ensure_column(conn, "users", "ref_set_at", "TEXT")

async def m_006_create_referral_entrypoints(conn: aiosqlite.Connection) -> None:
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS referral_entrypoints(
            code        TEXT PRIMARY KEY,   -- 'sber', 'vk', 'blog_october', ...
            event_id    INTEGER,            -- к какому событию вести (может быть NULL, но лучше всегда задавать)
            title       TEXT,               -- заголовок приглашения
            description TEXT,               -- описание приглашения
            is_active   INTEGER NOT NULL DEFAULT 1,
            created_at  TEXT NOT NULL,
            FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE SET NULL
        );
    """)
async def m_007_add_events_visibility(conn: aiosqlite.Connection) -> None:
    # если у тебя уже есть helper _ensure_column – пользуйся им
    await _ensure_column(conn, "events", "is_public", "INTEGER NOT NULL DEFAULT 1")

async def m_008_create_referral_entries(conn: aiosqlite.Connection) -> None:
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS referral_entries(
            code        TEXT PRIMARY KEY,     -- уникальный реф-код ('sber', 'vk', 'promo2025')
            event_id    INTEGER NOT NULL,     -- ID события, на которое ведёт код
            created_at  TEXT NOT NULL,        -- дата создания связи
            FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE
        );
    """)

async def m_009_add_confirmations_counts(conn: aiosqlite.Connection) -> None:
    """Добавляем детализацию подтверждения: количество взрослых и детей."""
    await _ensure_column(conn, "confirmations", "adults_count", "INTEGER")
    await _ensure_column(conn, "confirmations", "children_count", "INTEGER")

async def m_010_create_bot_settings(conn: aiosqlite.Connection) -> None:
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS bot_settings(
            key   TEXT PRIMARY KEY,
            value TEXT
        );
    """)



MIGRATIONS: list[tuple[str, Callable[[aiosqlite.Connection], Awaitable[None]]]] = [
    ("001_add_users_phone",           m_001_add_users_phone),
    ("002_add_events_photo_file_id",  m_002_add_events_photo_file_id),
    ("003_add_basic_indexes",         m_003_add_basic_indexes),
    ("004_create_confirmations",      m_004_create_confirmations),
    ("005_add_users_referral",        m_005_add_users_referral),
    ("006_create_referral_entrypoints", m_006_create_referral_entrypoints),
    ("007_add_events_visibility",      m_007_add_events_visibility),
    ("008_create_referral_entries", m_008_create_referral_entries),
    ("009_add_confirmations_counts",  m_009_add_confirmations_counts),
    ("010_create_bot_settings",       m_010_create_bot_settings),
]

async def run_migrations(conn: aiosqlite.Connection) -> None:
    await _ensure_schema_migrations_table(conn)
    await _ensure_column(conn, "users", "last_event_id", "INTEGER")
    applied = await _applied_versions(conn)
    for version, fn in MIGRATIONS:
        if version in applied:
            continue
        try:
            await conn.execute("BEGIN;")
            await fn(conn)
            await _record_applied(conn, version)
            await conn.execute("COMMIT;")
            print(f"[DB] migration applied: {version}")
        except Exception as e:
            await conn.execute("ROLLBACK;")
            raise RuntimeError(f"Migration {version} failed: {e}") from e

# --------- DATABASE CLASS ---------
class Database:
    def __init__(self, path: str = DB_PATH):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def set_last_event(self, user_id: int, event_id: int | None) -> None:
        """
        Сохраняем id последнего открытого пользователем мероприятия.
        Передай None, чтобы очистить.
        """
        await self._conn.execute(
            "UPDATE users SET last_event_id = ? WHERE user_id = ?;",
            (event_id, user_id),
        )
        await self._conn.commit()
    
    async def set_event_public(self, event_id: int, is_public: bool):
        await self._conn.execute(
            "UPDATE events SET is_public = ? WHERE id = ?;",
            (1 if is_public else 0, event_id),
        )
        await self._conn.commit()

    async def list_events_raw_admin(self):
        cur = await self._conn.execute(
            "SELECT id, title, description, photo_path, starts_at, ends_at, is_public FROM events ORDER BY id ASC;"
        )
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def get_event_full(self, event_id: int):
        cur = await self._conn.execute(
            "SELECT id, title, description, photo_path, starts_at, ends_at, photo_file_id, is_public FROM events WHERE id = ?;",
            (event_id,)
        )
        row = await cur.fetchone()
        await cur.close()
        return row
    

    async def get_setting(self, key: str) -> Optional[str]:
        cur = await self._conn.execute(
            "SELECT value FROM bot_settings WHERE key = ?;",
            (key,),
        )
        row = await cur.fetchone()
        await cur.close()
        return row[0] if row else None

    async def set_setting(self, key: str, value: Optional[str]) -> None:
        if value is None:
            await self._conn.execute("DELETE FROM bot_settings WHERE key = ?;", (key,))
        else:
            await self._conn.execute(
                """
                INSERT INTO bot_settings(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value;
                """,
                (key, value),
            )
        await self._conn.commit()
    


    async def add_referral(self, code: str, event_id: int):
        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        await self._conn.execute(
            """
            INSERT OR REPLACE INTO referral_entries(code, event_id, created_at)
            VALUES (?, ?, ?);
            """,
            (code, event_id, now),
        )
        await self._conn.commit()

    async def get_event_for_referral(self, code: str):
        cur = await self._conn.execute(
            "SELECT event_id FROM referral_entries WHERE code = ?;",
            (code,)
        )
        row = await cur.fetchone()
        await cur.close()
        return row[0] if row else None

    async def list_referrals_for_event(self, event_id: int) -> list[str]:
        """
        Вернёт список реф-кодов, привязанных к данному событию.
        """
        cur = await self._conn.execute(
            "SELECT code FROM referral_entries WHERE event_id = ? ORDER BY code ASC;",
            (event_id,),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [r[0] for r in rows]

    async def delete_referral(self, code: str) -> None:
        """
        Удаляет реф-код полностью.
        """
        await self._conn.execute(
            "DELETE FROM referral_entries WHERE code = ?;",
            (code,),
        )
        await self._conn.commit()

    async def get_last_event(self, user_id: int) -> int | None:
        """
        Возвращает id последнего мероприятия, на котором «остановился» пользователь,
        или None, если не задан.
        """
        cur = await self._conn.execute(
            "SELECT last_event_id FROM users WHERE user_id = ?;",
            (user_id,)
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row and row[0] is not None else None


    async def connect(self):
        print(f"[DB] Используется база данных: {os.path.abspath(self.path)}")
        self._conn = await aiosqlite.connect(self.path)
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA foreign_keys = ON;")

        # base tables
        await self._conn.execute(CREATE_USERS_SQL)
        await self._conn.execute(CREATE_EVENTS_SQL)
        await self._conn.execute(CREATE_REGISTRATIONS_SQL)

        # run migrations safely on every startup
        await run_migrations(self._conn)

        await self._conn.commit()

    async def close(self):
        if self._conn:
            await self._conn.close()
            self._conn = None

        # ---------- Confirmations ----------
    async def set_confirmation(
        self,
        event_id: int,
        user_id: int,
        status: str,
        adults_count: Optional[int] = None,
        children_count: Optional[int] = None,
    ) -> None:
        assert status in ("yes", "no")
        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        await self._conn.execute("""
            INSERT INTO confirmations(event_id, user_id, status, updated_at, adults_count, children_count)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id, user_id) DO UPDATE SET
                status = excluded.status,
                updated_at = excluded.updated_at,
                adults_count = excluded.adults_count,
                children_count = excluded.children_count;
        """, (event_id, user_id, status, now, adults_count, children_count))
        await self._conn.commit()


    async def get_confirmation(self, event_id: int, user_id: int) -> Optional[str]:
        cur = await self._conn.execute(
            "SELECT status FROM confirmations WHERE event_id = ? AND user_id = ?;",
            (event_id, user_id),
        )
        row = await cur.fetchone()
        await cur.close()
        return row[0] if row else None
    
    async def get_event_confirmation_report(self, event_id: int):
        sql = """
        SELECT
            u.user_id,
            u.phone,
            COALESCE(c.status, 'none') AS status,
            c.adults_count,
            c.children_count,
            u.username,
            u.first_name,
            u.last_name
        FROM registrations r
        JOIN users u ON u.user_id = r.user_id
        LEFT JOIN confirmations c
            ON c.event_id = r.event_id AND c.user_id = r.user_id
        WHERE r.event_id = ?
        ORDER BY status DESC, u.user_id;
        """
        cur = await self._conn.execute(sql, (event_id,))
        rows = await cur.fetchall()
        await cur.close()
        return rows
    async def list_registered_user_ids_pending_confirmation(self, event_id: int) -> list[int]:
        """
        Возвращает user_id всех записанных на event_id, у кого ещё НЕТ записи в confirmations.
        (то есть они не отвечали ни да/нет)
        """
        sql = """
        SELECT r.user_id
        FROM registrations r
        LEFT JOIN confirmations c
            ON c.event_id = r.event_id AND c.user_id = r.user_id
        WHERE r.event_id = ?
        AND c.user_id IS NULL
        ORDER BY r.user_id;
        """
        cur = await self._conn.execute(sql, (event_id,))
        rows = await cur.fetchall()
        await cur.close()
        return [int(r[0]) for r in rows]

    
    async def get_confirmation_details(
        self,
        event_id: int,
        user_id: int,
    ) -> Optional[tuple[str, Optional[int], Optional[int]]]:
        cur = await self._conn.execute(
            "SELECT status, adults_count, children_count FROM confirmations WHERE event_id = ? AND user_id = ?;",
            (event_id, user_id),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return None
        return row[0], row[1], row[2]

    
        # ---------- Referral entrypoints ----------
    async def upsert_referral_entrypoint(
        self,
        code: str,
        event_id: Optional[int],
        title: Optional[str],
        description: Optional[str],
        is_active: bool = True,
    ) -> None:
        """
        Создать или обновить реферальную входную точку.
        Используй это из админского скрипта/панели, а не из боевого хэндлера.
        """
        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        await self._conn.execute("""
            INSERT INTO referral_entrypoints(code, event_id, title, description, is_active, created_at)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                event_id    = excluded.event_id,
                title       = excluded.title,
                description = excluded.description,
                is_active   = excluded.is_active;
        """, (code, event_id, title, description, 1 if is_active else 0, now))
        await self._conn.commit()

    async def get_referral_entrypoint(self, code: str) -> Optional[tuple]:
        """
        Вернёт (code, event_id, title, description, is_active) для активного кода.
        """
        cur = await self._conn.execute(
            "SELECT code, event_id, title, description, is_active "
            "FROM referral_entrypoints WHERE code = ? AND is_active = 1;",
            (code,),
        )
        row = await cur.fetchone()
        await cur.close()
        return row


    # ----- STATS -----
    async def stats_totals(self) -> tuple[int, int, int]:
        cur = await self._conn.execute("SELECT COUNT(*) FROM users;")
        users = int((await cur.fetchone())[0]); await cur.close()

        cur = await self._conn.execute("SELECT COUNT(*) FROM events;")
        events = int((await cur.fetchone())[0]); await cur.close()

        cur = await self._conn.execute("SELECT COUNT(*) FROM registrations;")
        regs = int((await cur.fetchone())[0]); await cur.close()

        return users, events, regs

    async def stats_per_event(self) -> List[Tuple[int, str, int]]:
        sql = """
        SELECT e.id, e.title, COALESCE(COUNT(r.id), 0) AS cnt
        FROM events e
        LEFT JOIN registrations r ON r.event_id = e.id
        GROUP BY e.id, e.title
        ORDER BY e.id ASC;
        """
        cur = await self._conn.execute(sql)
        rows = await cur.fetchall()
        await cur.close()
        return [(int(r[0]), r[1], int(r[2])) for r in rows]

    async def stats_last_24h(self, since_iso: str) -> Tuple[int, int]:
        cur = await self._conn.execute(
            "SELECT COUNT(*) FROM users WHERE created_at >= ?;", (since_iso,)
        )
        users_24h = int((await cur.fetchone())[0]); await cur.close()

        cur = await self._conn.execute(
            "SELECT COUNT(*) FROM registrations WHERE registered_at >= ?;", (since_iso,)
        )
        regs_24h = int((await cur.fetchone())[0]); await cur.close()

        return users_24h, regs_24h

    async def stats_per_event_last_24h(self, since_iso: str) -> List[Tuple[int, str, int]]:
        sql = """
        SELECT
            e.id,
            e.title,
            COALESCE(SUM(CASE WHEN r.registered_at >= ? THEN 1 ELSE 0 END), 0) AS cnt_24h
        FROM events e
        LEFT JOIN registrations r ON r.event_id = e.id
        GROUP BY e.id, e.title
        ORDER BY e.id ASC;
        """
        cur = await self._conn.execute(sql, (since_iso,))
        rows = await cur.fetchall()
        await cur.close()
        return [(int(r[0]), r[1], int(r[2])) for r in rows]

    # ---------- Users ----------
    async def upsert_user(
        self,
        user_id: int,
        username: Optional[str],
        first_name: Optional[str],
        last_name: Optional[str],
        language: Optional[str],
    ):
        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        created_at = now
        await self._conn.execute(
            INSERT_OR_IGNORE_USER_SQL,
            (user_id, username, first_name, last_name, language, created_at, now),
        )
        await self._conn.execute(
            UPDATE_LAST_SEEN_SQL,
            (username, first_name, last_name, language, now, user_id),
        )
        await self._conn.commit()

    async def get_user(self, user_id: int):
        cur = await self._conn.execute(SELECT_USER_SQL, (user_id,))
        row = await cur.fetchone()
        await cur.close()
        return row

    async def list_all_user_ids(self) -> List[int]:
        cur = await self._conn.execute("SELECT user_id FROM users;")
        rows = await cur.fetchall()
        await cur.close()
        return [int(r[0]) for r in rows]

    async def list_registered_user_ids(self, event_id: int) -> List[int]:
        cur = await self._conn.execute(
            "SELECT user_id FROM registrations WHERE event_id = ?;",
            (event_id,),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [int(r[0]) for r in rows]

    async def get_user_phone(self, user_id: int) -> Optional[str]:
        cur = await self._conn.execute("SELECT phone FROM users WHERE user_id = ?;", (user_id,))
        row = await cur.fetchone()
        await cur.close()
        return row[0] if row and row[0] else None

    async def set_user_phone(self, user_id: int, phone: str) -> None:
        await self._conn.execute(
            "UPDATE users SET phone = ? WHERE user_id = ?;",
            (phone, user_id),
        )
        await self._conn.commit()
    
    async def save_referral(self, user_id: int, ref_code: str) -> None:
        """
        Фиксируем реферальный код для пользователя.
        Первый источник считаем главным — не перезаписываем, если уже есть.
        """
        cur = await self._conn.execute(
            "SELECT ref_code FROM users WHERE user_id = ?;",
            (user_id,),
        )
        row = await cur.fetchone()
        await cur.close()

        # Если ref_code уже есть — ничего не трогаем
        if row and row[0] is not None:
            return

        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        await self._conn.execute(
            "UPDATE users SET ref_code = ?, ref_set_at = ? WHERE user_id = ?;",
            (ref_code, now, user_id),
        )
        await self._conn.commit()

    async def get_user_referral(self, user_id: int) -> Optional[str]:
        """
        Возвращает реферальный код пользователя (если он есть).
        """
        cur = await self._conn.execute(
            "SELECT ref_code FROM users WHERE user_id = ?;",
            (user_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        return row[0] if row and row[0] is not None else None


    # ---------- Events ----------
        # ---------- Events ----------
    async def add_event(
        self,
        title: str,
        description: Optional[str] = None,
        photo_path: Optional[str] = None,
        starts_at: Optional[str] = None,
        ends_at: Optional[str] = None,
        is_public: bool = True,
    ) -> int:
        cur = await self._conn.execute(
            INSERT_EVENT_SQL,
            (
                title,
                description,
                photo_path,
                starts_at,
                ends_at,
                1 if is_public else 0,
            ),
        )
        await self._conn.commit()
        return int(cur.lastrowid)

    async def upsert_event(
        self,
        *,
        id: int,
        title: str,
        description: Optional[str] = None,
        photo_path: Optional[str] = None,
        starts_at: Optional[str] = None,
        ends_at: Optional[str] = None,
        is_public: bool = True,
    ) -> None:
        await self._conn.execute(
            UPSERT_EVENT_SQL,
            (
                id,
                title,
                description,
                photo_path,
                starts_at,
                ends_at,
                1 if is_public else 0,
            ),
        )
        await self._conn.commit()

    async def list_events(self) -> List[Tuple]:
        cur = await self._conn.execute(SELECT_EVENTS_SQL)
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def get_event(self, event_id: int):
        cur = await self._conn.execute(SELECT_EVENT_SQL, (event_id,))
        row = await cur.fetchone()
        await cur.close()
        return row

    async def update_event_description(self, event_id: int, description: Optional[str]) -> None:
        await self._conn.execute(
            "UPDATE events SET description = ? WHERE id = ?;",
            (description, event_id),
        )
        await self._conn.commit()

    async def set_event_photo_file_id(self, event_id: int, file_id: str) -> None:
        await self._conn.execute(
            "UPDATE events SET photo_file_id = ? WHERE id = ?;",
            (file_id, event_id),
        )
        await self._conn.commit()


    async def is_registered(self, event_id: int, user_id: int) -> bool:
        cur = await self._conn.execute(
            "SELECT 1 FROM registrations WHERE event_id = ? AND user_id = ?;",
            (event_id, user_id),
        )
        row = await cur.fetchone()
        await cur.close()
        return row is not None

    async def register(self, event_id: int, user_id: int) -> bool:
        now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        cur = await self._conn.execute(INSERT_REG_SQL, (event_id, user_id, now))
        await self._conn.commit()
        return cur.rowcount == 1

    async def registrations_count(self, event_id: int) -> int:
        cur = await self._conn.execute(COUNT_REGS_SQL, (event_id,))
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0
