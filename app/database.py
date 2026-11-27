"""SQLite 连接与建表。

时间一律存为归一化的 UTC ISO-8601 字符串（``YYYY-MM-DDTHH:MM:SS.ffffff+00:00``），
该格式可直接按字典序比较先后。半开失准窗口采用 ``[window_start, window_end)``。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "release.db"

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS gauge (
    gauge_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

-- 半开失准时间窗 [window_start, window_end)；未被任何窗口覆盖的时间视为有效
CREATE TABLE IF NOT EXISTS gauge_window (
    window_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    gauge_id    INTEGER NOT NULL REFERENCES gauge(gauge_id),
    window_start TEXT NOT NULL,
    window_end   TEXT NOT NULL,
    note        TEXT,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workpiece (
    workpiece_id INTEGER PRIMARY KEY AUTOINCREMENT,
    code         TEXT NOT NULL UNIQUE,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS process (
    process_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    workpiece_id INTEGER NOT NULL REFERENCES workpiece(workpiece_id),
    code         TEXT NOT NULL,
    name         TEXT NOT NULL,
    UNIQUE(workpiece_id, code)
);

CREATE TABLE IF NOT EXISTS process_dependency (
    process_id INTEGER NOT NULL REFERENCES process(process_id),
    prereq_id  INTEGER NOT NULL REFERENCES process(process_id),
    PRIMARY KEY (process_id, prereq_id),
    CHECK (process_id <> prereq_id)
);

CREATE TABLE IF NOT EXISTS item (
    item_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    process_id  INTEGER NOT NULL REFERENCES process(process_id),
    code        TEXT NOT NULL,
    name        TEXT NOT NULL,
    lower_limit INTEGER NOT NULL,
    upper_limit INTEGER NOT NULL,
    UNIQUE(process_id, code),
    CHECK (lower_limit <= upper_limit)
);

CREATE TABLE IF NOT EXISTS inspection (
    inspection_id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id       INTEGER NOT NULL REFERENCES item(item_id),
    gauge_id      INTEGER NOT NULL REFERENCES gauge(gauge_id),
    reading       INTEGER NOT NULL,
    measured_at   TEXT NOT NULL,
    accepted_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_inspection_item ON inspection(item_id, inspection_id);
CREATE INDEX IF NOT EXISTS idx_inspection_gauge_time ON inspection(gauge_id, measured_at);

-- 凭据：开工 / 复核（含复核恢复）/ 完工，全部永久保留
CREATE TABLE IF NOT EXISTS credential (
    credential_id INTEGER PRIMARY KEY AUTOINCREMENT,
    process_id    INTEGER NOT NULL REFERENCES process(process_id),
    kind          TEXT NOT NULL CHECK (kind IN ('START', 'REVIEW', 'COMPLETE')),
    basis_id      INTEGER REFERENCES credential(credential_id),
    operator      TEXT,
    created_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_credential_process ON credential(process_id);

-- 凭据对当时所依据检验记录的快照；登记新读数绝不回填这里
CREATE TABLE IF NOT EXISTS credential_inspection (
    credential_id INTEGER NOT NULL REFERENCES credential(credential_id),
    item_id       INTEGER NOT NULL REFERENCES item(item_id),
    inspection_id INTEGER NOT NULL REFERENCES inspection(inspection_id),
    PRIMARY KEY (credential_id, item_id)
);

CREATE TABLE IF NOT EXISTS process_state (
    process_id     INTEGER PRIMARY KEY REFERENCES process(process_id),
    status         TEXT NOT NULL CHECK (
                     status IN ('NOT_STARTED', 'STARTED', 'COMPLETED', 'QUARANTINED')),
    quarantined_at TEXT,
    reason         TEXT
);

CREATE TABLE IF NOT EXISTS process_event (
    event_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    process_id   INTEGER NOT NULL REFERENCES process(process_id),
    event_type   TEXT NOT NULL,
    detail       TEXT,
    operator     TEXT,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    process_id     INTEGER NOT NULL REFERENCES process(process_id),
    pre_status     TEXT NOT NULL,
    reason         TEXT NOT NULL,
    opened_at      TEXT NOT NULL,
    closed_at      TEXT
);
"""


def get_connection(db_path: Path | str | None = None) -> sqlite3.Connection:
    path = str(db_path if db_path is not None else DB_PATH)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    # WAL 让读不阻塞写；写仍由应用层全局锁串行化
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(db_path: Path | str | None = None) -> None:
    conn = get_connection(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()
