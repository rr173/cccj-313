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

-- ------------------------------------------------------------------ 工位交接
-- 工位缓冲位固定容量；一件占一位。ACTUAL 表示实物在位，RESERVED 表示已预留
-- 但尚无实物（待发收件位 / 发运后的退回保留位）。两者都占用容量。

CREATE TABLE IF NOT EXISTS workstation (
    workstation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    code           TEXT NOT NULL UNIQUE,
    name           TEXT NOT NULL,
    capacity       INTEGER NOT NULL CHECK (capacity >= 0),
    created_at     TEXT NOT NULL
);

-- 一件实物对应一个已建工件编号；关口工序必须属于该工件。
-- 不复制任何检验 / 完工凭据，关口有效性始终实时读取原工序状态。
CREATE TABLE IF NOT EXISTS piece (
    piece_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    workpiece_id          INTEGER NOT NULL UNIQUE REFERENCES workpiece(workpiece_id),
    gate_process_id       INTEGER NOT NULL REFERENCES process(process_id),
    status                TEXT NOT NULL CHECK (status IN ('AT_WORKSTATION', 'IN_TRANSIT')),
    current_workstation_id INTEGER NOT NULL REFERENCES workstation(workstation_id),
    created_at            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transfer_order (
    order_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_workstation_id INTEGER NOT NULL REFERENCES workstation(workstation_id),
    dest_workstation_id   INTEGER NOT NULL REFERENCES workstation(workstation_id),
    status                TEXT NOT NULL CHECK (
                              status IN ('PENDING', 'SHIPPED', 'COMPLETED', 'CANCELLED')),
    created_at            TEXT NOT NULL,
    shipped_at            TEXT,
    closed_at             TEXT,
    CHECK (source_workstation_id <> dest_workstation_id)
);

-- 单内每件的独立状态；released_at 非空表示该件已落地（签收 / 退回）或被撤销，
-- 不再被本单占用。部分唯一索引保证一件同时最多在一张待发 / 在途单上。
CREATE TABLE IF NOT EXISTS transfer_order_piece (
    order_id    INTEGER NOT NULL REFERENCES transfer_order(order_id),
    piece_id    INTEGER NOT NULL REFERENCES piece(piece_id),
    state       TEXT NOT NULL CHECK (
                    state IN ('PENDING', 'IN_TRANSIT', 'RECEIVED', 'RETURNED', 'CANCELLED')),
    released_at TEXT,
    PRIMARY KEY (order_id, piece_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_piece_order
    ON transfer_order_piece(piece_id) WHERE released_at IS NULL;

CREATE TABLE IF NOT EXISTS workstation_slot (
    slot_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    workstation_id INTEGER NOT NULL REFERENCES workstation(workstation_id),
    piece_id       INTEGER NOT NULL REFERENCES piece(piece_id),
    state          TEXT NOT NULL CHECK (state IN ('ACTUAL', 'RESERVED')),
    purpose        TEXT NOT NULL CHECK (purpose IN ('HOME', 'RECEIPT', 'RETURN')),
    order_id       INTEGER REFERENCES transfer_order(order_id),
    created_at     TEXT NOT NULL,
    UNIQUE (workstation_id, piece_id, purpose)
);
CREATE INDEX IF NOT EXISTS idx_slot_workstation ON workstation_slot(workstation_id);

-- 容量兜底：即使应用层校验遗漏，也决不超占。ACTUAL 与 RESERVED 均计数。
CREATE TRIGGER IF NOT EXISTS trg_workstation_slot_capacity
BEFORE INSERT ON workstation_slot
BEGIN
    SELECT RAISE(ABORT, 'workstation_capacity_exceeded')
    WHERE (SELECT COUNT(*) FROM workstation_slot WHERE workstation_id = NEW.workstation_id)
          >= (SELECT capacity FROM workstation WHERE workstation_id = NEW.workstation_id);
END;

-- 发运 / 签收 / 退回的请求号幂等记录，按调拨单隔离。失败请求不写此表。
CREATE TABLE IF NOT EXISTS transfer_action (
    action_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id    INTEGER NOT NULL REFERENCES transfer_order(order_id),
    request_id  TEXT NOT NULL,
    action      TEXT NOT NULL CHECK (action IN ('SHIP', 'RECEIVE', 'RETURN')),
    operator    TEXT NOT NULL,
    piece_key   TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    UNIQUE (order_id, request_id)
);

-- 每件落地留痕，用于查询“谁签收 / 退回了哪些件”。只增不改。
CREATE TABLE IF NOT EXISTS transfer_record (
    record_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id             INTEGER NOT NULL REFERENCES transfer_order(order_id),
    action_id            INTEGER NOT NULL REFERENCES transfer_action(action_id),
    piece_id             INTEGER NOT NULL REFERENCES piece(piece_id),
    action               TEXT NOT NULL CHECK (action IN ('RECEIVE', 'RETURN')),
    operator             TEXT NOT NULL,
    request_id           TEXT NOT NULL,
    from_workstation_id  INTEGER NOT NULL,
    to_workstation_id    INTEGER NOT NULL,
    created_at           TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transfer_record_order ON transfer_record(order_id);
"""


def get_connection(db_path: Path | str | None = None) -> sqlite3.Connection:
    path = str(db_path if db_path is not None else DB_PATH)
    # isolation_level=None：事务边界由服务层显式 BEGIN IMMEDIATE / COMMIT 控制，
    # 使多个进程（两个终端）共用同一个 SQLite 库时写操作真正串行化。
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    # WAL 让读不阻塞写；写事务由 BEGIN IMMEDIATE + busy_timeout 串行化
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
