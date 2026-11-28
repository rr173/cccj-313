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

-- ------------------------------------------------------------------ 实装 / 拆件
-- 当前实物组成树：parent 直接承载 child。一个 child 至多有一个直接承载件
-- （UNIQUE 约束兜底）；环由应用层“待装件必须是最外层整件”的校验排除。
CREATE TABLE IF NOT EXISTS piece_assembly (
    parent_piece_id  INTEGER NOT NULL REFERENCES piece(piece_id),
    child_piece_id   INTEGER NOT NULL UNIQUE REFERENCES piece(piece_id),
    installed_at     TEXT NOT NULL,
    install_event_id INTEGER NOT NULL,
    PRIMARY KEY (parent_piece_id, child_piece_id),
    CHECK (parent_piece_id <> child_piece_id)
);
CREATE INDEX IF NOT EXISTS idx_assembly_parent ON piece_assembly(parent_piece_id);

-- 装拆留痕（只增不改）：人、件、工位全部记录。REMOVE 时 detail 用 JSON 记下
-- 随子件一起拆下的整组成员，保证事后可查。
CREATE TABLE IF NOT EXISTS assembly_event (
    event_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    action         TEXT NOT NULL CHECK (action IN ('INSTALL', 'REMOVE')),
    parent_piece_id INTEGER NOT NULL REFERENCES piece(piece_id),
    child_piece_id  INTEGER NOT NULL REFERENCES piece(piece_id),
    workstation_id INTEGER NOT NULL REFERENCES workstation(workstation_id),
    operator       TEXT NOT NULL,
    detail         TEXT,
    created_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_assembly_event_child ON assembly_event(child_piece_id);
CREATE INDEX IF NOT EXISTS idx_assembly_event_parent ON assembly_event(parent_piece_id);

-- ------------------------------------------------------- 发运追溯证据（只增不改）
-- 每次发运为每个最外层整件留一份组成快照；只为上线后的新发运留证据，旧单无快照。
-- 快照独立于当前 piece_assembly：事后拆换 / 补测 / 复核都不改变这里的历史依据。
CREATE TABLE IF NOT EXISTS shipment_snapshot (
    snapshot_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id      INTEGER NOT NULL REFERENCES transfer_order(order_id),
    action_id     INTEGER NOT NULL REFERENCES transfer_action(action_id),
    root_piece_id INTEGER NOT NULL REFERENCES piece(piece_id),
    created_at    TEXT NOT NULL,
    UNIQUE (action_id, root_piece_id)
);
CREATE INDEX IF NOT EXISTS idx_ship_snapshot_order ON shipment_snapshot(order_id);

-- 发运当时整件内的每个成员（含最外层本身，depth=0）及其当时的父子位置
CREATE TABLE IF NOT EXISTS shipment_snapshot_member (
    snapshot_id      INTEGER NOT NULL REFERENCES shipment_snapshot(snapshot_id),
    piece_id         INTEGER NOT NULL REFERENCES piece(piece_id),
    workpiece_id     INTEGER NOT NULL REFERENCES workpiece(workpiece_id),
    gate_process_id  INTEGER NOT NULL REFERENCES process(process_id),
    parent_piece_id  INTEGER REFERENCES piece(piece_id),
    depth            INTEGER NOT NULL CHECK (depth >= 0),
    PRIMARY KEY (snapshot_id, piece_id)
);
CREATE INDEX IF NOT EXISTS idx_ship_snapshot_member_piece ON shipment_snapshot_member(piece_id);

-- 发运当时每个成员转运关口生效的开工 / 复核凭据（COMPLETE 不作为生效依据）。
-- 凭据所引用的检验通过既有 credential_inspection 关联，事后不回填、不改写。
CREATE TABLE IF NOT EXISTS shipment_snapshot_credential (
    snapshot_id    INTEGER NOT NULL REFERENCES shipment_snapshot(snapshot_id),
    piece_id       INTEGER NOT NULL REFERENCES piece(piece_id),
    credential_id  INTEGER REFERENCES credential(credential_id),
    process_id     INTEGER NOT NULL REFERENCES process(process_id),
    credential_kind TEXT,
    PRIMARY KEY (snapshot_id, piece_id)
);

-- ------------------------------------------------------------------ 班次派工
-- 一台机器、一副可周转夹具是两种可被预约的独占资源。派工把一个未开工工序
-- 编入一个班次，并在 [reserved_start, reserved_end) 内连续占用「机器 + 夹具」。
-- 所有区间均为左闭右开，且预约从整分钟开始。

CREATE TABLE IF NOT EXISTS machine (
    machine_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fixture (
    fixture_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

-- 资源停用时段：对机器或夹具按 resource_type 区分。停用是资源级事实，
-- 与班次无关；登记后由该班次的重排动作消费。
CREATE TABLE IF NOT EXISTS resource_downtime (
    downtime_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    resource_type TEXT NOT NULL CHECK (resource_type IN ('MACHINE', 'FIXTURE')),
    resource_id   INTEGER NOT NULL,
    window_start  TEXT NOT NULL,
    window_end    TEXT NOT NULL,
    operator      TEXT,
    note          TEXT,
    created_at    TEXT NOT NULL,
    CHECK (window_start < window_end)
);
CREATE INDEX IF NOT EXISTS idx_downtime_resource
    ON resource_downtime(resource_type, resource_id, window_start);

CREATE TABLE IF NOT EXISTS shift (
    shift_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    code         TEXT NOT NULL UNIQUE,
    operator     TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end   TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('PUBLISHED', 'COMPLETED')),
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    CHECK (window_start < window_end)
);

-- 每次派工尝试一行；is_current=1 表示该工序当前生效的那次派工。被重排换掉的
-- 旧派工保留为 REPLACED，暂停的保留为 SUSPENDED，均不再可执行。
-- 部分唯一索引保证一道工序在任一时刻最多只有一个当前派工（也就只能出现在
-- 一个班次里）。
CREATE TABLE IF NOT EXISTS dispatch (
    dispatch_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    shift_id         INTEGER NOT NULL REFERENCES shift(shift_id),
    process_id       INTEGER NOT NULL REFERENCES process(process_id),
    machine_id       INTEGER NOT NULL REFERENCES machine(machine_id),
    fixture_id       INTEGER NOT NULL REFERENCES fixture(fixture_id),
    window_start     TEXT NOT NULL,
    window_end       TEXT NOT NULL,
    duration_minutes INTEGER NOT NULL CHECK (duration_minutes > 0),
    reserved_start   TEXT NOT NULL,
    reserved_end     TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (
                         status IN ('PLANNED', 'STARTED', 'COMPLETED',
                                    'SUSPENDED', 'REPLACED')),
    is_current       INTEGER NOT NULL CHECK (is_current IN (0, 1)),
    actual_start_at  TEXT,
    actual_end_at    TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    CHECK (window_start < window_end),
    CHECK (reserved_start < reserved_end)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_current_dispatch_process
    ON dispatch(process_id) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS idx_dispatch_shift ON dispatch(shift_id);
CREATE INDEX IF NOT EXISTS idx_dispatch_resource_time
    ON dispatch(machine_id, reserved_start);
CREATE INDEX IF NOT EXISTS idx_dispatch_fixture_time
    ON dispatch(fixture_id, reserved_start);

-- 该次派工允许的「机器、夹具」组合（请求时给出的全部备选，含最终选中者）。
-- 重排时只能在原备选内换组合。
CREATE TABLE IF NOT EXISTS dispatch_option (
    dispatch_id INTEGER NOT NULL REFERENCES dispatch(dispatch_id),
    machine_id  INTEGER NOT NULL REFERENCES machine(machine_id),
    fixture_id  INTEGER NOT NULL REFERENCES fixture(fixture_id),
    PRIMARY KEY (dispatch_id, machine_id, fixture_id)
);
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
