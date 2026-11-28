"""业务规则层。

并发模型：所有写操作在进程内用一把可重入锁串行化，每个写方法在一个 SQLite
事务内完成“校验 + 落库 + 隔离重算”，保证失准登记、开工、复核竞争时，不会在
本应隔离的工序上继续放行。

关键规则：
* 每个项目以「服务最后接受的一次检验」为准（inspection_id 最大者），失效后
  不回退旧记录；
* 量具未被任何半开窗口 [start, end) 覆盖的测量时间一律视为有效；
* 开工 / 复核 / 完工均生成凭据，并快照当时依据的检验记录，事后不回填；
* 已开工或完工而失去有效依据的工序进入 QUARANTINED，并向下游传导；隔离只能
  由质量员逐道复核解除，补测上游不会自动释放下游。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from .database import get_connection, init_db

_service_lock = threading.RLock()


class ServiceError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


@contextmanager
def _immediate_tx(conn: sqlite3.Connection) -> Iterator[None]:
    """显式 BEGIN IMMEDIATE 事务：立即取得 SQLite 写锁，跨进程串行化写操作。

    连接使用 isolation_level=None，因此事务边界完全由这里控制。
    业务校验全部在写锁内完成，两个终端并发的重复发运 / 落地 / 容量占用
    都会被串行化，后到者看到前者已提交的状态并据此拒绝。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


# ---------------------------------------------------------------- time helpers

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_now_override: Optional[str] = None


@contextmanager
def override_now(value: Optional[str]) -> Iterator[None]:
    """测试 / 重排模拟用：临时固定“当前时刻”。仅供内部与测试使用。"""
    global _now_override
    previous = _now_override
    _now_override = normalize_ts(value) if value is not None else None
    try:
        yield
    finally:
        _now_override = previous


def current_ts() -> str:
    return _now_override if _now_override is not None else now_iso()


def normalize_ts(raw: str) -> str:
    """把 ISO-8601 时间归一化为可字典序比较的 UTC 字符串。"""
    text = raw.strip()
    # Python 3.11 之前 fromisoformat 不认末尾 Z
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ServiceError(422, f"无法解析时间 {raw!r}：{exc}") from exc
    if dt.tzinfo is None:
        # 无时区的输入按 UTC 处理
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


# ------------------------------------------------------------------ low level

def _gauge_valid_at(conn: sqlite3.Connection, gauge_id: int, ts: str) -> bool:
    """半开窗口 [window_start, window_end)；落在边界 end 上仍有效。"""
    row = conn.execute(
        """
        SELECT 1 FROM gauge_window
        WHERE gauge_id = ? AND window_start <= ? AND ? < window_end
        LIMIT 1
        """,
        (gauge_id, ts, ts),
    ).fetchone()
    return row is None


def _latest_inspection(conn: sqlite3.Connection, item_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM inspection WHERE item_id = ? ORDER BY inspection_id DESC LIMIT 1",
        (item_id,),
    ).fetchone()


def _active_credential(conn: sqlite3.Connection, process_id: int) -> Optional[sqlite3.Row]:
    """当前生效依据：最近一张 START / REVIEW 凭据（COMPLETE 不算开工依据）。"""
    return conn.execute(
        """
        SELECT * FROM credential
        WHERE process_id = ? AND kind IN ('START', 'REVIEW')
        ORDER BY credential_id DESC LIMIT 1
        """,
        (process_id,),
    ).fetchone()


def _prereq_ids(conn: sqlite3.Connection, process_id: int) -> list[int]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT prereq_id FROM process_dependency WHERE process_id = ? ORDER BY prereq_id",
            (process_id,),
        )
    ]


def _child_ids(conn: sqlite3.Connection, process_id: int) -> list[int]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT process_id FROM process_dependency WHERE prereq_id = ? ORDER BY process_id",
            (process_id,),
        )
    ]


def _get_process(conn: sqlite3.Connection, process_id: int) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM process WHERE process_id = ?", (process_id,)).fetchone()
    if row is None:
        raise ServiceError(404, f"工序 {process_id} 不存在")
    return row


def _get_state(conn: sqlite3.Connection, process_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM process_state WHERE process_id = ?", (process_id,)
    ).fetchone()
    if row is None:  # pragma: no cover - 建工件时一定初始化
        raise ServiceError(404, f"工序 {process_id} 无状态记录")
    return row


def _item_issue(conn: sqlite3.Connection, item: sqlite3.Row) -> Optional[str]:
    """单个项目按当前最新检验判定，返回不合格原因；合格返回 None。"""
    latest = _latest_inspection(conn, item["item_id"])
    if latest is None:
        return f"项目 {item['code']} 尚无检验记录"
    if not (item["lower_limit"] <= latest["reading"] <= item["upper_limit"]):
        return (
            f"项目 {item['code']} 最新检验读数 {latest['reading']} 超出整数公差 "
            f"[{item['lower_limit']}, {item['upper_limit']}]（含边界）"
        )
    if not _gauge_valid_at(conn, latest["gauge_id"], latest["measured_at"]):
        gauge = conn.execute(
            "SELECT code FROM gauge WHERE gauge_id = ?", (latest["gauge_id"],)
        ).fetchone()
        return (
            f"项目 {item['code']} 最新检验所用量具 {gauge['code']} "
            f"在测量时间 {latest['measured_at']} 处于失准窗口"
        )
    return None


def _basis_issues(conn: sqlite3.Connection, process_id: int) -> list[str]:
    """完整放行规则：本工序全部项目合格 + 量具测量时有效 + 先决工序完成且仍有效。"""
    issues: list[str] = []
    items = conn.execute(
        "SELECT * FROM item WHERE process_id = ? ORDER BY item_id", (process_id,)
    ).fetchall()
    for item in items:
        issue = _item_issue(conn, item)
        if issue:
            issues.append(issue)

    for prereq_id in _prereq_ids(conn, process_id):
        pre = _get_state(conn, prereq_id)
        pre_proc = conn.execute(
            "SELECT code FROM process WHERE process_id = ?", (prereq_id,)
        ).fetchone()
        if pre["status"] != "COMPLETED":
            label = {
                "NOT_STARTED": "尚未开工",
                "STARTED": "尚未完工",
                "QUARANTINED": "已隔离",
            }.get(pre["status"], pre["status"])
            issues.append(f"先决工序 {pre_proc['code']}（id={prereq_id}）{label}，未处于有效完工状态")
    return issues


def _record_event(
    conn: sqlite3.Connection,
    process_id: int,
    event_type: str,
    detail: Optional[str] = None,
    operator: Optional[str] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO process_event(process_id, event_type, detail, operator, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (process_id, event_type, detail, operator, now_iso()),
    )


def _insert_credential(
    conn: sqlite3.Connection,
    process_id: int,
    kind: str,
    operator: Optional[str],
    basis_id: Optional[int] = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO credential(process_id, kind, basis_id, operator, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (process_id, kind, basis_id, operator, now_iso()),
    )
    credential_id = cur.lastrowid
    # 快照当时每个项目所依据的最新检验；之后登记新读数不回填本快照
    items = conn.execute(
        "SELECT item_id FROM item WHERE process_id = ? ORDER BY item_id", (process_id,)
    ).fetchall()
    for item in items:
        latest = _latest_inspection(conn, item["item_id"])
        # 调用方已保证最新检验存在且合格
        conn.execute(
            """
            INSERT INTO credential_inspection(credential_id, item_id, inspection_id)
            VALUES (?, ?, ?)
            """,
            (credential_id, item["item_id"], latest["inspection_id"]),
        )
    return credential_id


def _quarantine_downstream(
    conn: sqlite3.Connection, root_ids: list[int], root_reason: str
) -> list[int]:
    """把根工序及其全部下游（含传递依赖）隔离。

    仅 STARTED / COMPLETED 工序会被隔离并留痕；NOT_STARTED 下游不产生隔离单，
    它们在尝试开工时由完整规则自然拦截。返回实际被隔离的工序 id。
    """
    now = now_iso()
    quarantined: list[int] = []
    handled: set[int] = set()
    for root_id in root_ids:
        root_proc = conn.execute(
            "SELECT code FROM process WHERE process_id = ?", (root_id,)
        ).fetchone()
        queue: deque[tuple[int, bool]] = deque([(root_id, False)])
        seen: set[int] = set()
        while queue:
            pid, is_descendant = queue.popleft()
            if pid in seen:
                continue
            seen.add(pid)
            state = _get_state(conn, pid)
            if pid not in handled and state["status"] in ("STARTED", "COMPLETED"):
                proc = conn.execute(
                    "SELECT code FROM process WHERE process_id = ?", (pid,)
                ).fetchone()
                reason = (
                    root_reason
                    if not is_descendant
                    else f"上游工序 {root_proc['code']}（id={root_id}）失去有效依据，传导隔离"
                )
                conn.execute(
                    """
                    UPDATE process_state
                    SET status = 'QUARANTINED', quarantined_at = ?, reason = ?
                    WHERE process_id = ?
                    """,
                    (now, reason, pid),
                )
                conn.execute(
                    """
                    INSERT INTO quarantine(process_id, pre_status, reason, opened_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (pid, state["status"], reason, now),
                )
                _record_event(conn, pid, "QUARANTINE", reason)
                quarantined.append(pid)
                handled.add(pid)
            for child in _child_ids(conn, pid):
                queue.append((child, True))
    return quarantined


# --------------------------------------------------------------------- gauges

def create_gauge(db_path: Path | str | None, code: str, name: str) -> dict[str, Any]:
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                try:
                    cur = conn.execute(
                        "INSERT INTO gauge(code, name, created_at) VALUES (?, ?, ?)",
                        (code, name, now_iso()),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ServiceError(409, f"量具编号 {code} 已存在") from exc
                gauge_id = cur.lastrowid
            row = conn.execute("SELECT * FROM gauge WHERE gauge_id = ?", (gauge_id,)).fetchone()
            return dict(row)
        finally:
            conn.close()


def list_gauges(db_path: Path | str | None) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM gauge ORDER BY gauge_id")]
    finally:
        conn.close()


def register_gauge_window(
    db_path: Path | str | None,
    gauge_id: int,
    window_start_raw: str,
    window_end_raw: str,
    note: Optional[str],
) -> dict[str, Any]:
    """登记过往半开失准窗口，并立即重算受影响凭据，隔离失效工序。"""
    window_start = normalize_ts(window_start_raw)
    window_end = normalize_ts(window_end_raw)
    if not window_start < window_end:
        raise ServiceError(422, "失准窗口要求 window_start < window_end（半开 [start, end)）")

    with _service_lock:
        conn = get_connection(db_path)
        try:
            gauge = conn.execute(
                "SELECT * FROM gauge WHERE gauge_id = ?", (gauge_id,)
            ).fetchone()
            if gauge is None:
                raise ServiceError(404, f"量具 {gauge_id} 不存在")
            with _immediate_tx(conn):
                cur = conn.execute(
                    """
                    INSERT INTO gauge_window(gauge_id, window_start, window_end, note, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (gauge_id, window_start, window_end, note, now_iso()),
                )
                window_id = cur.lastrowid
                # 当前生效（最近一张 START/REVIEW）凭据所引用、且测量落入新窗口的检验。
                # 注意：此处刻意避免在该多表连接上使用 `ps.status IN ('A', 'B')`
                # 写法——当前运行环境的 SQLite 3.40.1 在此查询计划下会对带空格的
                # 多值 IN 产生错误结果集，改用等值 OR 语义等价且稳定。
                roots = [
                    r[0]
                    for r in conn.execute(
                        """
                        SELECT DISTINCT c.process_id
                        FROM credential c
                        JOIN credential_inspection ci ON ci.credential_id = c.credential_id
                        JOIN inspection i ON i.inspection_id = ci.inspection_id
                        JOIN process_state ps ON ps.process_id = c.process_id
                        WHERE (c.kind = 'START' OR c.kind = 'REVIEW')
                          AND c.credential_id = (
                              SELECT c2.credential_id FROM credential c2
                              WHERE c2.process_id = c.process_id
                                AND (c2.kind = 'START' OR c2.kind = 'REVIEW')
                              ORDER BY c2.credential_id DESC LIMIT 1)
                          AND i.gauge_id = ?
                          AND i.measured_at >= ?
                          AND i.measured_at < ?
                          AND (ps.status = 'STARTED' OR ps.status = 'COMPLETED')
                        """,
                        (gauge_id, window_start, window_end),
                    )
                ]
                reason = (
                    f"量具 {gauge['code']} 登记失准窗口 [{window_start}, {window_end})，"
                    f"生效凭据引用的测量落入该窗口"
                )
                quarantined = _quarantine_downstream(conn, roots, reason)
            window = conn.execute(
                "SELECT * FROM gauge_window WHERE window_id = ?", (window_id,)
            ).fetchone()
            result = dict(window)
            result["quarantined_process_ids"] = quarantined
            return result
        finally:
            conn.close()


def list_gauge_windows(db_path: Path | str | None, gauge_id: int) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        if conn.execute(
            "SELECT 1 FROM gauge WHERE gauge_id = ?", (gauge_id,)
        ).fetchone() is None:
            raise ServiceError(404, f"量具 {gauge_id} 不存在")
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM gauge_window WHERE gauge_id = ? ORDER BY window_id",
                (gauge_id,),
            )
        ]
    finally:
        conn.close()


# ------------------------------------------------------------- workpieces/DAG

def _validate_dag(processes: list[dict[str, Any]]) -> None:
    codes = [p["code"] for p in processes]
    if len(codes) != len(set(codes)):
        raise ServiceError(422, "工序编号在工件内必须唯一")
    code_set = set(codes)
    for p in processes:
        for pre in p["prerequisites"]:
            if pre not in code_set:
                raise ServiceError(422, f"工序 {p['code']} 引用了不存在的先决工序 {pre}")
            if pre == p["code"]:
                raise ServiceError(422, f"工序 {p['code']} 不能以自身为先决工序")
        item_codes = [it["code"] for it in p["items"]]
        if len(item_codes) != len(set(item_codes)):
            raise ServiceError(422, f"工序 {p['code']} 内尺寸项目编号必须唯一")

    # Kahn 拓扑排序检测环
    indegree = {c: 0 for c in codes}
    adj: dict[str, list[str]] = {c: [] for c in codes}
    for p in processes:
        for pre in p["prerequisites"]:
            adj[pre].append(p["code"])
            indegree[p["code"]] += 1
    queue = deque(c for c in codes if indegree[c] == 0)
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for nxt in adj[node]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)
    if visited != len(codes):
        raise ServiceError(422, "工序依赖存在环，必须构成有向无环图（DAG）")


def create_workpiece(db_path: Path | str | None, payload: dict[str, Any]) -> dict[str, Any]:
    _validate_dag(payload["processes"])
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                try:
                    cur = conn.execute(
                        "INSERT INTO workpiece(code, created_at) VALUES (?, ?)",
                        (payload["code"], now_iso()),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ServiceError(409, f"工件编号 {payload['code']} 已存在") from exc
                workpiece_id = cur.lastrowid

                id_by_code: dict[str, int] = {}
                for p in payload["processes"]:
                    cur = conn.execute(
                        "INSERT INTO process(workpiece_id, code, name) VALUES (?, ?, ?)",
                        (workpiece_id, p["code"], p["name"]),
                    )
                    id_by_code[p["code"]] = cur.lastrowid
                for p in payload["processes"]:
                    pid = id_by_code[p["code"]]
                    for pre in p["prerequisites"]:
                        conn.execute(
                            "INSERT INTO process_dependency(process_id, prereq_id) VALUES (?, ?)",
                            (pid, id_by_code[pre]),
                        )
                    for it in p["items"]:
                        conn.execute(
                            """
                            INSERT INTO item(process_id, code, name, lower_limit, upper_limit)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (pid, it["code"], it["name"], it["lower_limit"], it["upper_limit"]),
                        )
                    conn.execute(
                        "INSERT INTO process_state(process_id, status) VALUES (?, 'NOT_STARTED')",
                        (pid,),
                    )
                    _record_event(conn, pid, "CREATED", "工序随工件建档")
            return get_workpiece(db_path, workpiece_id)
        finally:
            conn.close()


def get_workpiece(db_path: Path | str | None, workpiece_id: int) -> dict[str, Any]:
    conn = get_connection(db_path)
    try:
        wp = conn.execute(
            "SELECT * FROM workpiece WHERE workpiece_id = ?", (workpiece_id,)
        ).fetchone()
        if wp is None:
            raise ServiceError(404, f"工件 {workpiece_id} 不存在")
        result = dict(wp)
        processes = []
        for p in conn.execute(
            "SELECT * FROM process WHERE workpiece_id = ? ORDER BY process_id",
            (workpiece_id,),
        ):
            pdict = dict(p)
            pdict["prerequisites"] = _prereq_ids(conn, p["process_id"])
            pdict["items"] = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM item WHERE process_id = ? ORDER BY item_id",
                    (p["process_id"],),
                )
            ]
            processes.append(pdict)
        result["processes"] = processes
        return result
    finally:
        conn.close()


# ------------------------------------------------------------------ inspection

def register_inspection(
    db_path: Path | str | None,
    item_id: int,
    gauge_id: int,
    reading: int,
    measured_at_raw: str,
) -> dict[str, Any]:
    measured_at = normalize_ts(measured_at_raw)
    with _service_lock:
        conn = get_connection(db_path)
        try:
            item = conn.execute(
                "SELECT * FROM item WHERE item_id = ?", (item_id,)
            ).fetchone()
            if item is None:
                raise ServiceError(404, f"尺寸项目 {item_id} 不存在")
            if conn.execute(
                "SELECT 1 FROM gauge WHERE gauge_id = ?", (gauge_id,)
            ).fetchone() is None:
                raise ServiceError(404, f"量具 {gauge_id} 不存在")

            with _immediate_tx(conn):
                cur = conn.execute(
                    """
                    INSERT INTO inspection(item_id, gauge_id, reading, measured_at, accepted_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (item_id, gauge_id, reading, measured_at, now_iso()),
                )
                inspection_id = cur.lastrowid

                # 以最新检验重判本工序；失效不回退旧记录
                issue = _item_issue(conn, item)
                quarantined: list[int] = []
                if issue is not None:
                    proc_id = item["process_id"]
                    state = _get_state(conn, proc_id)
                    if state["status"] in ("STARTED", "COMPLETED"):
                        quarantined = _quarantine_downstream(
                            conn,
                            [proc_id],
                            f"项目 {item['code']} 最新检验（id={inspection_id}）不再合格：{issue}",
                        )
            row = conn.execute(
                "SELECT * FROM inspection WHERE inspection_id = ?", (inspection_id,)
            ).fetchone()
            result = dict(row)
            result["is_latest"] = True
            result["quarantined_process_ids"] = quarantined
            return result
        finally:
            conn.close()


def list_inspections(db_path: Path | str | None, item_id: int) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        if conn.execute("SELECT 1 FROM item WHERE item_id = ?", (item_id,)).fetchone() is None:
            raise ServiceError(404, f"尺寸项目 {item_id} 不存在")
        rows = conn.execute(
            "SELECT * FROM inspection WHERE item_id = ? ORDER BY inspection_id DESC",
            (item_id,),
        ).fetchall()
        latest_id = rows[0]["inspection_id"] if rows else None
        return [dict(r, is_latest=(r["inspection_id"] == latest_id)) for r in rows]
    finally:
        conn.close()


# ------------------------------------------------------------- process actions

def _credential_detail(conn: sqlite3.Connection, credential_id: int) -> dict[str, Any]:
    cred = conn.execute(
        "SELECT * FROM credential WHERE credential_id = ?", (credential_id,)
    ).fetchone()
    result = dict(cred)
    result["inspections"] = [
        dict(r)
        for r in conn.execute(
            "SELECT item_id, inspection_id FROM credential_inspection WHERE credential_id = ?",
            (credential_id,),
        )
    ]
    return result


def _current_dispatch(conn: sqlite3.Connection, process_id: int) -> Optional[sqlite3.Row]:
    """该工序当前生效的派工（is_current=1，至多一行）。"""
    return conn.execute(
        "SELECT * FROM dispatch WHERE process_id = ? AND is_current = 1",
        (process_id,),
    ).fetchone()


def _any_dispatch_ever(conn: sqlite3.Connection, process_id: int) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM dispatch WHERE process_id = ? LIMIT 1", (process_id,)
        ).fetchone()
        is not None
    )


def start_process(
    db_path: Path | str | None, process_id: int, operator: Optional[str]
) -> dict[str, Any]:
    # 原开工入口：没有派工绑定的工序按原方式开工；已被纳入派工的工序必须凭
    # 当前派工开工，见 start_dispatch。
    with _service_lock:
        conn = get_connection(db_path)
        try:
            proc = _get_process(conn, process_id)
            with _immediate_tx(conn):
                if _any_dispatch_ever(conn, process_id):
                    current = _current_dispatch(conn, process_id)
                    if current is None:
                        raise ServiceError(
                            409,
                            "该工序已被派工纳入班次但当前无生效派工（已暂停或被换掉），"
                            "不能从原开工入口执行；请由班组长重排后再凭派工开工",
                        )
                    raise ServiceError(
                        409,
                        f"该工序已纳入班次派工 {current['dispatch_id']}，"
                        f"必须在预约时段内凭该派工开工（POST /api/dispatches/"
                        f"{current['dispatch_id']}/start）",
                    )
                state = _get_state(conn, process_id)
                if state["status"] == "QUARANTINED":
                    raise ServiceError(409, "工序已隔离，须由质量员复核恢复后才能开工")
                if state["status"] in ("STARTED", "COMPLETED"):
                    raise ServiceError(409, f"工序当前状态为 {state['status']}，不能重复开工")

                issues = _basis_issues(conn, process_id)
                if issues:
                    raise ServiceError(409, "不满足开工条件：" + "；".join(issues))

                credential_id = _insert_credential(conn, process_id, "START", operator)
                conn.execute(
                    "UPDATE process_state SET status = 'STARTED', quarantined_at = NULL, reason = NULL"
                    " WHERE process_id = ?",
                    (process_id,),
                )
                _record_event(
                    conn, process_id, "START", f"开工凭据 credential_id={credential_id}", operator
                )
            return get_process_status(db_path, process_id)
        finally:
            conn.close()


def complete_process(
    db_path: Path | str | None, process_id: int, operator: Optional[str]
) -> dict[str, Any]:
    with _service_lock:
        conn = get_connection(db_path)
        try:
            _get_process(conn, process_id)
            with _immediate_tx(conn):
                state = _get_state(conn, process_id)
                if state["status"] == "QUARANTINED":
                    raise ServiceError(409, "工序已隔离，不能完工；须先复核恢复为开工状态")
                if state["status"] == "NOT_STARTED":
                    raise ServiceError(409, "工序尚未开工，不能完工")
                if state["status"] == "COMPLETED":
                    raise ServiceError(409, "工序已完工，不能重复完工")

                # 完工前防御性复核同一套规则
                issues = _basis_issues(conn, process_id)
                if issues:
                    raise ServiceError(409, "放行依据已失效，不能完工：" + "；".join(issues))

                active = _active_credential(conn, process_id)
                # 若该工序凭当前派工开工，完工时一并结束实际占用（真实完工才释放）。
                current = _current_dispatch(conn, process_id)
                now = current_ts()
                credential_id = _insert_credential(
                    conn, process_id, "COMPLETE", operator, basis_id=active["credential_id"]
                )
                conn.execute(
                    "UPDATE process_state SET status = 'COMPLETED' WHERE process_id = ?",
                    (process_id,),
                )
                if current is not None and current["status"] == "STARTED":
                    conn.execute(
                        "UPDATE dispatch SET status = 'COMPLETED', actual_end_at = ?, updated_at = ?"
                        " WHERE dispatch_id = ?",
                        (now, now, current["dispatch_id"]),
                    )
                    _record_event(
                        conn,
                        process_id,
                        "COMPLETE",
                        f"完工凭据 credential_id={credential_id}，基于凭据 "
                        f"{active['credential_id']}；结束派工 {current['dispatch_id']} 的实际占用",
                        operator,
                    )
                else:
                    _record_event(
                        conn,
                        process_id,
                        "COMPLETE",
                        f"完工凭据 credential_id={credential_id}，基于凭据 {active['credential_id']}",
                        operator,
                    )
            return get_process_status(db_path, process_id)
        finally:
            conn.close()


def review_process(
    db_path: Path | str | None, process_id: int, operator: Optional[str]
) -> dict[str, Any]:
    """质量员逐道复核：同一套规则全部满足后解除隔离。

    隔离前已完工的复核后仍为 COMPLETED；隔离前已开工未完工的恢复为 STARTED，
    须重新完工。补测上游不会自动走到这里，必须逐道调用本接口。
    """
    with _service_lock:
        conn = get_connection(db_path)
        try:
            _get_process(conn, process_id)
            state = _get_state(conn, process_id)
            if state["status"] != "QUARANTINED":
                raise ServiceError(409, f"工序当前状态为 {state['status']}，无需复核")

            issues = _basis_issues(conn, process_id)
            if issues:
                raise ServiceError(409, "复核不通过：" + "；".join(issues))

            open_q = conn.execute(
                "SELECT * FROM quarantine WHERE process_id = ? AND closed_at IS NULL"
                " ORDER BY quarantine_id DESC LIMIT 1",
                (process_id,),
            ).fetchone()
            if open_q is None:  # pragma: no cover - 状态与隔离单始终一致
                raise ServiceError(409, "未找到未关闭的隔离单")
            restored = open_q["pre_status"]

            with _immediate_tx(conn):
                credential_id = _insert_credential(conn, process_id, "REVIEW", operator)
                conn.execute(
                    "UPDATE quarantine SET closed_at = ? WHERE quarantine_id = ?",
                    (now_iso(), open_q["quarantine_id"]),
                )
                conn.execute(
                    "UPDATE process_state SET status = ?, quarantined_at = NULL, reason = NULL"
                    " WHERE process_id = ?",
                    (restored, process_id),
                )
                _record_event(
                    conn,
                    process_id,
                    "REVIEW",
                    f"复核通过，恢复为 {restored}；复核凭据 credential_id={credential_id}；"
                    f"关闭隔离单 quarantine_id={open_q['quarantine_id']}",
                    operator,
                )
            return get_process_status(db_path, process_id)
        finally:
            conn.close()


# --------------------------------------------------------------------- queries

def list_credentials(db_path: Path | str | None, process_id: int) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        _get_process(conn, process_id)
        rows = conn.execute(
            "SELECT credential_id FROM credential WHERE process_id = ? ORDER BY credential_id",
            (process_id,),
        ).fetchall()
        return [_credential_detail(conn, r["credential_id"]) for r in rows]
    finally:
        conn.close()


def list_events(db_path: Path | str | None, process_id: int) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        _get_process(conn, process_id)
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM process_event WHERE process_id = ? ORDER BY event_id",
                (process_id,),
            )
        ]
    finally:
        conn.close()


def list_quarantines(
    db_path: Path | str | None, process_id: Optional[int] = None, open_only: bool = False
) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        sql = "SELECT * FROM quarantine"
        params: list[Any] = []
        where = []
        if process_id is not None:
            where.append("process_id = ?")
            params.append(process_id)
        if open_only:
            where.append("closed_at IS NULL")
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY quarantine_id"
        return [dict(r) for r in conn.execute(sql, params)]
    finally:
        conn.close()


def get_process_status(db_path: Path | str | None, process_id: int) -> dict[str, Any]:
    conn = get_connection(db_path)
    try:
        proc = _get_process(conn, process_id)
        state = _get_state(conn, process_id)
        result = {
            "process_id": process_id,
            "workpiece_id": proc["workpiece_id"],
            "code": proc["code"],
            "name": proc["name"],
            "status": state["status"],
            "quarantined_at": state["quarantined_at"],
            "reason": state["reason"],
            "prerequisites": _prereq_ids(conn, process_id),
        }
        active = _active_credential(conn, process_id)
        result["active_credential"] = (
            _credential_detail(conn, active["credential_id"]) if active is not None else None
        )
        open_q = conn.execute(
            "SELECT * FROM quarantine WHERE process_id = ? AND closed_at IS NULL"
            " ORDER BY quarantine_id DESC LIMIT 1",
            (process_id,),
        ).fetchone()
        result["open_quarantine"] = dict(open_q) if open_q is not None else None

        if state["status"] == "NOT_STARTED":
            issues = _basis_issues(conn, process_id)
            result["can_start"] = not issues
            result["blocking_reasons"] = issues
        else:
            # 已开工 / 完工 / 隔离的工序不允许再次开工
            result["can_start"] = False
            result["blocking_reasons"] = (
                [f"工序当前状态为 {state['status']}"] if state["status"] != "NOT_STARTED" else []
            )

        # 班次派工绑定：纳入过派工的工序只能凭当前生效派工开工
        current = _current_dispatch(conn, process_id)
        ever = current is not None or _any_dispatch_ever(conn, process_id)
        result["dispatch_required"] = ever
        result["current_dispatch_id"] = current["dispatch_id"] if current is not None else None
        result["current_dispatch_status"] = current["status"] if current is not None else None
        result["dispatch_suspended"] = (
            current is not None and current["status"] == "SUSPENDED"
        )
        if ever and state["status"] == "NOT_STARTED":
            if current is None:
                result["can_start"] = False
                result["blocking_reasons"] = result["blocking_reasons"] + [
                    "工序已被派工纳入班次但当前无生效派工（已暂停或被换掉），须先重排"
                ]
            else:
                result["can_start"] = False
                result["blocking_reasons"] = result["blocking_reasons"] + [
                    f"工序已纳入班次派工 {current['dispatch_id']}，须在预约时段内凭该派工开工"
                ]
        return result
    finally:
        conn.close()


def list_workpiece_statuses(db_path: Path | str | None, workpiece_id: int) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        if conn.execute(
            "SELECT 1 FROM workpiece WHERE workpiece_id = ?", (workpiece_id,)
        ).fetchone() is None:
            raise ServiceError(404, f"工件 {workpiece_id} 不存在")
        ids = [
            r[0]
            for r in conn.execute(
                "SELECT process_id FROM process WHERE workpiece_id = ? ORDER BY process_id",
                (workpiece_id,),
            )
        ]
    finally:
        conn.close()
    return [get_process_status(db_path, pid) for pid in ids]


def ensure_db(db_path: Path | str | None = None) -> None:
    init_db(db_path)


# =============================================================== 工位交接
#
# 容量账本（每种工位）：
#   ACTUAL   实物在位（HOME 件 / 已签收或已退回落地的件）——占位
#   RESERVED 尚无实物的预留（待发收件位 / 发运后发件方的退回保留位）——占位
# 两种状态合计不得超过工位容量。签收 / 退回都是「释放一侧预留、占用另一侧
# 已预留的位置」，总占用不变，落地的一方由 RESERVED 转为 ACTUAL。
#
# 件状态：
#   AT_WORKSTATION  实物在 current_workstation_id
#   IN_TRANSIT      发运后实物在途中
# 调拨单件状态：
#   PENDING -> IN_TRANSIT -> RECEIVED / RETURNED；PENDING 可 CANCELLED。
#   某件的 released_at 一旦非空，该件不再受本单占用（可进入后续单据）。


def _get_workstation(conn: sqlite3.Connection, workstation_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM workstation WHERE workstation_id = ?", (workstation_id,)
    ).fetchone()
    if row is None:
        raise ServiceError(404, f"工位 {workstation_id} 不存在")
    return row


def _slot_counts(conn: sqlite3.Connection, workstation_id: int) -> tuple[int, int]:
    actual, reserved = conn.execute(
        """
        SELECT
          COALESCE(SUM(CASE WHEN state = 'ACTUAL' THEN 1 ELSE 0 END), 0),
          COALESCE(SUM(CASE WHEN state = 'RESERVED' THEN 1 ELSE 0 END), 0)
        FROM workstation_slot WHERE workstation_id = ?
        """,
        (workstation_id,),
    ).fetchone()
    return int(actual), int(reserved)


def _available_slots(conn: sqlite3.Connection, workstation_id: int) -> int:
    ws = _get_workstation(conn, workstation_id)
    actual, reserved = _slot_counts(conn, workstation_id)
    return int(ws["capacity"]) - actual - reserved


def _insert_slot(
    conn: sqlite3.Connection,
    workstation_id: int,
    piece_id: int,
    state: str,
    purpose: str,
    order_id: Optional[int] = None,
) -> None:
    """插入一个占位。容量由应用层预检 + 数据库触发器双重保证。"""
    if _available_slots(conn, workstation_id) <= 0:
        ws = _get_workstation(conn, workstation_id)
        raise ServiceError(
            409,
            f"工位 {ws['code']}（id={workstation_id}）缓冲位已满："
            f"容量 {ws['capacity']}，实际在位与已预留合计已占满",
        )
    try:
        conn.execute(
            """
            INSERT INTO workstation_slot(workstation_id, piece_id, state, purpose, order_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (workstation_id, piece_id, state, purpose, order_id, now_iso()),
        )
    except sqlite3.IntegrityError as exc:
        # 触发器兜底（理论上预检已拦住；并发下由 BEGIN IMMEDIATE 串行化）
        if "workstation_capacity_exceeded" in str(exc):
            raise ServiceError(409, f"工位 {workstation_id} 缓冲位已满（容量约束）") from exc
        raise


def _slot_exists(
    conn: sqlite3.Connection, workstation_id: int, piece_id: int, purpose: str
) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM workstation_slot WHERE workstation_id = ? AND piece_id = ? AND purpose = ?",
            (workstation_id, piece_id, purpose),
        ).fetchone()
        is not None
    )


def _order_row(conn: sqlite3.Connection, order_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM transfer_order WHERE order_id = ?", (order_id,)
    ).fetchone()
    if row is None:
        raise ServiceError(404, f"调拨单 {order_id} 不存在")
    return row


def _piece_row(conn: sqlite3.Connection, piece_id: int) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM piece WHERE piece_id = ?", (piece_id,)).fetchone()
    if row is None:
        raise ServiceError(404, f"件 {piece_id} 不存在")
    return row


def _gate_valid(conn: sqlite3.Connection, piece: sqlite3.Row) -> tuple[bool, list[str]]:
    """关口是否按现有工序规则有效完工。只读实时状态，不复制任何记录。"""
    gate_id = piece["gate_process_id"]
    state = _get_state(conn, gate_id)
    if state["status"] != "COMPLETED":
        label = {
            "NOT_STARTED": "尚未开工",
            "STARTED": "尚未完工",
            "QUARANTINED": "已隔离（失效完工状态）",
        }.get(state["status"], state["status"])
        return False, [f"关口工序当前状态为 {state['status']}（{label}）"]
    issues = _basis_issues(conn, gate_id)
    return (not issues), issues


def _quality_frozen(conn: sqlite3.Connection, piece: sqlite3.Row) -> bool:
    """物料（工件）关口当前是否不再有效完工 —— 当前库存即被质量冻结。"""
    valid, _ = _gate_valid(conn, piece)
    return not valid


def _active_order_for_piece(conn: sqlite3.Connection, piece_id: int) -> Optional[int]:
    row = conn.execute(
        """
        SELECT order_id FROM transfer_order_piece
        WHERE piece_id = ? AND released_at IS NULL
        """,
        (piece_id,),
    ).fetchone()
    return row["order_id"] if row is not None else None


# ------------------------------------------------------------- 实装组成树（当前态）
#
# 不变量：
# * 一个 child 至多有一个直接承载件（piece_assembly.child_piece_id UNIQUE）；
# * 只有最外层整件（root，无 parent）在 workstation_slot 中占位，内部子件不占位；
# * 内部子件的工位 / 在途状态始终与其最外层整件一致，读时沿 parent 链解析。

def _assembly_parent(conn: sqlite3.Connection, piece_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM piece_assembly WHERE child_piece_id = ?", (piece_id,)
    ).fetchone()


def _root_piece_id(conn: sqlite3.Connection, piece_id: int) -> int:
    """沿承载链找到最外层整件 id。组成环由“待装件必须是 root”的写入校验排除。"""
    cur = piece_id
    seen: set[int] = set()
    while True:
        row = conn.execute(
            "SELECT parent_piece_id FROM piece_assembly WHERE child_piece_id = ?", (cur,)
        ).fetchone()
        if row is None:
            return cur
        if cur in seen:  # pragma: no cover - 写入侧已排除环
            raise ServiceError(500, f"件 {piece_id} 的实装组成存在环，数据状态异常")
        seen.add(cur)
        cur = row["parent_piece_id"]


def _subtree(conn: sqlite3.Connection, root_id: int) -> list[tuple[int, Optional[int], int]]:
    """以 root_id 为最外层整件的完整子树：返回 (piece_id, parent_piece_id, depth)。

    深度优先、同父下按 piece_id 升序，depth=0 为最外层整件本身。
    """
    result: list[tuple[int, Optional[int], int]] = []
    stack: list[tuple[int, Optional[int], int]] = [(root_id, None, 0)]
    while stack:
        pid, parent_id, depth = stack.pop()
        result.append((pid, parent_id, depth))
        children = [
            r["child_piece_id"]
            for r in conn.execute(
                "SELECT child_piece_id FROM piece_assembly WHERE parent_piece_id = ?"
                " ORDER BY child_piece_id DESC",
                (pid,),
            )
        ]
        for child_id in children:
            stack.append((child_id, pid, depth + 1))
    return result


def _effective_piece_state(
    conn: sqlite3.Connection, piece: sqlite3.Row
) -> tuple[str, int, int]:
    """返回 (有效状态, 实际所在工位 id, 最外层整件 id)。

    内部子件自身的 piece 行不随发运 / 落地更新，其工位与在途状态一律以最外层
    整件为准——实物随整件一起移动。
    """
    root_id = _root_piece_id(conn, piece["piece_id"])
    if root_id == piece["piece_id"]:
        return piece["status"], piece["current_workstation_id"], root_id
    root = _piece_row(conn, root_id)
    return root["status"], root["current_workstation_id"], root_id


def _piece_dict(conn: sqlite3.Connection, piece: sqlite3.Row) -> dict[str, Any]:
    wp = conn.execute(
        "SELECT code FROM workpiece WHERE workpiece_id = ?", (piece["workpiece_id"],)
    ).fetchone()
    gate = conn.execute(
        "SELECT code FROM process WHERE process_id = ?", (piece["gate_process_id"],)
    ).fetchone()
    parent_row = _assembly_parent(conn, piece["piece_id"])
    root_id = _root_piece_id(conn, piece["piece_id"])
    eff_status, eff_ws_id, _ = _effective_piece_state(conn, piece)
    ws = conn.execute(
        "SELECT code FROM workstation WHERE workstation_id = ?", (eff_ws_id,)
    ).fetchone()
    child_ids = [
        r["child_piece_id"]
        for r in conn.execute(
            "SELECT child_piece_id FROM piece_assembly WHERE parent_piece_id = ?"
            " ORDER BY child_piece_id",
            (piece["piece_id"],)
        )
    ]

    # 自身关口的实时有效性
    own_valid, _ = _gate_valid(conn, piece)
    if root_id == piece["piece_id"]:
        # 整件的当前质量冻结随现有组成及各关口状态变化：任一成员关口失效即冻结
        all_valid = True
        for member_id, _, _ in _subtree(conn, root_id):
            member = _piece_row(conn, member_id)
            valid, _ = _gate_valid(conn, member)
            if not valid:
                all_valid = False
                break
        gate_valid = all_valid
    else:
        gate_valid = own_valid

    return {
        "piece_id": piece["piece_id"],
        "workpiece_id": piece["workpiece_id"],
        "workpiece_code": wp["code"],
        "gate_process_id": piece["gate_process_id"],
        "gate_process_code": gate["code"],
        "status": eff_status,
        "current_workstation_id": eff_ws_id,
        "current_workstation_code": ws["code"],
        "gate_valid": gate_valid,
        "quality_frozen": not gate_valid,
        "active_order_id": _active_order_for_piece(conn, root_id),
        "created_at": piece["created_at"],
        "direct_parent_piece_id": parent_row["parent_piece_id"] if parent_row else None,
        "root_piece_id": root_id,
        "direct_child_piece_ids": child_ids,
        "is_inner": parent_row is not None,
    }


# ------------------------------------------------------------------ workstations

def create_workstation(
    db_path: Path | str | None, code: str, name: str, capacity: int
) -> dict[str, Any]:
    if capacity < 0:
        raise ServiceError(422, "缓冲位容量不能为负")
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                try:
                    cur = conn.execute(
                        "INSERT INTO workstation(code, name, capacity, created_at) VALUES (?, ?, ?, ?)",
                        (code, name, capacity, now_iso()),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ServiceError(409, f"工位编号 {code} 已存在") from exc
                workstation_id = cur.lastrowid
            row = conn.execute(
                "SELECT * FROM workstation WHERE workstation_id = ?", (workstation_id,)
            ).fetchone()
            return dict(row)
        finally:
            conn.close()


def list_workstations(db_path: Path | str | None) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM workstation ORDER BY workstation_id")]
    finally:
        conn.close()


def get_workstation(db_path: Path | str | None, workstation_id: int) -> dict[str, Any]:
    conn = get_connection(db_path)
    try:
        ws = _get_workstation(conn, workstation_id)
        actual, reserved = _slot_counts(conn, workstation_id)
        slots = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM workstation_slot WHERE workstation_id = ? ORDER BY slot_id",
                (workstation_id,),
            )
        ]
        result = dict(ws)
        result.update(
            {
                "actual_count": actual,
                "reserved_count": reserved,
                "available": int(ws["capacity"]) - actual - reserved,
                "actual_pieces": [s["piece_id"] for s in slots if s["state"] == "ACTUAL"],
                "reserved_pieces": [s["piece_id"] for s in slots if s["state"] == "RESERVED"],
                "slots": slots,
            }
        )
        return result
    finally:
        conn.close()


# ------------------------------------------------------------------------ pieces

def create_piece(
    db_path: Path | str | None,
    workpiece_id: int,
    gate_process_id: int,
    workstation_id: int,
) -> dict[str, Any]:
    """用已有工件编号登记实物：初始工位占一个 ACTUAL 位。

    关口工序必须属于该工件；成品关口工序即真实完工关口，不复制检验 / 完工记录。
    """
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                if conn.execute(
                    "SELECT 1 FROM workpiece WHERE workpiece_id = ?", (workpiece_id,)
                ).fetchone() is None:
                    raise ServiceError(404, f"工件 {workpiece_id} 不存在")
                gate = conn.execute(
                    "SELECT * FROM process WHERE process_id = ?", (gate_process_id,)
                ).fetchone()
                if gate is None:
                    raise ServiceError(404, f"关口工序 {gate_process_id} 不存在")
                if gate["workpiece_id"] != workpiece_id:
                    raise ServiceError(422, "关口工序必须属于该工件")
                _get_workstation(conn, workstation_id)

                if conn.execute(
                    "SELECT 1 FROM piece WHERE workpiece_id = ?", (workpiece_id,)
                ).fetchone() is not None:
                    raise ServiceError(409, f"工件 {workpiece_id} 已登记为实物件，不能重复登记")

                cur = conn.execute(
                    """
                    INSERT INTO piece(workpiece_id, gate_process_id, status, current_workstation_id, created_at)
                    VALUES (?, ?, 'AT_WORKSTATION', ?, ?)
                    """,
                    (workpiece_id, gate_process_id, workstation_id, now_iso()),
                )
                piece_id = cur.lastrowid
                _insert_slot(conn, workstation_id, piece_id, "ACTUAL", "HOME")
            row = conn.execute("SELECT * FROM piece WHERE piece_id = ?", (piece_id,)).fetchone()
            return _piece_dict(conn, row)
        finally:
            conn.close()


def get_piece(db_path: Path | str | None, piece_id: int) -> dict[str, Any]:
    conn = get_connection(db_path)
    try:
        return _piece_dict(conn, _piece_row(conn, piece_id))
    finally:
        conn.close()


def list_pieces(db_path: Path | str | None) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        return [
            _piece_dict(conn, r)
            for r in conn.execute("SELECT * FROM piece ORDER BY piece_id")
        ]
    finally:
        conn.close()


# ------------------------------------------------------------------ transfer orders

def _order_pieces(conn: sqlite3.Connection, order_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT op.*, p.workpiece_id FROM transfer_order_piece op
        JOIN piece p ON p.piece_id = op.piece_id
        WHERE op.order_id = ? ORDER BY op.piece_id
        """,
        (order_id,),
    ).fetchall()
    result = []
    for r in rows:
        wp = conn.execute(
            "SELECT code FROM workpiece WHERE workpiece_id = ?", (r["workpiece_id"],)
        ).fetchone()
        result.append(
            {
                "piece_id": r["piece_id"],
                "workpiece_code": wp["code"],
                "state": r["state"],
                "released_at": r["released_at"],
            }
        )
    return result


def _order_dict(conn: sqlite3.Connection, order: sqlite3.Row) -> dict[str, Any]:
    result = dict(order)
    result["pieces"] = _order_pieces(conn, order["order_id"])
    return result


def _order_records(conn: sqlite3.Connection, order_id: int) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM transfer_record WHERE order_id = ? ORDER BY record_id", (order_id,)
        )
    ]


def _order_actions(conn: sqlite3.Connection, order_id: int) -> list[dict[str, Any]]:
    """发运 / 签收 / 退回的操作留痕（含请求号），只增不改。"""
    result = []
    for r in conn.execute(
        "SELECT * FROM transfer_action WHERE order_id = ? ORDER BY action_id", (order_id,)
    ):
        try:
            piece_ids = json.loads(r["piece_key"])
        except (TypeError, ValueError):  # pragma: no cover - 写入时始终是 JSON
            piece_ids = []
        result.append(
            {
                "action_id": r["action_id"],
                "order_id": r["order_id"],
                "request_id": r["request_id"],
                "action": r["action"],
                "operator": r["operator"],
                "piece_ids": piece_ids,
                "created_at": r["created_at"],
            }
        )
    return result


def create_transfer_order(
    db_path: Path | str | None,
    source_workstation_id: int,
    dest_workstation_id: int,
    piece_ids: list[int],
) -> dict[str, Any]:
    """建单：校验发 / 收工位不同、件当前在发件方且未被其它单占用，
    并为每件在收件方预留一个位置。只预留，不移动实物。

    关口是否有效完工属于**发运时**的整单条件，建单不因此拒绝——待发单可以
    先建好并留位，等关口完工后再发运。
    """
    if not piece_ids:
        raise ServiceError(422, "调拨单至少包含一个件")
    if len(piece_ids) != len(set(piece_ids)):
        raise ServiceError(422, "调拨单内件不能重复")
    if source_workstation_id == dest_workstation_id:
        raise ServiceError(422, "发件工位与收件工位必须不同")

    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                src = _get_workstation(conn, source_workstation_id)
                dst = _get_workstation(conn, dest_workstation_id)

                # ---- 先完整校验，任何一件不合格整单不建（不产生部分预留）----
                failures: list[str] = []
                pieces: list[sqlite3.Row] = []
                for piece_id in piece_ids:
                    try:
                        piece = _piece_row(conn, piece_id)
                    except ServiceError as exc:
                        failures.append(f"件 {piece_id}：{exc.detail}")
                        continue
                    pieces.append(piece)
                    if _assembly_parent(conn, piece_id) is not None:
                        failures.append(
                            f"件 {piece_id} 是总成内部子件，只有最外层整件可以单独列入调拨单"
                        )
                        continue
                    if piece["status"] != "AT_WORKSTATION":
                        failures.append(f"件 {piece_id} 在途中，不能上单")
                        continue
                    if piece["current_workstation_id"] != source_workstation_id:
                        failures.append(f"件 {piece_id} 不在发件工位 {src['code']}")
                    active = _active_order_for_piece(conn, piece_id)
                    if active is not None:
                        failures.append(f"件 {piece_id} 已在调拨单 {active} 上待发或在途")
                if failures:
                    raise ServiceError(409, "整单不能创建：" + "；".join(failures))

                # 收件方需为每件预留一个位置（容量校验按整批一次判断）
                if _available_slots(conn, dest_workstation_id) < len(pieces):
                    raise ServiceError(
                        409,
                        f"收件工位 {dst['code']} 可用缓冲位不足：需要 {len(pieces)}，"
                        f"剩余 {_available_slots(conn, dest_workstation_id)}",
                    )
                # 发件方每件仍占一个 ACTUAL 位，建单不动它

                cur = conn.execute(
                    """
                    INSERT INTO transfer_order(source_workstation_id, dest_workstation_id, status, created_at)
                    VALUES (?, ?, 'PENDING', ?)
                    """,
                    (source_workstation_id, dest_workstation_id, now_iso()),
                )
                order_id = cur.lastrowid
                for piece in pieces:
                    conn.execute(
                        """
                        INSERT INTO transfer_order_piece(order_id, piece_id, state)
                        VALUES (?, ?, 'PENDING')
                        """,
                        (order_id, piece["piece_id"]),
                    )
                    _insert_slot(
                        conn, dest_workstation_id, piece["piece_id"], "RESERVED", "RECEIPT", order_id
                    )
            order = _order_row(conn, order_id)
            return _order_dict(conn, order)
        finally:
            conn.close()


def get_transfer_order(db_path: Path | str | None, order_id: int) -> dict[str, Any]:
    conn = get_connection(db_path)
    try:
        order = _order_row(conn, order_id)
        result = _order_dict(conn, order)
        result["actions"] = _order_actions(conn, order_id)
        result["records"] = _order_records(conn, order_id)
        return result
    finally:
        conn.close()


def list_transfer_orders(db_path: Path | str | None) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        return [
            _order_dict(conn, r)
            for r in conn.execute("SELECT * FROM transfer_order ORDER BY order_id")
        ]
    finally:
        conn.close()


def cancel_transfer_order(db_path: Path | str | None, order_id: int) -> dict[str, Any]:
    """撤销待发单：释放收件方预留位，件留在发件方原位。已发运的单不能撤销。"""
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                order = _order_row(conn, order_id)
                if order["status"] != "PENDING":
                    raise ServiceError(409, f"调拨单状态为 {order['status']}，只有待发单可以撤销")
                now = now_iso()
                conn.execute(
                    "DELETE FROM workstation_slot WHERE order_id = ? AND purpose = 'RECEIPT'",
                    (order_id,),
                )
                conn.execute(
                    "UPDATE transfer_order_piece SET state = 'CANCELLED', released_at = ?"
                    " WHERE order_id = ? AND released_at IS NULL",
                    (now, order_id),
                )
                conn.execute(
                    "UPDATE transfer_order SET status = 'CANCELLED', closed_at = ? WHERE order_id = ?",
                    (now, order_id),
                )
            order = _order_row(conn, order_id)
            return _order_dict(conn, order)
        finally:
            conn.close()


# ------------------------------------------------------------- idempotent actions

def _piece_key_for_requests(conn: sqlite3.Connection, order_id: int, piece_ids: list[int]) -> str:
    """幂等键的比较口径：件集合排序后的列表（同号不同批次的件视为换内容）。"""
    return json.dumps(sorted(set(piece_ids)), separators=(",", ":"))


def _existing_action(
    conn: sqlite3.Connection, order_id: int, request_id: str, action: str
) -> Optional[sqlite3.Row]:
    """返回同号请求；同号换内容（动作 / 操作者 / 件批次不同）则 409，失败不占号。"""
    row = conn.execute(
        "SELECT * FROM transfer_action WHERE order_id = ? AND request_id = ?",
        (order_id, request_id),
    ).fetchone()
    return row


def _replay_or_conflict(
    existing: sqlite3.Row,
    action: str,
    operator: str,
    piece_key: str,
) -> dict[str, Any]:
    same = (
        existing["action"] == action
        and existing["operator"] == operator
        and existing["piece_key"] == piece_key
    )
    if not same:
        raise ServiceError(
            409,
            f"请求号 {existing['request_id']} 已被不同的请求内容使用，拒绝执行",
        )
    result = json.loads(existing["result_json"])
    result["replayed"] = True
    return result


def _empty_piece_key() -> str:
    return json.dumps([], separators=(",", ":"))


def ship_transfer_order(
    db_path: Path | str | None,
    order_id: int,
    operator: str,
    request_id: str,
) -> dict[str, Any]:
    """发运整单：所有件关口仍须有效完工；发件方为每件保留退回位置。

    幂等键包含整单的件集合；同号重发返回首次结果。
    """
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                order = _order_row(conn, order_id)
                rows = conn.execute(
                    "SELECT * FROM transfer_order_piece WHERE order_id = ? ORDER BY piece_id",
                    (order_id,),
                ).fetchall()
                all_piece_ids = [r["piece_id"] for r in rows]
                piece_key = _piece_key_for_requests(conn, order_id, all_piece_ids)

                existing = _existing_action(conn, order_id, request_id, "SHIP")
                if existing is not None:
                    return _replay_or_conflict(existing, "SHIP", operator, piece_key)

                # 校验放在写锁内：同号首次请求与其它写操作串行
                if order["status"] == "CANCELLED":
                    raise ServiceError(409, "调拨单已撤销，不能发运")
                if order["status"] != "PENDING":
                    raise ServiceError(409, f"调拨单状态为 {order['status']}，不是待发单")
                pending = [r for r in rows if r["state"] == "PENDING" and r["released_at"] is None]
                if len(pending) != len(rows) or not pending:
                    raise ServiceError(409, "调拨单存在已落地 / 释放的件，不能整单发运")

                # 关口有效性在发运瞬间实时校验（隔离 / 失效会立即拦截，不用旧快照）。
                # 整件本身及每层实装子件的转运关口都须按现有规则有效完工；
                # 任何一个关口不满足，整单不发。
                failures: list[str] = []
                for r in pending:
                    root_piece = _piece_row(conn, r["piece_id"])
                    for member_id, _, depth in _subtree(conn, root_piece["piece_id"]):
                        member = _piece_row(conn, member_id)
                        valid, issues = _gate_valid(conn, member)
                        if not valid:
                            label = "最外层整件" if depth == 0 else f"第 {depth} 层子件"
                            failures.append(
                                f"整件 {root_piece['piece_id']} 的{label} {member_id} "
                                f"关口未有效完工：" + "；".join(issues)
                            )
                if failures:
                    # 整单仍待发，不占用请求号
                    raise ServiceError(409, "存在关口未有效完工的件，整单仍待发：" + "；".join(failures))

                src_id = order["source_workstation_id"]
                # 发件方须为每件保留退回位置（发运后这些位置只接纳退回件）。
                # 计入本单各件当前占用的 HOME 位：发运时它们会被就地转为退回预留，
                # 因此只需保证扣除这些 HOME 位后的空余位置足够。
                pending_ids = [r["piece_id"] for r in pending]
                placeholders = ",".join("?" for _ in pending_ids)
                home_slots = conn.execute(
                    f"SELECT COUNT(*) FROM workstation_slot WHERE workstation_id = ?"
                    f" AND state = 'ACTUAL' AND piece_id IN ({placeholders})",
                    (src_id, *pending_ids),
                ).fetchone()[0]
                if _available_slots(conn, src_id) + home_slots < len(pending):
                    src = _get_workstation(conn, src_id)
                    raise ServiceError(
                        409,
                        f"发件工位 {src['code']} 无法为每件保留退回位置：需要 {len(pending)}，"
                        f"扣除本单实物占位后仅剩 "
                        f"{_available_slots(conn, src_id) + home_slots}",
                    )

                now = now_iso()
                for r in pending:
                    piece = _piece_row(conn, r["piece_id"])
                    # 释放发件方的在库占位，改预留为退回保留位
                    conn.execute(
                        "DELETE FROM workstation_slot WHERE workstation_id = ? AND piece_id = ?"
                        " AND state = 'ACTUAL'",
                        (src_id, piece["piece_id"]),
                    )
                    _insert_slot(conn, src_id, piece["piece_id"], "RESERVED", "RETURN", order_id)
                    conn.execute(
                        "UPDATE piece SET status = 'IN_TRANSIT' WHERE piece_id = ?",
                        (piece["piece_id"],),
                    )
                    # 内部子件随整件一起在途（工位字段在落地时再随整件更新）
                    for member_id, _, depth in _subtree(conn, piece["piece_id"]):
                        if depth > 0:
                            conn.execute(
                                "UPDATE piece SET status = 'IN_TRANSIT' WHERE piece_id = ?",
                                (member_id,),
                            )
                    conn.execute(
                        "UPDATE transfer_order_piece SET state = 'IN_TRANSIT' WHERE order_id = ?"
                        " AND piece_id = ?",
                        (order_id, piece["piece_id"]),
                    )
                conn.execute(
                    "UPDATE transfer_order SET status = 'SHIPPED', shipped_at = ? WHERE order_id = ?",
                    (now, order_id),
                )

                result = {
                    "order_id": order_id,
                    "action": "SHIP",
                    "request_id": request_id,
                    "operator": operator,
                    "piece_ids": all_piece_ids,
                    "order_status": "SHIPPED",
                    "replayed": False,
                }
                cur = conn.execute(
                    """
                    INSERT INTO transfer_action(order_id, request_id, action, operator, piece_key, result_json, created_at)
                    VALUES (?, ?, 'SHIP', ?, ?, ?, ?)
                    """,
                    (order_id, request_id, operator, piece_key, json.dumps(result), now),
                )
                ship_action_id = cur.lastrowid
                # 发运瞬间为每个整件留完整组成 + 各成员关口生效凭据证据；
                # 只写独立快照表，事后拆换 / 补测 / 复核都不改变这里。
                for r in pending:
                    _write_shipment_snapshot(conn, order_id, ship_action_id, r["piece_id"], now)
            return result
        finally:
            conn.close()


def _land_pieces(
    conn: sqlite3.Connection,
    order_id: int,
    action: str,
    operator: str,
    request_id: str,
    piece_ids: list[int],
) -> dict[str, Any]:
    """签收 / 退回共用的落地逻辑（调用方已持有写事务）。"""
    order = _order_row(conn, order_id)
    piece_key = _piece_key_for_requests(conn, order_id, piece_ids)
    existing = _existing_action(conn, order_id, request_id, action)
    if existing is not None:
        return _replay_or_conflict(existing, action, operator, piece_key)

    if order["status"] not in ("SHIPPED",):
        raise ServiceError(409, f"调拨单状态为 {order['status']}，只有发运后的单可以{_action_label(action)}")
    if not piece_ids:
        raise ServiceError(422, "至少指定一个件")
    if len(piece_ids) != len(set(piece_ids)):
        raise ServiceError(422, "同一请求内件不能重复")

    src_id = order["source_workstation_id"]
    dst_id = order["dest_workstation_id"]
    land_ws_id = dst_id if action == "RECEIVE" else src_id

    rows = {
        r["piece_id"]: r
        for r in conn.execute(
            "SELECT * FROM transfer_order_piece WHERE order_id = ?", (order_id,)
        )
    }
    # 整批先校验：任何一件无法处理则整批不生效（不占用请求号）
    failures: list[str] = []
    for piece_id in piece_ids:
        row = rows.get(piece_id)
        if row is None:
            failures.append(f"件 {piece_id} 不在本调拨单内")
        elif row["released_at"] is not None:
            failures.append(f"件 {piece_id} 已落地（{row['state']}），不再受本单占用")
        elif row["state"] != "IN_TRANSIT":
            failures.append(f"件 {piece_id} 状态为 {row['state']}，只有仍在途的件可以处理")
    if failures:
        raise ServiceError(409, "整批不生效：" + "；".join(failures))

    now = now_iso()
    result_piece_ids = sorted(set(piece_ids))
    # 先写请求幂等行，取得 action_id 供落地记录引用
    cur = conn.execute(
        """
        INSERT INTO transfer_action(order_id, request_id, action, operator, piece_key, result_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (order_id, request_id, action, operator, piece_key, "{}", now),
    )
    action_id = cur.lastrowid

    for piece_id in result_piece_ids:
        piece = _piece_row(conn, piece_id)
        # 落地一方：签收落在收件方（用建单预留的 RECEIPT 位），退回落在发件方
        # （用发运时保留的 RETURN 位）；另一方为该件预留的位置随之释放。
        landing_purpose = "RECEIPT" if action == "RECEIVE" else "RETURN"
        release_ws_id = src_id if action == "RECEIVE" else dst_id
        release_purpose = "RETURN" if action == "RECEIVE" else "RECEIPT"
        if not _slot_exists(conn, land_ws_id, piece_id, landing_purpose):
            # 正常不会发生（建单 / 发运已预留）；兜底补占仍受容量约束
            _insert_slot(conn, land_ws_id, piece_id, "RESERVED", landing_purpose, order_id)
        conn.execute(
            "UPDATE workstation_slot SET state = 'ACTUAL' WHERE workstation_id = ?"
            " AND piece_id = ? AND purpose = ?",
            (land_ws_id, piece_id, landing_purpose),
        )
        # 释放另一方为该件预留的位置（不影响单中其他件）
        conn.execute(
            "DELETE FROM workstation_slot WHERE workstation_id = ? AND piece_id = ?"
            " AND state = 'RESERVED' AND order_id = ? AND purpose = ?",
            (release_ws_id, piece_id, order_id, release_purpose),
        )
        conn.execute(
            "UPDATE piece SET status = 'AT_WORKSTATION', current_workstation_id = ?"
            " WHERE piece_id = ?",
            (land_ws_id, piece_id),
        )
        # 整件内的每层子件随整件一起落地：同步它们的工位与在途状态，
        # 否则日后拆出的新整件会带着旧工位，与其实际缓冲位不一致。
        for member_id, _, depth in _subtree(conn, piece_id):
            if depth == 0:
                continue
            conn.execute(
                "UPDATE piece SET status = 'AT_WORKSTATION', current_workstation_id = ?"
                " WHERE piece_id = ?",
                (land_ws_id, member_id),
            )
        new_state = "RECEIVED" if action == "RECEIVE" else "RETURNED"
        conn.execute(
            "UPDATE transfer_order_piece SET state = ?, released_at = ? WHERE order_id = ? AND piece_id = ?",
            (new_state, now, order_id, piece_id),
        )
        conn.execute(
            """
            INSERT INTO transfer_record(order_id, action_id, piece_id, action, operator, request_id,
                                        from_workstation_id, to_workstation_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (order_id, action_id, piece_id, action, operator, request_id,
             src_id if action == "RECEIVE" else dst_id, land_ws_id, now),
        )

    # 单状态：所有件都已落地（签收或退回）则关闭
    remaining = conn.execute(
        "SELECT COUNT(*) FROM transfer_order_piece WHERE order_id = ? AND released_at IS NULL",
        (order_id,),
    ).fetchone()[0]
    new_status = "COMPLETED" if remaining == 0 else "SHIPPED"
    if new_status == "COMPLETED":
        conn.execute(
            "UPDATE transfer_order SET status = 'COMPLETED', closed_at = ? WHERE order_id = ?",
            (now, order_id),
        )

    result = {
        "order_id": order_id,
        "action": action,
        "request_id": request_id,
        "operator": operator,
        "piece_ids": result_piece_ids,
        "order_status": new_status,
        "replayed": False,
    }
    conn.execute(
        "UPDATE transfer_action SET result_json = ? WHERE action_id = ?",
        (json.dumps(result), action_id),
    )
    return result


def _action_label(action: str) -> str:
    return {"SHIP": "发运", "RECEIVE": "签收", "RETURN": "退回"}.get(action, action)


def receive_transfer_order(
    db_path: Path | str | None,
    order_id: int,
    piece_ids: list[int],
    operator: str,
    request_id: str,
) -> dict[str, Any]:
    """分批签收至收件方：仅处理仍在途的件。"""
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                return _land_pieces(conn, order_id, "RECEIVE", operator, request_id, piece_ids)
        finally:
            conn.close()


def return_transfer_order(
    db_path: Path | str | None,
    order_id: int,
    piece_ids: list[int],
    operator: str,
    request_id: str,
) -> dict[str, Any]:
    """分批退回至发件方：仅处理仍在途的件。"""
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                return _land_pieces(conn, order_id, "RETURN", operator, request_id, piece_ids)
        finally:
            conn.close()



# ====================================================================== 实装 / 拆件
#
# 工位账本在装配语义下的不变量：
# * 只有最外层整件（root）占一个缓冲位；内部子件在 workstation_slot 中没有任何行。
# * 装配：待装件必须是 root（可自带子件），其原 ACTUAL 位被腾出；承载件所在的
#   整件不新增占位。
# * 拆件：被拆出的一组要在同工位占一个“真·空位”——ACTUAL 与 RESERVED 都计入
#   容量，已预留位置不算空位。
# * 待发 / 在途（transfer_order_piece.released_at IS NULL）的整件，其组成冻结。
# * 质量冻结不阻止拆换返工：装 / 拆不读取也不改动任何检验、凭据、完工记录。


def _assert_root_not_on_active_order(conn: sqlite3.Connection, root_id: int) -> None:
    active = _active_order_for_piece(conn, root_id)
    if active is not None:
        raise ServiceError(
            409,
            f"整件 {root_id} 在调拨单 {active} 上待发或在途，不能改变它的组成",
        )


def install_assembly(
    db_path: Path | str | None,
    carrier_piece_id: int,
    installed_piece_id: int,
    operator: str,
    workstation_id: Optional[int],
) -> dict[str, Any]:
    """把待装件实装到承载件上。

    待装件当前必须是最外层整件（不能已装在别的件里），但可以自带子件；承载件
    本身可以是更大总成的内件。两者须在同一工位、属于不同整件。
    """
    if carrier_piece_id == installed_piece_id:
        raise ServiceError(422, "承载件与待装件不能是同一件")

    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                carrier = _piece_row(conn, carrier_piece_id)
                installed = _piece_row(conn, installed_piece_id)

                # 待装件不能已装在别的件里
                if _assembly_parent(conn, installed_piece_id) is not None:
                    raise ServiceError(
                        409, f"待装件 {installed_piece_id} 已实装在别的件内，不能重复实装"
                    )
                # 两者必须属于不同的整件
                carrier_root = _root_piece_id(conn, carrier_piece_id)
                installed_root = _root_piece_id(conn, installed_piece_id)
                if carrier_root == installed_root:
                    raise ServiceError(
                        409, f"承载件 {carrier_piece_id} 与待装件 {installed_piece_id} 已属于同一整件"
                    )

                # 有效工位 / 在途状态沿整件解析；两者都须在工位上且同工位
                c_status, c_ws, _ = _effective_piece_state(conn, carrier)
                i_status, i_ws, _ = _effective_piece_state(conn, installed)
                if c_status != "AT_WORKSTATION" or i_status != "AT_WORKSTATION":
                    raise ServiceError(409, "在途的件不能实装，须先落地")
                if c_ws != i_ws:
                    raise ServiceError(
                        409,
                        f"承载件与待装件不在同一工位（分别为 {c_ws}、{i_ws}），不能实装",
                    )
                if workstation_id is not None:
                    _get_workstation(conn, workstation_id)
                    if workstation_id != c_ws:
                        raise ServiceError(
                            409, f"两件当前不在指定工位 {workstation_id}（实际在 {c_ws}）"
                        )
                ws_id = c_ws

                # 涉及的整件只要待发或在途，就不能改组成
                _assert_root_not_on_active_order(conn, carrier_root)
                _assert_root_not_on_active_order(conn, installed_root)

                now = now_iso()
                cur = conn.execute(
                    """
                    INSERT INTO assembly_event(action, parent_piece_id, child_piece_id,
                                               workstation_id, operator, detail, created_at)
                    VALUES ('INSTALL', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        carrier_piece_id,
                        installed_piece_id,
                        ws_id,
                        operator,
                        json.dumps(
                            {
                                "carrier_root_piece_id": carrier_root,
                                "installed_root_piece_id": installed_root,
                            },
                            ensure_ascii=False,
                        ),
                        now,
                    ),
                )
                event_id = cur.lastrowid
                conn.execute(
                    """
                    INSERT INTO piece_assembly(parent_piece_id, child_piece_id, installed_at, install_event_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (carrier_piece_id, installed_piece_id, now, event_id),
                )

                # 待装件是 root：腾出它原来的实物缓冲位（子件本就不占位，无需处理）。
                deleted = conn.execute(
                    "DELETE FROM workstation_slot WHERE piece_id = ?", (installed_piece_id,)
                ).rowcount
                if deleted == 0:  # pragma: no cover - root 在工位时必有 ACTUAL 位
                    raise ServiceError(
                        500, f"待装件 {installed_piece_id} 缺少实物缓冲位记录，数据状态异常"
                    )
            carrier_row = _piece_row(conn, carrier_piece_id)
            result = _piece_dict(conn, carrier_row)
            result["composition"] = _composition_dict(
                conn, _root_piece_id(conn, carrier_piece_id)
            )
            return result
        finally:
            conn.close()


def remove_assembly(
    db_path: Path | str | None,
    carrier_piece_id: int,
    child_piece_id: int,
    operator: str,
    workstation_id: Optional[int],
) -> dict[str, Any]:
    """从直接承载件上拆下指定子件；其原有下级随它一起拆下。"""
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                carrier = _piece_row(conn, carrier_piece_id)
                child = _piece_row(conn, child_piece_id)
                link = conn.execute(
                    "SELECT * FROM piece_assembly WHERE child_piece_id = ?", (child_piece_id,)
                ).fetchone()
                if link is None:
                    raise ServiceError(409, f"件 {child_piece_id} 不是任何件的实装子件，不能拆件")
                if link["parent_piece_id"] != carrier_piece_id:
                    raise ServiceError(
                        409,
                        f"件 {child_piece_id} 的直接承载件是 {link['parent_piece_id']}，"
                        f"不是 {carrier_piece_id}；拆件必须从直接承载件上拆",
                    )

                # 工位：两件的有效工位都沿承载整件解析，必须同工位且在工位上
                c_status, c_ws, carrier_root = _effective_piece_state(conn, carrier)
                ch_status, ch_ws, _ = _effective_piece_state(conn, child)
                if c_status != "AT_WORKSTATION" or ch_status != "AT_WORKSTATION":
                    raise ServiceError(409, "在途的件不能拆件，须先落地")
                if c_ws != ch_ws:  # pragma: no cover - 同一棵树工位必然一致
                    raise ServiceError(409, "承载件与子件不在同一工位，不能拆件")
                if workstation_id is not None:
                    _get_workstation(conn, workstation_id)
                    if workstation_id != c_ws:
                        raise ServiceError(
                            409, f"件当前不在指定工位 {workstation_id}（实际在 {c_ws}）"
                        )
                ws_id = c_ws

                # 待发 / 在途整件组成冻结（质量冻结不拦截）
                _assert_root_not_on_active_order(conn, carrier_root)

                # 被拆下的一组（含子件及其全部下级）需要在同工位占一个真·空位：
                # ACTUAL 与 RESERVED 都占容量，已预留位置不算空位。
                removed_members = _subtree(conn, child_piece_id)
                if _available_slots(conn, ws_id) < 1:
                    ws = _get_workstation(conn, ws_id)
                    raise ServiceError(
                        409,
                        f"工位 {ws['code']}（id={ws_id}）没有空缓冲位（已预留位置不算空位），"
                        f"无法容纳拆下的一组 {len(removed_members)} 件",
                    )

                now = now_iso()
                cur = conn.execute(
                    """
                    INSERT INTO assembly_event(action, parent_piece_id, child_piece_id,
                                               workstation_id, operator, detail, created_at)
                    VALUES ('REMOVE', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        carrier_piece_id,
                        child_piece_id,
                        ws_id,
                        operator,
                        json.dumps(
                            {
                                "removed_member_piece_ids": [pid for pid, _, _ in removed_members],
                            },
                            ensure_ascii=False,
                        ),
                        now,
                    ),
                )
                event_id = cur.lastrowid

                # 只断开直接承载边；孙级行随子件一起离开，但保留在表中，
                # 从而维持“原有下级随它一起拆下”的完整子树。
                conn.execute(
                    "DELETE FROM piece_assembly WHERE parent_piece_id = ? AND child_piece_id = ?",
                    (carrier_piece_id, child_piece_id),
                )
                # 拆下的一组成为新的最外层整件，占同工位一个 ACTUAL 位（整组只占一位）
                _insert_slot(conn, ws_id, child_piece_id, "ACTUAL", "HOME")
                # 留痕、断边与位次在同一事务内：失败整体回滚，不留半套组成或货位
            child_row = _piece_row(conn, child_piece_id)
            result = _piece_dict(conn, child_row)
            result["composition"] = _composition_dict(conn, child_piece_id)
            return result
        finally:
            conn.close()


def list_assembly_events(
    db_path: Path | str | None, piece_id: Optional[int] = None
) -> list[dict[str, Any]]:
    """装拆留痕：人、件、工位。可按件过滤（含其作为承载件或子件的记录）。"""
    conn = get_connection(db_path)
    try:
        if piece_id is not None:
            if conn.execute(
                "SELECT 1 FROM piece WHERE piece_id = ?", (piece_id,)
            ).fetchone() is None:
                raise ServiceError(404, f"件 {piece_id} 不存在")
            rows = conn.execute(
                "SELECT * FROM assembly_event WHERE parent_piece_id = ? OR child_piece_id = ?"
                " ORDER BY event_id",
                (piece_id, piece_id),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM assembly_event ORDER BY event_id"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _composition_dict(conn: sqlite3.Connection, piece_id: int) -> dict[str, Any]:
    """同连接内的组成查询（供写事务返回值复用，避免事务中另开连接）。"""
    piece = _piece_row(conn, piece_id)
    root_id = _root_piece_id(conn, piece_id)
    parent_row = _assembly_parent(conn, piece_id)
    eff_status, eff_ws_id, _ = _effective_piece_state(conn, piece)
    ws = conn.execute(
        "SELECT code FROM workstation WHERE workstation_id = ?", (eff_ws_id,)
    ).fetchone()
    wp = conn.execute(
        "SELECT code FROM workpiece WHERE workpiece_id = ?", (piece["workpiece_id"],)
    ).fetchone()

    # 以查询件为根的可见子树，depth 从 0 重新计
    tree: list[dict[str, Any]] = []
    for pid, parent_pid, depth in _subtree(conn, piece_id):
        member = _piece_row(conn, pid)
        member_wp = conn.execute(
            "SELECT code FROM workpiece WHERE workpiece_id = ?", (member["workpiece_id"],)
        ).fetchone()
        tree.append(
            {
                "piece_id": pid,
                "workpiece_id": member["workpiece_id"],
                "workpiece_code": member_wp["code"],
                "gate_process_id": member["gate_process_id"],
                "parent_piece_id": parent_pid,
                "depth": depth,
            }
        )

    own_valid, _ = _gate_valid(conn, piece)
    if root_id == piece_id:
        all_valid = True
        for member_id, _, _ in _subtree(conn, root_id):
            valid, _ = _gate_valid(conn, _piece_row(conn, member_id))
            if not valid:
                all_valid = False
                break
        gate_valid = all_valid
    else:
        gate_valid = own_valid

    return {
        "piece_id": piece_id,
        "workpiece_id": piece["workpiece_id"],
        "workpiece_code": wp["code"],
        "direct_parent_piece_id": parent_row["parent_piece_id"] if parent_row else None,
        "root_piece_id": root_id,
        "is_inner": parent_row is not None,
        "tree": tree,
        "status": eff_status,
        "current_workstation_id": eff_ws_id,
        "current_workstation_code": ws["code"],
        "in_transit": eff_status == "IN_TRANSIT",
        "gate_valid": gate_valid,
        "quality_frozen": not gate_valid,
    }


def get_piece_composition(db_path: Path | str | None, piece_id: int) -> dict[str, Any]:
    """查任一工件：直接承载件、最外层整件、实际所在工位或在途状态与完整父子树。"""
    conn = get_connection(db_path)
    try:
        # 先确认件存在（不存在抛 404），再走同连接组装
        _piece_row(conn, piece_id)
        return _composition_dict(conn, piece_id)
    finally:
        conn.close()


# ============================================================ 发运追溯证据（快照）

def _write_shipment_snapshot(
    conn: sqlite3.Connection,
    order_id: int,
    action_id: int,
    root_piece_id: int,
    now: str,
) -> int:
    """发运事务内调用：快照整件当时的完整组成与各成员关口的生效凭据。

    与当前 piece_assembly / credential 表完全解耦，之后的拆换、补测、复核都
    不会改写本快照（历史交接记录不可变）。
    """
    cur = conn.execute(
        """
        INSERT INTO shipment_snapshot(order_id, action_id, root_piece_id, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (order_id, action_id, root_piece_id, now),
    )
    snapshot_id = cur.lastrowid
    for pid, parent_pid, depth in _subtree(conn, root_piece_id):
        member = _piece_row(conn, pid)
        conn.execute(
            """
            INSERT INTO shipment_snapshot_member(
                snapshot_id, piece_id, workpiece_id, gate_process_id, parent_piece_id, depth)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                pid,
                member["workpiece_id"],
                member["gate_process_id"],
                parent_pid,
                depth,
            ),
        )
        active = _active_credential(conn, member["gate_process_id"])
        conn.execute(
            """
            INSERT INTO shipment_snapshot_credential(
                snapshot_id, piece_id, credential_id, process_id, credential_kind)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                pid,
                active["credential_id"] if active else None,
                member["gate_process_id"],
                active["kind"] if active else None,
            ),
        )
    return snapshot_id


def _snapshot_detail(conn: sqlite3.Connection, snap: sqlite3.Row) -> dict[str, Any]:
    root_wp = conn.execute(
        """
        SELECT w.code FROM workpiece w
        JOIN piece p ON p.workpiece_id = w.workpiece_id
        WHERE p.piece_id = ?
        """,
        (snap["root_piece_id"],),
    ).fetchone()
    members_out: list[dict[str, Any]] = []
    members = conn.execute(
        "SELECT * FROM shipment_snapshot_member WHERE snapshot_id = ? ORDER BY depth, piece_id",
        (snap["snapshot_id"],),
    ).fetchall()
    for m in members:
        wp = conn.execute(
            "SELECT code FROM workpiece WHERE workpiece_id = ?", (m["workpiece_id"],)
        ).fetchone()
        cred_row = conn.execute(
            "SELECT * FROM shipment_snapshot_credential WHERE snapshot_id = ? AND piece_id = ?",
            (snap["snapshot_id"], m["piece_id"]),
        ).fetchone()
        credential = None
        if cred_row is not None:
            inspections: list[dict[str, Any]] = []
            if cred_row["credential_id"] is not None:
                inspections = [
                    dict(r)
                    for r in conn.execute(
                        "SELECT item_id, inspection_id FROM credential_inspection"
                        " WHERE credential_id = ? ORDER BY item_id",
                        (cred_row["credential_id"],),
                    )
                ]
            credential = {
                "credential_id": cred_row["credential_id"],
                "process_id": cred_row["process_id"],
                "credential_kind": cred_row["credential_kind"],
                "inspections": inspections,
            }
        members_out.append(
            {
                "piece_id": m["piece_id"],
                "workpiece_id": m["workpiece_id"],
                "workpiece_code": wp["code"],
                "gate_process_id": m["gate_process_id"],
                "parent_piece_id": m["parent_piece_id"],
                "depth": m["depth"],
                "credential": credential,
            }
        )
    return {
        "snapshot_id": snap["snapshot_id"],
        "order_id": snap["order_id"],
        "action_id": snap["action_id"],
        "root_piece_id": snap["root_piece_id"],
        "root_workpiece_code": root_wp["code"],
        "created_at": snap["created_at"],
        "members": members_out,
    }


def get_shipment_trace(db_path: Path | str | None, order_id: int) -> dict[str, Any]:
    """查一次实际发运：每个整件当时完整的父子组成，以及每个成员转运关口当时
    生效的开工 / 复核凭据（含凭据快照引用的检验依据）。

    本功能上线前的旧调拨单没有快照，snapshots 返回空列表，不补猜旧单。
    """
    conn = get_connection(db_path)
    try:
        order = _order_row(conn, order_id)
        snaps = conn.execute(
            "SELECT * FROM shipment_snapshot WHERE order_id = ? ORDER BY snapshot_id",
            (order_id,),
        ).fetchall()
        return {
            "order_id": order_id,
            "order_status": order["status"],
            "shipped_at": order["shipped_at"],
            "snapshots": [_snapshot_detail(conn, s) for s in snaps],
        }
    finally:
        conn.close()


def find_gauge_impact(
    db_path: Path | str | None, gauge_id: int, window_id: int
) -> list[dict[str, Any]]:
    """给定一条既有量具失准登记，返回哪些发运单的哪些整件和成员曾引用该量具
    在该窗内的测量。

    判定口径严格限定为**发运快照里各关口生效凭据所引用的测量**
    （credential_inspection → inspection），因此事后拆换、补测、复核都不改变
    查到的历史依据。只覆盖上线后留有快照的新发运。
    """
    conn = get_connection(db_path)
    try:
        window = conn.execute(
            "SELECT * FROM gauge_window WHERE gauge_id = ? AND window_id = ?",
            (gauge_id, window_id),
        ).fetchone()
        if window is None:
            raise ServiceError(
                404, f"量具 {gauge_id} 的失准窗口 {window_id} 不存在"
            )
        rows = conn.execute(
            """
            SELECT
                o.order_id,
                o.status AS order_status,
                o.shipped_at,
                s.snapshot_id,
                s.root_piece_id,
                m.piece_id AS member_piece_id,
                m.workpiece_id AS member_workpiece_id,
                m.gate_process_id,
                sc.credential_id,
                sc.credential_kind,
                ci.item_id,
                i.inspection_id,
                i.measured_at,
                i.reading
            FROM shipment_snapshot s
            JOIN transfer_order o ON o.order_id = s.order_id
            JOIN shipment_snapshot_member m ON m.snapshot_id = s.snapshot_id
            JOIN shipment_snapshot_credential sc
                ON sc.snapshot_id = s.snapshot_id AND sc.piece_id = m.piece_id
            JOIN credential_inspection ci ON ci.credential_id = sc.credential_id
            JOIN inspection i ON i.inspection_id = ci.inspection_id
            WHERE i.gauge_id = ?
              AND i.measured_at >= ?
              AND i.measured_at < ?
            ORDER BY o.order_id, s.snapshot_id, m.depth, m.piece_id, ci.item_id
            """,
            (gauge_id, window["window_start"], window["window_end"]),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for r in rows:
            root_wp = conn.execute(
                """
                SELECT w.code FROM workpiece w
                JOIN piece p ON p.workpiece_id = w.workpiece_id
                WHERE p.piece_id = ?
                """,
                (r["root_piece_id"],),
            ).fetchone()
            member_wp = conn.execute(
                "SELECT code FROM workpiece WHERE workpiece_id = ?",
                (r["member_workpiece_id"],),
            ).fetchone()
            result.append(
                {
                    "gauge_id": gauge_id,
                    "window_id": window_id,
                    "window_start": window["window_start"],
                    "window_end": window["window_end"],
                    "order_id": r["order_id"],
                    "order_status": r["order_status"],
                    "shipped_at": r["shipped_at"],
                    "snapshot_id": r["snapshot_id"],
                    "root_piece_id": r["root_piece_id"],
                    "root_workpiece_code": root_wp["code"],
                    "member_piece_id": r["member_piece_id"],
                    "member_workpiece_id": r["member_workpiece_id"],
                    "member_workpiece_code": member_wp["code"],
                    "gate_process_id": r["gate_process_id"],
                    "credential_id": r["credential_id"],
                    "credential_kind": r["credential_kind"],
                    "item_id": r["item_id"],
                    "inspection_id": r["inspection_id"],
                    "measured_at": r["measured_at"],
                    "reading": r["reading"],
                }
            )
        return result
    finally:
        conn.close()


# ================================================================ 班次派工
#
# 模型：
# * 机器 machine、夹具 fixture 是两种独占资源；一次预约在 [reserved_start,
#   reserved_end) 内连续占用一对「机器 + 夹具」。预约从整分钟开始，区间左闭右开。
# * 派工 dispatch 把一个**未开工**工序编入一个班次，带上允许加工的窗口、所需
#   整分钟数，以及该工序可用的「机器、夹具」组合（dispatch_option）。
# * 一道工序在任一时刻至多有一个当前派工（部分唯一索引 uq_current_dispatch_process），
#   因此同一道工序只能出现在一个班次里。
# * 已开工 / 已完工的派工 actual_start_at / actual_end_at 记录真实占用：从开工成功
#   起到真实完工才结束，不随预约到时、停机登记、质量隔离或复核释放。只有实际占用
#   才阻止后面的预约开工；预约本身只需按预计时间互不重叠。
#
# 重排（reschedule）：
# * 未开工的预约可以互相挪动；已开工 / 已完工的派工不能改，其原约定时段仍算已占
#   （即使撞上后来登记的停用）。
# * 若能为全部未开工工序找到合法安排，就在同一事务里一起换成新安排（旧派工
#   REPLACED、新派工 PLANNED）；否则停用照常生效，与停用冲突的未开工预约暂停
#   （SUSPENDED），其余原预约保持不动。暂停项留在本班，可再次重排。
# * 新安排不能从过去开始（本次动作的当前时刻）。
#
# 并发：所有写操作共用进程内 _service_lock，并以显式 BEGIN IMMEDIATE 事务跨进程
# 串行化。调度（读全部相关状态）与落库在同一写锁内完成，因此两个进程同时重排 /
# 开工 / 登记质量变化时，不会用已失效预约开工、不会重复占用资源、也不会留下只发布
# 了一部分的排班。


ALLOWED_FIXTURE_STATUSES = ("PLANNED", "STARTED")


def _add_minutes(raw: str, minutes: int) -> str:
    return (datetime.fromisoformat(raw) + timedelta(minutes=minutes)).isoformat()


def _parse_dt(raw: str) -> datetime:
    return datetime.fromisoformat(raw)


def _overlaps(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    """半开区间重叠判定。"""
    return a_start < b_end and b_start < a_end


def _get_machine(conn: sqlite3.Connection, machine_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM machine WHERE machine_id = ?", (machine_id,)
    ).fetchone()
    if row is None:
        raise ServiceError(404, f"机器 {machine_id} 不存在")
    return row


def _get_fixture(conn: sqlite3.Connection, fixture_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM fixture WHERE fixture_id = ?", (fixture_id,)
    ).fetchone()
    if row is None:
        raise ServiceError(404, f"夹具 {fixture_id} 不存在")
    return row


def _get_shift(conn: sqlite3.Connection, shift_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM shift WHERE shift_id = ?", (shift_id,)
    ).fetchone()
    if row is None:
        raise ServiceError(404, f"班次 {shift_id} 不存在")
    return row


def _downtime_rows(
    conn: sqlite3.Connection, resource_type: str, resource_id: int
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM resource_downtime WHERE resource_type = ? AND resource_id = ?"
        " ORDER BY window_start",
        (resource_type, resource_id),
    ).fetchall()


def _resource_blocked(
    conn: sqlite3.Connection,
    resource_type: str,
    resource_id: int,
    start: str,
    end: str,
) -> bool:
    for d in _downtime_rows(conn, resource_type, resource_id):
        if _overlaps(start, end, d["window_start"], d["window_end"]):
            return True
    return False


# ---------------------------------------------------------------- registries

def create_machine(db_path: Path | str | None, code: str, name: str) -> dict[str, Any]:
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                try:
                    cur = conn.execute(
                        "INSERT INTO machine(code, name, created_at) VALUES (?, ?, ?)",
                        (code, name, now_iso()),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ServiceError(409, f"机器编号 {code} 已存在") from exc
                machine_id = cur.lastrowid
            row = conn.execute(
                "SELECT * FROM machine WHERE machine_id = ?", (machine_id,)
            ).fetchone()
            return dict(row)
        finally:
            conn.close()


def list_machines(db_path: Path | str | None) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM machine ORDER BY machine_id")]
    finally:
        conn.close()


def create_fixture(db_path: Path | str | None, code: str, name: str) -> dict[str, Any]:
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                try:
                    cur = conn.execute(
                        "INSERT INTO fixture(code, name, created_at) VALUES (?, ?, ?)",
                        (code, name, now_iso()),
                    )
                except sqlite3.IntegrityError as exc:
                    raise ServiceError(409, f"夹具编号 {code} 已存在") from exc
                fixture_id = cur.lastrowid
            row = conn.execute(
                "SELECT * FROM fixture WHERE fixture_id = ?", (fixture_id,)
            ).fetchone()
            return dict(row)
        finally:
            conn.close()


def list_fixtures(db_path: Path | str | None) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM fixture ORDER BY fixture_id")]
    finally:
        conn.close()


# ----------------------------------------------------------------- downtime

def register_resource_downtime(
    db_path: Path | str | None,
    resource_type: str,
    resource_id: int,
    window_start_raw: str,
    window_end_raw: str,
    operator: Optional[str],
    note: Optional[str],
) -> dict[str, Any]:
    """登记机器 / 夹具停用时段；只登记资源级事实，不改动已有派工。"""
    window_start = normalize_ts(window_start_raw)
    window_end = normalize_ts(window_end_raw)
    if not window_start < window_end:
        raise ServiceError(422, "停用时段要求 window_start < window_end（半开 [start, end)）")
    if resource_type not in ("MACHINE", "FIXTURE"):
        raise ServiceError(422, "resource_type 只能是 MACHINE 或 FIXTURE")

    with _service_lock:
        conn = get_connection(db_path)
        try:
            if resource_type == "MACHINE":
                _get_machine(conn, resource_id)
            else:
                _get_fixture(conn, resource_id)
            with _immediate_tx(conn):
                cur = conn.execute(
                    """
                    INSERT INTO resource_downtime(resource_type, resource_id,
                        window_start, window_end, operator, note, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (resource_type, resource_id, window_start, window_end,
                     operator, note, now_iso()),
                )
                downtime_id = cur.lastrowid
            row = conn.execute(
                "SELECT * FROM resource_downtime WHERE downtime_id = ?", (downtime_id,)
            ).fetchone()
            return dict(row)
        finally:
            conn.close()


def list_resource_downtimes(
    db_path: Path | str | None,
    resource_type: Optional[str] = None,
    resource_id: Optional[int] = None,
) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        sql = "SELECT * FROM resource_downtime"
        params: list[Any] = []
        where: list[str] = []
        if resource_type is not None:
            where.append("resource_type = ?")
            params.append(resource_type)
        if resource_id is not None:
            where.append("resource_id = ?")
            params.append(resource_id)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY downtime_id"
        return [dict(r) for r in conn.execute(sql, params)]
    finally:
        conn.close()


# --------------------------------------------------------- scheduling engine
#
# 排班是典型的区间 + 资源组合问题。这里不追求最优，只要求找到任意一份合法安排；
# 找不到就整体不发布。由于数据规模（一个班次的工序数）很小，采用带回溯的贪心
# 搜索：按「窗口结束最早优先」排序，逐个工序尝试其全部「机器、夹具」组合及候选
# 起点（在窗口内、整分钟、且避开已选排班的预计占用与资源停用）。


@dataclass
class _Demand:
    """一次待排的工序需求（一次派工尝试）。"""
    process_id: int
    window_start: str
    window_end: str
    duration_minutes: int
    options: list[tuple[int, int]] = field(default_factory=list)
    # 保留原派工 id（重排时为原 dispatch_id；首次排班为 None），用于尽量保留
    # 已合法的原预约。
    dispatch_id: Optional[int] = None
    # 重排时原派工的预约（用于优先原地保留）；首次排班为 None。
    preferred: Optional[tuple[int, int, str]] = None  # (machine, fixture, start)


@dataclass
class _Placement:
    demand: _Demand
    machine_id: int
    fixture_id: int
    start: str
    end: str


def _ceil_minute(dt: datetime) -> datetime:
    if dt.second or dt.microsecond:
        return dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return dt.replace(second=0, microsecond=0)


def _candidate_starts(
    demand: _Demand,
    earliest: datetime,
    occupied: dict[tuple[str, int], list[tuple[str, str]]],
) -> list[datetime]:
    """在加工窗口内、整分钟的候选起点。

    完备的候选集只要包含：窗口起点、每条相关资源停用时段的右端、以及当前已
    占用区间的右端（贴着既有占用之后安排）。任一可行整数分钟排班都可用这些
    事件点平移得到，因此回溯在这些点中搜索即可找到合法安排（规模很小）。
    """
    ws = max(_parse_dt(demand.window_start), earliest)
    ws = _ceil_minute(ws)
    we = _parse_dt(demand.window_end)
    dur = timedelta(minutes=demand.duration_minutes)
    points: set[datetime] = {ws}
    resource_keys = {("MACHINE", m) for m, _ in demand.options} | {
        ("FIXTURE", f) for _, f in demand.options
    }
    for key in resource_keys:
        for (_, oe) in occupied.get(key, []):
            points.add(_parse_dt(oe))
        for d in _downtime_cache.get(key, []):
            points.add(_parse_dt(d["window_end"]))
            points.add(_parse_dt(d["window_start"]))
    starts: list[datetime] = []
    for s in sorted(points):
        s = _ceil_minute(s)
        if s < ws or s + dur > we:
            continue
        if s not in starts:
            starts.append(s)
    # 重排时若原预约起点仍合法（在窗口内、没到期），优先原样保留。
    if demand.preferred is not None:
        pref = _parse_dt(demand.preferred[2])
        if pref >= ws and pref + dur <= we:
            starts = [pref] + [s for s in starts if s != pref]
    return starts


# 排班期间缓存停用时段（按资源），避免重复查询；由 build_schedule 填充 / 清理。
_downtime_cache: dict[tuple[str, int], list[sqlite3.Row]] = {}


def _load_downtime_cache(conn: sqlite3.Connection, demands: list[_Demand]) -> None:
    _downtime_cache.clear()
    keys: set[tuple[str, int]] = set()
    for d in demands:
        for m, f in d.options:
            keys.add(("MACHINE", m))
            keys.add(("FIXTURE", f))
    for rtype, rid in keys:
        _downtime_cache[(rtype, rid)] = _downtime_rows(conn, rtype, rid)


def _clear_downtime_cache() -> None:
    _downtime_cache.clear()


def build_schedule(
    demands: list[_Demand],
    fixed: list[_Placement],
    earliest: datetime,
) -> Optional[list[_Placement]]:
    """为全部 demands 找到一份合法安排，或返回 None（第一次排不齐就不发布）。

    * fixed 是已开工 / 已完工、不能挪动的实际占用，以固定区间参与资源冲突。
    * 每个工序只能选其 options 内的「机器、夹具」组合。
    * 任一资源（机器或夹具）都不得与 fixed、已选 placements、或自身停用时段重叠。
    """
    # 资源占用按 (类型, id) 聚合区间
    occupied: dict[tuple[str, int], list[tuple[str, str]]] = defaultdict(list)
    for p in fixed:
        occupied[("MACHINE", p.machine_id)].append((p.start, p.end))
        occupied[("FIXTURE", p.fixture_id)].append((p.start, p.end))

    def free(rtype: str, rid: int, s: str, e: str) -> bool:
        for (os_, oe) in occupied[(rtype, rid)]:
            if _overlaps(s, e, os_, oe):
                return False
        for d in _downtime_cache.get((rtype, rid), []):
            if _overlaps(s, e, d["window_start"], d["window_end"]):
                return False
        return True

    # 排序启发：窗口结束最早、时长最长优先，受限的组合先排。
    ordered = sorted(
        demands,
        key=lambda d: (d.window_end, -d.duration_minutes, d.process_id),
    )
    placed: list[_Placement] = []

    def backtrack(idx: int) -> bool:
        if idx == len(ordered):
            return True
        d = ordered[idx]
        # 重排时优先保留原组合
        opts = list(d.options)
        if d.preferred is not None and (d.preferred[0], d.preferred[1]) in opts:
            pref = (d.preferred[0], d.preferred[1])
            opts.remove(pref)
            opts.insert(0, pref)
        for s in _candidate_starts(d, earliest, occupied):
            e = _add_minutes(s.isoformat(), d.duration_minutes)
            if _parse_dt(e) > _parse_dt(d.window_end):
                continue
            s_iso = s.isoformat()
            for (m, f) in opts:
                if not free("MACHINE", m, s_iso, e):
                    continue
                if not free("FIXTURE", f, s_iso, e):
                    continue
                placement = _Placement(d, m, f, s_iso, e)
                occ_m = ("MACHINE", m)
                occ_f = ("FIXTURE", f)
                occupied[occ_m].append((s_iso, e))
                occupied[occ_f].append((s_iso, e))
                placed.append(placement)
                if backtrack(idx + 1):
                    return True
                placed.pop()
                occupied[occ_m].pop()
                occupied[occ_f].pop()
        return False

    if backtrack(0):
        return placed
    return None


# ------------------------------------------------------------ shift creation

def _process_dispatch_dict(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    proc = conn.execute(
        "SELECT * FROM process WHERE process_id = ?", (row["process_id"],)
    ).fetchone()
    wp = conn.execute(
        "SELECT code FROM workpiece WHERE workpiece_id = ?", (proc["workpiece_id"],)
    ).fetchone()
    machine = conn.execute(
        "SELECT code FROM machine WHERE machine_id = ?", (row["machine_id"],)
    ).fetchone()
    fixture = conn.execute(
        "SELECT code FROM fixture WHERE fixture_id = ?", (row["fixture_id"],)
    ).fetchone()
    options = [
        {"machine_id": r["machine_id"], "fixture_id": r["fixture_id"]}
        for r in conn.execute(
            "SELECT machine_id, fixture_id FROM dispatch_option WHERE dispatch_id = ?"
            " ORDER BY machine_id, fixture_id",
            (row["dispatch_id"],),
        )
    ]
    state = conn.execute(
        "SELECT status FROM process_state WHERE process_id = ?", (row["process_id"],)
    ).fetchone()
    status = row["status"]
    return {
        "dispatch_id": row["dispatch_id"],
        "shift_id": row["shift_id"],
        "process_id": row["process_id"],
        "process_code": proc["code"],
        "process_name": proc["name"],
        "workpiece_id": proc["workpiece_id"],
        "workpiece_code": wp["code"],
        "machine_id": row["machine_id"],
        "machine_code": machine["code"],
        "fixture_id": row["fixture_id"],
        "fixture_code": fixture["code"],
        "window_start": row["window_start"],
        "window_end": row["window_end"],
        "duration_minutes": row["duration_minutes"],
        "reserved_start": row["reserved_start"],
        "reserved_end": row["reserved_end"],
        "status": status,
        "is_current": bool(row["is_current"]),
        "suspended": status == "SUSPENDED",
        "started": status in ("STARTED", "COMPLETED"),
        "completed": status == "COMPLETED",
        "actual_start_at": row["actual_start_at"],
        "actual_end_at": row["actual_end_at"],
        "process_status": state["status"] if state is not None else "NOT_STARTED",
        "options": options,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _shift_dict(conn: sqlite3.Connection, shift: sqlite3.Row) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT * FROM dispatch WHERE shift_id = ? AND is_current = 1"
        " ORDER BY reserved_start, dispatch_id",
        (shift["shift_id"],),
    ).fetchall()
    dispatches = [_process_dispatch_dict(conn, r) for r in rows]
    return {
        "shift_id": shift["shift_id"],
        "code": shift["code"],
        "operator": shift["operator"],
        "window_start": shift["window_start"],
        "window_end": shift["window_end"],
        "status": shift["status"],
        "created_at": shift["created_at"],
        "updated_at": shift["updated_at"],
        "dispatches": dispatches,
        "has_suspended": any(d["suspended"] for d in dispatches),
        "all_started_or_completed": bool(dispatches)
        and all(d["started"] for d in dispatches),
    }


def _existing_fixed_placements(
    conn: sqlite3.Connection, exclude_dispatch_ids: set[int]
) -> list[_Placement]:
    """当前已开工 / 已完工的派工，其原约定时段仍算已占，参与冲突判定。

    即使这些预约遇到后来登记的停用，也不会被挪动；排班只避让它们的区间。
    """
    rows = conn.execute(
        "SELECT * FROM dispatch WHERE is_current = 1 AND status IN ('STARTED', 'COMPLETED')"
    ).fetchall()
    placements: list[_Placement] = []
    for r in rows:
        if r["dispatch_id"] in exclude_dispatch_ids:
            continue
        d = _Demand(
            process_id=r["process_id"],
            window_start=r["reserved_start"],
            window_end=r["reserved_end"],
            duration_minutes=r["duration_minutes"],
            options=[(r["machine_id"], r["fixture_id"])],
            dispatch_id=r["dispatch_id"],
        )
        placements.append(
            _Placement(d, r["machine_id"], r["fixture_id"], r["reserved_start"], r["reserved_end"])
        )
    return placements


def _planned_other_shifts(
    conn: sqlite3.Connection, shift_id: int, exclude_dispatch_ids: set[int]
) -> list[_Placement]:
    """其他班次里未开工、也未暂停的当前预约，同样按其预计时段参与资源避让。

    重排只改本班次；但不同班次的预约共享同一资源，因此新安排不能撞上其他班次
    仍在生效的预约。
    """
    rows = conn.execute(
        "SELECT * FROM dispatch WHERE is_current = 1 AND shift_id <> ?"
        " AND status IN ('PLANNED')",
        (shift_id,),
    ).fetchall()
    placements: list[_Placement] = []
    for r in rows:
        if r["dispatch_id"] in exclude_dispatch_ids:
            continue
        d = _Demand(
            process_id=r["process_id"],
            window_start=r["reserved_start"],
            window_end=r["reserved_end"],
            duration_minutes=r["duration_minutes"],
            options=[(r["machine_id"], r["fixture_id"])],
            dispatch_id=r["dispatch_id"],
        )
        placements.append(
            _Placement(d, r["machine_id"], r["fixture_id"], r["reserved_start"], r["reserved_end"])
        )
    return placements


def _validate_shift_spec(
    conn: sqlite3.Connection, spec_dispatch: dict[str, Any], seen_processes: set[int]
) -> None:
    pid = spec_dispatch["process_id"]
    if pid in seen_processes:
        raise ServiceError(422, f"同一工序 {pid} 在一次班次请求里重复出现；一道工序只能编入一个班次")
    seen_processes.add(pid)
    proc = _get_process(conn, pid)
    state = _get_state(conn, pid)
    if state["status"] != "NOT_STARTED":
        raise ServiceError(
            409,
            f"工序 {proc['code']}（id={pid}）当前状态为 {state['status']}，"
            f"只有未开工的工序可以编入班次",
        )
    # 已纳入其他派工的工序不能再编入（每道工序至多一个当前派工）
    if _any_dispatch_ever(conn, pid):
        raise ServiceError(
            409, f"工序 {proc['code']}（id={pid}）已经纳入过派工，不能重复编入新的班次"
        )
    ws = normalize_ts(spec_dispatch["window_start"])
    we = normalize_ts(spec_dispatch["window_end"])
    if not ws < we:
        raise ServiceError(422, f"工序 {proc['code']} 的加工窗口要求 window_start < window_end")
    if spec_dispatch["duration_minutes"] <= 0:
        raise ServiceError(422, f"工序 {proc['code']} 的所需分钟数必须为正整数")
    if not spec_dispatch["options"]:
        raise ServiceError(422, f"工序 {proc['code']} 至少需要一种「机器、夹具」组合")
    seen_opts: set[tuple[int, int]] = set()
    for opt in spec_dispatch["options"]:
        key = (opt["machine_id"], opt["fixture_id"])
        if key in seen_opts:
            raise ServiceError(422, f"工序 {proc['code']} 的「机器、夹具」组合重复")
        seen_opts.add(key)
        _get_machine(conn, opt["machine_id"])
        _get_fixture(conn, opt["fixture_id"])


def create_shift(
    db_path: Path | str | None, payload: dict[str, Any]
) -> dict[str, Any]:
    """创建班次并一次性排定全班：为每道工序选定组合与预约时段。

    所有工序都要求是未开工工序；任一工序找不到合法位置则整个请求失败，不发布
    半份排班。
    """
    shift_ws = normalize_ts(payload["window_start"])
    shift_we = normalize_ts(payload["window_end"])
    if not shift_ws < shift_we:
        raise ServiceError(422, "班次时段要求 window_start < window_end")

    specs = payload["dispatches"]
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                if conn.execute(
                    "SELECT 1 FROM shift WHERE code = ?", (payload["code"],)
                ).fetchone() is not None:
                    raise ServiceError(409, f"班次编号 {payload['code']} 已存在")

                seen_processes: set[int] = set()
                demands: list[_Demand] = []
                for spec in specs:
                    _validate_shift_spec(conn, spec, seen_processes)
                    ws = normalize_ts(spec["window_start"])
                    we = normalize_ts(spec["window_end"])
                    # 加工窗口须落在班次时段内
                    if ws < shift_ws or we > shift_we:
                        raise ServiceError(
                            409,
                            f"工序 {spec['process_id']} 的加工窗口 [{ws}, {we}) "
                            f"超出班次时段 [{shift_ws}, {shift_we})",
                        )
                    if _parse_dt(_add_minutes(ws, spec["duration_minutes"])) > _parse_dt(we):
                        raise ServiceError(
                            409,
                            f"工序 {spec['process_id']} 所需 {spec['duration_minutes']} 分钟"
                            f"放不进加工窗口 [{ws}, {we})",
                        )
                    demands.append(
                        _Demand(
                            process_id=spec["process_id"],
                            window_start=ws,
                            window_end=we,
                            duration_minutes=spec["duration_minutes"],
                            options=[
                                (o["machine_id"], o["fixture_id"]) for o in spec["options"]
                            ],
                        )
                    )

                _load_downtime_cache(conn, demands)
                try:
                    # 首次排班：只有其他班次已开工 / 生效的预约作为固定占用
                    fixed = _existing_fixed_placements(conn, set())
                    fixed += _planned_other_shifts(conn, -1, set())
                    earliest = _ceil_minute(_parse_dt(current_ts()))
                    schedule = build_schedule(demands, fixed, earliest)
                finally:
                    _clear_downtime_cache()

                if schedule is None:
                    raise ServiceError(
                        409,
                        "本班次无法排出一份覆盖全部工序的合法安排，未发布任何排班",
                    )

                now = now_iso()
                cur = conn.execute(
                    """
                    INSERT INTO shift(code, operator, window_start, window_end,
                                      status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'PUBLISHED', ?, ?)
                    """,
                    (payload["code"], payload["operator"], shift_ws, shift_we, now, now),
                )
                shift_id = cur.lastrowid
                for placement in schedule:
                    _insert_dispatch(conn, shift_id, placement, now)
            shift = _get_shift(conn, shift_id)
            return _shift_dict(conn, shift)
        finally:
            conn.close()


def _insert_dispatch(
    conn: sqlite3.Connection,
    shift_id: int,
    placement: _Placement,
    now: str,
    options: Optional[list[tuple[int, int]]] = None,
) -> int:
    d = placement.demand
    cur = conn.execute(
        """
        INSERT INTO dispatch(shift_id, process_id, machine_id, fixture_id,
            window_start, window_end, duration_minutes, reserved_start, reserved_end,
            status, is_current, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PLANNED', 1, ?, ?)
        """,
        (
            shift_id,
            d.process_id,
            placement.machine_id,
            placement.fixture_id,
            d.window_start,
            d.window_end,
            d.duration_minutes,
            placement.start,
            placement.end,
            now,
            now,
        ),
    )
    dispatch_id = cur.lastrowid
    for m, f in (options if options is not None else d.options):
        conn.execute(
            "INSERT INTO dispatch_option(dispatch_id, machine_id, fixture_id) VALUES (?, ?, ?)",
            (dispatch_id, m, f),
        )
    return dispatch_id


# -------------------------------------------------------------- queries

def get_shift(db_path: Path | str | None, shift_id: int) -> dict[str, Any]:
    conn = get_connection(db_path)
    try:
        shift = _get_shift(conn, shift_id)
        return _shift_dict(conn, shift)
    finally:
        conn.close()


def list_shifts(db_path: Path | str | None) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        return [
            _shift_dict(conn, r)
            for r in conn.execute("SELECT * FROM shift ORDER BY shift_id")
        ]
    finally:
        conn.close()


def list_dispatches(db_path: Path | str | None, shift_id: Optional[int] = None) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        if shift_id is not None:
            _get_shift(conn, shift_id)
            rows = conn.execute(
                "SELECT * FROM dispatch WHERE shift_id = ? ORDER BY dispatch_id",
                (shift_id,),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM dispatch ORDER BY dispatch_id").fetchall()
        return [_process_dispatch_dict(conn, r) for r in rows]
    finally:
        conn.close()


def get_dispatch(db_path: Path | str | None, dispatch_id: int) -> dict[str, Any]:
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM dispatch WHERE dispatch_id = ?", (dispatch_id,)
        ).fetchone()
        if row is None:
            raise ServiceError(404, f"派工 {dispatch_id} 不存在")
        return _process_dispatch_dict(conn, row)
    finally:
        conn.close()


def get_process_dispatches(db_path: Path | str | None, process_id: int) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        _get_process(conn, process_id)
        rows = conn.execute(
            "SELECT * FROM dispatch WHERE process_id = ? ORDER BY dispatch_id",
            (process_id,),
        ).fetchall()
        return [_process_dispatch_dict(conn, r) for r in rows]
    finally:
        conn.close()


# ------------------------------------------------------------- reschedule

def _shift_current_dispatches(conn: sqlite3.Connection, shift_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM dispatch WHERE shift_id = ? AND is_current = 1 ORDER BY dispatch_id",
        (shift_id,),
    ).fetchall()


def _dispatch_options(conn: sqlite3.Connection, dispatch_id: int) -> list[tuple[int, int]]:
    return [
        (r["machine_id"], r["fixture_id"])
        for r in conn.execute(
            "SELECT machine_id, fixture_id FROM dispatch_option WHERE dispatch_id = ?"
            " ORDER BY machine_id, fixture_id",
            (dispatch_id,),
        )
    ]


def _demand_from_dispatch(conn: sqlite3.Connection, row: sqlite3.Row) -> _Demand:
    return _Demand(
        process_id=row["process_id"],
        window_start=row["window_start"],
        window_end=row["window_end"],
        duration_minutes=row["duration_minutes"],
        options=_dispatch_options(conn, row["dispatch_id"]),
        dispatch_id=row["dispatch_id"],
        preferred=(row["machine_id"], row["fixture_id"], row["reserved_start"]),
    )


def reschedule_shift(
    db_path: Path | str | None,
    shift_id: int,
    operator: Optional[str] = None,
    downtime: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """登记（可选）停用并重排一个班次。

    * 未开工的预约可以互相挪动；已开工 / 已完工的派工不能改，其原约定时段仍算
      已占（即使撞上刚登记的停用）。
    * 若某道未开工工序与停用冲突，但由于其它资源组合 / 其它时段仍可合法落位，
      则为全部未开工工序寻找一份完整安排。
    * 找得到：在同一事务里一起换成新安排（旧派工 REPLACED，新派工 PLANNED）。
    * 找不到：停用仍然生效，与停用冲突的未开工预约暂停（SUSPENDED），其余原
      预约保持不动。
    * 新安排不能从过去开始。
    """
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                shift = _get_shift(conn, shift_id)

                # 可选：同事务登记停用（与重排原子）
                downtime_row: Optional[dict[str, Any]] = None
                if downtime is not None:
                    rtype = downtime["resource_type"]
                    rid = downtime["resource_id"]
                    if rtype == "MACHINE":
                        _get_machine(conn, rid)
                    elif rtype == "FIXTURE":
                        _get_fixture(conn, rid)
                    else:
                        raise ServiceError(422, "resource_type 只能是 MACHINE 或 FIXTURE")
                    dws = normalize_ts(downtime["window_start"])
                    dwe = normalize_ts(downtime["window_end"])
                    if not dws < dwe:
                        raise ServiceError(422, "停用时段要求 window_start < window_end")
                    cur = conn.execute(
                        """
                        INSERT INTO resource_downtime(resource_type, resource_id,
                            window_start, window_end, operator, note, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (rtype, rid, dws, dwe, operator, downtime.get("note"), now_iso()),
                    )
                    dt_id = cur.lastrowid
                    downtime_row = dict(
                        conn.execute(
                            "SELECT * FROM resource_downtime WHERE downtime_id = ?", (dt_id,)
                        ).fetchone()
                    )

                rows = _shift_current_dispatches(conn, shift_id)
                now_ts = current_ts()
                earliest = _ceil_minute(_parse_dt(now_ts))

                # 可重排的派工：状态为 PLANNED（未开工）或 SUSPENDED（暂停待重排）。
                # 已开工 / 已完工的派工不能改，原约定时段仍算已占。
                movable: list[_Demand] = []
                movable_rows: list[sqlite3.Row] = []
                for r in rows:
                    if r["status"] not in ("PLANNED", "SUSPENDED"):
                        continue
                    movable.append(_demand_from_dispatch(conn, r))
                    movable_rows.append(r)

                # 固定占用：已开工 / 已完工的派工（原约定时段仍算已占，即使撞停用）
                # + 其他班次仍在生效的预约。
                fixed = _existing_fixed_placements(conn, set())
                fixed += _planned_other_shifts(
                    conn, shift_id, {r["dispatch_id"] for r in rows}
                )

                _load_downtime_cache(conn, movable)
                try:
                    schedule = build_schedule(movable, fixed, earliest) if movable else []
                finally:
                    _clear_downtime_cache()

                suspended_ids: list[int] = []
                rescheduled = False
                reason: Optional[str] = None

                if schedule is not None:
                    # 找到覆盖全部未开工工序的安排：在同一事务内一起换新。
                    # “仍合法且没到期”的原预约可原样保留，只把需要挪动的换掉。
                    by_process: dict[int, _Placement] = {
                        p.demand.process_id: p for p in schedule
                    }
                    now = now_iso()
                    for r in movable_rows:
                        old_id = r["dispatch_id"]
                        placement = by_process[r["process_id"]]
                        same = (
                            placement.machine_id == r["machine_id"]
                            and placement.fixture_id == r["fixture_id"]
                            and placement.start == r["reserved_start"]
                            and placement.end == r["reserved_end"]
                        )
                        if same and r["status"] == "PLANNED":
                            # 原预约仍然合法且没到期：保留不动
                            continue
                        # 旧派工失效（被换掉 / 从暂停恢复），不再可执行
                        conn.execute(
                            "UPDATE dispatch SET status = 'REPLACED', is_current = 0,"
                            " updated_at = ? WHERE dispatch_id = ?",
                            (now, old_id),
                        )
                        _insert_dispatch(
                            conn, shift_id, placement, now,
                            options=_dispatch_options(conn, old_id),
                        )
                    conn.execute(
                        "UPDATE shift SET updated_at = ? WHERE shift_id = ?", (now, shift_id)
                    )
                    rescheduled = True
                else:
                    # 排不齐：停用照常生效；只把与停用冲突的未开工预约暂停，其余原
                    # 预约保持不动（不挪动、不改组合）。
                    now = now_iso()
                    for r in movable_rows:
                        if _dispatch_conflicts_downtime(conn, r):
                            conn.execute(
                                "UPDATE dispatch SET status = 'SUSPENDED', is_current = 1,"
                                " updated_at = ? WHERE dispatch_id = ?",
                                (now, r["dispatch_id"]),
                            )
                            suspended_ids.append(r["dispatch_id"])
                    conn.execute(
                        "UPDATE shift SET updated_at = ? WHERE shift_id = ?", (now, shift_id)
                    )
                    reason = (
                        "无法为本班次全部未开工工序找到合法安排；停用已生效，与停用冲突的"
                        "未开工预约已暂停，其余预约保持不动"
                    )
            shift = _get_shift(conn, shift_id)
            return {
                "shift": _shift_dict(conn, shift),
                "rescheduled": rescheduled,
                "suspended_dispatch_ids": suspended_ids,
                "reason": reason,
                "downtime": downtime_row,
            }
        finally:
            conn.close()


def _dispatch_conflicts_downtime(
    conn: sqlite3.Connection, dispatch_row: sqlite3.Row
) -> bool:
    """该派工当前预约是否与它涉及任一资源的停用时段冲突。"""
    s = dispatch_row["reserved_start"]
    e = dispatch_row["reserved_end"]
    if _resource_blocked(conn, "MACHINE", dispatch_row["machine_id"], s, e):
        return True
    if _resource_blocked(conn, "FIXTURE", dispatch_row["fixture_id"], s, e):
        return True
    return False


# ------------------------------------------------------- dispatch execution

def start_dispatch(
    db_path: Path | str | None, dispatch_id: int, operator: Optional[str]
) -> dict[str, Any]:
    """凭当前派工开工。

    必须：派工是当前生效的（is_current=1）、状态为 PLANNED、未暂停、工序未开工；
    当前时刻落在预约时段 [reserved_start, reserved_end) 内；开工仍须满足原有质量
    放行规则；且「机器、夹具」两项当前都不得停用、也不得被其他**实际开工**工序
    占着。成功即开始实际占用，直到真实完工才结束。
    """
    with _service_lock:
        conn = get_connection(db_path)
        try:
            with _immediate_tx(conn):
                row = conn.execute(
                    "SELECT * FROM dispatch WHERE dispatch_id = ?", (dispatch_id,)
                ).fetchone()
                if row is None:
                    raise ServiceError(404, f"派工 {dispatch_id} 不存在")
                if not row["is_current"]:
                    raise ServiceError(
                        409,
                        f"派工 {dispatch_id} 已被换掉（{row['status']}），不能执行",
                    )
                if row["status"] == "SUSPENDED":
                    raise ServiceError(409, f"派工 {dispatch_id} 已暂停，不能执行；请先重排")
                if row["status"] == "REPLACED":
                    raise ServiceError(409, f"派工 {dispatch_id} 已被换掉，不能执行")
                if row["status"] in ("STARTED", "COMPLETED"):
                    raise ServiceError(
                        409, f"派工 {dispatch_id} 状态为 {row['status']}，不能重复开工"
                    )

                process_id = row["process_id"]
                state = _get_state(conn, process_id)
                if state["status"] == "QUARANTINED":
                    raise ServiceError(409, "工序已隔离，须由质量员复核恢复后才能开工")
                if state["status"] in ("STARTED", "COMPLETED"):
                    raise ServiceError(409, f"工序当前状态为 {state['status']}，不能重复开工")

                now = current_ts()
                if not (row["reserved_start"] <= now < row["reserved_end"]):
                    raise ServiceError(
                        409,
                        f"当前时刻 {now} 不在派工预约时段 "
                        f"[{row['reserved_start']}, {row['reserved_end']}) 内，不能开工",
                    )

                # 原有质量放行规则
                issues = _basis_issues(conn, process_id)
                if issues:
                    raise ServiceError(409, "不满足开工条件：" + "；".join(issues))

                # 「当时」两项资源均不得停用，也不得被其他**实际开工**工序占着。
                # 停用与占用都以开工瞬间 now 判定（半开区间）。
                if _resource_blocked_at(conn, "MACHINE", row["machine_id"], now):
                    raise ServiceError(409, "机器在开工时刻处于停用时段，不能开工")
                if _resource_blocked_at(conn, "FIXTURE", row["fixture_id"], now):
                    raise ServiceError(409, "夹具在开工时刻处于停用时段，不能开工")

                if _resource_occupied_at(
                    conn, "MACHINE", row["machine_id"], now, dispatch_id
                ):
                    raise ServiceError(409, "机器仍被其他已开工工序实际占用，不能开工")
                if _resource_occupied_at(
                    conn, "FIXTURE", row["fixture_id"], now, dispatch_id
                ):
                    raise ServiceError(409, "夹具仍被其他已开工工序实际占用，不能开工")

                credential_id = _insert_credential(conn, process_id, "START", operator)
                wall_now = now_iso()
                conn.execute(
                    "UPDATE process_state SET status = 'STARTED', quarantined_at = NULL,"
                    " reason = NULL WHERE process_id = ?",
                    (process_id,),
                )
                conn.execute(
                    "UPDATE dispatch SET status = 'STARTED', actual_start_at = ?, updated_at = ?"
                    " WHERE dispatch_id = ?",
                    (now, wall_now, dispatch_id),
                )
                _record_event(
                    conn, process_id, "START",
                    f"凭派工 {dispatch_id} 开工，credential_id={credential_id}", operator,
                )
            result = get_process_status(db_path, process_id)
            result["dispatch"] = get_dispatch(db_path, dispatch_id)
            return result
        finally:
            conn.close()


def _resource_blocked_at(
    conn: sqlite3.Connection, resource_type: str, resource_id: int, at: str
) -> bool:
    """资源在给定瞬间是否处于停用时段。"""
    for d in _downtime_rows(conn, resource_type, resource_id):
        if d["window_start"] <= at < d["window_end"]:
            return True
    return False


def _resource_occupied_at(
    conn: sqlite3.Connection,
    resource_type: str,
    resource_id: int,
    at: str,
    skip_dispatch_id: int,
) -> bool:
    """资源在给定瞬间是否被其他「实际已开工」的派工占着。

    实际占用 = [actual_start_at, actual_end_at)，actual_end_at 为空表示仍在占用；
    到真实完工才结束，不随预约到期、停机、隔离或复核释放。
    """
    col = "machine_id" if resource_type == "MACHINE" else "fixture_id"
    rows = conn.execute(
        f"SELECT * FROM dispatch WHERE {col} = ? AND status = 'STARTED'"
        f" AND dispatch_id <> ?",
        (resource_id, skip_dispatch_id),
    ).fetchall()
    for r in rows:
        os_ = r["actual_start_at"]
        oe = r["actual_end_at"] or "9999-12-31T23:59:59.999999+00:00"
        if os_ <= at < oe:
            return True
    return False


def list_dispatch_events(db_path: Path | str | None, dispatch_id: int) -> list[dict[str, Any]]:
    """该派工相关的状态事件（来自工序事件链，按派工 id 过滤）。"""
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM dispatch WHERE dispatch_id = ?", (dispatch_id,)
        ).fetchone()
        if row is None:
            raise ServiceError(404, f"派工 {dispatch_id} 不存在")
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM process_event WHERE process_id = ?"
                " AND detail LIKE ? ORDER BY event_id",
                (row["process_id"], f"%派工 {dispatch_id}%"),
            )
        ]
    finally:
        conn.close()
