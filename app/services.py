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

import sqlite3
import threading
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .database import get_connection, init_db

_service_lock = threading.RLock()


class ServiceError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


# ---------------------------------------------------------------- time helpers

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
            with conn:
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
            with conn:
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
            with conn:
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

            with conn:
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


def start_process(
    db_path: Path | str | None, process_id: int, operator: Optional[str]
) -> dict[str, Any]:
    with _service_lock:
        conn = get_connection(db_path)
        try:
            proc = _get_process(conn, process_id)
            state = _get_state(conn, process_id)
            if state["status"] == "QUARANTINED":
                raise ServiceError(409, "工序已隔离，须由质量员复核恢复后才能开工")
            if state["status"] in ("STARTED", "COMPLETED"):
                raise ServiceError(409, f"工序当前状态为 {state['status']}，不能重复开工")

            issues = _basis_issues(conn, process_id)
            if issues:
                raise ServiceError(409, "不满足开工条件：" + "；".join(issues))

            with conn:
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
            state = _get_state(conn, process_id)
            if state["status"] == "QUARANTINED":
                raise ServiceError(409, "工序已隔离，不能完工；须先复核恢复为开工状态")
            if state["status"] == "NOT_STARTED":
                raise ServiceError(409, "工序尚未开工，不能完工")
            if state["status"] == "COMPLETED":
                raise ServiceError(409, "工序已完工，不能重复完工")

            # 完工前防御性复核同一套规则（锁内无并发变更，正常应为空）
            issues = _basis_issues(conn, process_id)
            if issues:
                raise ServiceError(409, "放行依据已失效，不能完工：" + "；".join(issues))

            active = _active_credential(conn, process_id)
            with conn:
                credential_id = _insert_credential(
                    conn, process_id, "COMPLETE", operator, basis_id=active["credential_id"]
                )
                conn.execute(
                    "UPDATE process_state SET status = 'COMPLETED' WHERE process_id = ?",
                    (process_id,),
                )
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

            with conn:
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
