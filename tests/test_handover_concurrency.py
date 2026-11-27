"""两个终端（独立进程）共用同一个 SQLite 库的并发与重启行为。

进程内 ``_service_lock`` 只保护单进程；跨进程依赖 ``BEGIN IMMEDIATE`` 写事务 +
SQLite 唯一约束 / 容量触发器。这些用例用独立进程直接调用服务层，模拟两个终端。
"""
import multiprocessing as mp

import pytest

from conftest import make_gauge, make_workpiece, post_inspection, item_id


def _ctx():
    return mp.get_context("fork")


# ----------------------------------------------------------------- workers

def _ship_worker(db_path, order_id, operator, request_id, barrier, queue):
    from app.services import ServiceError, ship_transfer_order

    barrier.wait()
    try:
        queue.put({"ok": True, "result": ship_transfer_order(db_path, order_id, operator, request_id)})
    except ServiceError as exc:
        queue.put({"ok": False, "status": exc.status_code, "detail": exc.detail})


def _create_order_worker(db_path, src, dst, piece_ids, barrier, queue):
    from app.services import ServiceError, create_transfer_order

    barrier.wait()
    try:
        queue.put({"ok": True, "result": create_transfer_order(db_path, src, dst, piece_ids)})
    except ServiceError as exc:
        queue.put({"ok": False, "status": exc.status_code, "detail": exc.detail})


def _receive_worker(db_path, order_id, piece_ids, operator, request_id, barrier, queue):
    from app.services import ServiceError, receive_transfer_order

    barrier.wait()
    try:
        queue.put({"ok": True, "result": receive_transfer_order(
            db_path, order_id, piece_ids, operator, request_id)})
    except ServiceError as exc:
        queue.put({"ok": False, "status": exc.status_code, "detail": exc.detail})


def _ship_vs_quarantine_workers(db_path, order_id, item_id_, gauge_id, barrier, queue):
    """同时发运 vs 登记超差读数（使关口失效）。"""
    from app.services import ServiceError, register_inspection, ship_transfer_order

    barrier.wait()
    try:
        queue.put(("ship", {"ok": True, "result": ship_transfer_order(db_path, order_id, "A", "S-RACE")}))
    except ServiceError as exc:
        queue.put(("ship", {"ok": False, "status": exc.status_code, "detail": exc.detail}))
    try:
        register_inspection(db_path, item_id_, gauge_id, 99, "2026-11-27T20:00:00+00:00")
        queue.put(("inspect", {"ok": True}))
    except ServiceError as exc:  # pragma: no cover
        queue.put(("inspect", {"ok": False, "status": exc.status_code, "detail": exc.detail}))


def _run_pair(target, args_per_proc):
    ctx = _ctx()
    barrier = ctx.Barrier(len(args_per_proc))
    queue = ctx.Queue()
    procs = [ctx.Process(target=target, args=(*args, barrier, queue)) for args in args_per_proc]
    for p in procs:
        p.start()
    for p in procs:
        p.join(60)
        assert p.exitcode == 0
    return [queue.get(timeout=10) for _ in args_per_proc]


# ----------------------------------------------------------------- helpers

def _complete(client, wp, g, code):
    p = next(x for x in wp["processes"] if x["code"] == code)
    for it in p["items"]:
        post_inspection(client, it["item_id"], g["gauge_id"], (it["lower_limit"] + it["upper_limit"]) // 2)
    pid = p["process_id"]
    assert client.post(f"/api/processes/{pid}/start", json={}).status_code == 200
    assert client.post(f"/api/processes/{pid}/complete", json={}).status_code == 200
    return pid


def _ws(client, code, capacity):
    r = client.post("/api/workstations", json={"code": code, "name": code, "capacity": capacity})
    assert r.status_code == 201, r.text
    return r.json()


def _piece(client, wp, gate, ws):
    r = client.post(
        "/api/pieces",
        json={"workpiece_id": wp["workpiece_id"], "gate_process_id": gate, "workstation_id": ws["workstation_id"]},
    )
    assert r.status_code == 201, r.text
    return r.json()


def _order(client, src, dst, pieces):
    r = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": dst["workstation_id"],
            "piece_ids": [p["piece_id"] for p in pieces],
        },
    )
    assert r.status_code == 201, r.text
    return r.json()


# ----------------------------------------------------------------- tests

def test_concurrent_same_request_ships_once(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    src, dst = _ws(client, "CS", 5), _ws(client, "CD", 5)
    p = _piece(client, wp, _complete(client, wp, g, "A"), src)
    oid = _order(client, src, dst, [p])["order_id"]

    results = _run_pair(
        _ship_worker,
        [(str(client.db_path), oid, "张三", "REQ-S")] * 2,
    )
    # 同号同内容重发返回首次成功结果
    assert all(r["ok"] for r in results)
    assert sum(1 for r in results if r["result"]["replayed"] is False) == 1
    assert sum(1 for r in results if r["result"]["replayed"] is True) == 1

    conn_detail = client.get(f"/api/transfer-orders/{oid}").json()
    assert conn_detail["status"] == "SHIPPED"
    s = client.get(f"/api/workstations/{src['workstation_id']}").json()
    d = client.get(f"/api/workstations/{dst['workstation_id']}").json()
    # 只发生一次发运：发件方恰有 1 个退回保留位，收件方恰 1 个收件预留位
    assert s["reserved_pieces"] == [p["piece_id"]] and s["available"] == 4
    assert d["reserved_pieces"] == [p["piece_id"]] and d["available"] == 4


def test_concurrent_different_requests_only_one_ships(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    src, dst = _ws(client, "DS", 5), _ws(client, "DD", 5)
    p = _piece(client, wp, _complete(client, wp, g, "A"), src)
    oid = _order(client, src, dst, [p])["order_id"]

    results = _run_pair(
        _ship_worker,
        [(str(client.db_path), oid, "张三", "REQ-A"), (str(client.db_path), oid, "张三", "REQ-B")],
    )
    assert sum(1 for r in results if r["ok"]) == 1
    assert sum(1 for r in results if not r["ok"] and r["status"] == 409) == 1
    assert client.get(f"/api/transfer-orders/{oid}").json()["status"] == "SHIPPED"


def test_concurrent_order_creation_cannot_overbook_capacity(client):
    src = _ws(client, "OS", 5)
    tiny = _ws(client, "OT", 1)
    pieces = []
    for i in range(2):
        wp = make_workpiece(client, code=f"CAP-{i}")
        g = make_gauge(client, code=f"CAPG-{i}")
        pieces.append(_piece(client, wp, _complete(client, wp, g, "A"), src))

    results = _run_pair(
        _create_order_worker,
        [
            (str(client.db_path), src["workstation_id"], tiny["workstation_id"], [pieces[0]["piece_id"]]),
            (str(client.db_path), src["workstation_id"], tiny["workstation_id"], [pieces[1]["piece_id"]]),
        ],
    )
    assert sum(1 for r in results if r["ok"]) == 1
    assert sum(1 for r in results if not r["ok"] and r["status"] == 409) == 1
    t = client.get(f"/api/workstations/{tiny['workstation_id']}").json()
    assert t["reserved_count"] == 1 and t["available"] == 0


def test_concurrent_receive_lands_once(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    src, dst = _ws(client, "RS", 5), _ws(client, "RD", 5)
    p = _piece(client, wp, _complete(client, wp, g, "A"), src)
    oid = _order(client, src, dst, [p])["order_id"]
    assert client.post(f"/api/transfer-orders/{oid}/ship", json={"operator": "A", "request_id": "S"}).status_code == 200

    results = _run_pair(
        _receive_worker,
        [
            (str(client.db_path), oid, [p["piece_id"]], "李四", "R-A"),
            (str(client.db_path), oid, [p["piece_id"]], "李四", "R-B"),
        ],
    )
    assert sum(1 for r in results if r["ok"]) == 1
    assert sum(1 for r in results if not r["ok"] and r["status"] == 409) == 1
    d = client.get(f"/api/workstations/{dst['workstation_id']}").json()
    assert d["actual_pieces"] == [p["piece_id"]] and d["reserved_pieces"] == []
    records = client.get(f"/api/transfer-orders/{oid}").json()["records"]
    assert len(records) == 1


def test_ship_never_uses_already_invalid_gate(client):
    """并发发运 vs 关口失效：串行化后不允许用已失效的完工状态通过发运。

    若发运成功，其 shipped_at 必须早于失准读数造成的隔离时刻；若隔离先提交，
    发运必须 409 且单保持待发。
    """
    for i in range(3):
        wp = make_workpiece(client, code=f"RACE-{i}")
        g = make_gauge(client, code=f"RACEG-{i}")
        src, dst = _ws(client, f"RAS-{i}", 5), _ws(client, f"RAD-{i}", 5)
        gate = _complete(client, wp, g, "A")
        p = _piece(client, wp, gate, src)
        oid = _order(client, src, dst, [p])["order_id"]

        results = _run_pair(
            _ship_vs_quarantine_workers,
            [(str(client.db_path), oid, item_id(wp, "A", "A-L"), g["gauge_id"])],
        )
        detail = client.get(f"/api/transfer-orders/{oid}").json()
        proc = client.get(f"/api/processes/{gate}/status").json()
        assert proc["status"] == "QUARANTINED"  # 读数确实造成了失效

        ship_ok = any(kind == "ship" and r["ok"] for kind, r in results)
        if ship_ok:
            # 发运先提交：发运时刻早于隔离发生时刻
            q = client.get(f"/api/processes/{gate}/quarantines").json()[-1]
            assert detail["shipped_at"] < q["opened_at"]
        else:
            assert detail["status"] == "PENDING"
            # 关口失效后再次发运必然 409
            r = client.post(f"/api/transfer-orders/{oid}/ship", json={"operator": "A", "request_id": "S2"})
            assert r.status_code == 409


def test_state_survives_restart_and_continues_original_order(client):
    """每一步都在新进程里完成：重启后按原单继续处理，不重置也不重复。"""
    wp = make_workpiece(client)
    g = make_gauge(client)
    src, dst = _ws(client, "PS", 5), _ws(client, "PD", 5)
    p = _piece(client, wp, _complete(client, wp, g, "A"), src)
    oid = _order(client, src, dst, [p])["order_id"]

    # 新进程发运
    r = _run_pair(_ship_worker, [(str(client.db_path), oid, "A", "S1")])[0]
    # barrier size 1 is valid
    assert r["ok"] is True

    # 另一个新进程签收，并重放同一请求号
    ctx = _ctx()
    q = ctx.Queue()
    barrier = ctx.Barrier(1)

    def _worker(db_path, queue, barrier):
        _receive_worker(db_path, oid, [p["piece_id"]], "B", "C1", barrier, queue)
        _receive_worker(db_path, oid, [p["piece_id"]], "B", "C1", barrier, queue)

    proc = ctx.Process(target=_worker, args=(str(client.db_path), q, barrier))
    proc.start()
    proc.join(60)
    assert proc.exitcode == 0
    res1 = q.get(timeout=10)
    res2 = q.get(timeout=10)
    assert res1["ok"] and res2["ok"]
    assert res1["result"]["replayed"] is False
    assert res2["result"]["replayed"] is True

    detail = client.get(f"/api/transfer-orders/{oid}").json()
    assert detail["status"] == "COMPLETED"
    assert len(detail["records"]) == 1
