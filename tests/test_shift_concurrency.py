"""跨进程（两个服务终端）并发：同号开工、同资源开工、重排 vs 开工。

进程内 ``_service_lock`` 只保护单进程；跨进程依赖 ``BEGIN IMMEDIATE`` 写事务。
这些用例用独立进程直接调用服务层，模拟两个终端同时操作同一个 SQLite 库。
"""
import multiprocessing as mp
from datetime import datetime, timezone

from conftest import make_gauge, make_workpiece, post_inspection, proc_id

DAY = "2026-12-01"
S0900 = f"{DAY}T09:00:00+00:00"
S1000 = f"{DAY}T10:00:00+00:00"
S1100 = f"{DAY}T11:00:00+00:00"
S1300 = f"{DAY}T13:00:00+00:00"
S1700 = f"{DAY}T17:00:00+00:00"


def _ctx():
    return mp.get_context("fork")


def _run(target, args_per_proc):
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


def _start_worker(db_path, dispatch_id, now, barrier, queue):
    from app import services

    barrier.wait()
    try:
        with services.override_now(now):
            result = services.start_dispatch(db_path, dispatch_id, "工")
        queue.put({"ok": True, "result": result})
    except services.ServiceError as exc:
        queue.put({"ok": False, "status": exc.status_code, "detail": exc.detail})


def _reschedule_worker(db_path, shift_id, downtime, now, barrier, queue):
    from app import services

    barrier.wait()
    try:
        with services.override_now(now):
            result = services.reschedule_shift(db_path, shift_id, "班长", downtime)
        queue.put({"ok": True, "result": result})
    except services.ServiceError as exc:
        queue.put({"ok": False, "status": exc.status_code, "detail": exc.detail})


# ----------------------------------------------------------------- setup

def _prep(client, n=2):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pids = []
    for code in ["A", "B"][:n]:
        proc = next(p for p in wp["processes"] if p["code"] == code)
        for it in proc["items"]:
            val = (it["lower_limit"] + it["upper_limit"]) // 2
            post_inspection(client, it["item_id"], g["gauge_id"], val)
        pids.append(proc["process_id"])
    m = client.post("/api/machines", json={"code": "M1", "name": "M1"}).json()
    f = client.post("/api/fixtures", json={"code": "F1", "name": "F1"}).json()
    return wp, pids, m, f


def _shift(client, pids, m, f, windows):
    opts = [{"machine_id": m["machine_id"], "fixture_id": f["fixture_id"]}]
    payload = {
        "code": "S-1", "operator": "班长", "window_start": S0900, "window_end": S1700,
        "dispatches": [
            {"process_id": pid, "window_start": ws, "window_end": we,
             "duration_minutes": 60, "options": opts}
            for pid, (ws, we) in zip(pids, windows)
        ],
    }
    r = client.post("/api/shifts", json=payload)
    assert r.status_code == 201, r.text
    return r.json()


# ----------------------------------------------------------------- tests

def test_concurrent_same_dispatch_starts_only_once(client):
    _, pids, m, f = _prep(client, n=1)
    shift = _shift(client, pids, m, f, [(S0900, S1000)])
    did = shift["dispatches"][0]["dispatch_id"]
    results = _run(
        _start_worker,
        [(str(client.db_path), did, "2026-12-01T09:10:00+00:00")] * 2,
    )
    assert sum(1 for r in results if r["ok"]) == 1
    assert sum(1 for r in results if not r["ok"] and r["status"] == 409) == 1
    d = client.get(f"/api/dispatches/{did}").json()
    assert d["status"] == "STARTED"


def test_concurrent_two_dispatches_same_machine_never_double_book_actual(client):
    """两道预约在时间上不重叠（调度保证），但若 A 仍实际占用，B 同时开工不得也成功。"""
    _, pids, m, f = _prep(client, n=2)
    shift = _shift(client, pids, m, f, [(S0900, S1000), (S1000, S1100)])
    byp = {d["process_id"]: d for d in shift["dispatches"]}
    did_a = byp[pids[0]]["dispatch_id"]
    did_b = byp[pids[1]]["dispatch_id"]

    # 先把 A 开起来（实际占用从 09:05 到未知）
    from app import services
    with services.override_now("2026-12-01T09:05:00+00:00"):
        assert client.post(f"/api/dispatches/{did_a}/start", json={}).status_code == 200

    # B 的预约在 10:00；A 仍占用。B 在 10:05 开工必被拒绝
    results = _run(
        _start_worker,
        [
            (str(client.db_path), did_b, "2026-12-01T10:05:00+00:00"),
            (str(client.db_path), did_b, "2026-12-01T10:05:00+00:00"),
        ],
    )
    assert all(not r["ok"] for r in results)
    assert all(r["status"] == 409 for r in results)
    assert client.get(f"/api/dispatches/{did_b}").json()["status"] == "PLANNED"


def test_concurrent_reschedule_and_start_is_consistent(client):
    """重排与开工竞争：串行化后二者不会同时用同一预约造成冲突。"""
    _, pids, m, f = _prep(client, n=2)
    # A 窗口宽（可挪到 09:30 之后），B 固定 10:00-11:00
    shift = _shift(client, pids, m, f, [(S0900, S1300), (S1000, S1100)])
    byp = {d["process_id"]: d for d in shift["dispatches"]}
    did_a = byp[pids[0]]["dispatch_id"]
    assert byp[pids[0]]["reserved_start"] == S0900

    downtime = {
        "resource_type": "MACHINE", "resource_id": m["machine_id"],
        "window_start": S0900, "window_end": "2026-12-01T09:30:00+00:00",
    }
    ctx = _ctx()
    barrier = ctx.Barrier(2)
    queue = ctx.Queue()

    def start_proc():
        _start_worker(str(client.db_path), did_a, "2026-12-01T09:10:00+00:00", barrier, queue)

    def resched_proc():
        _reschedule_worker(str(client.db_path), shift["shift_id"], downtime,
                           "2026-12-01T09:10:00+00:00", barrier, queue)

    procs = [
        ctx.Process(target=start_proc),
        ctx.Process(target=resched_proc),
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(60)
        assert p.exitcode == 0
    results = [queue.get(timeout=10), queue.get(timeout=10)]

    # 无论谁先提交，都不会出现“用已失效预约开工”：
    # * 若开工先提交，A 已成为 STARTED 且实际占用，重排不能挪它；
    # * 若重排先提交，A 的原预约被挪到 09:30 之后，09:10 开工必失败。
    rescheds = [r for r in results if r.get("ok") and "shift" in r.get("result", {})]
    assert len(rescheds) == 1  # 重排本身不应失败
    shift_now = client.get(f"/api/shifts/{shift['shift_id']}").json()
    a_disp = next(d for d in shift_now["dispatches"] if d["process_id"] == pids[0])
    ok_starts = [r for r in results if r.get("ok") and "dispatch" in r.get("result", {})]
    if a_disp["status"] == "STARTED":
        # 开工先提交：实际占用不早于 09:10，且重排不可能把它挪动
        assert len(ok_starts) == 1
        assert a_disp["actual_start_at"] is not None
        assert a_disp["reserved_start"] == S0900
        assert client.get(f"/api/processes/{pids[0]}/status").json()["status"] == "STARTED"
    else:
        # 重排先提交：A 被挪到停用之后，09:10 开工失败
        assert ok_starts == []
        assert a_disp["reserved_start"] >= "2026-12-01T09:30:00+00:00"


def test_reschedule_never_publishes_partial_schedule(client):
    """重排要么整班换成新安排，要么只暂停冲突项；不会留下半个班次。"""
    _, pids, m, f = _prep(client, n=2)
    shift = _shift(client, pids, m, f, [(S0900, S1100), (S1100, S1300)])
    before = {d["process_id"]: d for d in shift["dispatches"]}

    downtime = {
        "resource_type": "MACHINE", "resource_id": m["machine_id"],
        "window_start": S0900, "window_end": S1100,
    }
    results = _run(
        _reschedule_worker,
        [
            (str(client.db_path), shift["shift_id"], downtime, "2026-12-01T08:00:00+00:00"),
            (str(client.db_path), shift["shift_id"], downtime, "2026-12-01T08:00:00+00:00"),
        ],
    )
    # 两个并发重排都成功（同一个停用被登记两次也无妨，都是合法资源事实）
    after = client.get(f"/api/shifts/{shift['shift_id']}").json()
    current = [d for d in after["dispatches"] if d["is_current"]]
    assert len(current) == 2
    # 每道工序恰好一个当前派工
    assert {d["process_id"] for d in current} == set(pids)
    # 当前安排之间资源不重叠
    intervals = sorted((d["reserved_start"], d["reserved_end"]) for d in current)
    for (s1, e1), (s2, e2) in zip(intervals, intervals[1:]):
        assert e1 <= s2


def _inspect_worker(db_path, item_id_, gauge_id, reading, barrier, queue):
    from app import services

    barrier.wait()
    try:
        services.register_inspection(db_path, item_id_, gauge_id, reading,
                                     "2026-12-01T09:10:00+00:00")
        queue.put({"ok": True})
    except services.ServiceError as exc:
        queue.put({"ok": False, "status": exc.status_code, "detail": exc.detail})


def test_concurrent_quality_change_and_dispatch_start(client):
    """同时登记失准读数与凭派工开工：串行化后绝不在应隔离的工序上放行。"""
    from conftest import item_id, make_gauge, make_workpiece, post_inspection

    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = next(p["process_id"] for p in wp["processes"] if p["code"] == "A")
    il = item_id(wp, "A", "A-L")
    post_inspection(client, il, g["gauge_id"], 11, "2026-12-01T08:00:00+00:00")
    post_inspection(client, item_id(wp, "A", "A-W"), g["gauge_id"], 5, "2026-12-01T08:00:00+00:00")
    m = client.post("/api/machines", json={"code": "M1", "name": "M1"}).json()
    f = client.post("/api/fixtures", json={"code": "F1", "name": "F1"}).json()
    shift = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": "2026-12-01T09:00:00+00:00",
        "window_end": "2026-12-01T17:00:00+00:00",
        "dispatches": [{
            "process_id": pa, "window_start": "2026-12-01T09:00:00+00:00",
            "window_end": "2026-12-01T12:00:00+00:00", "duration_minutes": 60,
            "options": [{"machine_id": m["machine_id"], "fixture_id": f["fixture_id"]}],
        }],
    }).json()
    did = shift["dispatches"][0]["dispatch_id"]

    ctx = _ctx()
    barrier = ctx.Barrier(2)
    queue = ctx.Queue()

    def start_proc():
        _start_worker(str(client.db_path), did, "2026-12-01T09:10:00+00:00", barrier, queue)

    def inspect_proc():
        _inspect_worker(str(client.db_path), il, g["gauge_id"], 99, barrier, queue)

    procs = [ctx.Process(target=start_proc), ctx.Process(target=inspect_proc)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(60)
        assert p.exitcode == 0
    results = [queue.get(timeout=10), queue.get(timeout=10)]

    s = client.get(f"/api/processes/{pa}/status").json()
    start_ok = any(r.get("ok") and "dispatch" in r.get("result", {}) for r in results)
    # 只可能是：开工先成功后被隔离；或隔离先则开工 409。都不会是“失准却仍可继续放行”
    if start_ok:
        assert s["status"] == "QUARANTINED"
        assert client.get(f"/api/dispatches/{did}").json()["status"] == "STARTED"
    else:
        assert s["status"] == "NOT_STARTED"
        assert client.get(f"/api/dispatches/{did}").json()["status"] == "PLANNED"
    assert s["status"] != "STARTED"
