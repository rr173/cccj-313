"""班次派工：发布、资源窗口、停机重排、开工 / 完工与派工绑定。"""
import pytest

from conftest import item_id, make_gauge, make_workpiece, post_inspection, proc_id, status

# 统一的未来班次时段（与测试机的“现在”无关，测试用 override_now 控制动作时刻）
DAY = "2026-12-01"
S0900 = f"{DAY}T09:00:00+00:00"
S0930 = f"{DAY}T09:30:00+00:00"
S1000 = f"{DAY}T10:00:00+00:00"
S1100 = f"{DAY}T11:00:00+00:00"
S1200 = f"{DAY}T12:00:00+00:00"
S1300 = f"{DAY}T13:00:00+00:00"
S1700 = f"{DAY}T17:00:00+00:00"


def _machine(client, code):
    r = client.post("/api/machines", json={"code": code, "name": code})
    assert r.status_code == 201, r.text
    return r.json()


def _fixture(client, code):
    r = client.post("/api/fixtures", json={"code": code, "name": code})
    assert r.status_code == 201, r.text
    return r.json()


def _inspected(client, wp, g, code, reading=None):
    """给某工序的全部项目登记合格读数。"""
    proc = next(p for p in wp["processes"] if p["code"] == code)
    for it in proc["items"]:
        val = reading if reading is not None else (it["lower_limit"] + it["upper_limit"]) // 2
        assert post_inspection(client, it["item_id"], g["gauge_id"], val).status_code == 201
    return proc["process_id"]


def _disp(process_id, options, ws=S0900, we=S1200, minutes=60):
    return {
        "process_id": process_id,
        "window_start": ws,
        "window_end": we,
        "duration_minutes": minutes,
        "options": options,
    }


def _opts(*pairs):
    return [{"machine_id": m, "fixture_id": f} for m, f in pairs]


# --------------------------------------------------------------- publication

def test_shift_publishes_complete_schedule_and_reserves_both_resources(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    pb = _inspected(client, wp, g, "B")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")

    r = client.post("/api/shifts", json={
        "code": "S-1", "operator": "班长",
        "window_start": S0900, "window_end": S1700,
        "dispatches": [
            _disp(pa, _opts((m["machine_id"], f["fixture_id"])), ws=S0900, we=S1200),
            _disp(pb, _opts((m["machine_id"], f["fixture_id"])), ws=S0900, we=S1200),
        ],
    })
    assert r.status_code == 201, r.text
    shift = r.json()
    assert len(shift["dispatches"]) == 2
    # 两项资源同一时刻只能服务一道工序：两个预约不重叠
    a, b = shift["dispatches"]
    assert a["reserved_end"] <= b["reserved_start"] or b["reserved_end"] <= a["reserved_start"]
    for d in shift["dispatches"]:
        # 预约从整分钟开始、左闭右开、落在加工窗口内
        assert d["reserved_start"].endswith(":00+00:00")
        assert d["window_start"] <= d["reserved_start"] < d["reserved_end"] <= d["window_end"]

    # 已编入班次的工序不能再从原开工入口开工
    s = status(client, pa)
    assert s["dispatch_required"] is True
    assert s["current_dispatch_id"] == a["dispatch_id"]
    assert s["can_start"] is False
    assert client.post(f"/api/processes/{pa}/start", json={}).status_code == 409


def test_unschedulable_shift_is_not_published_at_all(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    pb = _inspected(client, wp, g, "B")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")

    # 两个工序都要在 09:00-10:00 用同一对唯一资源，各 60 分钟 —— 无法共存
    r = client.post("/api/shifts", json={
        "code": "S-BAD", "operator": "班长",
        "window_start": S0900, "window_end": S1700,
        "dispatches": [
            _disp(pa, _opts((m["machine_id"], f["fixture_id"])), ws=S0900, we=S1000),
            _disp(pb, _opts((m["machine_id"], f["fixture_id"])), ws=S0900, we=S1000),
        ],
    })
    assert r.status_code == 409
    # 半份排班都不应发布
    assert client.get("/api/shifts").json() == []
    # 两个工序都仍未纳入派工，仍可从原入口开工
    assert status(client, pa)["dispatch_required"] is False
    assert status(client, pb)["dispatch_required"] is False


def test_alternative_machine_fixture_combination_is_used(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    pb = _inspected(client, wp, g, "B")
    m1, m2 = _machine(client, "M1"), _machine(client, "M2")
    f1, f2 = _fixture(client, "F1"), _fixture(client, "F2")

    # A 独占 M1+F1；B 同时段只能用 M2+F2 的组合
    r = client.post("/api/shifts", json={
        "code": "S-ALT", "operator": "班长",
        "window_start": S0900, "window_end": S1700,
        "dispatches": [
            _disp(pa, _opts((m1["machine_id"], f1["fixture_id"])), ws=S0900, we=S1000),
            _disp(pb, _opts((m1["machine_id"], f2["fixture_id"]),
                            (m2["machine_id"], f2["fixture_id"])), ws=S0900, we=S1000),
        ],
    })
    assert r.status_code == 201, r.text
    by_proc = {d["process_id"]: d for d in r.json()["dispatches"]}
    assert by_proc[pa]["machine_id"] == m1["machine_id"]
    assert by_proc[pb]["machine_id"] == m2["machine_id"]
    assert by_proc[pb]["fixture_id"] == f2["fixture_id"]


def test_one_process_cannot_be_in_two_shifts(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    opts = _opts((m["machine_id"], f["fixture_id"]))
    assert client.post("/api/shifts", json={
        "code": "S-A", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pa, opts)],
    }).status_code == 201
    r = client.post("/api/shifts", json={
        "code": "S-B", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pa, opts)],
    })
    assert r.status_code == 409


def test_dispatched_process_requires_unstarted_status(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    # 先按原方式开工 A
    assert client.post(f"/api/processes/{pa}/start", json={}).status_code == 200
    r = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pa, _opts((m["machine_id"], f["fixture_id"])))],
    })
    assert r.status_code == 409


# ------------------------------------------------------------- downtime & replan

def _publish_two(client, ws1, we1, ws2, we2):
    """建两道已检验工序、一台机器一副夹具，发布一个含两个预约的班次。"""
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    pb = _inspected(client, wp, g, "B")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    opts = _opts((m["machine_id"], f["fixture_id"]))
    r = client.post("/api/shifts", json={
        "code": "S-1", "operator": "班长",
        "window_start": S0900, "window_end": S1700,
        "dispatches": [
            _disp(pa, opts, ws=ws1, we=we1),
            _disp(pb, opts, ws=ws2, we=we2),
        ],
    })
    assert r.status_code == 201, r.text
    return wp, g, pa, pb, m, f, r.json()


def test_downtime_triggers_replan_and_moves_reservations(client):
    # A 窗口 09-12，B 窗口 12-15；先正常发布，A 在 09:00
    wp, g, pa, pb, m, f, shift = _publish_two(
        client, S0900, S1200, S1200, "2026-12-01T15:00:00+00:00"
    )
    byp = {d["process_id"]: d for d in shift["dispatches"]}
    first_start = byp[pa]["reserved_start"]

    # 登记机器 09:00-10:00 停用并要求重排：A 须挪到 10:00 之后，仍能全部排下
    r = client.post(f"/api/shifts/{shift['shift_id']}/reschedule", json={
        "operator": "班长",
        "resource_type": "MACHINE", "resource_id": m["machine_id"],
        "window_start": S0900, "window_end": S1000,
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["rescheduled"] is True
    assert body["suspended_dispatch_ids"] == []
    new = {d["process_id"]: d for d in body["shift"]["dispatches"]}
    # A 已挪开停用时段
    assert new[pa]["reserved_start"] >= S1000
    assert new[pa]["status"] == "PLANNED"
    # 换掉的旧派工不再生效
    old = client.get(f"/api/dispatches/{byp[pa]['dispatch_id']}").json()
    assert old["is_current"] is False and old["status"] == "REPLACED"


def test_downtime_that_makes_full_schedule_impossible_suspends_conflicts(client):
    # A、B 都只能在 09-13 用唯一资源；A 09-11、B 11-13 各 120 分钟
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    pb = _inspected(client, wp, g, "B")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    opts = _opts((m["machine_id"], f["fixture_id"]))
    r = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [
            _disp(pa, opts, ws=S0900, we=S1100, minutes=120),
            _disp(pb, opts, ws=S1100, we=S1300, minutes=120),
        ],
    })
    assert r.status_code == 201, r.text
    shift = r.json()
    byp = {d["process_id"]: d for d in shift["dispatches"]}
    assert {byp[pa]["reserved_start"], byp[pb]["reserved_start"]} == {S0900, S1100}

    # 机器 09:00-11:00 停用：A 与停用冲突；只剩 11-13 两小时无法同时容纳 A、B
    r = client.post(f"/api/shifts/{shift['shift_id']}/reschedule", json={
        "operator": "x",
        "resource_type": "MACHINE", "resource_id": m["machine_id"],
        "window_start": S0900, "window_end": S1100,
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["rescheduled"] is False
    ids = set(body["suspended_dispatch_ids"])
    assert ids == {byp[pa]["dispatch_id"]}
    new = {d["process_id"]: d for d in body["shift"]["dispatches"]}
    assert new[pa]["suspended"] is True
    # 其余原预约保持不动
    assert new[pb]["reserved_start"] == S1100
    assert new[pb]["status"] == "PLANNED"
    assert client.get(f"/api/processes/{pa}/status").json()["dispatch_suspended"] is True
    # 停用已生效
    dts = client.get("/api/resource-downtimes").json()
    assert any(d["resource_id"] == m["machine_id"] for d in dts)


def test_suspended_item_can_be_rescheduled_later(client):
    """暂停项留在本班；班组长再次重排（新增一块可容纳的时段）后可恢复。"""
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    opts = _opts((m["machine_id"], f["fixture_id"]))
    # 加工窗口 09-11、60 分钟；初始排在 09-10
    shift = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pa, opts, ws=S0900, we=S1100, minutes=60)],
    }).json()
    did = shift["dispatches"][0]["dispatch_id"]
    assert shift["dispatches"][0]["reserved_start"] == S0900

    # 机器 09-11 全部停用：整个加工窗口被停用覆盖，无法落位 → 暂停
    r = client.post(f"/api/shifts/{shift['shift_id']}/reschedule", json={
        "operator": "x",
        "resource_type": "MACHINE", "resource_id": m["machine_id"],
        "window_start": S0900, "window_end": S1100,
    }).json()
    assert r["rescheduled"] is False
    assert r["suspended_dispatch_ids"] == [did]
    assert client.get(f"/api/dispatches/{did}").json()["suspended"] is True

    # 再重排仍然暂停（窗口仍被覆盖）
    r = client.post(f"/api/shifts/{shift['shift_id']}/reschedule", json={"operator": "x"}).json()
    assert r["rescheduled"] is False
    assert client.get(f"/api/dispatches/{did}").json()["suspended"] is True


def test_started_dispatch_is_immutable_and_still_occupies_its_slot(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    pb = _inspected(client, wp, g, "B")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    opts = _opts((m["machine_id"], f["fixture_id"]))
    shift = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [
            _disp(pa, opts, ws=S0900, we=S1000, minutes=60),
            _disp(pb, opts, ws=S0900, we=S1200, minutes=60),
        ],
    }).json()
    byp = {d["process_id"]: d for d in shift["dispatches"]}

    from app import services
    with services.override_now("2026-12-01T09:10:00+00:00"):
        r = client.post(f"/api/dispatches/{byp[pa]['dispatch_id']}/start", json={"operator": "工"})
        assert r.status_code == 200, r.text
        # 已开工派工的状态 / 预约不可改
        assert byp[pa]["reserved_start"] == S0900

        # 现在登记 09:00-09:30 机器停用：A 已开工不能挪，但其原时段仍算已占
        rr = client.post(f"/api/shifts/{shift['shift_id']}/reschedule", json={
            "operator": "x",
            "resource_type": "MACHINE", "resource_id": m["machine_id"],
            "window_start": S0900, "window_end": "2026-12-01T09:30:00+00:00",
        })
    assert rr.status_code == 200, rr.text
    body = rr.json()
    a = client.get(f"/api/dispatches/{byp[pa]['dispatch_id']}").json()
    assert a["status"] == "STARTED" and a["is_current"] is True
    assert a["reserved_start"] == S0900  # 不可被挪动


def test_start_dispatch_outside_reservation_is_rejected(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    shift = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pa, _opts((m["machine_id"], f["fixture_id"])),
                            ws=S0900, we=S1000, minutes=60)],
    }).json()
    did = shift["dispatches"][0]["dispatch_id"]
    from app import services
    with services.override_now("2026-12-01T10:30:00+00:00"):
        r = client.post(f"/api/dispatches/{did}/start", json={})
    assert r.status_code == 409


def test_replaced_dispatch_cannot_start_and_old_reservation_is_gone(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    shift = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pa, _opts((m["machine_id"], f["fixture_id"])),
                            ws=S0900, we=S1200, minutes=60)],
    }).json()
    old_id = shift["dispatches"][0]["dispatch_id"]
    # 登记 09-10 停用：原预约在 09:00，必须挪到 10:00，旧派工被换掉
    r = client.post(f"/api/shifts/{shift['shift_id']}/reschedule", json={
        "operator": "x",
        "resource_type": "MACHINE", "resource_id": m["machine_id"],
        "window_start": S0900, "window_end": S1000,
    })
    assert r.status_code == 200 and r.json()["rescheduled"] is True
    old = client.get(f"/api/dispatches/{old_id}").json()
    assert old["is_current"] is False and old["status"] == "REPLACED"
    from app import services
    with services.override_now("2026-12-01T10:10:00+00:00"):
        rr = client.post(f"/api/dispatches/{old_id}/start", json={})
    assert rr.status_code == 409


def test_start_requires_quality_release_even_with_dispatch(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    # 不给 A 登记任何检验
    pa = proc_id(wp, "A")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    shift = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pa, _opts((m["machine_id"], f["fixture_id"])),
                            ws=S0900, we=S1200, minutes=60)],
    }).json()
    did = shift["dispatches"][0]["dispatch_id"]
    from app import services
    with services.override_now("2026-12-01T09:10:00+00:00"):
        r = client.post(f"/api/dispatches/{did}/start", json={})
    assert r.status_code == 409
    assert "检验" in r.json()["detail"]


def test_resource_downtime_blocks_start_even_within_window(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    pb = _inspected(client, wp, g, "B")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    opts = _opts((m["machine_id"], f["fixture_id"]))
    shift = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [
            _disp(pa, opts, ws=S0900, we=S1000, minutes=60),
            _disp(pb, opts, ws=S1000, we=S1100, minutes=60),
        ],
    }).json()
    byp = {d["process_id"]: d for d in shift["dispatches"]}
    # 只登记停用（不重排），B 的预约与停用冲突但还没被暂停
    client.post("/api/resource-downtimes", json={
        "resource_type": "FIXTURE", "resource_id": f["fixture_id"],
        "window_start": S1000, "window_end": S1100,
    })
    from app import services
    with services.override_now("2026-12-01T10:10:00+00:00"):
        r = client.post(f"/api/dispatches/{byp[pb]['dispatch_id']}/start", json={})
    assert r.status_code == 409


# ------------------------------------------------------------ occupancy semantics

def test_later_reservation_cannot_start_until_actual_resources_freed(client):
    """后一道预约排在前一道预计结束之后，但不实际完工就不能开工。"""
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    pb = _inspected(client, wp, g, "B")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    opts = _opts((m["machine_id"], f["fixture_id"]))
    # A 09-10、B 10-12；两者都 30 分钟。A 实际到 09:45 还没完工，B 到 10:00
    # 预约起点理论上可用，但 A 仍占着机器 → B 不能开工。
    shift = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [
            _disp(pa, opts, ws=S0900, we=S1000, minutes=30),
            _disp(pb, opts, ws=S1000, we=S1200, minutes=30),
        ],
    }).json()
    byp = {d["process_id"]: d for d in shift["dispatches"]}
    from app import services
    # A 09:05 开工，预计 09:35 结束但仍未完工
    with services.override_now("2026-12-01T09:05:00+00:00"):
        assert client.post(f"/api/dispatches/{byp[pa]['dispatch_id']}/start",
                           json={}).status_code == 200
    # B 预约在 10:00，但 A 仍实际占用 → B 在 10:05 不能开工
    with services.override_now("2026-12-01T10:05:00+00:00"):
        r = client.post(f"/api/dispatches/{byp[pb]['dispatch_id']}/start", json={})
    assert r.status_code == 409
    # A 真实完工后释放资源
    with services.override_now("2026-12-01T10:10:00+00:00"):
        assert client.post(f"/api/processes/{pa}/complete", json={}).status_code == 200
        # B 的预约起点 10:00，10:10 仍在 [10:00, 10:30) 内 → 现在可以开工
        r = client.post(f"/api/dispatches/{byp[pb]['dispatch_id']}/start", json={})
    assert r.status_code == 200, r.text


def test_actual_occupancy_not_released_by_downtime_or_quarantine(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    shift = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pa, _opts((m["machine_id"], f["fixture_id"])),
                            ws=S0900, we=S1200, minutes=60)],
    }).json()
    did = shift["dispatches"][0]["dispatch_id"]
    from app import services
    with services.override_now("2026-12-01T09:10:00+00:00"):
        assert client.post(f"/api/dispatches/{did}/start", json={}).status_code == 200
    # 事后登记停用、质量隔离：实际占用不释放
    client.post("/api/resource-downtimes", json={
        "resource_type": "MACHINE", "resource_id": m["machine_id"],
        "window_start": S0900, "window_end": S1000,
    })
    d = client.get(f"/api/dispatches/{did}").json()
    assert d["status"] == "STARTED" and d["actual_end_at"] is None


def test_normal_complete_closes_actual_occupancy(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    shift = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pa, _opts((m["machine_id"], f["fixture_id"])),
                            ws=S0900, we=S1200, minutes=60)],
    }).json()
    did = shift["dispatches"][0]["dispatch_id"]
    from app import services
    with services.override_now("2026-12-01T09:10:00+00:00"):
        assert client.post(f"/api/dispatches/{did}/start", json={}).status_code == 200
    with services.override_now("2026-12-01T09:50:00+00:00"):
        r = client.post(f"/api/processes/{pa}/complete", json={})
        assert r.status_code == 200
    d = client.get(f"/api/dispatches/{did}").json()
    assert d["status"] == "COMPLETED"
    assert d["actual_end_at"] is not None
    assert d["completed"] is True


def test_undispatched_process_still_follows_original_flow(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    # 从不建班次：A 按原方式可开工完工
    assert client.post(f"/api/processes/{pa}/start", json={}).status_code == 200
    assert client.post(f"/api/processes/{pa}/complete", json={}).status_code == 200


def test_quarantine_and_review_do_not_release_actual_occupancy(client):
    """隔离 / 复核不释放实际占用；复核恢复为 STARTED 后再完工才结束占用。"""
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    il = item_id(wp, "A", "A-L")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    shift = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pa, _opts((m["machine_id"], f["fixture_id"])),
                            ws=S0900, we=S1200, minutes=60)],
    }).json()
    did = shift["dispatches"][0]["dispatch_id"]
    from app import services
    with services.override_now("2026-12-01T09:10:00+00:00"):
        assert client.post(f"/api/dispatches/{did}/start", json={}).status_code == 200
    # 超差读数使 A 隔离：实际占用仍在
    r = post_inspection(client, il, g["gauge_id"], 99, "2026-12-01T09:20:00+00:00")
    assert r.status_code == 201
    d = client.get(f"/api/dispatches/{did}").json()
    assert d["status"] == "STARTED" and d["actual_end_at"] is None
    assert client.get(f"/api/processes/{pa}/status").json()["status"] == "QUARANTINED"
    # 合格读数 + 复核恢复为 STARTED，占用仍不释放
    post_inspection(client, il, g["gauge_id"], 11, "2026-12-01T09:25:00+00:00")
    assert client.post(f"/api/processes/{pa}/review", json={}).status_code == 200
    d = client.get(f"/api/dispatches/{did}").json()
    assert d["status"] == "STARTED" and d["actual_end_at"] is None
    # 完工才结束占用
    with services.override_now("2026-12-01T09:40:00+00:00"):
        assert client.post(f"/api/processes/{pa}/complete", json={}).status_code == 200
    d = client.get(f"/api/dispatches/{did}").json()
    assert d["status"] == "COMPLETED" and d["actual_end_at"] is not None


def test_new_arrangement_cannot_start_in_the_past(client):
    """重排 / 排班的新安排不能从过去开始：窗口整段已过去则排不出。"""
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    from app import services
    with services.override_now("2026-12-01T12:00:00+00:00"):
        r = client.post("/api/shifts", json={
            "code": "S-PAST", "operator": "x", "window_start": S0900, "window_end": S1000,
            "dispatches": [_disp(pa, _opts((m["machine_id"], f["fixture_id"])),
                                ws=S0900, we=S1000, minutes=60)],
        })
    assert r.status_code == 409
    assert client.get("/api/shifts").json() == []


def test_cross_shift_schedule_avoids_other_published_reservations(client):
    """不同班次共享资源：新班次不能与其他班次仍在生效的预约撞资源。"""
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    pb = _inspected(client, wp, g, "B")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    opts = _opts((m["machine_id"], f["fixture_id"]))
    # 班次 1：A 占 09-10
    assert client.post("/api/shifts", json={
        "code": "S1", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pa, opts, ws=S0900, we=S1000, minutes=60)],
    }).status_code == 201
    # 班次 2：B 也只能 09-10 用同一资源 → 撞车，排不出
    r = client.post("/api/shifts", json={
        "code": "S2", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pb, opts, ws=S0900, we=S1000, minutes=60)],
    })
    assert r.status_code == 409
    # 放宽到 09-11 后可以排在 10-11
    r = client.post("/api/shifts", json={
        "code": "S2b", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pb, opts, ws=S0900, we=S1100, minutes=60)],
    })
    assert r.status_code == 201, r.text
    d = r.json()["dispatches"][0]
    assert d["reserved_start"] >= S1000


def test_shift_query_reports_each_process_arrangement_and_lifecycle(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    shift = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [_disp(pa, _opts((m["machine_id"], f["fixture_id"])),
                            ws=S0900, we=S1200, minutes=60)],
    }).json()
    did = shift["dispatches"][0]["dispatch_id"]
    s = client.get(f"/api/shifts/{shift['shift_id']}").json()
    d = s["dispatches"][0]
    assert d["process_id"] == pa
    assert d["status"] == "PLANNED" and d["suspended"] is False
    assert d["started"] is False and d["completed"] is False
    from app import services
    with services.override_now("2026-12-01T09:10:00+00:00"):
        client.post(f"/api/dispatches/{did}/start", json={})
    s = client.get(f"/api/shifts/{shift['shift_id']}").json()
    d = s["dispatches"][0]
    assert d["status"] == "STARTED" and d["started"] is True and d["completed"] is False
    with services.override_now("2026-12-01T09:30:00+00:00"):
        client.post(f"/api/processes/{pa}/complete", json={})
    s = client.get(f"/api/shifts/{shift['shift_id']}").json()
    assert s["dispatches"][0]["completed"] is True
    assert s["all_started_or_completed"] is True


def test_one_process_listed_twice_in_a_shift_is_rejected(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    pa = _inspected(client, wp, g, "A")
    m = _machine(client, "M1")
    f = _fixture(client, "F1")
    r = client.post("/api/shifts", json={
        "code": "S", "operator": "x", "window_start": S0900, "window_end": S1700,
        "dispatches": [
            _disp(pa, _opts((m["machine_id"], f["fixture_id"])), ws=S0900, we=S1000),
            _disp(pa, _opts((m["machine_id"], f["fixture_id"])), ws=S1000, we=S1100),
        ],
    })
    assert r.status_code == 422
