"""DAG 建档校验、输入错误与并发竞争。"""
import threading

from conftest import item_id, make_gauge, make_workpiece, post_inspection, proc_id


def test_cyclic_dag_rejected(client):
    payload = {
        "code": "CYC",
        "processes": [
            {"code": "X", "name": "x", "prerequisites": ["Z"], "items": [
                {"code": "i", "name": "i", "lower_limit": 0, "upper_limit": 1}]},
            {"code": "Y", "name": "y", "prerequisites": ["X"], "items": [
                {"code": "i", "name": "i", "lower_limit": 0, "upper_limit": 1}]},
            {"code": "Z", "name": "z", "prerequisites": ["Y"], "items": [
                {"code": "i", "name": "i", "lower_limit": 0, "upper_limit": 1}]},
        ],
    }
    r = client.post("/api/workpieces", json=payload)
    assert r.status_code == 422 and "环" in r.json()["detail"]


def test_missing_prereq_and_duplicate_codes_rejected(client):
    base = {
        "code": "BAD",
        "processes": [
            {"code": "X", "name": "x", "prerequisites": ["GHOST"], "items": [
                {"code": "i", "name": "i", "lower_limit": 0, "upper_limit": 1}]},
        ],
    }
    r = client.post("/api/workpieces", json=base)
    assert r.status_code == 422

    base["processes"][0]["prerequisites"] = []
    base["processes"].append(
        {"code": "X", "name": "dup", "prerequisites": [], "items": [
            {"code": "j", "name": "j", "lower_limit": 0, "upper_limit": 1}]}
    )
    r = client.post("/api/workpieces", json=base)
    assert r.status_code == 422


def test_bad_inputs(client):
    make_workpiece(client)
    g = make_gauge(client)
    # 不存在的项目 / 量具
    r = client.post(
        "/api/inspections",
        json={"item_id": 999, "gauge_id": g["gauge_id"], "reading": 1, "measured_at": "2026-11-27T08:00:00Z"},
    )
    assert r.status_code == 404
    r = client.post(
        "/api/inspections",
        json={"item_id": 1, "gauge_id": 999, "reading": 1, "measured_at": "2026-11-27T08:00:00Z"},
    )
    assert r.status_code == 404
    # 非整数读数 / 坏时间 / 缺字段由 422 拦截
    r = client.post(
        "/api/inspections",
        json={"item_id": 1, "gauge_id": g["gauge_id"], "reading": 1.5, "measured_at": "2026-11-27T08:00:00Z"},
    )
    assert r.status_code == 422
    r = client.post(
        "/api/inspections",
        json={"item_id": 1, "gauge_id": g["gauge_id"], "reading": 1, "measured_at": "not-a-time"},
    )
    assert r.status_code == 422
    # 重复量具编号
    r = client.post("/api/gauges", json={"code": "G-1", "name": "另一个"})
    assert r.status_code == 409


def test_naive_timestamp_treated_as_utc(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    r = post_inspection(client, item_id(wp, "A", "A-L"), g["gauge_id"], 11, "2026-11-27T08:00:00")
    assert r.status_code == 201
    assert r.json()["measured_at"].endswith("+00:00")


def test_concurrent_window_vs_start_no_release_on_quarantined(client):
    """竞争场景：开工与失准登记并发，结束后不允许在应隔离的工序上放行。"""
    wp = make_workpiece(client)
    g = make_gauge(client)
    post_inspection(client, item_id(wp, "A", "A-L"), g["gauge_id"], 11, "2026-11-27T09:00:00+00:00")
    post_inspection(client, item_id(wp, "A", "A-W"), g["gauge_id"], 5, "2026-11-27T09:05:00+00:00")
    pa = proc_id(wp, "A")

    errors = []

    def do_start():
        r = client.post(f"/api/processes/{pa}/start", json={"operator": "t1"})
        if r.status_code not in (200, 409):
            errors.append(("start", r.status_code, r.text))

    def do_window():
        r = client.post(
            f"/api/gauges/{g['gauge_id']}/out-of-calibration-windows",
            json={
                "window_start": "2026-11-27T08:30:00+00:00",
                "window_end": "2026-11-27T09:30:00+00:00",
            },
        )
        if r.status_code != 201:
            errors.append(("window", r.status_code, r.text))

    t1 = threading.Thread(target=do_start)
    t2 = threading.Thread(target=do_window)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert not errors

    s = client.get(f"/api/processes/{pa}/status").json()
    # 锁把两个写操作串行化，只可能有两种次序：
    # 1) 开工先提交：生效凭据引用的测量落入后登记的窗口 -> 追加成 QUARANTINED；
    # 2) 窗口先提交：最新测量已失准，开工被同一套规则 409 拦截 -> 保持 NOT_STARTED。
    # 两种情况下都绝不可能出现“依据失准却仍 STARTED 可继续放行”。
    if s["status"] == "QUARANTINED":
        assert s["open_quarantine"] is not None
    else:
        assert s["status"] == "NOT_STARTED"
        assert s["can_start"] is False
        assert any("失准窗口" in x for x in s["blocking_reasons"])
    assert s["status"] != "STARTED"

    # 重复并发开工，最多一个成功
    wp2 = make_workpiece(client, code="WP-2")
    post_inspection(client, item_id(wp2, "A", "A-L"), g["gauge_id"], 11, "2026-11-28T08:00:00+00:00")
    post_inspection(client, item_id(wp2, "A", "A-W"), g["gauge_id"], 5, "2026-11-28T08:05:00+00:00")
    pa2 = proc_id(wp2, "A")
    results = []
    barrier = threading.Barrier(2)

    def start2():
        barrier.wait()
        results.append(client.post(f"/api/processes/{pa2}/start", json={}).status_code)

    threads = [threading.Thread(target=start2) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results) == [200, 409]
