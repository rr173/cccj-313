"""量具失准窗口、隔离传导与质量员逐道复核规则。"""
from conftest import item_id, make_gauge, make_workpiece, post_inspection, proc_id, status


def _build_started_chain(client):
    """A 完工、B 完工、C 完工、D 已开工未完工。返回 (wp, gauge, ids)。"""
    wp = make_workpiece(client)
    g = make_gauge(client)
    post_inspection(client, item_id(wp, "A", "A-L"), g["gauge_id"], 11, "2026-11-27T08:00:00+00:00")
    post_inspection(client, item_id(wp, "A", "A-W"), g["gauge_id"], 5, "2026-11-27T08:05:00+00:00")
    post_inspection(client, item_id(wp, "B", "B-D"), g["gauge_id"], 21, "2026-11-27T09:00:00+00:00")
    post_inspection(client, item_id(wp, "C", "C-D"), g["gauge_id"], 8, "2026-11-27T10:00:00+00:00")
    post_inspection(client, item_id(wp, "D", "D-R"), g["gauge_id"], 2, "2026-11-27T11:00:00+00:00")
    ids = {c: proc_id(wp, c) for c in ("A", "B", "C", "D")}
    for code in ("A", "B", "C"):
        assert client.post(f"/api/processes/{ids[code]}/start", json={}).status_code == 200
        assert client.post(f"/api/processes/{ids[code]}/complete", json={}).status_code == 200
    assert client.post(f"/api/processes/{ids['D']}/start", json={}).status_code == 200
    return wp, g, ids


def test_window_is_half_open_boundary_valid(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    # 窗口 [09:00, 10:00)，08:00 与 10:00 的测量均有效
    r = client.post(
        f"/api/gauges/{g['gauge_id']}/out-of-calibration-windows",
        json={
            "window_start": "2026-11-27T09:00:00+00:00",
            "window_end": "2026-11-27T10:00:00+00:00",
            "note": "送检发现漂移",
        },
    )
    assert r.status_code == 201, r.text

    post_inspection(client, item_id(wp, "A", "A-L"), g["gauge_id"], 11, "2026-11-27T08:00:00+00:00")
    post_inspection(client, item_id(wp, "A", "A-W"), g["gauge_id"], 5, "2026-11-27T10:00:00+00:00")
    s = status(client, proc_id(wp, "A"))
    assert s["can_start"] is True

    # 落在窗口内（09:30）则量具无效，项目不合格
    post_inspection(client, item_id(wp, "A", "A-W"), g["gauge_id"], 5, "2026-11-27T09:30:00+00:00")
    s = status(client, proc_id(wp, "A"))
    assert s["can_start"] is False
    assert any("失准窗口" in x for x in s["blocking_reasons"])

    # 非法窗口
    r = client.post(
        f"/api/gauges/{g['gauge_id']}/out-of-calibration-windows",
        json={"window_start": "2026-11-27T11:00:00+00:00", "window_end": "2026-11-27T11:00:00+00:00"},
    )
    assert r.status_code == 422


def test_window_quarantines_active_credentials_and_propagates(client):
    wp, g, ids = _build_started_chain(client)

    # 登记覆盖 B 测量时刻 (09:00) 的失准窗口：B 开工凭据引用的测量落入窗口
    r = client.post(
        f"/api/gauges/{g['gauge_id']}/out-of-calibration-windows",
        json={
            "window_start": "2026-11-27T08:30:00+00:00",
            "window_end": "2026-11-27T09:30:00+00:00",
        },
    )
    assert r.status_code == 201
    quarantined = sorted(r.json()["quarantined_process_ids"])
    # B 本体 + 已开工/完工的下游 C、D；A 的测量在 08:00/08:05，不受影响
    assert quarantined == sorted([ids["B"], ids["C"], ids["D"]])

    assert status(client, ids["A"])["status"] == "COMPLETED"
    for code in ("B", "C", "D"):
        s = status(client, ids[code])
        assert s["status"] == "QUARANTINED"
        assert s["open_quarantine"] is not None

    # 隔离单保留隔离前状态：B/C 是 COMPLETED，D 是 STARTED
    qs = {q["process_id"]: q for q in client.get("/api/quarantines?open_only=true").json()}
    assert qs[ids["B"]]["pre_status"] == "COMPLETED"
    assert qs[ids["D"]]["pre_status"] == "STARTED"

    # 隔离工序上不能继续开工 / 完工
    assert client.post(f"/api/processes/{ids['B']}/start", json={}).status_code == 409
    assert client.post(f"/api/processes/{ids['D']}/complete", json={}).status_code == 409

    # 当时实际做过什么仍可查：凭据与事件全保留
    events = client.get(f"/api/processes/{ids['D']}/events").json()
    assert [e["event_type"] for e in events] == ["CREATED", "START", "QUARANTINE"]
    creds = client.get(f"/api/processes/{ids['B']}/credentials").json()
    assert [c["kind"] for c in creds] == ["START", "COMPLETE"]


def test_upstream_rework_alone_does_not_release_quarantined_downstream(client):
    wp, g, ids = _build_started_chain(client)
    # B 失准导致 B/C/D 隔离
    client.post(
        f"/api/gauges/{g['gauge_id']}/out-of-calibration-windows",
        json={
            "window_start": "2026-11-27T08:30:00+00:00",
            "window_end": "2026-11-27T09:30:00+00:00",
        },
    )
    # 用另一台有效量具补测 B 的项目
    g2 = make_gauge(client, code="G-2", name="备用卡尺")
    post_inspection(client, item_id(wp, "B", "B-D"), g2["gauge_id"], 21, "2026-11-27T12:00:00+00:00")

    # 仅补测不会自动解除任何隔离
    for code in ("B", "C", "D"):
        assert status(client, ids[code])["status"] == "QUARANTINED"

    # 下游不能跳过 B 先复核：C 的先决 B 仍隔离
    r = client.post(f"/api/processes/{ids['C']}/review", json={})
    assert r.status_code == 409 and "已隔离" in r.json()["detail"]

    # B 复核：隔离前已完工 -> 复核后仍为完工，不重做
    r = client.post(f"/api/processes/{ids['B']}/review", json={"operator": "李四"})
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "COMPLETED"
    assert r.json()["active_credential"]["kind"] == "REVIEW"

    # B 恢复后 C 仍隔离，必须逐道复核
    assert status(client, ids["C"])["status"] == "QUARANTINED"
    r = client.post(f"/api/processes/{ids['C']}/review", json={})
    assert r.status_code == 200
    assert r.json()["status"] == "COMPLETED"

    # D 隔离前是 STARTED：复核后恢复开工，必须再完工
    r = client.post(f"/api/processes/{ids['D']}/review", json={})
    assert r.status_code == 200
    assert r.json()["status"] == "STARTED"
    r = client.post(f"/api/processes/{ids['D']}/complete", json={})
    assert r.status_code == 200 and r.json()["status"] == "COMPLETED"

    # 所有隔离单已关闭，事件链完整
    assert client.get("/api/quarantines?open_only=true").json() == []
    events = client.get(f"/api/processes/{ids['D']}/events").json()
    assert [e["event_type"] for e in events] == [
        "CREATED", "START", "QUARANTINE", "REVIEW", "COMPLETE"
    ]


def test_review_still_requires_same_rules(client):
    wp, g, ids = _build_started_chain(client)
    client.post(
        f"/api/gauges/{g['gauge_id']}/out-of-calibration-windows",
        json={
            "window_start": "2026-11-27T08:30:00+00:00",
            "window_end": "2026-11-27T09:30:00+00:00",
        },
    )
    # B 不补测就复核 -> 不通过（最新测量仍落入失准窗口）
    r = client.post(f"/api/processes/{ids['B']}/review", json={})
    assert r.status_code == 409
    assert any("失准窗口" in x for x in [r.json()["detail"]])

    # 补测合格后复核通过
    g2 = make_gauge(client, code="G-2", name="备用卡尺")
    post_inspection(client, item_id(wp, "B", "B-D"), g2["gauge_id"], 21, "2026-11-27T12:00:00+00:00")
    assert client.post(f"/api/processes/{ids['B']}/review", json={}).status_code == 200

    # 非隔离工序调用复核报错
    r = client.post(f"/api/processes/{ids['A']}/review", json={})
    assert r.status_code == 409


def test_new_reading_does_not_backfill_start_credential(client):
    wp, g, ids = _build_started_chain(client)
    first_cred = client.get(f"/api/processes/{ids['A']}/credentials").json()[0]
    snap_before = {x["item_id"]: x["inspection_id"] for x in first_cred["inspections"]}

    # A 已完工；登记一条新的合格读数
    post_inspection(client, item_id(wp, "A", "A-L"), g["gauge_id"], 12, "2026-11-27T13:00:00+00:00")
    first_cred_after = client.get(f"/api/processes/{ids['A']}/credentials").json()[0]
    snap_after = {x["item_id"]: x["inspection_id"] for x in first_cred_after["inspections"]}
    assert snap_before == snap_after  # 凭据快照未被回填


def test_window_does_not_quarantine_unstarted_process_but_blocks_start(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    # A 项目测量在 09:00，但 A 不开工
    post_inspection(client, item_id(wp, "A", "A-L"), g["gauge_id"], 11, "2026-11-27T09:00:00+00:00")
    post_inspection(client, item_id(wp, "A", "A-W"), g["gauge_id"], 5, "2026-11-27T09:05:00+00:00")

    r = client.post(
        f"/api/gauges/{g['gauge_id']}/out-of-calibration-windows",
        json={
            "window_start": "2026-11-27T08:30:00+00:00",
            "window_end": "2026-11-27T09:30:00+00:00",
        },
    )
    # 没有任何生效凭据引用该窗口 -> 无隔离
    assert r.json()["quarantined_process_ids"] == []
    # 但 A 的最新测量失效，开工被同一套规则拦截
    s = status(client, proc_id(wp, "A"))
    assert s["can_start"] is False
    assert any("失准窗口" in x for x in s["blocking_reasons"])


def test_failed_inspection_after_completion_quarantines_tree(client):
    wp, g, ids = _build_started_chain(client)
    # 对已完工的 A 登记一条超差读数
    r = post_inspection(client, item_id(wp, "A", "A-L"), g["gauge_id"], 99, "2026-11-27T14:00:00+00:00")
    # A 及全部已开工/完工下游 B、C、D 全部隔离
    assert sorted(r.json()["quarantined_process_ids"]) == sorted(ids.values())
    for code in ("A", "B", "C", "D"):
        assert status(client, ids[code])["status"] == "QUARANTINED"


def test_only_same_workpiece_dag_affected(client):
    # 第二个工件完全独立，不受第一个工件事件影响
    wp1 = make_workpiece(client, code="WP-1")
    wp2 = make_workpiece(client, code="WP-2")
    g = make_gauge(client)
    post_inspection(client, item_id(wp1, "A", "A-L"), g["gauge_id"], 11, "2026-11-27T09:00:00+00:00")
    post_inspection(client, item_id(wp1, "A", "A-W"), g["gauge_id"], 5, "2026-11-27T09:05:00+00:00")
    pa1 = proc_id(wp1, "A")
    client.post(f"/api/processes/{pa1}/start", json={})
    client.post(
        f"/api/gauges/{g['gauge_id']}/out-of-calibration-windows",
        json={
            "window_start": "2026-11-27T08:30:00+00:00",
            "window_end": "2026-11-27T09:30:00+00:00",
        },
    )
    assert status(client, pa1)["status"] == "QUARANTINED"
    # WP-2 的 A 从未开工，且没有任何检验，状态保持 NOT_STARTED
    assert status(client, proc_id(wp2, "A"))["status"] == "NOT_STARTED"
