"""正常放行链路、公差边界与“最后接受检验为准”规则。"""
from conftest import item_id, make_gauge, make_workpiece, post_inspection, proc_id, status


def test_full_happy_path(client):
    wp = make_workpiece(client)
    g = make_gauge(client)

    # 初始：所有工序都没有检验，全部不可开工；A 的阻断理由是缺检验而非缺先决
    for code in ("A", "B", "C", "D"):
        s = status(client, proc_id(wp, code))
        assert s["can_start"] is False
        assert s["status"] == "NOT_STARTED"
    s_a = status(client, proc_id(wp, "A"))
    assert any("尚无检验记录" in x for x in s_a["blocking_reasons"])
    assert not any("先决工序" in x for x in s_a["blocking_reasons"])

    # A 的两个项目登记合格读数
    assert post_inspection(client, item_id(wp, "A", "A-L"), g["gauge_id"], 11).status_code == 201
    assert post_inspection(client, item_id(wp, "A", "A-W"), g["gauge_id"], 5).status_code == 201

    # 开工 A -> 完工 A
    pa = proc_id(wp, "A")
    r = client.post(f"/api/processes/{pa}/start", json={"operator": "张三"})
    assert r.status_code == 200 and r.json()["status"] == "STARTED"
    r = client.post(f"/api/processes/{pa}/complete", json={"operator": "张三"})
    assert r.status_code == 200 and r.json()["status"] == "COMPLETED"

    # B 仍缺检验，不能开工
    pb = proc_id(wp, "B")
    r = client.post(f"/api/processes/{pb}/start", json={})
    assert r.status_code == 409 and "检验记录" in r.json()["detail"]

    assert post_inspection(client, item_id(wp, "B", "B-D"), g["gauge_id"], 21).status_code == 201
    assert client.post(f"/api/processes/{pb}/start", json={}).status_code == 200
    assert client.post(f"/api/processes/{pb}/complete", json={}).status_code == 200

    # C 依赖 A、B（均完工），D 依赖 C
    pc, pd_ = proc_id(wp, "C"), proc_id(wp, "D")
    assert post_inspection(client, item_id(wp, "C", "C-D"), g["gauge_id"], 8).status_code == 201
    assert client.post(f"/api/processes/{pc}/start", json={}).status_code == 200
    assert client.post(f"/api/processes/{pc}/complete", json={}).status_code == 200
    assert post_inspection(client, item_id(wp, "D", "D-R"), g["gauge_id"], 2).status_code == 201
    assert client.post(f"/api/processes/{pd_}/start", json={}).status_code == 200
    assert client.post(f"/api/processes/{pd_}/complete", json={}).status_code == 200

    # 全程留凭据：A 有 START + COMPLETE 两张
    creds = client.get(f"/api/processes/{pa}/credentials").json()
    kinds = [c["kind"] for c in creds]
    assert kinds == ["START", "COMPLETE"]
    # 凭据快照引用了当时两个项目的检验
    assert len(creds[0]["inspections"]) == 2


def test_integer_tolerance_is_inclusive(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    il = item_id(wp, "A", "A-L")

    def a_l_blocked() -> bool:
        s = status(client, proc_id(wp, "A"))
        return any("A-L" in x for x in s["blocking_reasons"])

    # 边界值 10、12 均合格；9 和 13 不合格（A-W 始终未测，只看 A-L 是否被拦）
    for reading, ok in [(10, True), (12, True), (9, False), (13, False)]:
        r = post_inspection(client, il, g["gauge_id"], reading)
        assert r.status_code == 201
        assert a_l_blocked() is (not ok), reading

    # 最后一次是 13（超差）-> 不合格，且不会回退到此前合格的 12
    assert "超出整数公差" in status(client, proc_id(wp, "A"))["blocking_reasons"][0]

    # 重新登记合格读数，A-L 立即恢复（A 从未开工，不存在隔离）；此时只剩 A-W 未测
    post_inspection(client, il, g["gauge_id"], 11)
    s = status(client, proc_id(wp, "A"))
    assert s["can_start"] is False
    assert any("A-W" in x for x in s["blocking_reasons"])
    assert all("A-L" not in x for x in s["blocking_reasons"])


def test_latest_accepted_wins_no_fallback(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    il = item_id(wp, "A", "A-L")
    iw = item_id(wp, "A", "A-W")
    post_inspection(client, il, g["gauge_id"], 11)
    post_inspection(client, iw, g["gauge_id"], 5)

    pa = proc_id(wp, "A")
    client.post(f"/api/processes/{pa}/start", json={})

    # 历史检验列表按 id 倒序，最新一条被标记 is_latest
    hist = client.get(f"/api/items/{il}/inspections").json()
    assert hist[0]["is_latest"] is True
    assert all(h["is_latest"] is False for h in hist[1:])

    # 登记一条失准量具读数（超差），A 立刻被隔离，不回退旧的合格记录
    r = post_inspection(client, il, g["gauge_id"], 99)
    assert r.status_code == 201
    assert r.json()["quarantined_process_ids"] == [pa]
    s = status(client, pa)
    assert s["status"] == "QUARANTINED"
    assert s["open_quarantine"]["pre_status"] == "STARTED"

    # 旧开工凭据仍可查到，快照没有被新读数回填
    creds = client.get(f"/api/processes/{pa}/credentials").json()
    start_cred = next(c for c in creds if c["kind"] == "START")
    snap = next(x for x in start_cred["inspections"] if x["item_id"] == il)
    current_latest = client.get(f"/api/items/{il}/inspections").json()[0]
    assert snap["inspection_id"] != current_latest["inspection_id"]


def test_prerequisite_must_be_completed_and_valid(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    post_inspection(client, item_id(wp, "A", "A-L"), g["gauge_id"], 11)
    post_inspection(client, item_id(wp, "A", "A-W"), g["gauge_id"], 5)
    post_inspection(client, item_id(wp, "B", "B-D"), g["gauge_id"], 21)
    pa, pb = proc_id(wp, "A"), proc_id(wp, "B")

    # A 只开工不完工，B 不能开工
    client.post(f"/api/processes/{pa}/start", json={})
    r = client.post(f"/api/processes/{pb}/start", json={})
    assert r.status_code == 409 and "尚未完工" in r.json()["detail"]

    client.post(f"/api/processes/{pa}/complete", json={})
    assert client.post(f"/api/processes/{pb}/start", json={}).status_code == 200


def test_repeated_actions_rejected(client):
    wp = make_workpiece(client)
    g = make_gauge(client)
    post_inspection(client, item_id(wp, "A", "A-L"), g["gauge_id"], 11)
    post_inspection(client, item_id(wp, "A", "A-W"), g["gauge_id"], 5)
    pa = proc_id(wp, "A")
    client.post(f"/api/processes/{pa}/start", json={})
    assert client.post(f"/api/processes/{pa}/start", json={}).status_code == 409
    client.post(f"/api/processes/{pa}/complete", json={})
    assert client.post(f"/api/processes/{pa}/complete", json={}).status_code == 409
    # 未开工不能完工
    pb = proc_id(wp, "B")
    assert client.post(f"/api/processes/{pb}/complete", json={}).status_code == 409
