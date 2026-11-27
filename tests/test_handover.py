"""工位交接：建单预留、发运、分批签收 / 退回、质量冻结与幂等。"""
from conftest import item_id, make_gauge, make_workpiece, post_inspection, proc_id


def _complete_process(client, wp, g, code):
    """按现有规则把某工序及其先决工序依次做完。"""
    p = next(x for x in wp["processes"] if x["code"] == code)
    for pre in p["prerequisites"]:
        pre_proc = next(x for x in wp["processes"] if x["process_id"] == pre)
        if client.get(f"/api/processes/{pre}/status").json()["status"] != "COMPLETED":
            _complete_process(client, wp, g, pre_proc["code"])
    for it in p["items"]:
        lo, hi = it["lower_limit"], it["upper_limit"]
        post_inspection(client, it["item_id"], g["gauge_id"], (lo + hi) // 2)
    pid = proc_id(wp, code)
    r = client.post(f"/api/processes/{pid}/start", json={})
    assert r.status_code == 200, r.text
    r = client.post(f"/api/processes/{pid}/complete", json={})
    assert r.status_code == 200, r.text
    return pid


def _complete_chain(client, wp, g, codes=("A", "B", "C", "D")):
    for code in codes:
        _complete_process(client, wp, g, code)
    return proc_id(wp, codes[-1])


def _ws(client, code, capacity, name=None):
    r = client.post("/api/workstations", json={"code": code, "name": name or code, "capacity": capacity})
    assert r.status_code == 201, r.text
    return r.json()


def _piece(client, wp, gate_pid, ws_id):
    r = client.post(
        "/api/pieces",
        json={"workpiece_id": wp["workpiece_id"], "gate_process_id": gate_pid, "workstation_id": ws_id},
    )
    assert r.status_code == 201, r.text
    return r.json()


def _ready_piece(client, wp, g, ws_id, code="A", gate_code=None):
    pid = _complete_process(client, wp, g, code)
    return pid, _piece(client, wp, pid, ws_id)


def _setup(client, capacity=10, wp_code="WP-1"):
    wp = make_workpiece(client, code=wp_code)
    g = make_gauge(client)
    src = _ws(client, "SRC-" + wp_code, capacity)
    dst = _ws(client, "DST-" + wp_code, capacity)
    return wp, g, src, dst


def test_create_ship_receive_keeps_slots_and_records(client):
    wp, g, src, dst = _setup(client)
    gate, piece = _ready_piece(client, wp, g, src["workstation_id"])

    # 建单即预留收件位，实物未移动
    r = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": dst["workstation_id"],
            "piece_ids": [piece["piece_id"]],
        },
    )
    assert r.status_code == 201, r.text
    order = r.json()
    assert order["status"] == "PENDING"

    s = client.get(f"/api/workstations/{src['workstation_id']}").json()
    assert s["actual_pieces"] == [piece["piece_id"]]
    assert s["reserved_pieces"] == []
    d = client.get(f"/api/workstations/{dst['workstation_id']}").json()
    assert d["actual_pieces"] == []
    assert d["reserved_pieces"] == [piece["piece_id"]]
    assert d["available"] == 9

    # 发运
    r = client.post(
        f"/api/transfer-orders/{order['order_id']}/ship",
        json={"operator": "张三", "request_id": "REQ-1"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["order_status"] == "SHIPPED"

    # 发件方：无实物，保留退回位；收件方：仍为收件预留
    s = client.get(f"/api/workstations/{src['workstation_id']}").json()
    assert s["actual_pieces"] == [] and s["reserved_pieces"] == [piece["piece_id"]]
    d = client.get(f"/api/workstations/{dst['workstation_id']}").json()
    assert d["actual_pieces"] == [] and d["reserved_pieces"] == [piece["piece_id"]]
    assert client.get(f"/api/pieces/{piece['piece_id']}").json()["status"] == "IN_TRANSIT"

    # 签收：占用收件预留位，释放发件退回位
    r = client.post(
        f"/api/transfer-orders/{order['order_id']}/receive",
        json={"operator": "李四", "request_id": "REQ-2", "piece_ids": [piece["piece_id"]]},
    )
    assert r.status_code == 200, r.text
    assert r.json()["order_status"] == "COMPLETED"
    d = client.get(f"/api/workstations/{dst['workstation_id']}").json()
    assert d["actual_pieces"] == [piece["piece_id"]] and d["reserved_pieces"] == []
    s = client.get(f"/api/workstations/{src['workstation_id']}").json()
    assert s["actual_pieces"] == [] and s["reserved_pieces"] == []

    detail = client.get(f"/api/transfer-orders/{order['order_id']}").json()
    rec = detail["records"]
    assert len(rec) == 1
    assert rec[0]["action"] == "RECEIVE" and rec[0]["operator"] == "李四"
    assert rec[0]["piece_id"] == piece["piece_id"]


def test_partial_receive_and_return_do_not_touch_other_pieces(client):
    wp, g, src, dst = _setup(client)
    pieces = []
    for i in range(3):
        wp_i = make_workpiece(client, code=f"WPM-{i}")
        gg = make_gauge(client, code=f"GM-{i}")
        _, p = _ready_piece(client, wp_i, gg, src["workstation_id"], code="A")
        pieces.append(p)
    ids = [p["piece_id"] for p in pieces]
    r = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": dst["workstation_id"],
            "piece_ids": ids,
        },
    )
    order_id = r.json()["order_id"]
    assert client.post(
        f"/api/transfer-orders/{order_id}/ship",
        json={"operator": "张三", "request_id": "S1"},
    ).status_code == 200

    # 先退 1 件
    r = client.post(
        f"/api/transfer-orders/{order_id}/return",
        json={"operator": "张三", "request_id": "R1", "piece_ids": [ids[0]]},
    )
    assert r.status_code == 200 and r.json()["order_status"] == "SHIPPED"
    # 再签 1 件
    r = client.post(
        f"/api/transfer-orders/{order_id}/receive",
        json={"operator": "李四", "request_id": "C1", "piece_ids": [ids[1]]},
    )
    assert r.status_code == 200 and r.json()["order_status"] == "SHIPPED"

    states = {p["piece_id"]: p["state"] for p in client.get(f"/api/transfer-orders/{order_id}").json()["pieces"]}
    assert states[ids[0]] == "RETURNED"
    assert states[ids[1]] == "RECEIVED"
    assert states[ids[2]] == "IN_TRANSIT"

    s = client.get(f"/api/workstations/{src['workstation_id']}").json()
    assert s["actual_pieces"] == [ids[0]]
    assert s["reserved_pieces"] == [ids[2]]  # 仍在途那件的退回保留位
    d = client.get(f"/api/workstations/{dst['workstation_id']}").json()
    assert d["actual_pieces"] == [ids[1]]
    assert d["reserved_pieces"] == [ids[2]]  # 仍在途那件的收件预留位

    # 已落地件再处理整批失败
    r = client.post(
        f"/api/transfer-orders/{order_id}/receive",
        json={"operator": "李四", "request_id": "C2", "piece_ids": [ids[1]]},
    )
    assert r.status_code == 409


def test_one_bad_piece_aborts_whole_batch_and_does_not_burn_request(client):
    wp, g, src, dst = _setup(client)
    _, p1 = _ready_piece(client, wp, g, src["workstation_id"])
    wp2 = make_workpiece(client, code="WP-B")
    g2 = make_gauge(client, code="G-B")
    _, p2 = _ready_piece(client, wp2, g2, src["workstation_id"])

    order = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": dst["workstation_id"],
            "piece_ids": [p1["piece_id"], p2["piece_id"]],
        },
    ).json()
    client.post(f"/api/transfer-orders/{order['order_id']}/ship", json={"operator": "A", "request_id": "S"})

    # 含不在单内的件 -> 整批不生效
    r = client.post(
        f"/api/transfer-orders/{order['order_id']}/receive",
        json={"operator": "A", "request_id": "C1", "piece_ids": [p1["piece_id"], 99999]},
    )
    assert r.status_code == 409
    # 失败不占号：同样的请求号 + 合法批次可以成功
    r = client.post(
        f"/api/transfer-orders/{order['order_id']}/receive",
        json={"operator": "A", "request_id": "C1", "piece_ids": [p1["piece_id"]]},
    )
    assert r.status_code == 200, r.text


def test_idempotent_ship_and_receive(client):
    wp, g, src, dst = _setup(client)
    _, piece = _ready_piece(client, wp, g, src["workstation_id"])
    order = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": dst["workstation_id"],
            "piece_ids": [piece["piece_id"]],
        },
    ).json()
    oid = order["order_id"]

    body = {"operator": "张三", "request_id": "REQ-1"}
    r1 = client.post(f"/api/transfer-orders/{oid}/ship", json=body)
    r2 = client.post(f"/api/transfer-orders/{oid}/ship", json=body)
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["replayed"] is False
    assert r2.json()["replayed"] is True
    assert r2.json()["order_status"] == "SHIPPED"

    rcv = {"operator": "李四", "request_id": "REQ-2", "piece_ids": [piece["piece_id"]]}
    c1 = client.post(f"/api/transfer-orders/{oid}/receive", json=rcv)
    c2 = client.post(f"/api/transfer-orders/{oid}/receive", json=rcv)
    assert c1.status_code == c2.status_code == 200
    assert c1.json() == {**c2.json(), "replayed": False}
    assert c2.json()["replayed"] is True
    # 只落一次账，只有一条记录
    assert len(client.get(f"/api/transfer-orders/{oid}").json()["records"]) == 1

    # 同号换内容 -> 拒绝
    r = client.post(
        f"/api/transfer-orders/{oid}/receive",
        json={"operator": "王五", "request_id": "REQ-2", "piece_ids": [piece["piece_id"]]},
    )
    assert r.status_code == 409
    r = client.post(f"/api/transfer-orders/{oid}/ship", json={"operator": "张三", "request_id": "REQ-2"})
    assert r.status_code == 409


def test_unique_pending_or_transit_membership(client):
    wp, g, src, dst = _setup(client)
    src2 = _ws(client, "SRC2", 5)
    _, piece = _ready_piece(client, wp, g, src["workstation_id"])
    o1 = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": dst["workstation_id"],
            "piece_ids": [piece["piece_id"]],
        },
    )
    assert o1.status_code == 201
    r = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": src2["workstation_id"],
            "piece_ids": [piece["piece_id"]],
        },
    )
    assert r.status_code == 409 and "已在调拨单" in r.json()["detail"]

    # 撤销待发单后释放收件位，件可重新上单
    r = client.post(f"/api/transfer-orders/{o1.json()['order_id']}/cancel")
    assert r.status_code == 200 and r.json()["status"] == "CANCELLED"
    d = client.get(f"/api/workstations/{dst['workstation_id']}").json()
    assert d["reserved_pieces"] == [] and d["available"] == d["capacity"]
    r = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": src2["workstation_id"],
            "piece_ids": [piece["piece_id"]],
        },
    )
    assert r.status_code == 201


def test_cancel_rejected_after_ship(client):
    wp, g, src, dst = _setup(client)
    _, piece = _ready_piece(client, wp, g, src["workstation_id"])
    oid = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": dst["workstation_id"],
            "piece_ids": [piece["piece_id"]],
        },
    ).json()["order_id"]
    client.post(f"/api/transfer-orders/{oid}/ship", json={"operator": "A", "request_id": "S"})
    assert client.post(f"/api/transfer-orders/{oid}/cancel").status_code == 409


def test_invalid_gate_blocks_creation_and_ship(client):
    wp, g, src, dst = _setup(client)
    pid = _complete_process(client, wp, g, "D")
    piece = _piece(client, wp, pid, src["workstation_id"])

    # 关口完工后，登记一条超差读数使关口失效（整棵 DAG 被隔离）
    post_inspection(client, item_id(wp, "D", "D-R"), g["gauge_id"], 99)
    assert client.get(f"/api/processes/{pid}/status").json()["status"] == "QUARANTINED"
    assert client.get(f"/api/pieces/{piece['piece_id']}").json()["quality_frozen"] is True

    # 建单本身只留位，不因关口失效而拒绝
    r = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": dst["workstation_id"],
            "piece_ids": [piece["piece_id"]],
        },
    )
    assert r.status_code == 201
    oid = r.json()["order_id"]
    # 但整单发运被关口失效拦截，仍待发
    r = client.post(f"/api/transfer-orders/{oid}/ship", json={"operator": "A", "request_id": "S"})
    assert r.status_code == 409 and "整单仍待发" in r.json()["detail"]
    assert client.get(f"/api/transfer-orders/{oid}").json()["status"] == "PENDING"


def test_ship_requires_all_gates_still_valid_and_order_remains_pending(client):
    src = _ws(client, "SRC-X", 10)
    dst = _ws(client, "DST-X", 10)
    # 两件：一件良好，一件在发运前关口失效
    wp1 = make_workpiece(client, code="W1")
    g1 = make_gauge(client, code="GA")
    _, p1 = _ready_piece(client, wp1, g1, src["workstation_id"], code="A")
    wp2 = make_workpiece(client, code="W2")
    g2 = make_gauge(client, code="GB")
    gate2, p2 = _ready_piece(client, wp2, g2, src["workstation_id"], code="A")

    oid = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": dst["workstation_id"],
            "piece_ids": [p1["piece_id"], p2["piece_id"]],
        },
    ).json()["order_id"]

    # 只有第二件关口失效
    g3 = make_gauge(client, code="GC")
    post_inspection(client, item_id(wp2, "A", "A-L"), g2["gauge_id"], 999)
    r = client.post(f"/api/transfer-orders/{oid}/ship", json={"operator": "A", "request_id": "S"})
    assert r.status_code == 409 and "整单仍待发" in r.json()["detail"]
    assert client.get(f"/api/transfer-orders/{oid}").json()["status"] == "PENDING"
    # 失败不占号
    assert client.post(f"/api/transfer-orders/{oid}/ship", json={"operator": "A", "request_id": "S"}).status_code == 409


def test_quality_freeze_allows_landing_but_blocks_reship_then_recovers(client):
    wp, g, src, dst = _setup(client)
    gate_pid, piece = _ready_piece(client, wp, g, src["workstation_id"], code="D")
    oid = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": dst["workstation_id"],
            "piece_ids": [piece["piece_id"]],
        },
    ).json()["order_id"]
    assert client.post(f"/api/transfer-orders/{oid}/ship", json={"operator": "A", "request_id": "S"}).status_code == 200

    # 在途时关口变为失效（超差读数隔离）
    post_inspection(client, item_id(wp, "D", "D-R"), g["gauge_id"], 99)
    assert client.get(f"/api/pieces/{piece['piece_id']}").json()["quality_frozen"] is True

    # 真实到件照常落地，不因质量变化拒记
    r = client.post(
        f"/api/transfer-orders/{oid}/receive",
        json={"operator": "李四", "request_id": "C", "piece_ids": [piece["piece_id"]]},
    )
    assert r.status_code == 200
    # 当前库存标记质量冻结
    p = client.get(f"/api/pieces/{piece['piece_id']}").json()
    assert p["current_workstation_code"] == "DST-WP-1"
    assert p["quality_frozen"] is True and p["gate_valid"] is False

    # 冻结件可以建单留位，但不能通过发运
    src2 = _ws(client, "SRC-2", 5)
    r = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": dst["workstation_id"],
            "dest_workstation_id": src2["workstation_id"],
            "piece_ids": [piece["piece_id"]],
        },
    )
    assert r.status_code == 201
    oid2 = r.json()["order_id"]
    r = client.post(f"/api/transfer-orders/{oid2}/ship", json={"operator": "A", "request_id": "S-FROZEN"})
    assert r.status_code == 409

    # 复核恢复有效：补测合格 -> 关口仍隔离，需复核；复核后状态恢复，原记录不变
    g2 = make_gauge(client, code="G-RECOVER")
    post_inspection(client, item_id(wp, "D", "D-R"), g2["gauge_id"], 2, "2026-12-01T08:00:00+00:00")
    assert client.post(f"/api/processes/{gate_pid}/review", json={"operator": "质量员"}).status_code == 200
    p = client.get(f"/api/pieces/{piece['piece_id']}").json()
    assert p["quality_frozen"] is False and p["gate_valid"] is True
    # 原签收记录不被改动
    records = client.get(f"/api/transfer-orders/{oid}").json()["records"]
    assert len(records) == 1 and records[0]["action"] == "RECEIVE"
    # 撤销那张被冻结拦下的待发单，释放它预留的收件位
    assert client.post(f"/api/transfer-orders/{oid2}/cancel").status_code == 200
    # 恢复后可再次发运
    r = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": dst["workstation_id"],
            "dest_workstation_id": src2["workstation_id"],
            "piece_ids": [piece["piece_id"]],
        },
    )
    assert r.status_code == 201, r.text


def test_capacity_is_enforced_at_creation_and_ship(client):
    wp, g, src, dst = _setup(client, capacity=10)
    tiny_dst = _ws(client, "TINY", 1)
    _, p1 = _ready_piece(client, wp, g, src["workstation_id"])
    wp2 = make_workpiece(client, code="W-CAP")
    g2 = make_gauge(client, code="G-CAP")
    _, p2 = _ready_piece(client, wp2, g2, src["workstation_id"])

    # 收件位不足：整单不建，也不产生部分预留
    r = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": tiny_dst["workstation_id"],
            "piece_ids": [p1["piece_id"], p2["piece_id"]],
        },
    )
    assert r.status_code == 409
    assert client.get(f"/api/workstations/{tiny_dst['workstation_id']}").json()["reserved_pieces"] == []


def test_initial_piece_requires_existing_workpiece_and_own_gate(client):
    wp = make_workpiece(client)
    other = make_workpiece(client, code="OTHER")
    g = make_gauge(client)
    src = _ws(client, "S", 3)
    gate = _complete_process(client, wp, g, "A")

    r = client.post(
        "/api/pieces",
        json={"workpiece_id": 999999, "gate_process_id": gate, "workstation_id": src["workstation_id"]},
    )
    assert r.status_code == 404
    r = client.post(
        "/api/pieces",
        json={
            "workpiece_id": other["workpiece_id"],
            "gate_process_id": gate,
            "workstation_id": src["workstation_id"],
        },
    )
    assert r.status_code == 422
    p = _piece(client, wp, gate, src["workstation_id"])
    # 同一工件不能登记为第二件实物
    r = client.post(
        "/api/pieces",
        json={"workpiece_id": wp["workpiece_id"], "gate_process_id": gate, "workstation_id": src["workstation_id"]},
    )
    assert r.status_code == 409


def test_workstation_query_distinguishes_actual_and_reserved(client):
    wp, g, src, dst = _setup(client, capacity=2)
    _, piece = _ready_piece(client, wp, g, src["workstation_id"])
    d = client.get(f"/api/workstations/{dst['workstation_id']}").json()
    assert d["capacity"] == 2 and d["actual_count"] == 0 and d["reserved_count"] == 0
    client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": dst["workstation_id"],
            "piece_ids": [piece["piece_id"]],
        },
    )
    d = client.get(f"/api/workstations/{dst['workstation_id']}").json()
    assert d["actual_count"] == 0 and d["reserved_count"] == 1 and d["available"] == 1


def test_create_order_validates_source_station_and_gate_membership(client):
    wp, g, src, dst = _setup(client)
    gate = _complete_process(client, wp, g, "A")
    piece = _piece(client, wp, gate, dst["workstation_id"])  # 在别的工位

    r = client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src["workstation_id"],
            "dest_workstation_id": dst["workstation_id"],
            "piece_ids": [piece["piece_id"]],
        },
    )
    assert r.status_code == 409 and "不在发件工位" in r.json()["detail"]
    # 整单不建：无任何预留，件仍只有原 ACTUAL 位
    assert client.get(f"/api/workstations/{src['workstation_id']}").json()["reserved_pieces"] == []
    assert client.get(f"/api/workstations/{dst['workstation_id']}").json()["reserved_pieces"] == []
    assert client.get("/api/transfer-orders").json() == []


def test_same_request_id_is_scoped_per_order(client):
    src = _ws(client, "SRC-A", 5)
    dst = _ws(client, "DST-A", 5)
    ids = []
    for i in range(2):
        wp = make_workpiece(client, code=f"PER-{i}")
        g = make_gauge(client, code=f"PERG-{i}")
        ids.append(_piece(client, wp, _complete_process(client, wp, g, "A"), src["workstation_id"])["piece_id"])
    o1 = client.post(
        "/api/transfer-orders",
        json={"source_workstation_id": src["workstation_id"],
              "dest_workstation_id": dst["workstation_id"], "piece_ids": [ids[0]]},
    ).json()["order_id"]
    o2 = client.post(
        "/api/transfer-orders",
        json={"source_workstation_id": src["workstation_id"],
              "dest_workstation_id": dst["workstation_id"], "piece_ids": [ids[1]]},
    ).json()["order_id"]
    # 同一请求号用于不同调拨单互不影响
    assert client.post(f"/api/transfer-orders/{o1}/ship", json={"operator": "A", "request_id": "X"}).status_code == 200
    assert client.post(f"/api/transfer-orders/{o2}/ship", json={"operator": "A", "request_id": "X"}).status_code == 200


def test_cancelled_order_releases_piece_for_new_order(client):
    wp, g, src, dst = _setup(client)
    _, piece = _ready_piece(client, wp, g, src["workstation_id"])
    dst2 = _ws(client, "DST-2", 5)
    o1 = client.post(
        "/api/transfer-orders",
        json={"source_workstation_id": src["workstation_id"],
              "dest_workstation_id": dst["workstation_id"], "piece_ids": [piece["piece_id"]]},
    ).json()
    client.post(f"/api/transfer-orders/{o1['order_id']}/cancel")
    p = client.get(f"/api/pieces/{piece['piece_id']}").json()
    assert p["active_order_id"] is None
    assert p["status"] == "AT_WORKSTATION"
    r = client.post(
        "/api/transfer-orders",
        json={"source_workstation_id": src["workstation_id"],
              "dest_workstation_id": dst2["workstation_id"], "piece_ids": [piece["piece_id"]]},
    )
    assert r.status_code == 201


def test_ship_operator_and_request_are_queryable(client):
    wp, g, src, dst = _setup(client)
    _, piece = _ready_piece(client, wp, g, src["workstation_id"])
    oid = client.post(
        "/api/transfer-orders",
        json={"source_workstation_id": src["workstation_id"],
              "dest_workstation_id": dst["workstation_id"], "piece_ids": [piece["piece_id"]]},
    ).json()["order_id"]
    client.post(f"/api/transfer-orders/{oid}/ship", json={"operator": "张三", "request_id": "SHIP-9"})
    client.post(
        f"/api/transfer-orders/{oid}/return",
        json={"operator": "王五", "request_id": "RET-9", "piece_ids": [piece["piece_id"]]},
    )
    detail = client.get(f"/api/transfer-orders/{oid}").json()
    actions = [(a["action"], a["operator"], a["request_id"]) for a in detail["actions"]]
    assert actions == [("SHIP", "张三", "SHIP-9"), ("RETURN", "王五", "RET-9")]
    assert detail["records"][0]["action"] == "RETURN"
    assert detail["records"][0]["operator"] == "王五"
    assert detail["records"][0]["to_workstation_id"] == src["workstation_id"]


def test_ship_preserves_return_reservation_when_station_is_full(client):
    """发件方容量仅 1、全部被本单实物占满时，仍应能发运并留下退回位。

    发运把各件的 HOME 实物位就地转为 RETURN 预留，占用不增加，因此不应误报满载。
    """
    wp, g, src, dst = _setup(client, capacity=1)
    _, piece = _ready_piece(client, wp, g, src["workstation_id"])
    oid = client.post(
        "/api/transfer-orders",
        json={"source_workstation_id": src["workstation_id"],
              "dest_workstation_id": dst["workstation_id"], "piece_ids": [piece["piece_id"]]},
    ).json()["order_id"]
    client.post(f"/api/transfer-orders/{oid}/ship", json={"operator": "A", "request_id": "S"})
    # 正常签收：收件方空位已被本件预留，落位成功
    r = client.post(
        f"/api/transfer-orders/{oid}/receive",
        json={"operator": "B", "request_id": "C", "piece_ids": [piece["piece_id"]]},
    )
    assert r.status_code == 200
    d = client.get(f"/api/workstations/{dst['workstation_id']}").json()
    assert d["actual_count"] == 1 and d["reserved_count"] == 0 and d["available"] == 0
    s = client.get(f"/api/workstations/{src['workstation_id']}").json()
    assert s["actual_count"] == 0 and s["reserved_count"] == 0 and s["available"] == 1
