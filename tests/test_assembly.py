"""实装 / 拆件、缓冲位账本、整件发运与发运追溯证据。"""
import multiprocessing as mp

import pytest

from conftest import item_id, make_gauge, make_workpiece, post_inspection, proc_id


# --------------------------------------------------------------------- helpers

def _ws(client, code, capacity):
    r = client.post("/api/workstations", json={"code": code, "name": code, "capacity": capacity})
    assert r.status_code == 201, r.text
    return r.json()


def _complete_gate(client, wp, gauge, code="A"):
    """把指定工序及其先决做完，返回关口工序 id。"""
    p = next(x for x in wp["processes"] if x["code"] == code)
    for pre_id in p["prerequisites"]:
        pre = next(x for x in wp["processes"] if x["process_id"] == pre_id)
        if client.get(f"/api/processes/{pre_id}/status").json()["status"] != "COMPLETED":
            _complete_gate(client, wp, gauge, pre["code"])
    for it in p["items"]:
        post_inspection(
            client, it["item_id"], gauge["gauge_id"], (it["lower_limit"] + it["upper_limit"]) // 2
        )
    pid = p["process_id"]
    assert client.post(f"/api/processes/{pid}/start", json={}).status_code == 200
    assert client.post(f"/api/processes/{pid}/complete", json={}).status_code == 200
    return pid


def _make_piece(client, code, ws_id, gate_code="A", gauge_code=None):
    """建工件 + 量具，关口完工后登记为实物件，返回 (wp, gauge, piece)。"""
    wp = make_workpiece(client, code=f"WP-{code}")
    g = make_gauge(client, code=gauge_code or f"G-{code}")
    gate = _complete_gate(client, wp, g, gate_code)
    r = client.post(
        "/api/pieces",
        json={"workpiece_id": wp["workpiece_id"], "gate_process_id": gate, "workstation_id": ws_id},
    )
    assert r.status_code == 201, r.text
    return wp, g, r.json()


def _install(client, carrier, child, operator="装配工", ws_id=None):
    return client.post(
        "/api/assemblies/install",
        json={
            "carrier_piece_id": carrier,
            "installed_piece_id": child,
            "operator": operator,
            "workstation_id": ws_id,
        },
    )


def _remove(client, carrier, child, operator="返工工", ws_id=None):
    return client.post(
        "/api/assemblies/remove",
        json={
            "carrier_piece_id": carrier,
            "child_piece_id": child,
            "operator": operator,
            "workstation_id": ws_id,
        },
    )


def _order(client, src, dst, piece_ids):
    src_id = src["workstation_id"] if isinstance(src, dict) else src
    dst_id = dst["workstation_id"] if isinstance(dst, dict) else dst
    return client.post(
        "/api/transfer-orders",
        json={
            "source_workstation_id": src_id,
            "dest_workstation_id": dst_id,
            "piece_ids": piece_ids,
        },
    )


# --------------------------------------------------------------------- 实装基础

def test_install_frees_slot_and_inner_piece_occupies_nothing(client):
    ws = _ws(client, "WS-1", 3)
    _, _, p1 = _make_piece(client, "1", ws["workstation_id"])
    _, _, p2 = _make_piece(client, "2", ws["workstation_id"])
    _, _, p3 = _make_piece(client, "3", ws["workstation_id"])

    detail = client.get(f"/api/workstations/{ws['workstation_id']}").json()
    assert detail["actual_count"] == 3 and detail["available"] == 0

    # P2 实装进 P1：腾出 P2 原来的位置，P1 整件仍只占一位
    r = _install(client, p1["piece_id"], p2["piece_id"])
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["piece_id"] == p1["piece_id"]
    assert body["direct_child_piece_ids"] == [p2["piece_id"]]
    assert body["root_piece_id"] == p1["piece_id"]
    assert {m["piece_id"] for m in body["composition"]["tree"]} == {
        p1["piece_id"],
        p2["piece_id"],
    }

    detail = client.get(f"/api/workstations/{ws['workstation_id']}").json()
    assert sorted(detail["actual_pieces"]) == sorted([p1["piece_id"], p3["piece_id"]])
    assert detail["actual_count"] == 2 and detail["available"] == 1

    # 内部子件不占位、可查到承载件与最外层整件，工位随整件
    inner = client.get(f"/api/pieces/{p2['piece_id']}").json()
    assert inner["is_inner"] is True
    assert inner["direct_parent_piece_id"] == p1["piece_id"]
    assert inner["root_piece_id"] == p1["piece_id"]
    assert inner["direct_child_piece_ids"] == []
    assert inner["current_workstation_id"] == ws["workstation_id"]
    assert inner["active_order_id"] is None

    comp = client.get(f"/api/pieces/{p2['piece_id']}/composition").json()
    assert comp["direct_parent_piece_id"] == p1["piece_id"]
    assert comp["root_piece_id"] == p1["piece_id"]
    assert [(m["piece_id"], m["depth"]) for m in comp["tree"]] == [(p2["piece_id"], 0)]

    # 承载件也可以是更大总成的内件：P3 装到 P2 上，形成三层
    r = _install(client, p2["piece_id"], p3["piece_id"])
    assert r.status_code == 201, r.text
    detail = client.get(f"/api/workstations/{ws['workstation_id']}").json()
    assert detail["actual_pieces"] == [p1["piece_id"]]
    assert detail["available"] == 2
    tree = client.get(f"/api/pieces/{p1['piece_id']}/composition").json()["tree"]
    assert [(m["piece_id"], m["parent_piece_id"], m["depth"]) for m in tree] == [
        (p1["piece_id"], None, 0),
        (p2["piece_id"], p1["piece_id"], 1),
        (p3["piece_id"], p2["piece_id"], 2),
    ]
    # 装拆留痕：人、件、工位齐全
    events = client.get("/api/assembly-events").json()
    assert [(e["action"], e["operator"]) for e in events] == [
        ("INSTALL", "装配工"),
        ("INSTALL", "装配工"),
    ]
    assert all(e["workstation_id"] == ws["workstation_id"] for e in events)


def test_install_rejects_inner_child_cross_station_and_same_root(client):
    ws1 = _ws(client, "WS-A", 5)
    ws2 = _ws(client, "WS-B", 5)
    _, _, p1 = _make_piece(client, "1", ws1["workstation_id"])
    _, _, p2 = _make_piece(client, "2", ws1["workstation_id"])
    _, _, p3 = _make_piece(client, "3", ws2["workstation_id"])

    # 不同工位不能实装
    r = _install(client, p1["piece_id"], p3["piece_id"])
    assert r.status_code == 409 and "同一工位" in r.json()["detail"]

    # 指定工位与实际不符
    r = _install(client, p1["piece_id"], p2["piece_id"], ws_id=ws2["workstation_id"])
    assert r.status_code == 409

    assert _install(client, p1["piece_id"], p2["piece_id"]).status_code == 201
    # 待装件已装在别的件里
    r = _install(client, p1["piece_id"], p2["piece_id"])
    assert r.status_code == 409 and "已实装" in r.json()["detail"]
    # 同一整件内不能互相实装
    r = _install(client, p2["piece_id"], p1["piece_id"])
    assert r.status_code == 409 and "同一整件" in r.json()["detail"]
    # 内部子件不能列入调拨单
    r = _order(client, ws1, ws2, [p2["piece_id"]])
    assert r.status_code == 409 and "最外层整件" in r.json()["detail"]
    # 失败不留半套：组成仍是 P1 -> P2
    tree = client.get(f"/api/pieces/{p1['piece_id']}/composition").json()["tree"]
    assert len(tree) == 2


def test_installed_piece_may_carry_its_own_subtree(client):
    ws = _ws(client, "WS-X", 5)
    _, _, a = _make_piece(client, "A", ws["workstation_id"])
    _, _, b = _make_piece(client, "B", ws["workstation_id"])
    _, _, c = _make_piece(client, "C", ws["workstation_id"])
    assert _install(client, b["piece_id"], c["piece_id"]).status_code == 201
    # B 自带子件 C，整体作为待装件装进 A
    r = _install(client, a["piece_id"], b["piece_id"])
    assert r.status_code == 201
    tree = client.get(f"/api/pieces/{a['piece_id']}/composition").json()["tree"]
    assert [(m["piece_id"], m["depth"]) for m in tree] == [
        (a["piece_id"], 0),
        (b["piece_id"], 1),
        (c["piece_id"], 2),
    ]
    detail = client.get(f"/api/workstations/{ws['workstation_id']}").json()
    assert detail["actual_pieces"] == [a["piece_id"]]


# --------------------------------------------------------------------- 拆件

def test_remove_requires_direct_parent_and_takes_subtree_to_free_slot(client):
    ws = _ws(client, "WS-R", 3)
    _, _, p1 = _make_piece(client, "R1", ws["workstation_id"])
    _, _, p2 = _make_piece(client, "R2", ws["workstation_id"])
    _, _, p3 = _make_piece(client, "R3", ws["workstation_id"])
    _install(client, p1["piece_id"], p2["piece_id"])
    _install(client, p2["piece_id"], p3["piece_id"])
    assert client.get(f"/api/workstations/{ws['workstation_id']}").json()["available"] == 2

    # 只能从直接承载件拆：P3 的直接承载件是 P2，不是 P1
    r = _remove(client, p1["piece_id"], p3["piece_id"])
    assert r.status_code == 409 and "直接承载件" in r.json()["detail"]

    # 拆 P2：P2 连同下级 P3 一起拆下，整组在同工位占一个空位
    r = _remove(client, p1["piece_id"], p2["piece_id"])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["piece_id"] == p2["piece_id"]
    assert body["root_piece_id"] == p2["piece_id"]
    assert {m["piece_id"] for m in body["composition"]["tree"]} == {
        p2["piece_id"],
        p3["piece_id"],
    }
    detail = client.get(f"/api/workstations/{ws['workstation_id']}").json()
    assert sorted(detail["actual_pieces"]) == sorted([p1["piece_id"], p2["piece_id"]])
    assert detail["available"] == 1

    # P2 重新成为最外层整件，可以单独上单
    ws2 = _ws(client, "WS-RR", 3)
    assert _order(client, ws, ws2, [p2["piece_id"]]).status_code == 201

    # 拆件留痕记下随拆的整组成员
    event = [e for e in client.get("/api/assembly-events").json() if e["action"] == "REMOVE"][0]
    assert event["parent_piece_id"] == p1["piece_id"]
    assert event["child_piece_id"] == p2["piece_id"]
    import json

    assert set(json.loads(event["detail"])["removed_member_piece_ids"]) == {
        p2["piece_id"],
        p3["piece_id"],
    }


def test_remove_blocked_when_no_real_free_slot_reserved_does_not_count(client):
    wsa = _ws(client, "WS-FULL", 2)
    wsb = _ws(client, "WS-OTHER", 2)
    _, _, p1 = _make_piece(client, "F1", wsa["workstation_id"])
    _, _, p2 = _make_piece(client, "F2", wsa["workstation_id"])
    _, _, p4 = _make_piece(client, "F4", wsb["workstation_id"])
    _install(client, p1["piece_id"], p2["piece_id"])
    # 装配后 WSA：1 个 ACTUAL，空 1 位
    assert client.get(f"/api/workstations/{wsa['workstation_id']}").json()["available"] == 1

    # 从 WSB 建单到 WSA，把唯一空位变成收件预留（已预留不算空位）
    r = _order(client, wsb, wsa, [p4["piece_id"]])
    assert r.status_code == 201
    detail = client.get(f"/api/workstations/{wsa['workstation_id']}").json()
    assert detail["available"] == 0 and detail["reserved_count"] == 1

    events_before = len(client.get("/api/assembly-events").json())
    r = _remove(client, p1["piece_id"], p2["piece_id"])
    assert r.status_code == 409 and "空缓冲位" in r.json()["detail"]
    # 失败回滚：组成、货位、留痕都不变
    assert len(client.get("/api/assembly-events").json()) == events_before
    tree = client.get(f"/api/pieces/{p1['piece_id']}/composition").json()["tree"]
    assert {m["piece_id"] for m in tree} == {p1["piece_id"], p2["piece_id"]}
    detail = client.get(f"/api/workstations/{wsa['workstation_id']}").json()
    assert detail["actual_pieces"] == [p1["piece_id"]]

    # 撤销单据释放预留后即可拆
    pending = client.get("/api/transfer-orders").json()
    order_id = next(o["order_id"] for o in pending if o["dest_workstation_id"] == wsa["workstation_id"])
    assert client.post(f"/api/transfer-orders/{order_id}/cancel").status_code == 200
    assert _remove(client, p1["piece_id"], p2["piece_id"]).status_code == 200


# ------------------------------------------------- 待发 / 在途冻结组成，质量冻结不拦

def test_active_order_freezes_composition_on_both_roots(client):
    ws1 = _ws(client, "WS-P", 5)
    ws2 = _ws(client, "WS-Q", 5)
    _, _, a = _make_piece(client, "PA", ws1["workstation_id"])
    _, _, b = _make_piece(client, "PB", ws1["workstation_id"])
    _, _, c = _make_piece(client, "PC", ws2["workstation_id"])

    oid = _order(client, ws1, ws2, [a["piece_id"]]).json()["order_id"]
    # 待发：承载整件不能改组成
    r = _install(client, a["piece_id"], b["piece_id"])
    assert r.status_code == 409 and "待发或在途" in r.json()["detail"]
    # 待装整件本身在单上也不行（c 在 WS-Q，先因不同工位被拦，再换同工位验证冻结）
    r = _install(client, c["piece_id"], a["piece_id"])
    assert r.status_code == 409 and "同一工位" in r.json()["detail"]
    r = _install(client, b["piece_id"], a["piece_id"])
    assert r.status_code == 409 and "待发或在途" in r.json()["detail"]

    assert client.post(
        f"/api/transfer-orders/{oid}/ship", json={"operator": "司机", "request_id": "S1"}
    ).status_code == 200
    # 在途同样不能装 / 拆；与本单无关的两件（b、c）因不同工位也被拦
    r = _install(client, c["piece_id"], b["piece_id"])
    assert r.status_code == 409 and "同一工位" in r.json()["detail"]  # b 在 WS-P、c 在 WS-Q
    r = _install(client, a["piece_id"], c["piece_id"])
    assert r.status_code == 409 and "在途" in r.json()["detail"]
    # 落地到 WS-Q 后组成恢复可改；c 已在 WS-Q，同工位可装
    assert client.post(
        f"/api/transfer-orders/{oid}/receive",
        json={"operator": "收件", "request_id": "R1", "piece_ids": [a["piece_id"]]},
    ).status_code == 200
    assert _install(client, a["piece_id"], c["piece_id"]).status_code == 201


def test_quality_frozen_root_can_still_be_disassembled_for_rework(client):
    ws1 = _ws(client, "WS-Z", 5)
    ws2 = _ws(client, "WS-Z2", 5)
    wp_b, g_b, b = _make_piece(client, "ZB", ws1["workstation_id"])
    _, _, a = _make_piece(client, "ZA", ws1["workstation_id"])
    _install(client, a["piece_id"], b["piece_id"])
    oid = _order(client, ws1, ws2, [a["piece_id"]]).json()["order_id"]
    assert client.post(
        f"/api/transfer-orders/{oid}/ship", json={"operator": "司机", "request_id": "S"}
    ).status_code == 200

    # 在途时子件关口失效（超差 → 隔离），整件当前质量冻结
    post_inspection(client, item_id(wp_b, "A", "A-L"), g_b["gauge_id"], 999)
    gate = proc_id(wp_b, "A")
    assert client.get(f"/api/processes/{gate}/status").json()["status"] == "QUARANTINED"
    # 质量变化不阻止如实落地
    assert client.post(
        f"/api/transfer-orders/{oid}/receive",
        json={"operator": "收件", "request_id": "R", "piece_ids": [a["piece_id"]]},
    ).status_code == 200
    root = client.get(f"/api/pieces/{a['piece_id']}").json()
    assert root["quality_frozen"] is True and root["gate_valid"] is False

    # 质量冻结的在位整件仍可拆换返工
    r = _remove(client, a["piece_id"], b["piece_id"])
    assert r.status_code == 200, r.text
    # 装拆不改变检验 / 完工 / 隔离记录
    assert client.get(f"/api/processes/{gate}/status").json()["status"] == "QUARANTINED"
    creds = client.get(f"/api/processes/{gate}/credentials").json()
    assert [c["kind"] for c in creds] == ["START", "COMPLETE"]


def test_inner_piece_physical_location_tracks_root_through_transfer_and_remove(client):
    """回归：内部子件自身的 piece 行在整件发运 / 落地后必须同步物理工位，
    否则拆出成为新整件时会带着旧工位，与实际缓冲位矛盾。"""
    src = _ws(client, "L-SRC", 5)
    dst = _ws(client, "L-DST", 5)
    _, _, a = _make_piece(client, "LA", src["workstation_id"])
    _, _, b = _make_piece(client, "LB", src["workstation_id"])
    _install(client, a["piece_id"], b["piece_id"])
    oid = _order(client, src, dst, [a["piece_id"]]).json()["order_id"]
    assert client.post(
        f"/api/transfer-orders/{oid}/ship",
        json={"operator": "司机", "request_id": "LS"},
    ).status_code == 200
    # 在途：内件随整件在途
    inner = client.get(f"/api/pieces/{b['piece_id']}").json()
    assert inner["status"] == "IN_TRANSIT" and inner["is_inner"] is True
    assert client.post(
        f"/api/transfer-orders/{oid}/receive",
        json={"operator": "收件", "request_id": "LR", "piece_ids": [a["piece_id"]]},
    ).status_code == 200
    # 落地：内件工位同步到收件方
    inner = client.get(f"/api/pieces/{b['piece_id']}").json()
    assert inner["status"] == "AT_WORKSTATION"
    assert inner["current_workstation_id"] == dst["workstation_id"]

    # 在收件方拆下 B：新整件 B 的工位必须是收件方，与它新占的缓冲位一致
    r = _remove(client, a["piece_id"], b["piece_id"])
    assert r.status_code == 200, r.text
    new_root = r.json()
    assert new_root["current_workstation_id"] == dst["workstation_id"]
    assert new_root["root_piece_id"] == b["piece_id"]
    d = client.get(f"/api/workstations/{dst['workstation_id']}").json()
    assert sorted(d["actual_pieces"]) == sorted([a["piece_id"], b["piece_id"]])


def test_root_freeze_aggregates_all_member_gates(client):
    ws = _ws(client, "WS-AGG", 5)
    wp_b, g_b, b = _make_piece(client, "GB", ws["workstation_id"])
    _, _, a = _make_piece(client, "GA", ws["workstation_id"])
    _install(client, a["piece_id"], b["piece_id"])
    root = client.get(f"/api/pieces/{a['piece_id']}").json()
    assert root["quality_frozen"] is False

    # 子件关口失效：整件冻结；子件自身视角也冻结
    post_inspection(client, item_id(wp_b, "A", "A-L"), g_b["gauge_id"], 999)
    assert client.get(f"/api/pieces/{a['piece_id']}").json()["quality_frozen"] is True
    assert client.get(f"/api/pieces/{b['piece_id']}").json()["quality_frozen"] is True


# ----------------------------------------------------- 整件发运：逐层关口 + 快照

def test_ship_checks_every_member_gate_and_snapshots_evidence(client):
    src = _ws(client, "SRC-A", 5)
    dst = _ws(client, "DST-A", 5)
    wp1, g1, p1 = _make_piece(client, "H1", src["workstation_id"])
    wp2, g2, p2 = _make_piece(client, "H2", src["workstation_id"])
    wp3, g3, p3 = _make_piece(client, "H3", src["workstation_id"])
    _install(client, p1["piece_id"], p2["piece_id"])
    _install(client, p2["piece_id"], p3["piece_id"])

    oid = _order(client, src, dst, [p1["piece_id"]]).json()["order_id"]
    # 让第二层子件 P3 关口失效：整单不发
    post_inspection(client, item_id(wp3, "A", "A-L"), g3["gauge_id"], 999)
    r = client.post(
        f"/api/transfer-orders/{oid}/ship", json={"operator": "司机", "request_id": "S-BAD"}
    )
    assert r.status_code == 409 and "整单仍待发" in r.json()["detail"]
    assert f"{p3['piece_id']}" in r.json()["detail"]
    assert client.get(f"/api/transfer-orders/{oid}").json()["status"] == "PENDING"
    # 失败不占号、无快照
    assert client.get(f"/api/transfer-orders/{oid}/shipment-trace").json()["snapshots"] == []

    # 复核恢复 P3
    gate3 = proc_id(wp3, "A")
    g_fix = make_gauge(client, code="G-FIX")
    post_inspection(
        client, item_id(wp3, "A", "A-L"), g_fix["gauge_id"], 11,
        "2026-12-02T08:00:00+00:00",
    )
    assert client.post(f"/api/processes/{gate3}/review", json={"operator": "质量员"}).status_code == 200

    assert client.post(
        f"/api/transfer-orders/{oid}/ship", json={"operator": "司机", "request_id": "S-OK"}
    ).status_code == 200

    trace = client.get(f"/api/transfer-orders/{oid}/shipment-trace").json()
    assert len(trace["snapshots"]) == 1
    snap = trace["snapshots"][0]
    assert snap["root_piece_id"] == p1["piece_id"]
    by_piece = {m["piece_id"]: m for m in snap["members"]}
    assert [m["depth"] for m in snap["members"]] == [0, 1, 2]
    for pid in (p1["piece_id"], p2["piece_id"], p3["piece_id"]):
        m = by_piece[pid]
        assert m["credential"] is not None
        # P3 经历过隔离复核，其生效凭据为 REVIEW；其余为 START
        assert m["credential"]["credential_kind"] in ("START", "REVIEW")
        assert m["credential"]["inspections"], "凭据须带当时生效的检验快照"
    assert by_piece[p3["piece_id"]]["credential"]["credential_kind"] == "REVIEW"

    # 在途时内部子件随整件在途
    inner = client.get(f"/api/pieces/{p2['piece_id']}").json()
    assert inner["status"] == "IN_TRANSIT" and inner["root_piece_id"] == p1["piece_id"]

    # 签收落地：子件工位随整件到收件方
    assert client.post(
        f"/api/transfer-orders/{oid}/receive",
        json={"operator": "收件", "request_id": "RCV", "piece_ids": [p1["piece_id"]]},
    ).status_code == 200
    inner = client.get(f"/api/pieces/{p2['piece_id']}").json()
    assert inner["status"] == "AT_WORKSTATION"
    assert inner["current_workstation_id"] == dst["workstation_id"]
    # 收件方只有整件一个实物位
    detail = client.get(f"/api/workstations/{dst['workstation_id']}").json()
    assert detail["actual_pieces"] == [p1["piece_id"]]


def test_historical_snapshot_survives_later_disassembly_and_rework(client):
    src = _ws(client, "SRC-H", 5)
    dst = _ws(client, "DST-H", 5)
    wp_a, g_a, a = _make_piece(client, "HA", src["workstation_id"])
    wp_b, g_b, b = _make_piece(client, "HB", src["workstation_id"])
    wp_c, g_c, c = _make_piece(client, "HC", src["workstation_id"])
    _install(client, a["piece_id"], b["piece_id"])

    oid1 = _order(client, src, dst, [a["piece_id"]]).json()["order_id"]
    assert client.post(
        f"/api/transfer-orders/{oid1}/ship", json={"operator": "司机", "request_id": "S1"}
    ).status_code == 200
    snap1 = client.get(f"/api/transfer-orders/{oid1}/shipment-trace").json()["snapshots"][0]
    assert {m["piece_id"] for m in snap1["members"]} == {a["piece_id"], b["piece_id"]}
    b_credential = next(
        m["credential"]["credential_id"]
        for m in snap1["members"]
        if m["piece_id"] == b["piece_id"]
    )

    # 落地后拆下 B、换装 C，再发第二单
    assert client.post(
        f"/api/transfer-orders/{oid1}/receive",
        json={"operator": "收件", "request_id": "R1", "piece_ids": [a["piece_id"]]},
    ).status_code == 200
    assert _remove(client, a["piece_id"], b["piece_id"]).status_code == 200
    # B 拆在收件方，C 也得到收件方才能装：先把 C 调过来
    ws_mid = _ws(client, "WS-MID", 5)
    oid_c = _order(client, src, dst, [c["piece_id"]]).json()["order_id"]
    assert client.post(
        f"/api/transfer-orders/{oid_c}/ship", json={"operator": "司机", "request_id": "SC"}
    ).status_code == 200
    assert client.post(
        f"/api/transfer-orders/{oid_c}/receive",
        json={"operator": "收件", "request_id": "RC", "piece_ids": [c["piece_id"]]},
    ).status_code == 200
    assert _install(client, a["piece_id"], c["piece_id"]).status_code == 201
    oid2 = _order(client, dst, ws_mid, [a["piece_id"]]).json()["order_id"]
    assert client.post(
        f"/api/transfer-orders/{oid2}/ship", json={"operator": "司机", "request_id": "S2"}
    ).status_code == 200

    # 历史第一单快照仍是 A+B，且凭据仍是当时那张
    trace1 = client.get(f"/api/transfer-orders/{oid1}/shipment-trace").json()
    old_b = next(m for m in trace1["snapshots"][0]["members"] if m["piece_id"] == b["piece_id"])
    assert old_b["credential"]["credential_id"] == b_credential
    # 第二单快照是 A+C
    trace2 = client.get(f"/api/transfer-orders/{oid2}/shipment-trace").json()
    assert {m["piece_id"] for m in trace2["snapshots"][0]["members"]} == {
        a["piece_id"],
        c["piece_id"],
    }

    # 对 B 补测 + 复核生成新生效凭据，历史快照仍引用旧凭据：
    # 先用失准量具的超差读数造成隔离，再用有效量具补测合格，最后复核
    gate_b = proc_id(wp_b, "A")
    post_inspection(
        client, item_id(wp_b, "A", "A-L"), g_b["gauge_id"], 999,
        "2026-12-03T08:00:00+00:00",
    )
    assert client.get(f"/api/processes/{gate_b}/status").json()["status"] == "QUARANTINED"
    g_fix2 = make_gauge(client, code="G-FIX2")
    post_inspection(
        client, item_id(wp_b, "A", "A-L"), g_fix2["gauge_id"], 11,
        "2026-12-03T09:00:00+00:00",
    )
    post_inspection(
        client, item_id(wp_b, "A", "A-W"), g_fix2["gauge_id"], 5,
        "2026-12-03T09:05:00+00:00",
    )
    assert client.post(
        f"/api/processes/{gate_b}/review", json={"operator": "质量员"}
    ).status_code == 200
    old_b = next(
        m for m in client.get(f"/api/transfer-orders/{oid1}/shipment-trace").json()["snapshots"][0]["members"]
        if m["piece_id"] == b["piece_id"]
    )
    assert old_b["credential"]["credential_id"] == b_credential
    # 新复核凭据与历史快照引用的不是同一张
    current = client.get(f"/api/processes/{gate_b}/credentials").json()
    assert current[-1]["kind"] == "REVIEW"
    assert current[-1]["credential_id"] != b_credential


# ------------------------------------------------------------- 量具失准影响追溯

def test_gauge_window_impact_finds_only_historical_credential_measurements(client):
    src = _ws(client, "SRC-G", 5)
    dst = _ws(client, "DST-G", 5)
    wp_a, g_a, a = _make_piece(client, "GA", src["workstation_id"])
    wp_b, g_b, b = _make_piece(client, "GB", src["workstation_id"])
    _install(client, a["piece_id"], b["piece_id"])
    oid = _order(client, src, dst, [a["piece_id"]]).json()["order_id"]
    assert client.post(
        f"/api/transfer-orders/{oid}/ship", json={"operator": "司机", "request_id": "S"}
    ).status_code == 200

    # 登记失准窗口，覆盖 B 关口测量时间（默认 2026-11-27T08:00:00Z）
    win = client.post(
        f"/api/gauges/{g_b['gauge_id']}/out-of-calibration-windows",
        json={
            "window_start": "2026-11-27T07:00:00+00:00",
            "window_end": "2026-11-27T09:00:00+00:00",
            "note": "事后发现失准",
        },
    ).json()
    impact = client.get(
        f"/api/gauges/{g_b['gauge_id']}/out-of-calibration-windows/{win['window_id']}/impact"
    ).json()
    assert len(impact) >= 1
    rows = [r for r in impact if r["order_id"] == oid]
    assert {r["member_piece_id"] for r in rows} == {b["piece_id"]}
    row = rows[0]
    assert row["root_piece_id"] == a["piece_id"]
    assert row["credential_kind"] == "START"
    assert row["window_id"] == win["window_id"]
    # A 用的是另一把量具，不受影响
    assert all(r["member_piece_id"] != a["piece_id"] for r in impact)

    # 不相关量具 / 窗口返回空
    other_win = client.post(
        f"/api/gauges/{g_a['gauge_id']}/out-of-calibration-windows",
        json={
            "window_start": "2026-12-10T00:00:00+00:00",
            "window_end": "2026-12-11T00:00:00+00:00",
        },
    ).json()
    assert client.get(
        f"/api/gauges/{g_a['gauge_id']}/out-of-calibration-windows/{other_win['window_id']}/impact"
    ).json() == []
    # 未知窗口 404
    assert client.get(
        f"/api/gauges/{g_b['gauge_id']}/out-of-calibration-windows/999999/impact"
    ).status_code == 404

    # 拆换 + 复核后历史依据不变
    client.post(
        f"/api/transfer-orders/{oid}/receive",
        json={"operator": "收件", "request_id": "R", "piece_ids": [a["piece_id"]]},
    )
    _remove(client, a["piece_id"], b["piece_id"])
    impact_after = client.get(
        f"/api/gauges/{g_b['gauge_id']}/out-of-calibration-windows/{win['window_id']}/impact"
    ).json()
    assert [
        (r["order_id"], r["member_piece_id"], r["inspection_id"]) for r in impact_after
    ] == [(r["order_id"], r["member_piece_id"], r["inspection_id"]) for r in impact]


# ----------------------------------------------------------------- 并发一致性

def _install_worker(db_path, carrier, child, barrier, queue):
    from app.services import ServiceError, install_assembly

    barrier.wait()
    try:
        queue.put(("install", {"ok": True, "result": install_assembly(
            db_path, carrier, child, "并发装配工", None)}))
    except ServiceError as exc:
        queue.put(("install", {"ok": False, "status": exc.status_code, "detail": exc.detail}))


def _ship_worker(db_path, order_id, barrier, queue):
    from app.services import ServiceError, ship_transfer_order

    barrier.wait()
    try:
        queue.put(("ship", {"ok": True, "result": ship_transfer_order(
            db_path, order_id, "并发司机", "RACE-SHIP")}))
    except ServiceError as exc:
        queue.put(("ship", {"ok": False, "status": exc.status_code, "detail": exc.detail}))


def test_concurrent_install_vs_ship_never_leaves_inconsistent_state(client):
    """装 P2 进 P1 与发运 P1 竞争：两种串行结果都必须自洽。

    * 发运先：发运成功（旧组成），装配因整件在途被 409；
    * 装配先：装配成功；P2 关口未完工时发运必须 409，单保持待发。
    """
    src = _ws(client, "CSRC", 5)
    dst = _ws(client, "CDST", 5)
    wp1 = make_workpiece(client, code="WP-C1")
    g1 = make_gauge(client, code="G-C1")
    gate1 = _complete_gate(client, wp1, g1)
    p1 = client.post(
        "/api/pieces",
        json={"workpiece_id": wp1["workpiece_id"], "gate_process_id": gate1,
              "workstation_id": src["workstation_id"]},
    ).json()
    # P2 只建档不做关口：若装配先提交，发运必被它拦住
    wp2 = make_workpiece(client, code="WP-C2")
    p2 = client.post(
        "/api/pieces",
        json={"workpiece_id": wp2["workpiece_id"], "gate_process_id": wp2["processes"][0]["process_id"],
              "workstation_id": src["workstation_id"]},
    ).json()
    oid = _order(client, src, dst, [p1["piece_id"]]).json()["order_id"]

    ctx = mp.get_context("fork")
    barrier = ctx.Barrier(2)
    queue = ctx.Queue()
    procs = [
        ctx.Process(target=_install_worker, args=(
            str(client.db_path), p1["piece_id"], p2["piece_id"], barrier, queue)),
        ctx.Process(target=_ship_worker, args=(str(client.db_path), oid, barrier, queue)),
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(60)
        assert p.exitcode == 0
    results = dict(queue.get(timeout=10) for _ in range(2))

    order = client.get(f"/api/transfer-orders/{oid}").json()
    comp = client.get(f"/api/pieces/{p1['piece_id']}/composition").json()
    if results["ship"]["ok"]:
        # 发运先提交：装配必须被拒，组成只有 P1，单已发运
        assert results["install"]["ok"] is False
        assert results["install"]["status"] == 409
        assert order["status"] == "SHIPPED"
        assert [m["piece_id"] for m in comp["tree"]] == [p1["piece_id"]]
    else:
        # 装配先提交：发运被未完工的 P2 关口拦下，单仍待发，组成含 P2
        assert results["ship"]["status"] == 409
        assert results["install"]["ok"] is True
        assert order["status"] == "PENDING"
        assert {m["piece_id"] for m in comp["tree"]} == {p1["piece_id"], p2["piece_id"]}

    # 缓冲位守恒：工位实际在位数量始终等于最外层整件数量
    detail = client.get(f"/api/workstations/{src['workstation_id']}").json()
    roots = [p for p in client.get("/api/pieces").json() if not p["is_inner"]
             and p["current_workstation_id"] == src["workstation_id"]
             and p["status"] == "AT_WORKSTATION"]
    assert detail["actual_count"] == len(roots)
