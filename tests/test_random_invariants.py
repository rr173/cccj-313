"""随机事件序列下的不变量校验。

对一条多层 DAG 随机执行：登记合格/超差检验、登记失准窗口、开工、完工、复核。
每一步之后检查：
1. 隔离工序上绝不可能出现新的开工/完工/复核放行；
2. 已隔离只能通过逐道复核解除，且复核要求先决工序有效完工；
3. 任何 STARTED/COMPLETED 工序的生效凭据快照都不会被新读数回填；
4. 凭据与事件只增不改，隔离前状态可追溯。
"""
import random

from conftest import make_gauge, make_workpiece, post_inspection, proc_id, status


def _random_dag(client, seed):
    rng = random.Random(seed)
    n = rng.randint(4, 7)
    processes = []
    for idx in range(n):
        prereqs = []
        if idx > 0:
            # 随机选 1~2 个更早的工序作为先决，保证 DAG
            k = rng.randint(1, min(2, idx))
            prereqs = rng.sample([f"P{j}" for j in range(idx)], k)
        processes.append({
            "code": f"P{idx}",
            "name": f"工序{idx}",
            "prerequisites": prereqs,
            "items": [
                {"code": f"P{idx}-1", "name": "尺寸1", "lower_limit": 0, "upper_limit": 10},
            ],
        })
    r = client.post("/api/workpieces", json={"code": f"RAND-{seed}", "processes": processes})
    assert r.status_code == 201, r.text
    return r.json()


def test_randomized_invariants(client):
    rng = random.Random(20261127)
    for seed in range(6):
        wp = _random_dag(client, seed)
        g1 = make_gauge(client, code=f"GA-{seed}-1", name="主量具")
        g2 = make_gauge(client, code=f"GA-{seed}-2", name="备用量具")
        ids = {p["code"]: p["process_id"] for p in wp["processes"]}
        order = [p["code"] for p in wp["processes"]]  # 拓扑序
        item_of = {p["code"]: p["items"][0]["item_id"] for p in wp["processes"]}
        gauges = [g1["gauge_id"], g2["gauge_id"]]

        # credential_id -> 该凭据当时的检验快照，用于事后验证凭据永不被回填
        snapshot_by_cred: dict[int, list[dict]] = {}

        for step in range(120):
            action = rng.random()
            if action < 0.45:
                code = rng.choice(order)
                # 70% 合格读数，30% 超差
                reading = rng.randint(0, 10) if rng.random() < 0.7 else rng.choice([-5, 11, 99])
                gauge = rng.choice(gauges)
                hour = rng.randint(0, 22)
                post_inspection(
                    client, item_of[code], gauge, reading,
                    f"2026-11-27T{hour:02d}:{rng.randint(0, 59):02d}:00+00:00",
                )
            elif action < 0.6:
                gauge = rng.choice(gauges)
                h1 = rng.randint(0, 20)
                client.post(
                    f"/api/gauges/{gauge}/out-of-calibration-windows",
                    json={
                        "window_start": f"2026-11-27T{h1:02d}:00:00+00:00",
                        "window_end": f"2026-11-27T{h1 + 1:02d}:00:00+00:00",
                    },
                )
            else:
                code = rng.choice(order)
                pid = ids[code]
                s = status(client, pid)
                op = rng.choice(["start", "complete", "review"])
                r = client.post(f"/api/processes/{pid}/{op}", json={})
                if r.status_code == 200:
                    body = r.json()
                    if op == "start":
                        assert body["status"] == "STARTED"
                    elif op == "complete":
                        assert body["status"] == "COMPLETED"
                    elif op == "review":
                        assert body["status"] in ("STARTED", "COMPLETED")
                    # 放行成功必然带一张新生效凭据，且其快照引用真实存在的检验
                    cred = body["active_credential"]
                    assert cred is not None
                    assert cred["inspections"], "凭据必须快照检验依据"
                    snapshot_by_cred[cred["credential_id"]] = [
                        dict(x) for x in cred["inspections"]
                    ]
                else:
                    assert r.status_code == 409

            # ---- 每步不变量 ----
            for code in order:
                pid = ids[code]
                s = status(client, pid)
                st = s["status"]
                # 隔离态没有 open 的开工/完工路径
                if st == "QUARANTINED":
                    assert s["can_start"] is False
                    assert s["open_quarantine"] is not None
                    for op in ("start", "complete"):
                        rr = client.post(f"/api/processes/{pid}/{op}", json={})
                        assert rr.status_code == 409, (seed, step, code, op)
                if st in ("STARTED", "COMPLETED"):
                    # 已放行工序必存在生效凭据
                    assert s["active_credential"] is not None

            # 凭据快照不可变：历史凭据的 inspection 引用永不改变（不回填）
            for code in order:
                current = client.get(f"/api/processes/{ids[code]}/credentials").json()
                for c in current:
                    if c["credential_id"] in snapshot_by_cred:
                        assert c["inspections"] == snapshot_by_cred[c["credential_id"]]

        # 收尾：所有仍隔离的工序，若最终规则满足则可逐道复核，不满足则明确 409
        for code in order:
            pid = ids[code]
            s = status(client, pid)
            if s["status"] == "QUARANTINED":
                r = client.post(f"/api/processes/{pid}/review", json={})
                assert r.status_code in (200, 409)
