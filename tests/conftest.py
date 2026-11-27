"""pytest 公共夹具：每个测试一个独立临时 SQLite 库。"""
import os
import sys
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture()
def client(monkeypatch):
    tmp = tempfile.TemporaryDirectory()
    db_path = Path(tmp.name) / "test_release.db"
    monkeypatch.setenv("RELEASE_DB", str(db_path))
    # 确保 app 包所有模块都按本测试的库路径重新导入
    for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
        del sys.modules[mod]
    from app.main import app  # noqa: E402

    with TestClient(app) as c:
        c.db_path = db_path
        yield c
    tmp.cleanup()


# ------------------------------------------------------------------ DAG 工厂

# A -> B -> C -> D，另加 A -> C 的直接依赖
# 每个工序两个尺寸项目，整数公差区间（含边界）
def make_workpiece(client, code="WP-1"):
    payload = {
        "code": code,
        "processes": [
            {
                "code": "A",
                "name": "下料",
                "prerequisites": [],
                "items": [
                    {"code": "A-L", "name": "长度", "lower_limit": 10, "upper_limit": 12},
                    {"code": "A-W", "name": "宽度", "lower_limit": 5, "upper_limit": 5},
                ],
            },
            {
                "code": "B",
                "name": "粗车",
                "prerequisites": ["A"],
                "items": [
                    {"code": "B-D", "name": "外径", "lower_limit": 20, "upper_limit": 22},
                ],
            },
            {
                "code": "C",
                "name": "精车",
                "prerequisites": ["A", "B"],
                "items": [
                    {"code": "C-D", "name": "内径", "lower_limit": 8, "upper_limit": 9},
                ],
            },
            {
                "code": "D",
                "name": "磨削",
                "prerequisites": ["C"],
                "items": [
                    {"code": "D-R", "name": "粗糙度读数", "lower_limit": 1, "upper_limit": 3},
                ],
            },
        ],
    }
    r = client.post("/api/workpieces", json=payload)
    assert r.status_code == 201, r.text
    return r.json()


def make_gauge(client, code="G-1", name="卡尺"):
    r = client.post("/api/gauges", json={"code": code, "name": name})
    assert r.status_code == 201, r.text
    return r.json()


def item_id(wp, proc_code, item_code):
    proc = next(p for p in wp["processes"] if p["code"] == proc_code)
    return next(i["item_id"] for i in proc["items"] if i["code"] == item_code)


def proc_id(wp, proc_code):
    return next(p["process_id"] for p in wp["processes"] if p["code"] == proc_code)


def post_inspection(client, item, gauge, reading, ts="2026-11-27T08:00:00+00:00"):
    return client.post(
        "/api/inspections",
        json={
            "item_id": item,
            "gauge_id": gauge,
            "reading": reading,
            "measured_at": ts,
        },
    )


def status(client, pid):
    return client.get(f"/api/processes/{pid}/status").json()
