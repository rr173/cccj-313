"""FastAPI 入口与路由。

可用环境变量 RELEASE_DB 覆盖 SQLite 文件位置（测试用临时库）。
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import schemas, services

DB_PATH: Optional[Path] = Path(os.environ["RELEASE_DB"]) if os.environ.get("RELEASE_DB") else None


@asynccontextmanager
async def lifespan(app: FastAPI):
    services.ensure_db(DB_PATH)
    yield


app = FastAPI(
    title="机加工工序放行后端",
    version="1.0.0",
    description=(
        "基于工件工序 DAG、尺寸项目整数公差与量具失准窗口的工序放行 / 隔离 / 复核服务。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(services.ServiceError)
def _service_error_handler(request: Request, exc: services.ServiceError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


# --------------------------------------------------------------------- gauges

@app.post("/api/gauges", response_model=schemas.GaugeOut, status_code=201, tags=["量具"])
def create_gauge(payload: schemas.GaugeCreate) -> dict:
    return services.create_gauge(DB_PATH, payload.code, payload.name)


@app.get("/api/gauges", response_model=list[schemas.GaugeOut], tags=["量具"])
def list_gauges() -> list[dict]:
    return services.list_gauges(DB_PATH)


@app.post(
    "/api/gauges/{gauge_id}/out-of-calibration-windows",
    response_model=schemas.GaugeWindowOut,
    status_code=201,
    tags=["量具"],
)
def register_window(gauge_id: int, payload: schemas.GaugeWindowCreate) -> dict:
    return services.register_gauge_window(
        DB_PATH, gauge_id, payload.window_start, payload.window_end, payload.note
    )


@app.get(
    "/api/gauges/{gauge_id}/out-of-calibration-windows",
    response_model=list[schemas.GaugeWindowOut],
    tags=["量具"],
)
def list_windows(gauge_id: int) -> list[dict]:
    return services.list_gauge_windows(DB_PATH, gauge_id)


# ----------------------------------------------------------------- workpieces

@app.post(
    "/api/workpieces",
    response_model=schemas.WorkpieceOut,
    status_code=201,
    tags=["工件与工序"],
)
def create_workpiece(payload: schemas.WorkpieceCreate) -> dict:
    return services.create_workpiece(DB_PATH, payload.model_dump())


@app.get("/api/workpieces/{workpiece_id}", response_model=schemas.WorkpieceOut, tags=["工件与工序"])
def get_workpiece(workpiece_id: int) -> dict:
    return services.get_workpiece(DB_PATH, workpiece_id)


@app.get(
    "/api/workpieces/{workpiece_id}/process-status",
    response_model=list[schemas.ProcessStatusOut],
    tags=["工件与工序"],
)
def workpiece_status(workpiece_id: int) -> list[dict]:
    return services.list_workpiece_statuses(DB_PATH, workpiece_id)


# ----------------------------------------------------------------- inspection

@app.post(
    "/api/inspections",
    response_model=schemas.InspectionOut,
    status_code=201,
    tags=["检验"],
)
def register_inspection(payload: schemas.InspectionCreate) -> dict:
    return services.register_inspection(
        DB_PATH,
        payload.item_id,
        payload.gauge_id,
        payload.reading,
        payload.measured_at,
    )


@app.get(
    "/api/items/{item_id}/inspections",
    response_model=list[schemas.InspectionOut],
    tags=["检验"],
)
def list_inspections(item_id: int) -> list[dict]:
    return services.list_inspections(DB_PATH, item_id)


# ------------------------------------------------------------- process actions

@app.post(
    "/api/processes/{process_id}/start",
    response_model=schemas.ProcessStatusOut,
    tags=["工序动作"],
)
def start_process(process_id: int, payload: schemas.ActionRequest) -> dict:
    return services.start_process(DB_PATH, process_id, payload.operator)


@app.post(
    "/api/processes/{process_id}/complete",
    response_model=schemas.ProcessStatusOut,
    tags=["工序动作"],
)
def complete_process(process_id: int, payload: schemas.ActionRequest) -> dict:
    return services.complete_process(DB_PATH, process_id, payload.operator)


@app.post(
    "/api/processes/{process_id}/review",
    response_model=schemas.ProcessStatusOut,
    tags=["工序动作"],
)
def review_process(process_id: int, payload: schemas.ActionRequest) -> dict:
    return services.review_process(DB_PATH, process_id, payload.operator)


@app.get(
    "/api/processes/{process_id}/status",
    response_model=schemas.ProcessStatusOut,
    tags=["工序动作"],
)
def process_status(process_id: int) -> dict:
    return services.get_process_status(DB_PATH, process_id)


@app.get(
    "/api/processes/{process_id}/credentials",
    response_model=list[schemas.CredentialOut],
    tags=["工序动作"],
)
def process_credentials(process_id: int) -> list[dict]:
    return services.list_credentials(DB_PATH, process_id)


@app.get(
    "/api/processes/{process_id}/events",
    response_model=list[schemas.ProcessEventOut],
    tags=["工序动作"],
)
def process_events(process_id: int) -> list[dict]:
    return services.list_events(DB_PATH, process_id)


@app.get(
    "/api/processes/{process_id}/quarantines",
    response_model=list[schemas.QuarantineOut],
    tags=["工序动作"],
)
def process_quarantines(process_id: int) -> list[dict]:
    return services.list_quarantines(DB_PATH, process_id=process_id)


@app.get("/api/quarantines", response_model=list[schemas.QuarantineOut], tags=["工序动作"])
def all_quarantines(open_only: bool = False) -> list[dict]:
    return services.list_quarantines(DB_PATH, open_only=open_only)
