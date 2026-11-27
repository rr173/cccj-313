"""Pydantic 请求 / 响应模型。"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ---------- 量具 ----------

class GaugeCreate(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=128)


class GaugeOut(BaseModel):
    gauge_id: int
    code: str
    name: str
    created_at: str


class GaugeWindowCreate(BaseModel):
    window_start: str = Field(description="ISO-8601，含边界")
    window_end: str = Field(description="ISO-8601，不含边界")
    note: Optional[str] = None

    @field_validator("window_start", "window_end")
    @classmethod
    def _strip(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("时间不能为空")
        return v


class GaugeWindowOut(BaseModel):
    window_id: int
    gauge_id: int
    window_start: str
    window_end: str
    note: Optional[str]
    created_at: str
    # 登记该窗口后被连带隔离的工序（历史查询不含此字段）
    quarantined_process_ids: list[int] = Field(default_factory=list)


# ---------- 工件 / 工序 DAG ----------

class ItemSpec(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=128)
    lower_limit: int
    upper_limit: int

    @field_validator("upper_limit")
    @classmethod
    def _check_bounds(cls, v: int, info) -> int:
        lower = info.data.get("lower_limit")
        if lower is not None and v < lower:
            raise ValueError("upper_limit 不能小于 lower_limit")
        return v


class ProcessSpec(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=128)
    prerequisites: list[str] = Field(default_factory=list)
    items: list[ItemSpec] = Field(min_length=1)


class WorkpieceCreate(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    processes: list[ProcessSpec] = Field(min_length=1)


class ItemOut(BaseModel):
    item_id: int
    process_id: int
    code: str
    name: str
    lower_limit: int
    upper_limit: int


class ProcessOut(BaseModel):
    process_id: int
    workpiece_id: int
    code: str
    name: str
    prerequisites: list[int]
    items: list[ItemOut]


class WorkpieceOut(BaseModel):
    workpiece_id: int
    code: str
    created_at: str
    processes: list[ProcessOut]


# ---------- 检验 ----------

class InspectionCreate(BaseModel):
    item_id: int
    gauge_id: int
    reading: int
    measured_at: str

    @field_validator("measured_at")
    @classmethod
    def _strip(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("测量时间不能为空")
        return v


class InspectionOut(BaseModel):
    inspection_id: int
    item_id: int
    gauge_id: int
    reading: int
    measured_at: str
    accepted_at: str
    is_latest: bool
    # 登记该检验后被连带隔离的工序（历史查询不含此字段）
    quarantined_process_ids: list[int] = Field(default_factory=list)


# ---------- 工序动作 ----------

class ActionRequest(BaseModel):
    operator: Optional[str] = Field(default=None, max_length=64)


class ProcessEventOut(BaseModel):
    event_id: int
    process_id: int
    event_type: str
    detail: Optional[str]
    operator: Optional[str]
    created_at: str


class CredentialInspectionOut(BaseModel):
    item_id: int
    inspection_id: int


class CredentialOut(BaseModel):
    credential_id: int
    process_id: int
    kind: str
    basis_id: Optional[int]
    operator: Optional[str]
    created_at: str
    inspections: list[CredentialInspectionOut]


class QuarantineOut(BaseModel):
    quarantine_id: int
    process_id: int
    pre_status: str
    reason: str
    opened_at: str
    closed_at: Optional[str]


class ProcessStatusOut(BaseModel):
    process_id: int
    workpiece_id: int
    code: str
    name: str
    status: str
    quarantined_at: Optional[str]
    reason: Optional[str]
    prerequisites: list[int]
    active_credential: Optional[CredentialOut]
    can_start: bool
    blocking_reasons: list[str]
    open_quarantine: Optional[QuarantineOut]
