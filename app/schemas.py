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


# ---------- 工位交接 ----------

class WorkstationCreate(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=128)
    capacity: int = Field(ge=0, description="缓冲位数量，一件占一位")


class WorkstationOut(BaseModel):
    workstation_id: int
    code: str
    name: str
    capacity: int
    created_at: str


class SlotOut(BaseModel):
    slot_id: int
    workstation_id: int
    piece_id: int
    state: str
    purpose: str
    order_id: Optional[int]
    created_at: str


class WorkstationDetailOut(WorkstationOut):
    actual_count: int
    reserved_count: int
    available: int
    # 实际在位的件
    actual_pieces: list[int]
    # 已预留但尚无实物的件（收件预留 / 退回保留）
    reserved_pieces: list[int]
    slots: list[SlotOut]


class PieceCreate(BaseModel):
    workpiece_id: int = Field(description="已有工件编号，不新建工件")
    gate_process_id: int = Field(description="该工件的转运关口工序")
    workstation_id: int = Field(description="实物初始工位")


class PieceOut(BaseModel):
    piece_id: int
    workpiece_id: int
    workpiece_code: str
    gate_process_id: int
    gate_process_code: str
    status: str
    current_workstation_id: int
    current_workstation_code: str
    # 关口当前是否仍按现有规则有效完工；冻结件不能再次发运
    gate_valid: bool
    quality_frozen: bool
    active_order_id: Optional[int]
    created_at: str


class TransferOrderCreate(BaseModel):
    source_workstation_id: int
    dest_workstation_id: int
    piece_ids: list[int] = Field(min_length=1)


class TransferOrderPieceOut(BaseModel):
    piece_id: int
    workpiece_code: str
    state: str
    released_at: Optional[str]


class TransferRecordOut(BaseModel):
    record_id: int
    order_id: int
    action_id: int
    piece_id: int
    action: str
    operator: str
    request_id: str
    from_workstation_id: int
    to_workstation_id: int
    created_at: str


class TransferActionOut(BaseModel):
    action_id: int
    order_id: int
    request_id: str
    action: str
    operator: str
    piece_ids: list[int]
    created_at: str


class TransferOrderOut(BaseModel):
    order_id: int
    source_workstation_id: int
    dest_workstation_id: int
    status: str
    created_at: str
    shipped_at: Optional[str]
    closed_at: Optional[str]
    pieces: list[TransferOrderPieceOut]


class TransferOrderDetailOut(TransferOrderOut):
    # 每次发运 / 签收 / 退回的操作者与扫码请求号（按时间顺序）
    actions: list[TransferActionOut]
    # 逐件的签收 / 退回落地记录
    records: list[TransferRecordOut]


class TransferRequest(BaseModel):
    operator: str = Field(min_length=1, max_length=64)
    request_id: str = Field(min_length=1, max_length=128)


class TransferBatchRequest(TransferRequest):
    piece_ids: list[int] = Field(min_length=1)


class TransferActionResult(BaseModel):
    order_id: int
    action: str
    request_id: str
    operator: str
    piece_ids: list[int]
    order_status: str
    replayed: bool = False

