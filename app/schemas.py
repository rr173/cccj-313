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
    # 是否已纳入班次派工：一旦纳入，原开工入口不再放行，必须凭当前派工开工
    dispatch_required: bool = False
    current_dispatch_id: Optional[int] = None
    current_dispatch_status: Optional[str] = None
    dispatch_suspended: bool = False


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
    # 实装结构：直接承载件（为空表示本身就是最外层整件）
    direct_parent_piece_id: Optional[int] = None
    # 最外层整件 id（未实装进任何件时即自身）
    root_piece_id: int = 0
    # 直接实装在本件内的子件
    direct_child_piece_ids: list[int] = Field(default_factory=list)
    # 是否为内部子件（不另占缓冲位、不能单独列入调拨单）
    is_inner: bool = False


# ------------------------------------------------------------------ 实装 / 拆件

class AssemblyInstallRequest(BaseModel):
    carrier_piece_id: int = Field(description="承载件（可以是更大总成的内件）")
    installed_piece_id: int = Field(description="待装件：当前必须是最外层整件，可自带子件")
    operator: str = Field(min_length=1, max_length=64)
    workstation_id: Optional[int] = Field(
        default=None, description="可选校验工位；不传则以两件当前工位为准，两者必须同工位"
    )


class AssemblyRemoveRequest(BaseModel):
    carrier_piece_id: int = Field(description="直接承载子件的件")
    child_piece_id: int = Field(description="要从直接承载件上拆下的子件，其下级随它一起拆下")
    operator: str = Field(min_length=1, max_length=64)
    workstation_id: Optional[int] = Field(default=None, description="可选校验工位")


class AssemblyEventOut(BaseModel):
    event_id: int
    action: str
    parent_piece_id: int
    child_piece_id: int
    workstation_id: int
    operator: str
    detail: Optional[str]
    created_at: str


class PieceCompositionOut(BaseModel):
    piece_id: int
    workpiece_id: int
    workpiece_code: str
    direct_parent_piece_id: Optional[int]
    root_piece_id: int
    is_inner: bool
    # 以本件为根的完整父子树（递归），depth=0 为本件
    tree: list["CompositionMemberOut"]
    status: str
    current_workstation_id: int
    current_workstation_code: str
    in_transit: bool
    gate_valid: bool
    quality_frozen: bool


class CompositionMemberOut(BaseModel):
    piece_id: int
    workpiece_id: int
    workpiece_code: str
    gate_process_id: int
    parent_piece_id: Optional[int]
    depth: int


class AssemblyActionOut(PieceOut):
    # 装 / 拆完成后，涉及整件（拆下时为新最外层整件）的完整父子组成
    composition: "PieceCompositionOut"


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


# ------------------------------------------------------------------ 发运追溯

class ShipmentCredentialEvidenceOut(BaseModel):
    credential_id: Optional[int]
    process_id: int
    credential_kind: Optional[str]
    # 该生效凭据当时快照引用的检验（每个项目一条；事后登记新读数不改变这里）
    inspections: list[CredentialInspectionOut]


class ShipmentSnapshotMemberOut(BaseModel):
    piece_id: int
    workpiece_id: int
    workpiece_code: str
    gate_process_id: int
    parent_piece_id: Optional[int]
    depth: int
    # 发运当时该成员关口生效的开工 / 复核凭据及其检验快照
    credential: Optional[ShipmentCredentialEvidenceOut]


class ShipmentSnapshotOut(BaseModel):
    snapshot_id: int
    order_id: int
    action_id: int
    root_piece_id: int
    root_workpiece_code: str
    created_at: str
    members: list[ShipmentSnapshotMemberOut]


class ShipmentTraceOut(BaseModel):
    order_id: int
    order_status: str
    shipped_at: Optional[str]
    # 上线前的旧单没有快照，这里为空列表，不补猜旧单
    snapshots: list[ShipmentSnapshotOut]


class GaugeImpactOut(BaseModel):
    gauge_id: int
    window_id: int
    window_start: str
    window_end: str
    order_id: int
    order_status: str
    shipped_at: Optional[str]
    snapshot_id: int
    root_piece_id: int
    root_workpiece_code: str
    member_piece_id: int
    member_workpiece_id: int
    member_workpiece_code: str
    gate_process_id: int
    credential_id: Optional[int]
    credential_kind: Optional[str]
    item_id: int
    inspection_id: int
    measured_at: str
    reading: int



# PieceCompositionOut 前向引用了稍后定义的 CompositionMemberOut，显式重建一次
PieceCompositionOut.model_rebuild()


# ------------------------------------------------------------------ 班次派工

class MachineCreate(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=128)


class MachineOut(BaseModel):
    machine_id: int
    code: str
    name: str
    created_at: str


class FixtureCreate(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=128)


class FixtureOut(BaseModel):
    fixture_id: int
    code: str
    name: str
    created_at: str


class ResourceDowntimeCreate(BaseModel):
    resource_type: str = Field(description="MACHINE 或 FIXTURE")
    resource_id: int
    window_start: str = Field(description="ISO-8601，左闭")
    window_end: str = Field(description="ISO-8601，右开")
    operator: Optional[str] = Field(default=None, max_length=64)
    note: Optional[str] = None

    @field_validator("resource_type")
    @classmethod
    def _check_type(cls, v: str) -> str:
        v = v.strip().upper()
        if v not in ("MACHINE", "FIXTURE"):
            raise ValueError("resource_type 只能是 MACHINE 或 FIXTURE")
        return v

    @field_validator("window_start", "window_end")
    @classmethod
    def _strip(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("时间不能为空")
        return v


class ResourceDowntimeOut(BaseModel):
    downtime_id: int
    resource_type: str
    resource_id: int
    window_start: str
    window_end: str
    operator: Optional[str]
    note: Optional[str]
    created_at: str


class DispatchOptionIn(BaseModel):
    machine_id: int
    fixture_id: int


class ShiftDispatchSpec(BaseModel):
    process_id: int
    window_start: str = Field(description="允许加工的起止窗口，左闭")
    window_end: str = Field(description="右开；预约须整体落在该窗口内")
    duration_minutes: int = Field(gt=0, description="所需整分钟数")
    options: list[DispatchOptionIn] = Field(
        min_length=1, description="可用的「机器、夹具」组合，至少一种"
    )

    @field_validator("window_start", "window_end")
    @classmethod
    def _strip(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("时间不能为空")
        return v


class ShiftCreate(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    operator: str = Field(min_length=1, max_length=64)
    window_start: str
    window_end: str
    dispatches: list[ShiftDispatchSpec] = Field(min_length=1)

    @field_validator("window_start", "window_end")
    @classmethod
    def _strip(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("时间不能为空")
        return v


class DispatchOptionOut(BaseModel):
    machine_id: int
    fixture_id: int


class DispatchOut(BaseModel):
    dispatch_id: int
    shift_id: int
    process_id: int
    process_code: str
    process_name: str
    workpiece_id: int
    workpiece_code: str
    machine_id: int
    machine_code: str
    fixture_id: int
    fixture_code: str
    # 允许加工的窗口（请求值）
    window_start: str
    window_end: str
    duration_minutes: int
    # 实际预约占用的两项资源时段，左闭右开，整分钟起
    reserved_start: str
    reserved_end: str
    status: str
    is_current: bool
    suspended: bool
    started: bool
    completed: bool
    actual_start_at: Optional[str]
    actual_end_at: Optional[str]
    # 该工序当前实时状态（NOT_STARTED / STARTED / COMPLETED / QUARANTINED）
    process_status: str
    options: list[DispatchOptionOut]
    created_at: str
    updated_at: str


class ShiftOut(BaseModel):
    shift_id: int
    code: str
    operator: str
    window_start: str
    window_end: str
    status: str
    created_at: str
    updated_at: str
    # 该班次当前生效的派工（已换掉的旧派工不在其中，可在工序派工历史里查）
    dispatches: list[DispatchOut]
    has_suspended: bool
    all_started_or_completed: bool


class ShiftDispatchRequest(BaseModel):
    operator: Optional[str] = Field(default=None, max_length=64)


class ShiftDowntimeRequest(ResourceDowntimeCreate):
    pass


class RescheduleOut(BaseModel):
    shift: ShiftOut
    rescheduled: bool
    suspended_dispatch_ids: list[int]
    reason: Optional[str]
    # 若请求同时登记了停用，这里返回该登记
    downtime: Optional[ResourceDowntimeOut] = None


class ShiftRescheduleRequest(BaseModel):
    operator: Optional[str] = Field(default=None, max_length=64)
    # 可选：在同一事务里登记一段停用，然后重排
    resource_type: Optional[str] = None
    resource_id: Optional[int] = None
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    note: Optional[str] = None

    @field_validator("resource_type")
    @classmethod
    def _check_type(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip().upper()
        if v not in ("MACHINE", "FIXTURE"):
            raise ValueError("resource_type 只能是 MACHINE 或 FIXTURE")
        return v


class DispatchStartOut(ProcessStatusOut):
    dispatch: DispatchOut
