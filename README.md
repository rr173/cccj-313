# 机加工工序放行后端

单件机加工工序的检验登记、开工 / 完工放行、量具失准隔离与质量员复核服务，
外加班组之间的**工位交接**（工位缓冲位、件、调拨单、分批签收 / 退回），
以及**实装 / 拆件**（把已登记工件装成总成、返工拆换、按整件交接并保留发运追溯证据）。
技术栈：Python + FastAPI + SQLite（标准库 `sqlite3`，无 ORM）。

## 1. 领域模型与状态

### 1.1 静态结构（随工件建档，构成 DAG）

- **工件 workpiece**：包含若干 **工序 process**。
- **工序依赖 process_dependency**：有向无环图，边 `process -> prereq` 表示先决工序。
  建档时做编号唯一、先决存在、Kahn 拓扑排序环检测。
- **尺寸项目 item**：每道工序挂一个或多个项目，带整数公差闭区间
  `[lower_limit, upper_limit]`，边界值合格。

### 1.2 动态数据

- **量具 gauge** 与 **失准窗口 gauge_window**：半开区间 `[window_start, window_end)`。
  未被任何窗口覆盖的测量时间一律视为量具有效。
- **检验 inspection**：针对单工件、单项目登记读数、量具、测量时间；服务接受时刻
  `accepted_at` 由服务端生成。每个项目以 **inspection_id 最大（最后接受）的一次检验
  为准**，旧检验永久保留，失效后不回退。
- **凭据 credential**：`START`（开工）/ `REVIEW`（复核，含隔离恢复）/ `COMPLETE`（完工）。
  每张凭据通过 `credential_inspection` **快照**当时各项目依据的检验 id；之后登记新读数
  绝不回填历史凭据。`COMPLETE` 凭据通过 `basis_id` 指向其开工 / 复核依据。
- **工序状态 process_state**：
  - `NOT_STARTED` → `STARTED` → `COMPLETED` 为正常路径；
  - 任何 `STARTED` / `COMPLETED` 工序失去有效依据时转 `QUARANTINED`，并向下游传导；
  - `QUARANTINED` 只能由质量员逐道 **复核** 解除：隔离前完工的复核后仍 `COMPLETED`，
    隔离前已开工未完工的复核后恢复 `STARTED`，须再完工。
- **process_event / quarantine**：所有状态变化与隔离单（含隔离前状态 `pre_status`、
  原因、开关时间）永久留痕，隔离后仍可查到当时实际做过什么。

## 2. 放行规则（开工与复核共用同一套）

一道工序可放行，当且仅当：

1. 本工序每个项目的**最新检验**读数落在整数公差闭区间内；
2. 该最新检验所用量具在**测量时刻**有效（不落入任何失准窗口）；
3. 全部先决工序处于 `COMPLETED` 且仍有效（未隔离）。

任一不满足时，状态接口给出逐项 `blocking_reasons`，动作接口返回 `409`。

## 3. 失效与隔离语义

两类事件触发重算，均在单个事务内完成：

1. **登记新检验**：最新检验不再合格（超差，或所用量具测量时失准）时，若所属工序已
   开工 / 完工，则该工序及其全部已开工 / 完工下游被隔离；未开工下游不产生隔离单，
   而是在尝试开工时被规则自然拦截。
2. **登记失准窗口**：扫描所有**当前生效的 START/REVIEW 凭据**，凡其快照引用的检验
   落入新窗口，对应工序及其已开工 / 完工下游被隔离。

关键约束（均有测试覆盖）：

- 登记新读数不回填原开工凭据，只改变“当前最新检验”的判定；
- 仅补测上游**不会**自动释放已隔离的下游——必须在先决工序重新有效后由质量员逐道
  调用复核接口；复核仍走第 2 节的同一套规则；
- 隔离前已完工的复核后仍为完工，不要求假装重做；已开工未完工的复核后恢复开工，
  才能完工；
- 只影响本工件依赖相关的工序，不接设备或生产计划系统。

## 4. 并发模型

- 进程内一把可重入锁 `_service_lock` 串行化所有写操作；每个写方法在一个 SQLite
  事务（`with conn:`）内完成“校验 + 落库 + 隔离重算”。
- 因此失准登记、开工、复核竞争时，只会出现两种串行化结果：开工先提交则被后登记的
  窗口追加成隔离；窗口先提交则开工被规则 409 拦截。**不会在应隔离的工序上继续放行。**
- SQLite 开启 WAL 与 `foreign_keys=ON`，读接口不持锁。

> 环境提示：本机 Python 3.11 自带的 SQLite 为 3.40.1，实测其查询计划器在一个多表
> 连接 + 关联标量子查询的特定计划上，会对 `列 IN ('A', 'B')`（带空格多值列表）
> 产生错误空结果。窗口扫描 SQL 因此刻意改写为等值 `OR` 形式（见
> `app/services.py` 中 `register_gauge_window` 的注释），语义等价且结果稳定，
> 并由 `tests/test_quarantine.py` 回归覆盖。

## 5. 工位交接

### 5.1 模型

- **工位 workstation**：固定容量 `capacity` 的缓冲位，一件占一位。
- **件 piece**：用**已有工件编号**登记的一件实物。一件对应一个工件，登记时指定其
  **初始工位**和一道属于该件的**转运关口工序**。不复制任何检验 / 完工记录；关口
  有效性始终实时读取该工序的当前状态与现有放行规则。
- **调拨单 transfer_order**：指定不同的发件 / 收件工位，以及一批在发件方的件。
- **缓冲位槽 workstation_slot**：`ACTUAL`（实物在位）或 `RESERVED`（已预留、尚无
  实物）。**两者都占用容量**。工位查询分别返回 `actual_pieces` / `reserved_pieces`，
  以及剩余的 `available`。

### 5.2 容量与位次账本

| 时点 | 发件方 | 收件方 |
| --- | --- | --- |
| 件初次登记 | `ACTUAL`（HOME） | — |
| 建单（待发） | `ACTUAL` | `RESERVED`（RECEIPT，为每件留好收件位） |
| 发运（在途） | `RESERVED`（RETURN，每件的退回位置，不得接纳别的件） | `RESERVED`（RECEIPT） |
| 签收落地 | 释放该件 RETURN 预留 | 该件 RECEIPT 由 `RESERVED` → `ACTUAL` |
| 退回落地 | 该件 RETURN 由 `RESERVED` → `ACTUAL` | 释放该件 RECEIPT 预留 |

- **占用守恒**：建单、发运、单件落地都不改变两侧合计占用量，只是状态转换与
  “预留哪件”的变化；因此发运不需要额外容量。
- 一个工位的 `ACTUAL + RESERVED` 不得超过容量：应用层预检 + 数据库触发器双重保证。
- 同一件同时最多在一张待发 / 在途单上（`transfer_order_piece` 的部分唯一索引
  `released_at IS NULL`）。已落地（签收 / 退回）或被撤销的件 `released_at` 非空，
  即可进入后续单据。
- 待发单可撤销：释放收件方预留位、件留在发件方原位。已发运的单不能撤销。

### 5.3 发运与落地规则

1. **建单**只校验发 / 收工位不同、件当前在发件方且未被其它单占用，并立即留好
   收件位；关口是否完工不在此拦截（可先建单留位，待关口完工后再发运）。
2. **发运**要求单内**每一件**的关口当前都按现有工序规则**有效完工**
   （`COMPLETED` 且本工序项目最新检验合格、量具在测量时刻有效、先决工序有效完工）。
   有一件不满足则整单**仍待发**、返回 409。发运时把发件方各件的在库位就地转为
   该件的退回保留位，占用不增加。
3. **签收 / 退回**只处理**仍在途**的件，可分批，且互不影响单中其他件。
   请求里只要有一件无法处理（不在单内 / 已落地 / 未在途），**整批不生效**。
4. **质量变化不拒收**：关口已不再有效完工的件照常落地并留签收 / 退回记录，
   但当前库存 `piece.quality_frozen = true`、`gate_valid = false`，**不能再次发运**。
   复核 / 补测使关口恢复有效后，件的可用状态自动恢复；不改动任何历史落地记录，
   也不移动实物。
5. 单内全部件落地后单状态为 `COMPLETED`；否则保持 `SHIPPED` 继续分批处理。

### 5.4 请求号幂等

`ship` / `receive` / `return` 均接收 `request_id`，并按**调拨单**隔离：

- 同 `request_id` + 同动作 + 同操作者 + 同一批件：返回首次成功结果（`replayed=true`），
  不重复记账、不重复占用 / 释放位置；
- 同 `request_id` 但动作 / 操作者 / 件批次任一不同：409，拒绝执行；
- 失败请求不写幂等记录，因此不占号，可用同一号重试；
- 幂等比较发生在 `BEGIN IMMEDIATE` 写锁内，两个终端同号并发只会有一个真正执行。

### 5.5 并发与重启

- 每个写操作在进程内锁之外，再以显式 `BEGIN IMMEDIATE` 事务取得 SQLite 写锁，
  跨进程（两个终端）串行化；唯一索引与容量触发器作为最后一道防线。
- 因此不会出现重复发运 / 重复落地 / 超占容量；发运瞬间实时读取关口状态，绝不
  用缓存的旧完工状态通过发运。
- 所有交接状态都落在同一个 SQLite 库中，重启后按原单（含件状态、位次、幂等记录）
  继续处理。

### 5.6 HTTP 接口（交接部分，前缀 `/api`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workstations` | 建工位（`code`, `name`, `capacity`） |
| GET | `/workstations` | 工位列表 |
| GET | `/workstations/{id}` | 实际在位 / 已预留无实物 / 剩余位置 |
| POST | `/pieces` | 登记实物（已有 `workpiece_id` + `gate_process_id` + 初始 `workstation_id`） |
| GET | `/pieces` / `/pieces/{id}` | 每件现在在哪、是否质量冻结、占用哪张单 |
| POST | `/transfer-orders` | 建单（发 / 收工位、件批次；即为每件预留收件位） |
| GET | `/transfer-orders` / `/{id}` | 单列表 / 单详情（含落地记录：谁签收或退回了哪些件） |
| POST | `/transfer-orders/{id}/cancel` | 撤销待发单，释放收件位 |
| POST | `/transfer-orders/{id}/ship` | 整单发运（`operator`, `request_id`）；发运瞬间快照组成与凭据证据 |
| POST | `/transfer-orders/{id}/receive` | 分批签收（`operator`, `request_id`, `piece_ids`） |
| POST | `/transfer-orders/{id}/return` | 分批退回（同上） |

## 5A. 实装 / 拆件与发运追溯

### 5A.1 当前组成模型

- **实装边 piece_assembly**：`parent 直接承载 child`。一个 child 至多有一个直接承载件
  （`child_piece_id UNIQUE`），因此每件实物恒属于唯一一棵组成树；树的最外层称为
  **整件（root）**，无承载件。
- **工位账本只认整件**：只有 root 在 `workstation_slot` 占一个缓冲位；内部子件
  不另占位。内部子件的工位 / 在途状态读时沿承载链解析，始终与其最外层整件一致，
  发运、签收、退回都不逐件改它们的位置。
- **装拆留痕 assembly_event**：只增不改，记录动作（`INSTALL` / `REMOVE`）、人、
  承载件 / 子件、工位；`REMOVE` 的 `detail` 用 JSON 记下随子件一起拆下的整组成员。
- 装 / 拆**不读取也不改动**任何检验、凭据、完工 / 隔离记录；质量冻结的在位整件
  仍可拆换返工。

### 5A.2 装配规则（POST `/api/assemblies/install`）

请求：`carrier_piece_id`（承载件）、`installed_piece_id`（待装件）、`operator`，
可选 `workstation_id`（传则额外校验两件确在该工位）。

1. 待装件当前必须是**最外层整件**（不能已装在别的件里），但可以自带整棵子件；
2. 承载件本身可以是更大总成的内件；两件必须属于**不同整件**；
3. 两件的有效工位必须相同，且都在工位上（在途不可装）；
4. 涉及的两个整件只要有一个在待发 / 在途单上（`released_at IS NULL`），拒绝改组成；
5. 装配后待装件原来的实物缓冲位被**腾出**（其下级本就不占位），承载件所在整件
   不新增占位——一个整件始终只占一位。

### 5A.3 拆件规则（POST `/api/assemblies/remove`）

请求：`carrier_piece_id`、`child_piece_id`、`operator`，可选 `workstation_id`。

1. 必须从**直接承载件**上拆（传错承载件 409）；只断开这一条承载边，子件原有下级
   随它整体离开，仍是一棵完整子树；
2. 涉及整件待发 / 在途时拒绝（质量冻结不拦截）；
3. 拆下的一组要在**同工位占一个真·空位**：`ACTUAL` 与 `RESERVED` 都计入容量，
   已预留的位置不算空位，位满则整笔失败回滚；
4. 拆出的子件成为新的最外层整件，整组只占一个 `ACTUAL(HOME)` 位。

### 5A.4 整件交接与逐层关口

- **只有最外层整件可以单独列入调拨单**；内部子件上单直接 409。
- **发运**时，整件本身及每层实装子件的转运关口都须按第 2 节现有规则**有效完工**；
  任一成员关口不满足，整单不发、仍待发，且不占用请求号。
- 签收 / 退回仍按扫码的整件记账，质量变化不阻止如实落地；整件的当前
  `quality_frozen` 随其**现有组成及各成员关口实时状态**变化（任一成员失效即冻结），
  不改写任何历史交接记录。
- 内部子件随整件在途、随整件落地，落地工位即其当前实际工位。

### 5A.5 发运追溯证据（只增不改，仅覆盖上线后的新发运）

发运成功的同一事务内，为单内每个整件写一份独立快照（与当前组成 / 凭据表解耦）：

- `shipment_snapshot` / `shipment_snapshot_member`：发运当时完整的父子组成
  （含每件的 `parent_piece_id`、`depth`，depth=0 为整件本身）；
- `shipment_snapshot_credential`：每个成员转运关口**当时生效**的开工 / 复核凭据
  （最近一张 `START` / `REVIEW`），凭据快照引用的检验仍通过既有
  `credential_inspection` 取得；事后登记新读数、补测、复核都不回填这里。
- 因此件后来被拆换、补测或复核，**查到的历史依据不变**；上线前的旧单没有快照，
  追溯接口返回空 `snapshots`，不补猜旧单。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/assemblies/install` | 实装（承载件、待装件、操作人、可选校验工位） |
| POST | `/assemblies/remove` | 从直接承载件拆子件，下级随拆，占同工位一个空位 |
| GET | `/assembly-events` | 装拆留痕（人、件、工位），可 `?piece_id=` 过滤 |
| GET | `/pieces/{id}/composition` | 直接承载件、最外层整件、工位 / 在途状态与完整父子树 |
| GET | `/transfer-orders/{id}/shipment-trace` | 一次发运中每个整件当时的组成与各成员生效凭据 |
| GET | `/gauges/{gid}/out-of-calibration-windows/{wid}/impact` | 该失准窗内的测量曾被哪些发运单的哪些整件 / 成员凭据引用 |

`GET /pieces/{id}` 的返回同时给出 `direct_parent_piece_id`、`root_piece_id`、
`direct_child_piece_ids`、`is_inner`；内部子件的 `status` / 工位以其整件为准。

量具失准影响查询的判定口径严格限定为**发运快照里各关口凭据所引用的测量**
（快照凭据 → `credential_inspection` → `inspection`，量具与测量时间落在该半开窗内），
逐行返回发运单、整件、成员、关口、凭据与具体检验。

### 5A.6 并发与一致性

装 / 拆与既有写操作共用进程内 `_service_lock`，并在显式 `BEGIN IMMEDIATE`
事务内完成“校验 + 组成改写 + 缓冲位变动 + 留痕 / 快照”，跨进程串行化；唯一约束
（一个 child 一个承载件、一件至多一张活动单）与容量触发器作为最后防线。
失败整体回滚，不留下半套组成、半套货位或半条交接证据。

### 5A.7 通过 HTTP 装拆并查发运追溯

```bash
B=http://127.0.0.1:8000/api

# 1) 实装：先把 C 装进 B，再把自带子件的 B 整体装进 A（两件须同工位）
curl -X POST $B/assemblies/install -H 'content-type: application/json' \
  -d '{"carrier_piece_id":2,"installed_piece_id":3,"operator":"装配工-赵"}'
curl -X POST $B/assemblies/install -H 'content-type: application/json' \
  -d '{"carrier_piece_id":1,"installed_piece_id":2,"operator":"装配工-赵"}'

# 2) 查任一工件的承载关系与完整父子树（内部子件工位随其整件）
curl $B/pieces/2/composition

# 3) 只有整件 1 能上单；发运逐层校验 A、B、C 的关口，并在发运瞬间留快照
curl -X POST $B/transfer-orders -H 'content-type: application/json' \
  -d '{"source_workstation_id":1,"dest_workstation_id":2,"piece_ids":[1]}'
curl -X POST $B/transfer-orders/1/ship -H 'content-type: application/json' \
  -d '{"operator":"司机-钱","request_id":"SHIP-1"}'
curl -X POST $B/transfer-orders/1/receive -H 'content-type: application/json' \
  -d '{"operator":"收件-孙","request_id":"RCV-1","piece_ids":[1]}'

# 4) 查这次实际发运：当时的完整父子组成 + 每个成员关口当时生效的凭据 / 检验
curl $B/transfer-orders/1/shipment-trace

# 5) 事后发现量具失准：登记窗口后，反查哪些发运的哪些成员凭据引用了窗内测量
curl -X POST $B/gauges/2/out-of-calibration-windows -H 'content-type: application/json' \
  -d '{"window_start":"2026-11-27T07:00:00+00:00",
       "window_end":"2026-11-27T09:00:00+00:00","note":"事后失准"}'
curl $B/gauges/2/out-of-calibration-windows/1/impact

# 6) 返工拆件：从直接承载件 1 拆下 2（其下级 3 随拆），同工位须有一个真·空位
curl -X POST $B/assemblies/remove -H 'content-type: application/json' \
  -d '{"carrier_piece_id":1,"child_piece_id":2,"operator":"返工工-周"}'
# 拆换不改变第 4 步查到的历史快照；装拆留痕可按件查
curl "$B/assembly-events?piece_id=2"
```

## 6. HTTP 接口（既有放行部分，前缀 `/api`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/gauges` | 登记量具 |
| GET | `/gauges` | 量具列表 |
| POST | `/gauges/{gauge_id}/out-of-calibration-windows` | 登记半开失准窗口，返回连带隔离的工序 id |
| GET | `/gauges/{gauge_id}/out-of-calibration-windows` | 量具失准窗口列表 |
| POST | `/workpieces` | 按 DAG + 项目公差建工件（含环 / 重复校验） |
| GET | `/workpieces/{id}` | 工件结构 |
| GET | `/workpieces/{id}/process-status` | 工件全部工序的实时状态与阻断原因 |
| POST | `/inspections` | 登记一次检验，返回连带隔离的工序 id |
| GET | `/items/{item_id}/inspections` | 项目检验历史（最新一条标 `is_latest`） |
| POST | `/processes/{id}/start` | 开工（全规则校验，生成 START 凭据） |
| POST | `/processes/{id}/complete` | 完工（生成 COMPLETE 凭据） |
| POST | `/processes/{id}/review` | 质量员复核，解除隔离 |
| GET | `/processes/{id}/status` | 单工序状态、生效凭据、可否开工 |
| GET | `/processes/{id}/credentials` | 该工序全部凭据及检验快照 |
| GET | `/processes/{id}/events` | 状态变化事件链 |
| GET | `/processes/{id}/quarantines` | 该工序隔离单 |
| GET | `/quarantines?open_only=true` | 全量 / 未关闭隔离单 |

时间字段统一接收 ISO-8601（带时区或无时区的按 UTC 处理），内部归一化为可字典序
比较的 UTC 字符串，因此窗口比较可直接用 SQL 字符串比较。业务错误返回
`409`（规则不满足 / 冲突）、`404`（实体不存在）、`422`（输入校验 / 环 / 窗口反向）。

## 7. 运行与测试

```bash
pip install -r requirements.txt

# 启动（默认库文件 ./release.db，可用 RELEASE_DB 覆盖）
./run.sh
# 或：uvicorn app.main:app --reload

# 测试（每个用例使用独立临时库）
pytest -q
```

## 8. 目录结构

```
app/
  database.py   # SQLite 连接、PRAGMA、建表 DDL（含实装树、装拆留痕、发运快照）
  schemas.py    # Pydantic 请求 / 响应模型
  services.py   # 全部业务规则、隔离传导、工位交接、实装拆件、发运追溯、跨进程写锁
  main.py       # FastAPI 路由与异常映射
tests/          # 放行链路、隔离 / 复核、DAG 校验、并发、随机不变量、工位交接、实装拆件与追溯
run.sh          # 启动脚本
```

所有建表 DDL 均为 `CREATE TABLE IF NOT EXISTS`，对既有库直接启动即幂等迁移，
旧数据（工件、检验、凭据、工位、调拨单、落地记录）原样保留；发运追溯证据只对
迁移后的新发运生成，不回填、不补猜旧单。
