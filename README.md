# 机加工工序放行后端

单件机加工工序的检验登记、开工 / 完工放行、量具失准隔离与质量员复核服务。
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

## 5. HTTP 接口（前缀 `/api`）

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

## 6. 运行与测试

```bash
pip install -r requirements.txt

# 启动（默认库文件 ./release.db，可用 RELEASE_DB 覆盖）
./run.sh
# 或：uvicorn app.main:app --reload

# 测试（每个用例使用独立临时库）
pytest -q
```

## 7. 目录结构

```
app/
  database.py   # SQLite 连接、PRAGMA、建表 DDL
  schemas.py    # Pydantic 请求 / 响应模型
  services.py   # 全部业务规则、隔离传导、并发锁
  main.py       # FastAPI 路由与异常映射
tests/          # 放行链路、隔离 / 复核、DAG 校验、并发、随机不变量
run.sh          # 启动脚本
```
