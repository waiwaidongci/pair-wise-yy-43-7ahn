# 溢油应急响应与任务追踪

围控、回收、岸线保护和废弃物处置任务，按证据和监测结果闭环。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限和关闭不变量。
- `src/repository.py`：SQLite建表、事务、版本控制和审计链。
- `src/service.py`：权限检查、用例编排、并发控制和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则和失败测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8320
```

默认端口为`8320`，首次启动自动建库。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `GET /api/items`
- `POST /api/items`
- `GET /api/items/{id}`（含待确认挂起列表）
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `GET /api/audit`
- `POST /api/sync/batch`：现场离线台账批量同步
- `GET /api/sync/pending`，可选`?item_id=`
- `POST /api/sync/pending/{id}/confirm`
- `POST /api/sync/pending/{id}/reject`，可附`reason`

允许角色：observer, response_commander, operations, viewer。估算油量、海况和未完成任务数影响响应等级；关闭前必须完成回收和岸线监测记录。

## 离线同步

现场应用离线登记处置记录，网络恢复后按批同步：

```json
POST /api/sync/batch
{"batch_id": "B-001", "operations": [
  {"op_id": "客户端唯一键", "item_external_ref": "OS-1", "actor": "现场记录人", "role": "operations",
   "record": {"kind": "recovery", "detail": "回收油污12袋", "status": "closed", "external_ref": "台账证据号"},
   "target_status": "recovering", "field_status": "containing"}
]}
```

- 同步接口本身要求`response_commander`或`operations`身份；每条操作按`op_id`幂等，断网重传只收一次，不多出处置记录或审计事件。
- 现场阶段早于中心时，晚到内容只补充证据（`evidence_only`），事件状态不回退。
- 现场记到`monitoring`或`closed`而中心更早时，该条挂起（`pending`），由有权限角色确认或否决；确认时仍校验状态机、角色和关闭不变量。
- 证据缺失（缺`detail`或`external_ref`）或越权的那条单独退回（`rejected`），其余照常入库；退回条目不写入台账，修正后随下一批重发即可续传，已入库条目自动去重。
- 历史数据按中心录入兼容（`source=center`），同一`external_ref`的补录与现场同步自动去重；旧接口行为不变。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
