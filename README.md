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
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `POST /api/sync`，离线处置记录按批同步
- `GET /api/sync/pending`，挂起待确认记录
- `GET /api/sync/batches/{batch_key}`，批次同步结果
- `POST /api/sync/batches/{batch_key}/confirm`，人工确认/退回挂起记录
- `GET /api/audit`

允许角色：observer, response_commander, operations, viewer。估算油量、海况和未完成任务数影响响应等级；关闭前必须完成回收和岸线监测记录。

## 离线同步

现场无信号时先在本地登记处置记录，恢复网络后通过 `POST /api/sync` 按批回传。请求体：

```json
{
  "batch_key": "现场终端生成的批次唯一标识",
  "records": [
    {
      "client_ref": "记录的幂等标识",
      "item_id": 1,
      "kind": "containment",
      "detail": "围控已完成",
      "status": "open",
      "evidence": "现场证据编号或说明",
      "target_stage": "containing"
    }
  ]
}
```

- **幂等去重**：`batch_key` 为批次幂等键，`client_ref` 为每条记录的幂等键。断网重发同一批次时，已入账记录只返回 `duplicate`，不会多出处置记录或重复审计事件。
- **失败续传**：被退回的记录补填证据后，用同一 `client_ref` 再次提交即可重新入账；已入账部分不受影响。
- **阶段冲突**：现场阶段早于中心时，晚到内容只补充证据，不回退事件状态；现场记到 `monitoring`/`closed` 而中心更早时，该条先置为 `pending` 挂起，等待人工确认。
- **逐条退回**：证据缺失或角色越权的记录单独标记为 `rejected` 并写明原因，其余记录照常入库。
- **人工裁决**：`POST /api/sync/batches/{batch_key}/confirm` 提交 `{"client_ref":"...","decision":"confirm|reject","reason":"..."}`；不填 `client_ref` 则裁决该批次全部挂起记录。仅 `response_commander` 可裁决。

历史数据（无 `client_ref` 的补录记录）按未同步兼容处理，旧接口行为保持不变。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
