# Local-first Sync Service

这是一个面向通用协作产品的 Local-first 多端同步后端。长期目标是提供离线编辑、多设备增量同步、冲突合并、历史恢复、设备身份、权限撤回、附件去重以及推送通道，并把 CRDT、事件日志和权限控制沉淀为可复用服务。

仓库采用 Go，当前提供进程健康检查与持久化文档增量同步。后续能力必须通过独立题目逐步实现；每个题目都应定义可观察的公共行为、兼容边界和失败语义，不得依赖未公开内部 API。

## 启动

```bash
go run ./cmd/syncd
```

服务默认监听 `127.0.0.1:8080`。可通过 `SYNC_ADDR` 修改监听地址，通过 `SYNC_DATA` 指定 SQLite 数据库路径（默认 `data/sync.db`，启动时自动创建目录）。变更在提交时同步落盘，进程重启（含崩溃）后数据与游标可读。

## 验证

```bash
go test ./...
```

## HTTP 接口

### `GET /healthz`

返回 `{"status":"ok"}`。

### `POST /v1/documents/{documentID}/changes`

仅接受 `Content-Type: application/json`。请求体：

```json
{
  "deviceId": "device-1",
  "changes": [
    {"id": "change-1", "payload": {"any": "json"}},
    {"id": "change-2", "payload": [1, true, null]}
  ]
}
```

- `documentID`、`deviceId`、每个 change 的 `id` 均为非空字符串；`changes` 为非空数组，元素含非空 `id` 和任意 JSON `payload`。
- 不合格式、字段类型错误或批内 `id` 重复返回 `400` JSON 错误（`{"error": "..."}`），整批零写入。
- 同一文档内按 `id` 去重：新 `id` 获得递增 cursor；已存在的 `id` 仅当 `deviceId` 与解码后的 JSON 值都相同才幂等（`created=false`，cursor 为首次值），否则返回 `409` JSON 错误，整批零写入。
- 成功返回 `200`：`{"results":[{"id","created","cursor"}, ...]}`，顺序与请求一致。
- 有效批次原子提交；并发批次不会重复分配 cursor，也不会丢记录。

### `GET /v1/documents/{documentID}/changes`

分页游标读取：

- `after`：非负整数，默认 `0`，只返回 cursor 严格大于它的变更。
- `limit`：`1..1000` 的整数，默认 `100`。
- 参数非法返回 `400` JSON 错误。
- 响应：`{"changes":[{"id","deviceId","payload","cursor"}, ...], "nextCursor": N}`，`changes` 按 cursor 递增。
- 已知文档但 `after` 之后无数据时 `nextCursor` 等于 `after`；未知文档返回空列表且 `nextCursor` 为 `0`。

游标在每个文档内独立、从 1 开始、连续不重复，可作为断点续传位置持久保存。

### `POST /v1/documents/{documentID}/merge`

在游标位置上追加单个变更，并支持落后客户端的安全合并。仅接受 `Content-Type: application/json`。请求体：

```json
{
  "deviceId": "device-1",
  "baseCursor": 3,
  "change": {"id": "change-9", "payload": {"title": "new"}}
}
```

- `deviceId`、`change.id` 均为非空字符串；`baseCursor` 为非负整数（不接受小数、指数、字符串或布尔）；`change.payload` 必须是 JSON 对象（含 `{}`，不接受数组、标量或 `null`）。
- 格式错误、字段类型错误，或未知文档携带非零 `baseCursor`，或 `baseCursor` 大于该文档当前 cursor，均返回 `400` JSON 错误（`{"error": "..."}`）且零写入。
- `documentID` 路径段为空（如 `/v1/documents//changes`、`/v1/documents//merge`）时，所有接口统一返回 `400` JSON 错误，不产生重定向或 HTML 响应。
- 事务内校验与写入（并发不会绕过任何检查，失败一律零写入）：
  - `change.id` 已存在：沿用原幂等规则——`deviceId` 与解码后的 JSON payload 都相同则 `200`，`outcome` 为 `idempotent`，响应含原 `result`（`{id, created:false, cursor}`）；否则 `409`。
  - 新 `id` 且 `baseCursor` 等于当前 cursor：直接追加，`outcome` 为 `applied`。
  - 新 `id` 且 `baseCursor` 落后：仅当其**后**（cursor 严格大于 `baseCursor`）的每个 payload 都是 JSON 对象，且它们的顶层字段与新 payload 的顶层字段均不重名时才追加，`outcome` 为 `merged`；否则 `409`。只比较顶层字段名，不递归比较嵌套内容。
- 成功返回 `200`：`{"id":"...","outcome":"applied|merged|idempotent","cursor":N,"result"?:{...}}`，`result` 仅在幂等时出现。
- 提交同步落盘；进程重启后已追加的记录与 cursor 继续可读，未知文档规则同样持久有效。
