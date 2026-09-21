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

仅接受 `Content-Type: application/json`。请求体：

```json
{
  "deviceId": "device-1",
  "baseCursor": 2,
  "change": {"id": "change-3", "payload": {"title": "notes"}}
}
```

- `documentID`、`deviceId`、`change.id` 均为非空字符串；`baseCursor` 为非负整数（不接受小数、字符串、布尔或 `null`）；`change.payload` 必须是 JSON 对象。
- 格式错误、对未知文档使用非零 `baseCursor`、或 `baseCursor` 大于当前 cursor，均返回 `400` JSON 错误且零写入。
- 成功返回 `200`，响应为单个结果：`{"id","outcome","cursor"}`；幂等结果另含原 `result`（`{"id","created":false,"cursor"}`）。
- 事务内校验与写入，结果分三种：
  - `idempotent`：`change.id` 已存在且 `deviceId` 与解码后的 payload 都相同，返回首次 cursor，不新增记录；任一字段不同则 `409`。
  - `applied`：新 `id` 且 `baseCursor` 等于当前 cursor，直接追加，分配下一个 cursor。
  - `merged`：新 `id` 但 `baseCursor` 落后于当前 cursor——仅当 baseCursor 之后的每个 payload 都是对象、且其顶层字段与新 payload 的顶层字段均不重名时才追加；存在非对象 payload 或任一同名字段则 `409`，零写入。
- 未知文档以 `baseCursor: 0` 提交第一条变更时按 `applied` 处理。
- 并发提交在序列化事务内完成，落后方无法绕过上述检查；提交同步落盘，重启后记录与 cursor 可读。

### `POST /v1/documents/{documentID}/snapshots`

仅接受 `Content-Type: application/json`。请求体：

```json
{"cursor": 2, "state": {"any": "json"}}
```

- `cursor` 为非负整数（不接受小数、字符串、布尔或 `null`）；`state` 必填，为任意合法 JSON 值。
- 仅已知文档的现有 cursor（`1..当前cursor`）可创建快照；未知文档、cursor 为 0 或超过当前 cursor 均返回 `400` JSON 错误且零写入。
- 同一文档同一 cursor 唯一：首次创建返回 `200` `{"cursor":N,"created":true}`；重试时 `state` 解码相同则幂等返回 `200` `{"cursor":N,"created":false}`，不同则返回 `409` JSON 错误且原快照不变。
- 快照同步落盘，重启后可读，幂等与冲突判定不变；快照不影响 changes 或 merge。

### `GET /v1/documents/{documentID}/snapshots/{cursor}`

- `cursor` 为十进制非负整数；格式错误或空 `documentID` 返回 `400` JSON 错误。
- 命中返回 `200` `{"cursor":N,"state":...}`；文档、cursor 或快照不存在返回 `404` JSON 错误。

### `POST /v1/documents/{documentID}/restore`

基于快照做历史恢复。仅接受 `Content-Type: application/json`。请求体：

```json
{"deviceId": "device-1", "changeId": "change-9", "snapshotCursor": 2}
```

- `documentID`、`deviceId`、`changeId` 均为非空字符串；`snapshotCursor` 为正整数（不接受小数、字符串、布尔、`null` 或缺失，也不接受 0）。
- 类型头错误、JSON 非法、尾随内容、空 `documentID` 或任一字段无效均返回 `400` JSON 错误且零写入。
- `snapshotCursor` 未命中该文档的快照（包含未知文档、cursor 无快照）返回 `404` JSON 错误且零写入。
- 命中后在单事务内把该快照的 `state` 作为 payload，以 `deviceId`、`changeId` 追加一条**普通 change**：旧记录不变，cursor 在文档当前高水位上加一并连续分配。
- 成功返回 `200`：`{"id":changeId,"created":true,"cursor":N,"restoredFrom":snapshotCursor}`。恢复出的记录可经 `GET .../changes` 读到，形态与普通 change 一致。
- 同一文档的同一 `changeId` 仅当 `deviceId`、`snapshotCursor`、来源快照 state 全部相同时才幂等：返回 `200`、`created=false`、cursor 为首次值、`restoredFrom` 为原快照 cursor。该 id 已被普通 change（经 `/changes` 或 `/merge` 写入）占用，或任一要素不符，返回 `409` JSON 错误且零写入。
- 恢复来源（provenance）同步落盘，重启后幂等与冲突判定不变；并发恢复在序列化事务内完成，cursor 唯一且连续。

### 路径中的空文档 ID

`/v1/documents//changes`、`/v1/documents//merge` 等 `documentID` 段为空的请求返回 `400` JSON 错误（`{"error": "..."}`），而不是重定向或 `404` HTML 页面。非空路径的语义保持不变。
