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

### `POST /v1/devices`

注册设备。仅接受 `Content-Type: application/json`。请求体：

```json
{"deviceId": "device-1"}
```

- `deviceId` 为非空字符串；类型头不符、JSON 非法、尾随内容、缺失或类型错误（数字、布尔、`null` 等）均返回 `400` JSON 错误且零写入。
- 首次注册返回 `200` `{"deviceId":"device-1","created":true}`；用同一 `deviceId` 重试是幂等的，返回 `200` `created=false`。

### `POST /v1/devices/{deviceId}/sessions`

为已注册设备创建同步会话。仅接受 `Content-Type: application/json`。请求体：

```json
{"sessionId": "session-1"}
```

- `sessionId` 为非空字符串；类型头不符、JSON 非法、尾随内容、缺失或类型错误均返回 `400` JSON 错误且零写入。
- 设备未注册返回 `404` JSON 错误且零写入。
- 首次创建返回 `200` `{"sessionId":"session-1","created":true}`；同一设备用同一 `sessionId` 重试幂等返回 `created=false`。
- `sessionId` 已归属其他设备时返回 `409` JSON 错误且零写入，归属永不改变。
- 创建在序列化事务内完成；并发创建同一 `sessionId` 恰好一个成功，其余同设备幂等、异设备 `409`。

### `DELETE /v1/devices/{deviceId}/sessions/{sessionId}`

仅删除该设备名下匹配的会话，无请求体。

- 命中返回 `200` `{"deleted":true}`。
- 设备不存在、会话不存在、会话归属其他设备、或重复删除，一律返回 `404` JSON 错误；非属主的删除不会移除会话。
- 删除为硬删除并同步落盘；删除后该 `sessionId` 可作为全新会话再次创建（`created=true`），重启后重复删除仍为 `404`。

### `GET /v1/sessions/{sessionId}/documents/{documentId}/changes`

会话视角的增量读取，查询参数与响应结构与 `GET /v1/documents/{documentID}/changes` 完全一致（`after`、`limit` 语义、`changes`/`nextCursor` 形状、未知文档返回空列表且 `nextCursor` 为 `0`）。

- `sessionId`、`documentId` 均为非空字符串；空段返回 `400` JSON 错误而非重定向。
- `after`、`limit` 非法返回 `400` JSON 错误（参数校验先于会话存在性检查）。
- 会话不存在或已删除返回 `404` JSON 错误；会话所属设备已被撤回该文档权限时返回 `403` JSON 错误，且不返回 `changes` 或 `nextCursor`；其余情况读取结果与既有 changes 查询逐字相同。

### `POST /v1/documents/{documentID}/permissions`

按文档和设备持久化访问权限。仅接受 `Content-Type: application/json`。请求体：

```json
{"deviceId": "device-1", "action": "revoke"}
```

- `deviceId` 为非空字符串，`action` 为 `"grant"` 或 `"revoke"`；类型头不符、JSON 非法、尾随内容、字段缺失或类型错误、`action` 取值非法均返回 `400` JSON 错误且零写入。
- 设备未注册返回 `404` JSON 错误且零写入。
- 每个（文档， 设备）对初始为已授权。成功返回 `200` `{"deviceId":"device-1","authorized":false,"changed":true}`：`revoke` 首次置为未授权（`changed=true`），重试幂等（`changed=false`）；`grant` 恢复授权，已授权时重试不改（`changed=false`）。
- 权限变更在序列化事务内完成并同步落盘：并发 grant/revoke 各自完整提交，重启后状态与幂等判定不变。
- 撤回不删除变更、快照或会话，也不影响 documents changes、merge、restore；仅会话视角的 changes 读取返回 `403`。

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

把某个快照的历史 state 作为一次普通变更追加回当前文档。仅接受 `Content-Type: application/json`。请求体：

```json
{"deviceId": "device-1", "changeId": "change-9", "snapshotCursor": 2}
```

- `documentID`、`deviceId`、`changeId` 均为非空字符串；`snapshotCursor` 为正整数（拒绝小数、字符串、布尔、`null` 或缺失）。
- 类型头不符、JSON 非法、尾随内容、空 `documentID` 或任一字段无效均返回 `400` JSON 错误且零写入。
- `snapshotCursor` 未命中该文档的快照（含未知文档）返回 `404` JSON 错误且零写入。
- 命中后在单事务内把该快照的 `state` 作为 payload，以 `deviceId`、`changeId` 追加一条普通 change；历史记录不变，文档 cursor 加一。成功返回 `200`：`{"id":changeId,"created":true,"cursor":N,"restoredFrom":snapshotCursor}`。
- 同一文档同一 `changeId` 仅当 `deviceId`、`snapshotCursor` 与来源 state 都相同时才幂等：返回 `200`、`created=false`、首次 cursor；该 id 已被普通变更占用，或任一条件不符，均返回 `409` JSON 错误且零写入。
- 恢复来源随数据落盘，重启后幂等与冲突判定不变；并发恢复在序列化事务内分配唯一且连续的 cursor。

### 路径中的空标识

`/v1/documents//changes`、`/v1/documents//merge`、`/v1/devices//sessions`、`/v1/devices/{id}/sessions/`、`/v1/sessions//documents/{id}/changes`、`/v1/sessions/{id}/documents//changes` 等任一标识段为空（连续斜杠或以斜杠结尾）的请求返回 `400` JSON 错误（`{"error": "..."}`），而不是重定向或 `404` HTML 页面。非空路径的语义保持不变。

## 附件（分块、可恢复、去重）

附件是面向已注册设备的可恢复分块上传。创建、分块、完成和读取子资源全部挂在同一设备路径下，服务以路径中的 `deviceId` 判断创建者/归属，不依赖任何其他认证机制。

创建时声明完整元数据：客户端标识、总字节数、分块大小和内容的小写十六进制 SHA-256 摘要；之后分块以二进制上传，序号从零开始，允许乱序提交与断线重试。所有操作均同步落盘，进程重启后上传进度、归属、幂等与去重判定保持一致。

### `POST /v1/devices/{deviceId}/attachments`

仅接受 `Content-Type: application/json`。请求体：

```json
{
  "attachmentId": "att-1",
  "size": 10,
  "chunkSize": 4,
  "sha256": "2c70e75d9b4a8d0f6d2f1e5b0a9c8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3a2b1c"
}
```

- `attachmentId` 为非空字符串；`size`、`chunkSize` 为正整数（拒绝小数、字符串、布尔、`null`、零或负数）；`sha256` 为恰好 64 位小写十六进制字符。
- 类型头不符、JSON 非法、尾随内容、字段缺失或类型错误、非正数或摘要格式错误均返回 `400` JSON 错误且零写入。
- 设备未注册返回 `404` JSON 错误。
- 同一设备以完全相同的元数据重试返回 `200` `{"attachmentId","size","chunkSize","sha256","created":false}`；首次创建 `"created":true`。
- 标识已被其他设备占用时，即使元数据完全相同也返回 `409` JSON 首次记录不得改变；同一设备以不同元数据重试同样 `409`，原声明不变。

### `PUT /v1/devices/{deviceId}/attachments/{attachmentId}/chunks/{index}`

上传一个分块。请求体为二进制，必须携带 `Content-Type: application/octet-stream`；`{index}` 为十进制非负整数，从零开始。

- 内容类型错误或序号格式错误返回 `400`。
- 未知附件返回 `404`；附件归属其他设备返回 `403`。
- 序号越界、非末块长度不等于 `chunkSize`、或末块长度超过声明的余数均返回 `400` 且零写入。
- 同序号再次提交相同字节返回 `200` 幂等（响应携带 `attachmentId`、`index`、`bytes`）；同序号提交不同字节返回 `409` 并保留首次内容。
- 分块允许乱序上传，已提交的分块持久化，断线后可继续。

### `POST /v1/devices/{deviceId}/attachments/{attachmentId}/complete`

封存上传，无请求体。只有创建者可调用：非属主设备返回 `403`；未知附件返回 `404`。

- 缺少分块，或已收分块累计长度不等于声明总长度，返回 `409` 且上传保持可继续。
- 分块齐全且长度相符时按序号拼接并校验 SHA-256：摘要不一致返回 `422` 并保持未完成（分块仍可读取、上传可修正后重试）。
- 成功返回 `200` `{"attachmentId","size","sha256","completed":true,"reused":…}`；重复完成返回相同结果。
- 当已有其他已完成附件具有相同摘要和大小，直接复用其内容：`"reused":true` 并以 `"reusedFrom"` 标明来源附件，不复制字节。摘要相同但大小不同不得复用，返回 `409` 且当前上传仍可恢复。

### `GET /v1/devices/{deviceId}/attachments/{attachmentId}`

创建者读取附件元数据与上传进度：

- 命中返回 `200` `{"attachmentId","deviceId","size","chunkSize","sha256","completed","received":[…]}`，`received` 为已收到分块的序号（升序）。
- 非创建设备返回 `403`；未知附件返回 `404`。

### `GET /v1/devices/{deviceId}/attachments/{attachmentId}/chunks/{index}`

读取指定分块的原始字节（`Content-Type: application/octet-stream`）。

- 序号格式非法（非十进制非负整数）返回 `400`；附件未知返回 `404`；非创建设备返回 `403`。
- 序号越界返回 `400`；序号合法但该分块尚未收到（或已被首次内容占用后无法读取其他版本）返回 `404`。

### 空标识与持久化

附件路径下任一段为空（连续斜杠或以斜杠结尾）同样返回 `400` JSON 错误。创建、分块、完成和读取均在序列化事务内同步持久化（WAL + `synchronous=FULL`）；重启后元数据、已收分块、完成状态、幂等/冲突判定以及内容去重关系均保持不变。本次不改变设备、会话、权限、变更、合并、快照和恢复的既有语义。
