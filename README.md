# Local-first Sync Service

这是一个面向通用协作产品的 Local-first 多端同步后端。长期目标是提供离线编辑、多设备增量同步、冲突合并、历史恢复、设备身份、权限撤回、附件去重以及推送通道，并把 CRDT、事件日志和权限控制沉淀为可复用服务。

仓库采用 Go，当前提供进程健康检查与持久化文档增量同步。后续能力必须通过独立题目逐步实现；每个题目都应定义可观察的公共行为、兼容边界和失败语义，不得依赖未公开内部 API。

## 启动

```bash
go run ./cmd/syncd
```

服务默认监听 `127.0.0.1:8080`。可通过 `SYNC_ADDR` 修改监听地址，通过 `SYNC_DATA` 指定 SQLite 数据库路径（默认 `data/sync.db`，启动时自动创建目录）。变更在提交时同步落盘，进程重启（含崩溃）后数据与游标可读。

启动与监听语义：

- `SYNC_ADDR` 必须是合法的 `host:port`，端口为 `0..65535` 的整数。格式错误（缺少端口、端口非数字或越界等）直接以非零状态退出并输出明确错误，不会回退到默认端口。
- 固定端口只绑定声明的地址：该地址被占用时以非零状态退出并保留监听错误，不复用其他进程的监听器、也不更换端口；端口释放后按同一地址重启仍受同样约束。
- 端口设为 `0`（如 `127.0.0.1:0`）时由系统分配空闲端口。启动成功后日志输出实际可访问的监听地址与本进程数据路径：

  ```
  sync service listening on 127.0.0.1:51314 (data: /var/lib/sync/sync.db)
  ```

  使用 `:0` 时应以该日志行给出的 `host:port` 访问服务，而不是猜测默认端口；通配绑定（`:0`）在日志中按 `127.0.0.1:<port>` 报告。
- 启动按“校验地址 → 绑定监听 → 创建数据目录并打开数据库”的顺序进行，任一步失败都会关闭已获取的资源并以非零状态退出，不会留下半就绪服务；监听成功且数据库打开之前不输出就绪日志。
- 每个进程只使用自己声明的地址和数据库。多个进程使用不同 `SYNC_ADDR`/`SYNC_DATA` 同时启动时互不覆盖端口、数据库或健康状态；同名设备、会话等数据按各自数据库独立，互不可见。
- 收到 `SIGINT` 或 `SIGTERM` 时停止接收新请求、等待在途请求结束并关闭数据库后以零状态退出；已提交的数据（含已封存附件）在下次启动后仍可读取。启动失败期间不会写入任何完成标记、附件记录或其他业务数据。

## 验证

```bash
go test ./...
```

## HTTP 接口

### `GET /healthz`

数据库成功打开且监听建立后返回 `200` `{"status":"ok"}`。监听尚未建立或数据库初始化失败（即启动未完成）时不宣称就绪：此时整个服务表面（含 `/healthz` 自身）返回 `503` JSON 错误（`{"error":"service is not ready"}`），健康检查不会得到成功响应，业务接口也不可达；就绪后设备、会话、权限、文档与附件接口才在实际监听地址上提供服务，且 `/healthz` 的 `200` 状态码与 JSON 形状不变。

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

### `GET /v1/documents/{documentID}/changes/poll`

断线客户端的长轮询续传入口，在 `GET .../changes` 的分页语义上增加一个等待期限。查询参数：

- `after`：非负整数，默认 `0`，只返回 cursor 严格大于它的变更（沿用既有变更读取约束）。
- `limit`：`1..1000` 的整数，默认 `100`（沿用既有变更读取约束）。
- `waitMs`：`0..30000` 的整数，默认 `0`，即愿意为后续变更等待的毫秒数。

行为：

- `after` 之后已有变更时立即返回，不进入等待；响应结果继续按 cursor 递增。
- 已知文档但暂无新变更时，请求保持到期限届满或首条新变更提交；新变更（普通提交、merge、restore、replay 任一途径）一提交即被唤醒并立即返回同一结果结构。
- 未知文档立即返回空列表与 `nextCursor: 0`，不挂起请求。
- 期限届满仍无变化时返回空列表、`nextCursor` 等于请求中的 `after`、`timedOut: true`，绝不伪造游标推进；数据就绪或未知文档的即时返回均带 `timedOut: false`。
- 响应：`{"changes":[{"id","deviceId","payload","cursor"}, ...], "nextCursor": N, "timedOut": bool}`，`changes` 按 cursor 递增。
- `after`、`limit` 或 `waitMs` 非法（含 `waitMs` 超出 `0..30000`、小数、非数字）返回 `400` JSON 错误且不写入。
- `documentID` 为空、路径段缺失或多余（如尾斜杠）、方法不匹配（非 GET）一律返回 `400` JSON 错误，不重定向、不输出 HTML。
- 客户端断开连接或服务关闭时等待立即取消；被中断的等待不留下任何变更、游标或其他记录，服务关闭时挂起的请求以 `503` JSON 错误返回。

### `GET /v1/sessions/{sessionId}/documents/{documentId}/changes/subscribe?cursor=N`

会话视角的 WebSocket 文档订阅（推送通道）。客户端携带已存在的会话标识、文档标识与起始游标发起 RFC 6455 升级握手，服务端在握手通过后建立一条**只推不写**的长连接。无新增认证机制：订阅身份完全由已存在的会话标识决定（会话所属设备即为订阅设备）。

握手请求：

```
GET /v1/sessions/sess-1/documents/doc-1/changes/subscribe?cursor=12 HTTP/1.1
Connection: Upgrade
Upgrade: websocket
Sec-WebSocket-Key: <16 字节随机值的 base64>
Sec-WebSocket-Version: 13
```

- `cursor` 为必填的非负十进制整数；为负数、小数、非数字或缺失时返回 `400` JSON 错误，不建立连接、不写入任何记录。
- 方法不是 `GET`、缺少或不合法的升级握手（`Connection: Upgrade`、`Upgrade: websocket`、`Sec-WebSocket-Version: 13`、合法 `Sec-WebSocket-Key` 任一不满足）返回 `400` JSON 错误，不重定向、不输出 HTML。
- `sessionId`、`documentId` 为空段、路径段缺失或多余（尾斜杠、额外段等）同样返回 `400` JSON 错误。
- 校验顺序固定为：游标与握手形状（`400`）→ 会话存在性（会话不存在或已删除为 `404`）→ 权限（会话所属设备对该文档权限被撤回为 `403`），全部发生在升级之前，错误均为 JSON 且不返回任何变更内容。
- 握手成功返回 `101 Switching Protocols` 与按 RFC 6455 计算的 `Sec-WebSocket-Accept`。

连接建立后的推送语义：

- 先按游标递增补齐起始游标之后**已存在**的变更（每页最多 1000 条，循环补齐），随后无缝接上实时推送；重连后从任意历史游标都能续上。
- 每条消息是一个文本帧，形状与变更读取返回的单条记录完全一致：`{"id","deviceId","payload","cursor"}`，顺序与分页读取逐字一致，不重排、不合并、不分包依赖。
- 普通提交（`POST .../changes`）、合并（`merge`）、恢复（`restore`）、离线重放（`replay`）任一途径写入的新变更，都在事务提交后立即推送给订阅方；幂等重复提交不产生新游标也不推送。
- 推送与读取共享同一份变更日志：观察到的游标可原样作为下次分页读取或重连续订的起点。推送本身不改变文档游标空间，不产生新变更记录，也不改变幂等判定。
- 连接只推不写：客户端经该连接发送的任何数据帧都会被读取并丢弃，不能提交、修改或删除任何变更；客户端的 `ping` 会收到 `pong`，`close` 按 RFC 6455 回应。

连接生命周期与关闭码：

- 订阅建立后权限被撤回时，服务端以关闭码 **4403** 结束订阅，随后停止推送任何变更；该撤回在本连接上是一次性且不可撤销的——即使紧接着重新授权，本连接仍以 4403 结束（客户端需以当前会话重新订阅）。
- 收到终止信号（`SIGINT`/`SIGTERM`）时，服务端以关闭码 **1001**（going away）结束全部订阅；已提交数据与游标保持可读，进程以零状态退出。终止信号优先于同时挂起的撤回。
- 客户端断开或网络中断时服务端立即释放订阅：不留下变更、游标、订阅或其他任何记录。
- 重启后已提交变更仍按原游标可读；订阅状态不持久化，新订阅可从任意历史游标开始补齐。

### `POST /v1/documents/{documentID}/replay`

离线操作重放入口。仅接受 `Content-Type: application/json`，请求体：

```json
{
  "deviceId": "device-1",
  "operations": [
    {"id": "change-1", "payload": {"any": "json"}},
    {"id": "change-2", "payload": [1, true, null]}
  ]
}
```

- 每一项都按既有变更提交语义的一次重试处理：`operations` 为非空数组，元素含非空字符串 `id` 和任意 JSON `payload`，按原顺序排列。
- 类型头不符、请求体不是合法 JSON、带尾随内容、`deviceId` 缺失或为空、`operations` 缺失或为空、元素缺少非空 `id` 或 payload、批内 `id` 重复，均返回 `400` JSON 错误且整批零写入。
- 设备未注册返回 `404` JSON 错误；该设备对此文档的权限已撤回返回 `403` JSON 错误；两类错误均不暴露任何变更内容。
- 同一文档已有标识只有在 `deviceId` 与解码后的负载都相同时才算幂等（`created=false`，cursor 为首次值）；已有标识的负载或设备不一致时返回 `409` JSON 错误：`{"error": "...", "conflictId": "..."}`，整批保持原状。
- 成功返回 `200` `{"results":[{"id","created","cursor"}, ...]}`，顺序与请求一致，逐项说明新建（`created=true`）或幂等（`created=false`）及对应 cursor。
- 离线重放与普通提交共享每个文档连续、不重复的游标空间，并在同一个序列化原子事务内提交；并发的普通提交与重放不会重复分配 cursor，也不会留下半批数据。
- 所有判定与负载同步落盘：重启后再次等待或重放得到一致的状态、幂等判定和原始负载。

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

### `GET /v1/documents/{documentID}/snapshots`

按游标区间一次性批量导出某文档的历史快照。仅接受 `GET`，无请求体。查询参数：

- `from`：闭区间起点，十进制非负整数，缺省为 `0`。
- `to`：闭区间终点，十进制非负整数；缺省时不设上界。终点小于起点返回 `400` JSON 错误。

- 只导出 cursor 落在闭区间 `[from, to]` 内的快照，按游标升序排列，同一游标最多出现一次。
- 未知文档或区间内没有任何快照时同样成功：返回 `200`，正文为空列表与零计数，而不是错误。
- 正文是一行紧凑 JSON、末尾一个换行，顶层键按 `snapshots`、`count` 的顺序固定；数组元素按游标递增，每项只有 `cursor` 与 `state` 两个键且键序固定。`state` 原样呈现创建时保存的 JSON 内容，可以是 `null`、数字、字符串、数组或对象，与单个读取入口完全一致。`count` 为本次导出的快照条数，与数组长度一致，为非负整数。例如：

```
{"snapshots":[{"cursor":1,"state":null},{"cursor":2,"state":{"any":"json"}}],"count":2}
```

- 导出是只读操作：不创建快照、不占用变更游标，也不产生变更记录或推送通知。
- 导出期间并发创建的快照要么整体出现在结果里，要么整体缺席，不会读到半个记录；快照同步落盘，进程重启后同一区间导出的正文逐字不变。
- `from`/`to` 不是十进制非负整数、终点小于起点、`documentID` 为空，均返回 `400` JSON 错误且零写入。
- 路径段缺失或多余（如 `/v1/documents/{id}/snapshots/`、`/v1/documents/{id}/snapshots/1/extra`）或方法不是 `GET`（集合路径上的 `POST` 仍是上面的创建入口）同样返回 `400` JSON 错误，不重定向也不输出 HTML；这些失败一律零写入。

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

### `POST /v1/devices/{deviceId}/attachments`

为已注册设备创建可恢复的分块附件上传。仅接受 `Content-Type: application/json`。请求体：

```json
{
  "attachmentId": "att-1",
  "totalBytes": 11,
  "chunkSize": 4,
  "sha256": "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
}
```

- `attachmentId` 为非空字符串（稳定标识）；`totalBytes`、`chunkSize` 为正整数（不接受小数、字符串、布尔、`null`、零或负数）；`sha256` 为 64 位小写十六进制摘要。类型头不符、JSON 非法、尾随内容、字段缺失或类型错误、数值非正、摘要格式错误均返回 `400` JSON 错误且零写入。
- 设备未注册返回 `404` JSON 错误且零写入。
- 首次创建返回 `200` `{"attachmentId":"att-1","created":true}`；同一设备以完全相同元数据重试幂等返回 `created=false`。
- `attachmentId` 已被占用时返回 `409` JSON 错误且原记录不变：无论占用者是其他设备（即使元数据相同）还是本设备但元数据不同。

### `PUT /v1/devices/{deviceId}/attachments/{attachmentId}/chunks/{index}`

上传一个分块，请求体为二进制，仅接受 `Content-Type: application/octet-stream`。序号 `index` 从零开始，分块可乱序到达，断线后可重复提交。

- 未知附件返回 `404` JSON 错误；路径设备不是创建者返回 `403` JSON 错误。
- 内容类型错误、序号非法或越界（`>= ceil(totalBytes/chunkSize)`）、非末块长度不等于 `chunkSize`、末块长度超出声明范围的剩余字节，均返回 `400` JSON 错误且零写入。
- 首次存储返回 `200` `{"index":N,"created":true}`；同序号再次提交相同字节幂等返回 `created=false`；同序号不同字节返回 `409` JSON 错误，保留首次内容。
- 附件一旦成功封存即不可变：此后任何分块写入（即使字节相同）均返回 `409` JSON 错误，封存状态与已保存内容不变。

### `POST /v1/devices/{deviceId}/attachments/{attachmentId}/complete`

封存上传。无请求体要求。

- 仅创建者可调用：非创建设备返回 `403` JSON 错误，未知附件返回 `404` JSON 错误。
- 分块未齐或累计长度不等于声明的 `totalBytes` 返回 `409` JSON 错误，上传保持可继续；按序拼接后摘要与声明不符返回 `422` JSON 错误，上传保持未完成。
- 成功返回 `200`：`{"attachmentId","size","sha256","complete":true,"reused":...}`；重复完成返回相同结果。
- 已有其他已完成附件拥有相同摘要和大小时复用其内容（`reused=true`），不复制字节；摘要相同但大小不同不得复用，返回 `409` JSON 错误且当前上传仍可恢复。

### `POST /v1/devices/{deviceId}/attachments/{attachmentId}/access`

为指定设备授予或撤回该附件的只读访问授权。仅创建者可调用。仅接受 `Content-Type: application/json`。请求体：

```json
{"deviceId": "device-2", "action": "grant"}
```

- `deviceId` 为非空字符串，`action` 为 `"grant"` 或 `"revoke"`；类型头不符、JSON 非法、尾随内容、字段缺失或类型错误、`action` 取值非法均返回 `400` JSON 错误且零写入。
- 未知附件返回 `404` JSON 错误；路径设备不是创建者返回 `403` JSON 错误；目标设备未注册返回 `404` JSON 错误；均零写入。
- 每个（附件， 设备）对初始为未授权。成功返回 `200` `{"deviceId":"device-2","authorized":true,"changed":true}`：`grant` 首次置为已授权（`changed=true`），重试幂等（`changed=false`）；`revoke` 恢复未授权，未授权时重试不改（`changed=false`）。
- 授权变更在序列化事务内完成并同步落盘：并发 grant/revoke 各自完整提交，重启后状态与幂等判定不变。
- 授权只影响读取面：被授权设备读取元数据与分块返回与创建者逐字一致的 `200`；撤回后读取立即回到 `403`，已封存内容不删除，去重复用判定不受影响；分块写入与封存仍仅创建者可调用，被授权设备写入一律 `403` 且零写入。授权与文档权限互不牵连。

### `GET /v1/devices/{deviceId}/attachments/{attachmentId}`

创建者或被授权设备读取附件元数据，返回 `200`：`{"attachmentId","totalBytes","chunkSize","sha256","complete","receivedChunks":[...]}`，其中 `receivedChunks` 为已收到序号的升序列表；被授权设备所见正文与创建者逐字一致。其他设备返回 `403` JSON 错误，未知附件返回 `404` JSON 错误。

### `GET /v1/devices/{deviceId}/attachments/{attachmentId}/chunks/{index}`

创建者或被授权设备读取指定分块，命中返回 `200`，`Content-Type: application/octet-stream`，正文为对应字节。空标识（设备或附件）返回 `400` JSON 错误；已知附件的序号为负数、非十进制或超出声明分块范围（`>= ceil(totalBytes/chunkSize)`）也统一返回 `400` JSON 错误而非重定向或 HTML；未知附件返回 `404`，序号合法但该分块尚未到达也返回 `404` JSON 错误；其他设备返回 `403` JSON 错误。

### `DELETE /v1/devices/{deviceId}/attachments/{attachmentId}`

创建设备彻底撤下自己的附件。无请求体、无新增认证；调用者身份由路径上的设备标识决定。

- 成功返回 `200` 一行 JSON：`{"attachmentId":"att-1","deleted":true}`。提交后数据库中不再保留该附件记录：已收到的分块（含未完成上传的部分分块与进度）、封存状态以及给其他设备的全部授权一并删除，删除结果同步落盘。
- 删除后任何人（创建者与曾被授权设备均不例外）再读取元数据或分块一律返回 `404` JSON 错误；对该附件继续上传分块、请求封存或调整授权也一律返回 `404` JSON 错误且零写入。
- 重复删除同一附件返回 `404` JSON 错误，不会再次改变任何状态。
- 未知附件返回 `404` JSON 错误；路径设备不是创建者（被授权设备同样无权删除）返回 `403` JSON 错误，且零写入。
- 未完成的上传也可以删除，其已收到的分块与进度一并消失，不再占用分块范围；删除后用同一附件标识重新创建按全新上传处理，返回新建成功。
- 摘要内容的去重复用不受删除影响：仍有其他已完成附件引用同一摘要时，底层字节保留，其他附件照常读到自己的字节，之后相同内容的上传仍可复用；只有当最后一个引用该内容的附件被删除时，底层字节才随之回收。
- 标识为空、路径段缺失或多余（如对 `.../attachments`、`.../attachments/{id}/chunks/0` 发起 DELETE）、方法不匹配均返回 `400` JSON 错误，不重定向也不输出 HTML。
- 删除在序列化事务内完成：同一附件的并发删除至多一次真正生效，其余返回 `404`；服务重启后删除结果保持一致，任何人读取已删除附件依旧返回 `404`。

附件的创建、分块、完成、删除与读取均同步落盘；进程重启后上传状态、内容去重、幂等判定、删除结果与封存后的拒写判定保持一致。

### `POST /v1/documents/{documentID}/crdt/ops`

在变更日志之外提交一批 CRDT 操作，驱动文档可自动合并的 CRDT 状态。仅接受 `Content-Type: application/json`。请求体声明文档类型（`"counter"` 计数器、`"gset"` 只增集合、`"register"` 单值寄存器或 `"orset"` 可移除集合）：

```json
{
  "deviceId": "device-1",
  "type": "counter",
  "ops": [
    {"id": "op-1", "value": 5}
  ]
}
```

只增集合的操作形如 `{"id":"op-1","elements":["apple","banana"]}`；register 的操作形如 `{"id":"op-1","version":3,"value":{"any":"json"}}`；orset 的操作形如 `{"id":"op-1","action":"add","element":"apple"}` 或 `{"id":"op-2","action":"remove","element":"apple"}`。

- 文档类型由**第一次成功提交**确定为计数器、只增集合、register 或 orset，此后不可更改；之后批次声明其他类型返回 `409` JSON 错误。并发声明两种类型时在序列化事务内恰好一个生效，另一个请求返回 `409`，双方都在同一条数据库连接上串行判定。
- 计数器：每个操作携带本设备的**累计贡献值**（非负整数，不接受小数、字符串、布尔、`null` 或负数）。同一设备的贡献值只能单调推进；倒退的推进值返回 `409` 且状态不变。合并状态是各设备贡献最大值之和。
- 只增集合：操作向集合添加一个或多个非空字符串元素；合并状态是全部已接受元素的并集，按升序呈现。重复添加同一元素不改变状态；已接受操作的合并结果与到达顺序无关（交换、结合、幂等）。
- register：操作携带任意合法 JSON 值（`null`、数字、字符串、数组或对象，按原样保存）与非负整数 `version`。同一设备后续提交的版本必须**严格大于**此前已接受的版本，倒退或持平返回 `409` 且状态不变。合并状态是逻辑版本最大的操作的值，版本相同时按操作 `id` 的字典序取较小者的值；合并结果与到达顺序无关（交换、结合、幂等），任何时刻读到的值都一致。
- orset：操作携带 `action`（`"add"` 或 `"remove"`）与非空字符串 `element`。每次添加为元素打上以操作 `id` 为标识的新标签；移除只移除该操作**已经观测到**的添加（即先于它被接受的添加），其余添加不受影响——与添加并发的移除不会删除该添加。对从未添加过或已完全移除的元素执行移除不报错也不改变集合。合并状态是仍持有有效标签的元素集合，按升序呈现；已接受操作的合并结果与到达顺序无关（交换、结合、幂等），任何时刻读到的值一致。
- 每个操作带稳定的非空字符串 `id`，在文档内唯一；`ops` 必须是非空数组，批内 `id` 重复返回 `400`。重复提交同一 `id`：设备与内容都相同视为幂等（`created=false`，计数器按值比较、集合按元素集合比较（与元素顺序无关）、register 按设备、版本与值三者比较、orset 按动作与元素比较）；内容或来源设备不同返回 `409` 且状态不变。
- 请求体格式错误、尾随内容、`deviceId`/`type` 缺失或非法、操作列表为空、操作缺 `id` 或 `id` 为空、内容字段缺失或类型错误，均返回 `400` JSON 错误且整批零写入。
- 设备未注册返回 `404` JSON 错误；该设备对此文档的权限被撤回返回 `403` JSON 错误；两类错误均在读取任何 CRDT 内容之前判定，不暴露类型或状态。
- 成功返回 `200`：`{"results":[{"id","created"}, ...]}`，顺序与请求一致。
- 操作与合并结果在同一个序列化事务内同步落盘；并发提交完整生效，重启后类型固定、幂等/冲突判定与合并结果不变。
- CRDT 状态独立于变更日志：不产生 change 记录、不占用文档游标，也不改变既有变更读取、长轮询、推送、合并、快照与恢复的形状。

### `GET /v1/documents/{documentID}/crdt/state`

读取文档当前的 CRDT 类型与合并结果。

- 尚无任何 CRDT 操作的文档（无论变更日志是否存在）返回 `404` JSON 错误。
- 计数器返回 `200` `{"type":"counter","value":N}`，`N` 为各设备最大贡献之和。
- 只增集合返回 `200` `{"type":"gset","value":[...]}`，`value` 为全部元素的升序数组（无元素时为 `[]`，而非 `null`；文档一旦有过操作即至少含一个元素）。
- register 返回 `200` `{"type":"register","value":...}`，`value` 原样呈现当前胜出的 JSON 内容（可以是 `null`、数字、字符串、数组或对象）。
- orset 返回 `200` `{"type":"orset","value":[...]}`，`value` 为当前仍有效的元素的升序数组（全部移除后为 `[]`，而非 `null`）。
- `documentID` 为空、路径段缺失或多余（如尾斜杠、`/crdt/其他段`）一律返回 `400` JSON 错误，不重定向、不输出 HTML。

### `POST /v1/documents/{documentID}/crdt/compact`

压缩文档的 CRDT 状态层存储：裁掉四种类型中不再影响合并结果的操作与墓碑，让长期运行的文档不再无限累积。仅接受 `Content-Type: application/json`。请求体沿用提交入口的设备判定：

```json
{"deviceId": "device-1"}
```

- 类型头不符、JSON 非法、带尾随内容、`deviceId` 缺失、为空或类型错误，均返回 `400` JSON 错误且零写入。
- 设备未注册返回 `404` JSON 错误；该设备对此文档的权限被撤回返回 `403` JSON 错误；尚无 CRDT 操作的文档返回 `404` JSON 错误。三类错误都不改动任何状态，也不暴露状态内容。
- `documentID` 为空、路径段缺失或多余（如尾斜杠、额外段）、方法不匹配（非 POST）一律返回 `400` JSON 错误，不重定向、不输出 HTML。
- 压缩按类型裁剪：计数器只保留每个设备的最大贡献操作；只增集合只保留覆盖并集所需的操作；register 只保留每个设备版本最大的操作；orset 裁掉已被墓碑覆盖的标签与对应墓碑。合并结果逐字不变（元素仍按升序、值原样呈现），幂等判定与冲突返回的规则不变。
- 被裁掉的操作标识不按新提交处理：裁剪时为每个被裁掉的标识保留其来源设备与各类型比较内容（计数器按值、只增集合按元素集合、register 按版本与值）的规范摘要，仅服务于幂等与冲突判定、不参与合并。压缩后再次提交同一标识：来源设备与比较内容都相同仍返回首次接受时的结果（`created=false`，即使该操作的版本或贡献已被同设备更新的记录覆盖）；比较内容或来源设备不一致仍返回 `409` 且状态不变。orset 的操作日志不参与裁剪，其增、删操作的重放判定照旧。保留信息为每条被裁标识一行定长摘要，不计入下文两个计数，也不会让存储涨回裁剪前的规模。
- 成功返回 `200`，正文与紧接着的 `GET .../crdt/snapshot` 逐字一致：紧凑单行 JSON，键按 `type`、`value`、`operations`、`tombstones` 顺序，末尾一个换行，如 `{"type":"counter","value":11,"operations":2,"tombstones":0}`。重复压缩不报错，返回同一正文。
- 压缩只影响 CRDT 状态层的存储规模：不占用变更游标、不产生变更记录；合并值没有真正变化，订阅方收不到推送，压缩本身不触发通知。
- 并发压缩在序列化事务内完成并同步落盘；重启后计数、状态与判定与之前一致。

### `GET /v1/documents/{documentID}/crdt/snapshot`

读取文档当前的 CRDT 状态快照：合并结果加上仍参与合并的操作条数与墓碑条数。不带设备判定，也不新增认证。

- 尚无 CRDT 操作的文档返回 `404` JSON 错误，与状态读取同一判定。
- 成功返回 `200`，正文为一行紧凑 JSON，末尾一个换行，四个键的顺序固定为 `type`、`value`、`operations`、`tombstones`：`type` 与 `value` 与 `GET .../crdt/state` 逐字一致；`operations` 与 `tombstones` 分别给出压缩后仍参与合并的操作条数与墓碑条数，取值为非负整数（计数器、只增集合、register 的 `tombstones` 恒为 `0`）。
- `documentID` 为空、路径段缺失或多余（如尾斜杠、额外段）、方法不匹配（非 GET）一律返回 `400` JSON 错误，不重定向、不输出 HTML。
- 该入口只读：不占用变更游标、不产生变更记录、不注册订阅；重启后返回的计数、状态与错误码保持一致。

### `GET /v1/sessions/{sessionId}/documents/{documentId}/crdt/state`

会话视角的 CRDT 状态读取：客户端用**已存在的会话身份**（会话所属设备即为读取设备，无新增认证机制）读取文档当前类型与合并结果。请求沿用既有会话路径形态，只接受 `GET`。

成功正文与文档级状态读取**逐字一致**：紧凑单行 JSON，键按 `type`、`value` 顺序，末尾带一个换行——计数器为 `{"type":"counter","value":N}`（`N` 为各设备最大贡献之和，非负整数），只增集合为 `{"type":"gset","value":[...]}`（全部元素的升序数组），register 为 `{"type":"register","value":...}`（当前胜出的 JSON 值，原样呈现），orset 为 `{"type":"orset","value":[...]}`（当前有效元素的升序数组）。任意时刻该正文都与订阅推送消息、`GET /v1/documents/{documentID}/crdt/state` 的结果完全一致。

校验顺序固定，前序失败优先：

- 请求形状（`400` JSON 错误）：方法不是 `GET`、`sessionId`/`documentId` 为空段、路径段缺失或多余（尾斜杠、额外段，如 `/crdt/state/`、`/crdt/state/x`、`/crdt`、`/crdt/subscribe`）；既不重定向也不输出 HTML。
- 会话存在性（`404` JSON 错误）：会话不存在或已删除。参数与形状校验先于会话存在性判定。
- 权限（`403` JSON 错误）：会话所属设备对该文档的权限被撤回，响应不含任何状态内容。
- CRDT 状态存在性（`404` JSON 错误）：尚无任何 CRDT 操作的文档（无论变更日志是否存在），与文档级读取保持同一判定。

会话已删除、文档无 CRDT 状态、权限被撤回是三类相互独立的结果，各自返回上述错误，不合并为同一错误。

该入口只读：客户端不能经它提交或修改任何 CRDT 操作；读取不改变文档游标、不产生变更记录，也不影响 CRDT 提交的类型固定与幂等判定，读取本身不留任何记录。进程重启后返回的状态、错误码与判定保持一致。CRDT 提交入口与文档级状态读取的既有成功与失败语义不变。

### `GET /v1/sessions/{sessionId}/documents/{documentId}/crdt/state/subscribe`

会话视角的 WebSocket CRDT 状态订阅（CRDT 状态的推送通道）。客户端携带已存在的会话标识与文档标识发起 RFC 6455 升级握手，服务端在握手通过后建立一条**只推不写**的长连接。无新增认证机制：订阅身份完全由已存在的会话标识决定（会话所属设备即为订阅设备）。

握手请求：

```
GET /v1/sessions/sess-1/documents/doc-1/crdt/state/subscribe HTTP/1.1
Connection: Upgrade
Upgrade: websocket
Sec-WebSocket-Key: <16 字节随机值的 base64>
Sec-WebSocket-Version: 13
```

- 方法不是 `GET`、缺少或不合法的升级握手（`Connection: Upgrade`、`Upgrade: websocket`、`Sec-WebSocket-Version: 13`、合法 `Sec-WebSocket-Key` 任一不满足）返回 `400` JSON 错误，不重定向、不输出 HTML。
- `sessionId`、`documentId` 为空段、路径段缺失或多余（尾斜杠、额外段等，如 `/crdt/state`、`/crdt/subscribe`、`/crdt/state/subscribe/x`）同样返回 `400` JSON 错误。
- 校验顺序固定为：握手形状（`400`）→ 会话存在性（会话不存在或已删除为 `404`）→ 权限（会话所属设备对该文档权限被撤回为 `403`），全部发生在升级之前，错误均为 JSON。
- 握手成功返回 `101 Switching Protocols` 与按 RFC 6455 计算的 `Sec-WebSocket-Accept`。

连接建立后的推送语义：

- 握手成功后先推一条文档**当前合并状态**；文档尚无任何 CRDT 操作时（无论变更日志是否存在）静默等待、不发任何帧，首个状态出现时再推。
- 计数器各设备最大贡献之和一旦变化、只增集合的并集一旦增长、register 的胜出值一旦变化、或 orset 的有效元素集合一旦变化，就在事务提交后立即把新的合并结果推送给该文档的全部订阅方；先后顺序以事务提交顺序为准。
- 幂等的重复提交、被拒绝的倒退推进或版本倒退/持平（`409`）和整批非法请求（`400`）都不改状态，也不产生任何通知；向只增集合重复添加已有元素（即使是新操作 id）、register 接受了一个不改变胜出值的操作、orset 重复添加已有元素或移除不存在/已移除的元素都不算状态变化，只有合并值真正变化时才推送一条消息。
- 每条消息是一个文本帧，内容与 `GET .../crdt/state` 的正文逐字一致：紧凑单行 JSON，键按 `type`、`value` 顺序，末尾带一个换行。计数器消息为 `{"type":"counter","value":N}\n`（N 为各设备最大贡献之和），只增集合消息为 `{"type":"gset","value":[...]}\n`（元素升序），register 消息为 `{"type":"register","value":...}\n`（胜出的 JSON 值，原样呈现），orset 消息为 `{"type":"orset","value":[...]}\n`（当前有效元素升序），数值一律非负整数。
- 推送与状态读取共享同一份 CRDT 合并状态：任意时刻收到的消息都可与状态读取入口的结果逐字对照。推送本身不占用文档游标、不产生变更记录、不写入 CRDT 操作，也不改变 CRDT 提交的类型固定、幂等判定与冲突返回；CRDT 操作同样不会唤醒变更订阅通道。
- 连接只推不写：客户端经该连接发送的任何数据帧都会被读取并丢弃，不能提交或修改任何操作；客户端的 `ping` 会收到 `pong`，`close` 按 RFC 6455 回应。

连接生命周期与关闭码：

- 订阅建立后权限被撤回时，服务端以关闭码 **4403** 结束订阅，随后停止推送任何状态；该撤回在本连接上是一次性且不可撤销的——即使紧接着重新授权，本连接仍以 4403 结束（客户端需以当前会话重新订阅，新连接立即拿到当前合并状态）。
- 收到终止信号（`SIGINT`/`SIGTERM`）时，服务端以关闭码 **1001**（going away）结束全部 CRDT 状态订阅；已提交的 CRDT 状态照常可读，进程以零状态退出。终止信号优先于同时挂起的撤回。
- 客户端断开或网络中断时服务端立即释放订阅，不留下任何记录。
- 订阅状态不持久化：进程重启后原订阅消失；重新订阅立即拿到一致的当前合并状态（文档尚无操作时继续静默等待首个状态）。

### 路径中的空标识

`/v1/documents//changes`、`/v1/documents//merge`、`/v1/documents//changes/poll`、`/v1/documents//replay`、`/v1/documents//crdt/ops`、`/v1/documents//crdt/state`、`/v1/documents//crdt/compact`、`/v1/documents//crdt/snapshot`、`/v1/devices//sessions`、`/v1/devices/{id}/sessions/`、`/v1/sessions//documents/{id}/changes`、`/v1/sessions/{id}/documents//changes`、`/v1/sessions//documents/{id}/changes/subscribe`、`/v1/sessions//documents/{id}/crdt/state`、`/v1/sessions/{id}/documents//crdt/state`、`/v1/sessions//documents/{id}/crdt/state/subscribe` 等任一标识段为空（连续斜杠或以斜杠结尾）的请求返回 `400` JSON 错误（`{"error": "..."}`），而不是重定向或 `404` HTML 页面；新长轮询/重放/CRDT/订阅端点的路径段缺失或多余（如 `/v1/documents/{id}/changes/poll/`、`/v1/documents/{id}/replay/x`、`/v1/documents/{id}/crdt/`、`/v1/documents/{id}/crdt/ops/x`、`/v1/documents/{id}/crdt/compact/x`、`/v1/documents/{id}/crdt/snapshot/`、`/v1/sessions/{id}/documents/{id}/changes/subscribe/extra`、`/v1/sessions/{id}/documents/{id}/crdt/state/extra`）以及方法不匹配同样返回 `400` JSON 错误。名为 `crdt`、`poll`、`replay`、`subscribe`、`state` 的文档/会话标识仍按普通标识处理（关键字只在端点自身的段位置才被识别），其既有变更读取、CRDT 状态读取与订阅行为与其它标识完全一致。非空路径的语义保持不变。
