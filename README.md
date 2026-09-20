# Local-first Sync Service

这是一个面向通用协作产品的 Local-first 多端同步后端。长期目标是提供离线编辑、多设备增量同步、冲突合并、历史恢复、设备身份、权限撤回、附件去重以及推送通道，并把 CRDT、事件日志和权限控制沉淀为可复用服务。

仓库采用 Go，当前冻结基线只提供进程健康检查。后续能力必须通过独立题目逐步实现；每个题目都应定义可观察的公共行为、兼容边界和失败语义，不得依赖未公开内部 API。

## 启动

```bash
go run ./cmd/syncd
```

服务默认监听 `127.0.0.1:8080`。可通过 `SYNC_ADDR` 修改监听地址。`GET /healthz` 返回 JSON 健康状态。

## 文档增量同步

- `POST /v1/documents/{documentID}/changes`：提交一批变更（仅接受 `application/json`）。请求体为 `{"deviceId": "...", "changes": [{"id": "...", "payload": <任意JSON>}]}`。同文档内按 `id` 去重：相同 `deviceId` 且解码 JSON 值相同的重放为幂等命中（返回首次 cursor），否则整批 409 且零写入；格式错误或批内 id 重复返回 400。有效批次原子生效，返回 `results`（按请求顺序含 `id`、`created`、`cursor`）。
- `GET /v1/documents/{documentID}/changes?after=0&limit=100`：按 cursor 递增返回 `after` 之后至多 `limit`（1–1000）条变更及 `nextCursor`。已知文档无结果时 `nextCursor=after`；未知文档返回空列表且 `nextCursor=0`。

变更与游标持久化于 `SYNC_DATA_DIR`（默认 `./data`）下的 `state.json`，重启后自动恢复。

## 验证

```bash
go test ./...
```

当前基线刻意不包含 CRDT、冲突合并或权限实现，以便后续任务从已冻结事实出发独立设计并验证这些能力。

