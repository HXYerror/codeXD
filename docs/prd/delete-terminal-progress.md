# Final 成功后删除 Turn Progress Message

## Status

Done

## Source

- GitHub Issue: https://github.com/HXYerror/codeXD/issues/4
- Base commit: `e3f6b09f7d51a1efeb43bafc29693d35a70454dd`

## Overview

Turn 的 progress message 只服务 queued/running/cancelling 期间。完整 final 内容、附件和
terminal footer 成功投递并 ack 后，以 durable outbox 删除整条 progress message，
避免在 Discord Thread 中永久保留重复的大块终态 Embed。

## Requirements

1. queued/running/cancelling 继续原位更新同一 progress message。
2. completed、failed、cancelled、interrupted 采用同一 cleanup 生命周期。
3. provider terminal 时允许终态 progress 短暂显示，并 supersede 未发送的旧 running
   revision；迟到事件不能重新创建 running revision。
4. 只有 `turn_final` 的全部内容、附件和 footer 成功且 outbox ack 为 `sent` 后，
   才在同一 SQLite transaction 幂等创建 progress delete outbox。
5. final retry 时 progress 保留；final dead_letter/superseded 时绝不解锁 cleanup。
6. delete payload 只标识 `kind=turn_progress_delete` 和 Turn/view；远端 message ID
   必须从 `turn_progress_views` 读取，不接受任意用户 message ID。
7. delete 精确 fetch/delete bot 创建的 progress message。NotFound/已手工删除/删除
   成功但 ack 前 crash 均视为幂等成功；429/5xx 重试，403 永久失败进入 diagnostics。
8. delete ack 清空 view 的远端 message ID，并持久记录 deleted 状态/时间；不得重新
   创建替代 progress message。
9. 没有远端 message ID 时 cleanup 可直接收敛完成，不调用 Discord。
10. 保持 destination ordering：progress send/edit reconciliation → final 全部投递
    → delete。cleanup 不参与 progress coalescing，也不能与下一 Turn 混淆。
11. cleanup 失败不改变 terminal Turn、不删除 final、不重复 final。
12. prompt reaction、terminal footer、TaskCard、Table、Notice、Attachment 等保持。
13. 只影响上线后完成的 Turn，不扫描或批量删除历史消息。

## Acceptance Criteria

- 所有 terminal state 在 final 成功后删除 progress message，final 与 footer 保留。
- progress 含流式 plain text 或 final 含多消息/表格/附件时，删除严格发生在最后一次
  final ack 之后。
- transient retry 保留 progress，permanent final failure 不排队 delete。
- 手工删除和 crash-before-delete-ack 通过 NotFound 幂等收敛。
- running edit in-flight 的恢复顺序正确，最终无迟到 terminal Embed。
- cleanup 重复 ack/recovery 不产生重复 outbox 或重复 final。

## Technical Approach

1. 在 `Repository.ack_outbox()` 的成功 transaction 中识别 `turn_final`，并通过
   唯一 dedupe key `turn:<turn_id>:progress:delete` 插入 delete outbox。
2. cleanup destination 复用 progress view；payload 不包含远端 message ID。
3. 扩展 Discord transport delete 分支，从 Repository/view 获取可信 message ID 并
   删除；NotFound 返回成功。
4. delete ack 在同一 transaction 清空 message ID、记录 deleted 状态/时间。若 schema
   需要新字段，使用新 migration，不编辑既有 migration。
5. 复用现有 lease、retry、permanent failure 和 destination queue ordering。

## Testing Strategy

- Storage：final ack 原子创建一次 cleanup；重复 ack/recovery 不重复；dead-letter 不创建。
- Transport：success、NotFound、429/5xx retry、Forbidden permanent。
- Ordering：in-flight edit、multi-part final、crash before ack、destination ordering。
- Terminal matrix：completed/failed/cancelled/interrupted 与 local terminal paths。
- Regression：progress coalescing、final rendering/attachments、prompt reactions/footer。
- 完整运行 ruff、mypy 与 pytest。

## Out of Scope

- 删除历史终态 progress、修改其他 Embed 或删除 prompt reaction/final。
- 在 provider terminal transaction 中直接调用 Discord 或用非持久 background task。
