# 首条请求驱动的 Discord Thread 标题

## Status

Done

## Source

- GitHub Issue: https://github.com/HXYerror/codeXD/issues/2
- Base commit: `e3f6b09f7d51a1efeb43bafc29693d35a70454dd`

## Overview

频道内通过 `@codexD` 创建会话时，用首条请求的本地、确定性、脱敏摘要作为
Discord Thread 标题主体，并保留 4 位 ingress ID 作为轻量排障标识。

标题格式：

    <脱敏摘要> · <ingress id 前 4 位>

例如：

- `修复登录后刷新白屏 · 5db2`
- `调研 Thread 自动命名 · 495a`
- `图片任务 · 5db2`

## Motivation

当前 `codex-<ingress id 前 8 位>` 只适合确定性重放和排障，无法帮助用户在多个
并行会话中快速识别目标 Thread。新标题应把可读语义放在最前，同时不降低现有
幂等性和安全边界。

## Requirements

1. 只在频道 mention 首次创建 Discord Thread 时生成自动标题。
2. 标题使用已经移除触发 bot mention 的首条文本；文本加图片仍以文本为主。
3. 复用现有 redaction，移除密钥、认证值、用户主目录、完整项目根路径和控制字符，
   并压平换行及连续空白。
4. 移除或中和 `@everyone`、`@here`、用户 mention、角色 mention 和频道 mention，
   使标题不包含可触发或原样泄露的 Discord mention 语法。
5. 摘要最多 72 个 Unicode 字符；添加 ` · ` 和 ingress ID 前 4 位后，最终长度必须
   保持在 Discord 的 1–100 字符约束内。
6. 无文本但有图片时使用 `图片任务`；脱敏后为空或无可读文本时使用 `新任务`。
7. 算法完全本地且确定性，不新增模型调用、网络调用或 Thread 创建前的阻塞工作。
8. raw prompt 只可在内存中参与生成标题，不得新增持久化字段或写入日志；outbox
   只保存最终安全标题。
9. 同一入口的重复处理复用已有 ingress/outbox，不改变标题或创建重复 Thread。
10. 创建失败重试、daemon 恢复和 existing-thread reconciliation 保持现有行为。
11. 不修改既有 Thread，也不改变 `/session rename`、resume、fork 或后续消息行为。
12. 更新详细设计 11.2，把“不得截取 raw prompt”修订为“不得使用未经脱敏、未经
    截断的 raw prompt”。

## Acceptance Criteria

- 新会话 Thread 首先显示可读摘要，而不是仅显示 `codex-xxxxxxxx`。
- 标题不包含触发 bot mention、其他 Discord mention、控制字符、已识别 secret、
  authentication value、用户主目录或完整项目根路径。
- 所有标题长度均为 1–100 个字符；中文、英文和 emoji 输入产生有效 Unicode 字符串。
- 纯图片和脱敏后空文本分别使用约定 fallback。
- 同一入口重复处理时 title 和 Thread identity 保持稳定。
- outbox retry、进程恢复和远端 Thread reconciliation 的既有幂等测试继续通过。
- `/session rename` 的显式 rename outbox 行为不变，后续消息不重新自动命名。
- 既有 Thread 不迁移、不改名。

## Technical Approach

1. 在 security/redaction 层增加一个小型纯函数，基于 `redacted_summary()` 生成
   mention-safe、长度受限的标题主体；不要让 Discord transport 依赖进入 storage
   层。
2. 让 bot 在移除自身触发 mention 后，把首条内存文本和是否含图片传给
   `Repository.request_thread_creation()`。继续传递并校验现有 content hash 和
   attachment manifest hash。
3. Repository 只在新建 ingress 时、取得 ingress ID 和 project root 后生成最终
   标题并写入原有 `create_thread` outbox payload。重复入口分支直接返回原 outbox。
4. Discord outbox delivery 继续只消费已持久化的安全 `name`，不重新计算，因此
   retry/reconciliation 保持确定性。
5. 保持 `/session rename` 代码路径完全独立。

## Testing Strategy

- Redaction unit tests：secret、Authorization、HOME/project path、控制字符、换行、
  空白、各类 Discord mention、中文、英文、emoji、长度边界和两个 fallback。
- Repository tests：标题格式、72 字符摘要上限、4 位后缀、最终长度、确定性、重复
  请求复用原 outbox，且 payload 不含 raw prompt。
- Discord bridge tests：文本、文本加图片、纯图片均把正确输入送入创建流程。
- Reliability regression：失败重试、restart recovery、existing-thread
  reconciliation 和单 Thread 创建语义不变。
- Session regression：`/session rename` 仍只执行显式 rename。
- 运行 `ruff check .`、`mypy` 和完整 `pytest`。

## Out of Scope

- 批量重命名既有 Thread。
- 调用 Codex/LLM 生成标题。
- 改变 `/session new`、resume、fork 或 `/session rename` 的命名行为。
- 修改 Discord Thread identity 或现有数据库 schema。
