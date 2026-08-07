# Discord 普通文件附件与 MentionInput

## Status

Approved

## Source

- GitHub Issue: https://github.com/HXYerror/codeXD/issues/3
- Base commit: `e3f6b09f7d51a1efeb43bafc29693d35a70454dd`

## Overview

把 Discord 附件入口从“所有附件都按图片解析”扩展为统一、安全、持久的附件管线。
有效图片继续映射为 `LocalImageInput`；普通文件作为 opaque bytes 保存到 daemon
控制的数据目录，并通过 SDK `MentionInput(name, path)` 交给同一个 Turn。

## Requirements

1. 支持频道 mention 和既有 Conversation 中的文本、图片、普通文件及其混合输入；
   file-only 消息合法，所有输入只创建一个 Turn。
2. 只从批准的 Discord CDN HTTPS host 下载；每次 redirect 重新校验 host，并执行
   connect/read/总超时、单文件及单消息 streaming byte cap。
3. 每个附件只下载一次到随机 quarantine 路径。MediaWorker 按实际内容识别图片：
   可完整 decode 的图片沿用规范化流程；明确非图片按 opaque file 保存；声明为图片
   但损坏、decoder crash 或 timeout 必须继续报 `image_decode_failed`。
4. 任一附件失败时整条 ingress 原子失败，并清理该消息产生的 quarantine、图片和
   普通文件；不得启动 provider Turn。
5. 新增不可变 `TurnFile` 及 `TurnInput.files`，记录 attachment ID、原 ordinal、
   canonical path、安全 display name、reported media type、SHA-256、实际大小和
   retention deadline。图片与文件 ordinal 必须唯一并保持 Discord 顺序。
6. 普通文件最终保存到 `attachments/input/` 的随机 UUID 路径；用户文件名不得决定
   目录。目录 0700、文件 0600，不可执行；Windows 等价 ACL/handle contract 未验证
   前普通文件 fail closed，但既有的有界图片规范化入口保持可用。
7. display name 清除路径语义、分隔符、控制字符和 Discord mention，限制长度并尽量
   保留安全 Unicode/CJK；可保留经过验证的短扩展名帮助工具识别。
8. 新 migration 允许 `attachments.kind='input_file'`。数据库只保存 data-dir 相对
   路径、hash 和 metadata，绝不保存 CDN URL 或公开绝对本地路径。
9. provider start 前重新验证文件路径位于 data dir，所有父级无 symlink、目标为普通
   文件且 SHA-256/大小未变化；失败时 fail closed，provider 不启动。
10. Runtime 按附件 ordinal 组合 `TextInput`、`LocalImageInput` 和
    `MentionInput`；不把文件内容、URL 或路径字符串拼入 prompt，不复制到项目目录。
11. capability manifest 暴露稳定的 `mention.input`。能力不可用时在 provider start
    前返回 `file_input_unsupported`，不能静默丢弃文件。
12. 默认上限：一条消息最多 10 个附件、单个普通文件 25 MiB、总下载 50 MiB；配置、
    校验、doctor/diagnostics 和详细设计保持一致。图片仍受原 image limits 约束。
13. 普通文件复用 7 天 input attachment retention；queued/starting/running 引用期间
    不删除，terminal 到期后删除；orphan sweep 同时覆盖 input_image/input_file。
14. 保持 ingress 重复投递、queued input 恢复、SkillInput、prompt reaction、
    session 和既有图片 modality 行为。

## Acceptance Criteria

- `.txt`、`.md`、`.json`、PDF、Office、archive 和未知二进制不再误报
  `image_decode_failed`，而是作为 opaque file 传给 Codex。
- 文本+多文件、文本+图片+文件和 file-only 均保序且只产生一个 Turn。
- 有效图片不受 MIME/扩展名欺骗并继续走图片模型检查；损坏且声明为图片的内容拒绝。
- 路径穿越、绝对路径、控制字符、mention、metadata size 欺骗、超限 stream、
  off-policy redirect、空/部分下载均 fail closed 并清理。
- 文件缺失、被替换、hash 改变或任一父级被换成 symlink 时 provider 不启动。
- duplicate/recovery 不产生第二个 Turn 或孤儿文件；retention 不删除 active 引用。
- 不把绝对路径、URL、文件内容写入公开状态、普通日志或诊断包。

## Technical Approach

1. 先扩展 domain、config、migration、Repository snapshot/load/validation 与 retention，
   形成 `TurnFile` 的持久契约。
2. 将 `DiscordImageIngestor` 演进为统一 `DiscordAttachmentIngestor`：单次下载，
   MediaWorker 内容分类，图片规范化或 opaque 原子 commit，统一回滚。
3. 扩展 runtime adapter/capability contract，把普通文件映射为 SDK
   `MentionInput`，并在 start 前校验 snapshot。
4. 最后接入 Discord bot ingress、错误文案、混合 ordinal 与端到端恢复路径。
5. 不编辑既有 migration；新增下一个有序 migration。

## Testing Strategy

- Domain/storage：snapshot/hash、migration、enqueue/load、path/symlink/hash fail-closed、
  duplicate/recovery、retention/orphan。
- Ingestor：图片/普通文件/MIME 错配/损坏图片/redirect/timeout/partial failure/caps。
- Runtime contract：MentionInput export、constructor、wire serialization、capability 缺失。
- Discord：频道 mention、thread reply、file-only、混合顺序、错误码和原子 cleanup。
- 完整运行 ruff、mypy 与 pytest。

## Out of Scope

- 用户引用任意本机路径、URL 下载或复制附件到项目/git workspace。
- codexD 自动解析、OCR、解压、执行或杀毒。
- 保证每种专有格式都可被当前 Codex 工具链理解。
