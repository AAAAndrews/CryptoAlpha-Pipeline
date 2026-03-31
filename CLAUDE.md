# Coding Agent 标准作业程序 (SOP)

你是一个运行在长周期项目迭代中的 Coding Agent，依托一套标准脚手架配置，对用户的代码进行构建。
所有关于 Claude Code 脚手架的内容配置都存储在当前工作区下的 `scarffold` 目录下。
**当前环境是windows 11，请确保你的代码和指令在该环境下能够正确运行**
你不会保留上一次会话记忆，每次唤醒必须严格执行以下流程：

你的代码规范如下：

## Code Style / 代码规范
- Pythonic First / Pythonic 优先：优先使用清晰、直接、可读的 Python 写法，遵循 PEP 8，避免过度设计。
- Naming / 命名：变量、函数、模块使用 `snake_case`，类使用 `PascalCase`，命名应短、准、可理解。
- Simple Words / 简单单词：注释与文档优先使用简单英文词汇，避免生僻词、长句和不必要术语。
- Bilingual Text Comments / 中英双语文本型注释：对关键逻辑使用中英双语块注释，先中文后英文，内容一致，涉及函数/类需要清晰说明参数的实际含义。
- Bilingual Single-line Comments / 中英双语单行注释：单行注释采用“中文 + English”同义表达，保持简短。
- Comment Rule / 注释原则：只注释“为什么”和“意图”，不重复“代码表面含义”。
- Consistency / 一致性：同一文件内注释语气、术语、语言顺序（中文在前，英文在后）保持一致。

## Step 1: Context Recovery / 恢复上下文
- 检查当前分支是否在dev上。如果项目没有dev分支，请你创建并切换到该分支上，并将内容进行initial commit。项目的主分支为main，禁止在main分支上进行任何开发活动。
- 读取 `scarffold/.agent/progress.txt` 以了解最新进度。
- 运行 `git log -n 3 --oneline` 以检查最近的变更。
- 阅读 `scarffold/.agent/architecture.md` 以对齐实现方案。
- 阅读 `scarffold/.agent/tasks.json` 并识别当前的任务图。

## Step 2: Task Selection / 任务认领
- 选择第一个 `passes: false` 且所有依赖 `deps` 已完成的任务。
- 每次唤醒仅处理有且仅有一个任务。
- 严禁乱序执行任务。

## Step 3: Development & Testing / 开发与测试
- 仅针对所选任务编写实现代码。
- 运行相关的测试或验证命令。
- 如果发生失败，在继续后续操作前进行本地修复，注意校验任务状态是否恢复。
- 在没有验证证据的情况下，不得将任务标记为已完成。

## Step 4: State Commit / 状态封存
- 更新 `scarffold/.agent/tasks.json`：将所选任务的 `passes` 设置为 `true`。
- 在 `scarffold/.agent/progress.txt` 顶部追加日志条目，并留下一些你认为有用的给开发者的备注，介绍功能用法。
- 使用清晰的提交信息提交所有必要的更改，例如：
  `feat: complete task [ID]` 或 `fix: complete task [ID]`
- 提交完成后，立即停止并退出。

## Hard Constraints / 硬约束
- 严禁跳过已完成任务的测试。
- 严禁在单次运行中处理多个任务。
- 除非有明确要求，否则严禁修改任务架构契约（`id`/`task`/`passes`/`deps`）。
