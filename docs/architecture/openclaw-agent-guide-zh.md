# OpenClaw Agent 中文详细说明（MIMIC-only）

## 1. Agent 定位

本项目的 OpenClaw Agent 是一个“医学论文复现引擎”，不是通用聊天助手。

它的核心目标是：

1. 从论文出发理解研究设计与方法。
2. 将论文语义收敛到可执行的 `TaskContract`。
3. 用本地确定性代码执行 SQL、统计与作图。
4. 产出可审计的复现工件、偏差诊断和报告。

当前运行范围明确锁定在 `MIMIC-IV`，即 `MIMIC-only`。

## 2. 框架逻辑（控制面 + 执行面）

系统采用“LLM 控制面 + 确定性执行面”的双层架构：

1. 论文理解与任务归一化（控制面）
2. 能力路由与执行决策（控制面）
3. 队列抽取、统计计算、图形生成（确定性执行面）
4. 对齐验证与复现报告（确定性执行面）

对应主链路：

`paper -> paper_evidence -> TaskContract -> agent_decision -> cohort/dataset/stats/figures -> verification/report`

关键原则：

- LLM 负责“理解、规划、补语义”，不直接给最终统计数值。
- 数值、图表、表格由本地代码执行产生，避免幻觉结果。

## 3. 目录结构与职责

核心目录按职责分层：

1. `src/repro_agent/paper`：论文解析、证据抽取、合同归一化
2. `src/repro_agent/agentic`：任务决策、会话编排、执行计划
3. `src/repro_agent/analysis`：统计分析与作图执行
4. `src/repro_agent/sql`：队列与分析数据集 SQL 构建
5. `src/repro_agent/registry`：语义映射与技能契约
6. `src/repro_agent/integrations`：OpenClaw 等外部集成
7. `src/repro_agent/legacy`：旧接口兼容层（仅兼容，不作为主路径）

文档目录建议按用途阅读：

1. `docs/architecture/`：架构与能力边界
2. `docs/operations/`：运行模板、skills 编排与验证流程
3. `docs/reports/`：复现对齐报告与阶段总结

## 4. 运行模式（建议优先级）

1. `paper-first agentic`（推荐）
2. `profile-first deterministic`（已有 profile 时可直接执行）
3. `legacy`（仅历史兼容，不建议新工作流使用）

## 5. 运行所需配置

最小必需配置：

1. MIMIC PostgreSQL 连接环境变量
2. 可访问的论文文件（`papers/*.pdf`）
3. 可用配置文件（`configs/*.yaml`）

常见数据库环境变量：

- `MIMIC_PG_HOST`
- `MIMIC_PG_PORT`
- `MIMIC_PG_DB`
- `MIMIC_PG_USER`
- `MIMIC_PG_PASSWORD`
- `MIMIC_PG_SSLMODE`

当需要 LLM 参与论文理解或验证时，再启用对应模型 API key。

## 6. 我们的硬边界（必须遵守）

1. 数据边界：只支持 `MIMIC-IV`，非 MIMIC 请求统一返回 `unsupported/planning`。
2. 真实性边界：禁止“未计算先报告”；禁止伪造数值与指标。
3. 执行边界：统计与图形必须走本地确定性执行器，不让 LLM 直接产结果。
4. 结果边界：若方法未补齐，必须标注 `method gap` 或 `fidelity gap`。
5. 治理边界：历史兼容链路不承接新主功能，避免架构回退。

## 7. 质量门槛与验收口径

一次“可接受”的自动化复现，至少满足：

1. 会话状态明确（`success` / `blocked` / `planning`）
2. 关键工件齐全（`task_contract`、`agent_decision`、`stats_summary`、`report`）
3. 结果可追踪（工件路径、参数、方法说明完整）
4. 对齐可解释（样本量、核心指标偏差有明确诊断）

如果当前缺证据或缺方法，应明确输出阻塞原因和下一步动作，不得“伪成功”。

## 8. 当前能力现状（截至 2026-03-28）

可稳定支持：

1. MIMIC 论文任务归一化与执行路由
2. 生存分析主干（baseline、KM、Cox、RCS、subgroup）
3. OpenClaw 单入口请求编排与会话工件输出

仍在收敛：

1. 部分论文的队列口径精确对齐
2. 部分敏感性分析补齐（如 MICE/PSM）
3. 部分复杂方法从“计划态”升级为“可执行态”

## 9. 结果与报告保留策略

为兼顾可复现与仓库治理，采用“双层保留”：

1. 结构化总结与对齐报告放在 `docs/reports/`，作为 Git 历史证据。
2. 运行期工件保留在 `shared/` 与 `results/` 本地目录，支持复盘与审计。

说明：

- 仓库默认避免把全量运行产物作为长期版本化负担。
- 对外展示与协作优先使用 `docs/reports/` 的凝练报告。

## 10. 推荐团队工作流

1. 先 `plan-task` 生成 `TaskContract` 与缺失项。
2. 再 `run-task` 或 profile 脚本执行确定性统计。
3. 查看 `shared/sessions/<session_id>/` 与 `docs/reports/` 做对齐诊断。
4. 若未对齐，优先修“队列口径与变量语义”，其次再调统计模型细节。

## 11. 参考入口

- `README.md`
- `docs/architecture/architecture.md`
- `docs/architecture/clinical-analysis-capability-map.md`
- `openclaw/SOUL.MD`
- `openclaw/AGENTS.md`
- `openclaw/skills/skills_manifest.yaml`
- `docs/report-index.md`
