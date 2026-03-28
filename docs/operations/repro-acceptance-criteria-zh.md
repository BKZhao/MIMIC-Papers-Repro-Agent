# OpenClaw 复现验收标准（中文，MIMIC-only）

## 1. 目的与适用范围

本标准用于统一评估本仓库中的论文复现结果，避免出现“有输出但不可验收”或“未对齐却误判成功”的情况。

适用范围：

1. 仅适用于本仓库当前支持的 `MIMIC-IV` 复现任务。
2. 适用于 `plan-task` / `run-task` / `openclaw-request` 三类入口。
3. 同时适用于 profile 路径和 agentic 路径。

## 2. 关键名词

1. `execution success`：任务已执行且满足最小工件与边界规则，不代表已完全对齐论文数值。
2. `alignment success`：关键指标达到约定阈值，可视为“高可信对齐”。
3. `blocked-not-failed`：因外部依赖、凭证、数据条件缺失导致阻塞；记录为阻塞而非算法失败。
4. `legacy`：历史兼容层，只用于旧命令/旧接口兼容，不承接新主功能开发。

## 3. 必须满足的硬门槛（Hard Gates）

任一条不满足，任务状态应为 `blocked` 或 `failed`，不得标记为通过：

1. 数据边界：非 `MIMIC` 请求必须返回 unsupported/planning 诊断。
2. 真实性边界：禁止 LLM 直接捏造统计值、效应量、图表结果。
3. 工件边界：至少产出以下四类核心工件：
   - `task_contract.json`
   - `agent_decision.json`
   - `stats_summary.json`（或等价 stats 汇总）
   - `reproduction_report.md`
4. 可追溯性：报告中必须出现“数据口径、建模口径、偏差说明”。

## 4. 对齐评估分级（建议阈值）

按任务类型分层，不要求所有论文共享一个阈值。

### 4.1 样本量（N）偏差阈值

1. `PASS`：相对偏差 `<= 5%`
2. `WARN`：`(5%, 10%]`
3. `FAIL`：`> 10%`

### 4.2 判别类指标（AUC / C-index）偏差阈值

1. `PASS`：绝对差值 `<= 0.03`
2. `WARN`：`(0.03, 0.06]`
3. `FAIL`：`> 0.06`

### 4.3 生存模型核心效应（HR/OR）偏差阈值

1. `PASS`：相对偏差 `<= 10%`
2. `WARN`：`(10%, 20%]`
3. `FAIL`：`> 20%`

说明：

- 对于明确声明“方法不完全可执行”的任务，可降级为 planning 结论，但必须显式列出 `method gap`。
- 若论文未给出某指标原始值，标注 `NA（paper-missing）`，不计入失败。

## 5. blocked-not-failed 判定规则

以下情形建议记为 `blocked-not-failed`：

1. API key/外部服务缺失（如检索服务、专有接口）。
2. 数据库权限不足或数据版本不完整。
3. 论文关键字段缺失，且无法从正文/附录可靠恢复。
4. 运行环境不满足必要依赖（且已在报告中记录）。

报告中必须包含：

1. 阻塞原因
2. 当前影响范围
3. 下一步解锁动作
4. 预计优先级

## 6. 执行成功与对齐成功的区分

### 6.1 执行成功（Execution Success）

同时满足以下条件即可：

1. `session_state.status = success`
2. 核心工件齐全
3. 边界规则合规（MIMIC-only、非伪造）
4. 结果报告中包含偏差诊断

### 6.2 对齐成功（Alignment Success）

在“执行成功”基础上，再满足：

1. 样本量 `N` 达到 `PASS/WARN`（建议至少 `<=10%`）
2. 核心判别指标（AUC / C-index）达到 `PASS/WARN`
3. 关键主模型效应量达到 `PASS/WARN`
4. 无高优先级 `method gap` 未解释

## 7. 报告模板最小字段

每次复现报告建议包含固定小节：

1. 任务概览（论文、数据版本、运行时间、入口命令）
2. 会话状态与工件清单
3. 队列与样本对齐（N、纳排差异）
4. 核心指标对齐（AUC/C-index/HR/OR）
5. 方法缺口与风险（method gap/fidelity gap）
6. 结论标签（execution success / alignment success / blocked-not-failed）

## 8. GitHub 提交规范（建议）

### 8.1 提交内容最小集合

1. 结构变更：`src/`、`configs/`、`openclaw/`、`docs/`。
2. 报告变更：`docs/reports/`（建议保留摘要和对齐文档）。
3. 不提交运行缓存与中间临时文件（遵循 `.gitignore`）。

### 8.2 提交前检查

1. `python3 -m pytest -q tests`
2. `git status` 为空后再 push
3. README 与 report-index 链接可达
4. 若涉及边界调整，需同步 `openclaw/SOUL.MD` 与 `openclaw/AGENTS.md`

### 8.3 推荐提交信息格式

1. `refactor: ...`（结构与兼容层调整）
2. `docs: ...`（报告与规范更新）
3. `feat: ...`（新增执行能力）
4. `fix: ...`（复现偏差修复）

## 9. 当前默认边界（2026-03-28）

1. 运行范围：`MIMIC-only`，当前主数据版本为 `MIMIC-IV`
2. 架构原则：`LLM 控制面 + 确定性执行面`
3. 兼容策略：`legacy` 只兼容旧接口，不作为新开发主路径
4. 结果声明：未对齐必须明确写出偏差与缺口，不得伪成功
