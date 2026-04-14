# MIMIC Paper Repro Agent 代码与框架整理说明（2026-04-14）

## 1. 目标与范围

本文档用于统一当前 `paper-repro-agent` 的代码组织、主链执行框架和维护约束，减少“功能可用但结构不可维护”的风险。

本次整理聚焦：

1. 明确主链路径（LLM 驱动 + 本地确定性执行）。
2. 明确包级模块职责与边界。
3. 明确兼容层与主路径的关系。
4. 给出可执行的 GitHub 更新流程与后续清理清单。

---

## 2. 当前主链架构（推荐认知）

### 2.1 控制面

- `src/repro_agent/agentic/runner.py`  
  负责 session 生命周期、阶段门禁、对齐迭代、统一 verdict 输出。
- `src/repro_agent/agentic/decision.py`  
  负责执行路径决策（generic / profile fallback / planning only）。
- `src/repro_agent/agentic/survival_compiler.py`  
  负责 survival 主线执行计划编译和变量映射诊断。

### 2.2 论文与合同面

- `src/repro_agent/paper/builder.py`  
  负责论文证据抽取、TaskContract 构建、follow-up 合并、证据阻塞策略。
- `src/repro_agent/paper/profiles.py` / `presets.py` / `templates.py`  
  负责可执行 profile 与模板化策略。

### 2.3 执行面

- `src/repro_agent/sql/*`  
  cohort 和 dataset 的 SQL/映射逻辑。
- `src/repro_agent/analysis/*`  
  统计执行与结果工件生成（Cox/KM/PSM/等）。

### 2.4 集成与接口面

- `src/repro_agent/cli.py`  
  本地统一入口（plan-task / run-task / continue-session / openclaw-request）。
- `src/repro_agent/integrations/openclaw.py`  
  OpenClaw 请求协议转换与会话衔接。

### 2.5 核心基础层

- `src/repro_agent/core/config.py`  
  配置解析、阈值策略、LLM 路由配置。
- `src/repro_agent/core/contracts.py`  
  数据契约与 verdict 结构定义。
- `src/repro_agent/core/runtime.py`  
  session 和 artifacts 的本地持久化。
- `src/repro_agent/core/llm.py`  
  OpenAI-compatible LLM 调用与使用量采集。

---

## 3. 目录职责分层（维护约束）

### 3.1 主路径

- `core/`, `agentic/`, `paper/`, `analysis/`, `sql/`, `integrations/`, `registry/`

这些目录是新功能的默认落点，新增能力优先进入此路径。

### 3.2 兼容路径

- `legacy/`
- 根层 façade：`config.py`, `contracts.py`, `llm.py`, `runtime.py`, `openclaw_bridge.py`

这些用于兼容旧入口，不应承载新业务逻辑。  
规则：新逻辑进 `core/` 或 `integrations/`，兼容层仅做转发与过渡。

---

## 4. 执行状态语义（必须统一）

所有运行结果必须归一到：

- `reproducibility_verdict.status`
- `reproducibility_verdict.sub_status`

禁止出现：

- planning_only/spec_only 被描述为“成功复现”
- 未达阈值但在报告中被弱化为“基本完成”

建议所有报告固定包含：

1. 适用阈值
2. 实际误差
3. 是否通过
4. 若失败，卡住阶段与原因分类（evidence/backend/alignment）

---

## 5. 运行环境依赖矩阵（对外口径）

### 5.1 真实复现（real run）

必需：

- MIMIC PostgreSQL 连接（`MIMIC_PG_*`）
- LLM 连接（主链是 LLM 驱动）
- 本地 Python 统计依赖

### 5.2 规划或演示（plan / dry-run）

可在不完整数据库环境下运行部分流程，但不能宣称真实复现完成。

---

## 6. GitHub 更新规范（本仓库建议）

1. 不在同一次提交混入“功能改动 + 报告产物 + 临时脚本”。
2. 提交粒度按主题拆分：
   - `feat(agentic): ...`
   - `refactor(core): ...`
   - `docs(architecture): ...`
3. 提交前至少执行：
   - 关键单测子集
   - `py_compile`（核心改动文件）
4. `shared/`、`results/`、临时生成报告默认不入主提交，除非明确发布需要。

---

## 7. 下一步整理清单（建议）

1. 将 `legacy` 依赖点做调用清单并标记退役优先级。  
2. 把 profile fallback 的触发条件写成统一诊断工件。  
3. 把 `skill_guardrail_policy.json` 固定纳入 session 必备工件。  
4. 在 `docs/architecture` 维护一份“模块责任变更日志”。  

---

## 8. 结论

当前框架已经具备“主链可运行 + 阶段可审计 + 失败可分类”的工程基础。  
本次整理的核心目标是让后续扩展继续沿着同一主路径演进，避免回到“功能堆叠但边界模糊”的状态。

