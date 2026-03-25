# OpenClaw Integration

## 定位

当前仓库适合接入 OpenClaw 作为一个 `临床论文复现引擎 v1`，而不是“任意论文一键通用复现平台”。

能力判断：

- 已具备真实能力
  - PDF / Markdown / DOCX 论文材料读取与统一 intake
  - 论文材料读取与 `TaskContract` 结构化建模
  - MIMIC-IV TyG sepsis preset 的 deterministic 复现链路
  - cohort / analysis dataset / stats / verify / report 的 artifact-first 输出
  - session 持久化、多 agent 路由、以及 machine-readable skill contracts
- 仍未具备
  - 任意论文从 `CohortSpec + VariableSpec` 自动编译成可执行 SQL
  - 任意 MIMIC 论文的通用变量映射与泛化特征工程
  - 非 preset 合同的真正端到端自动执行
  - 任意论文的 ROC / sensitivity / advanced diagnostics 全自动执行

## 推荐架构

采用 `Hybrid bridge`：

- OpenClaw 负责对话、主代理、权限与多轮交互
- `paper-repro-agent` 负责 `TaskContract`、deterministic 执行、验证和落盘

对外只暴露一个主代理：

- `paper-repro-scientist`

内部能力按 skill 组织：

- `paper_intake_and_contract`
- `mimic_cohort_execution`
- `analysis_dataset_expansion`
- `survival_stats_execution`
- `result_figure_generation`
- `paper_alignment_verification`
- `git_update`

## 关键资产

- `openclaw/SOUL.MD`
- `openclaw/skills/*/SKILL.md`
- `configs/openclaw.agentic.yaml`
- `configs/openclaw.mimic-real-run.yaml`
- `configs/mimic_variable_semantics.yaml`
- `docs/lobster_agent_contract.md`

## 稳定接口

程序化桥接入口位于 `src/repro_agent/openclaw_bridge.py`：

- `plan_task`
- `run_task`
- `export_contract`
- `run_preset_pipeline`
- `extract_analysis_dataset`
- `describe_openclaw_integration`

技能合同清单位于：

- `openclaw/skills/skills_manifest.yaml`

命令行入口：

- `paper-repro describe-openclaw`
- `paper-repro plan-task`
- `paper-repro run-task`
- `paper-repro run-preset-pipeline`
- `paper-repro extract-analysis-dataset --profile <paper_profile>`

推荐给龙虾的调用方式是：

1. 先 `plan_task`
2. 检查 `missing_high_impact_fields` 和 `execution_supported`
3. 对可执行的 MIMIC preset 任务切到 `configs/openclaw.mimic-real-run.yaml`
4. 再执行 `run_task`

## 设计规则

- `TaskContract` 是 OpenClaw 与执行引擎之间唯一主合同对象。
- 命中 preset 时优先 deterministic bridge。
- 未命中 preset 时输出 planning blueprint，不伪装成完成执行。
- `extract_analysis_dataset` 必须走 profile 驱动入口，避免回退到旧的 TyG 专用脚本。
- intake 允许从 PDF 直接抽取正文，但必须避免同目录旧论文材料污染当前任务。
- 所有产物优先从 `shared/`、`results/`、`shared/sessions/<session_id>/` 读取。
