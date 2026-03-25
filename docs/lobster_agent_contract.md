# Lobster Agent Contract

## Goal

给龙虾一个单一主代理 `paper-repro-scientist`，使其能够接收一篇与当前论文相似的 MIMIC 临床论文，连接 MIMIC 数据库，并输出相类似的结论图表与表格。

## Single external agent

- agent name: `paper-repro-scientist`
- one entrypoint only
- primary exchange object: `TaskContract`

龙虾不应该直接操作内部 SQL、脚本路径或 `shared/` 文件结构。

## Request shape

推荐请求字段：

- `paper_path` 或 `paper_content`
- 支持直接传 `pdf/md/text`
- `instructions`
- `config_path`
- `session_id`
- `run_mode`
- `use_llm`

推荐 `run_mode`：

- `plan_only`
- `agentic_repro`
- `preset_real_run`

## Response shape

主代理至少返回：

- `session_id`
- `status`
- `execution_backend`
- `execution_supported`
- `missing_high_impact_fields`
- `task_contract`
- `artifacts`

## Supported first-release behavior

当前首期目标不是“任意论文完全自动复现”，而是：

- 相似的 `MIMIC-IV` 临床观察性论文
- 优先支持生存分析 / Cox / KM / RCS / subgroup
- 命中 preset 时输出真实图表与表格
- 未命中 preset 时输出规划结果和缺失项

## Success definition

对龙虾来说，一次成功调用不只是看到 `success` 状态，而是至少满足以下之一：

- 返回真实 `csv/png/md/json` artifact
- 或返回清晰的 planning 结果，说明为什么不能真实执行

对 preset 论文，理想成功产物应包含：

- cohort / funnel
- analysis dataset / missingness
- baseline table
- Cox / KM / RCS / subgroup artifacts
- verification summary
- reproduction report

## Recommended configs

- planning: `configs/openclaw.agentic.yaml`
- real MIMIC preset run: `configs/openclaw.mimic-real-run.yaml`

## Handoff rule

龙虾调用顺序应该固定：

1. `plan_task`
2. 检查 `missing_high_impact_fields`
3. 检查 `execution_supported`
4. 若可执行，则 `run_task`
5. 读取 artifact 并回显结果

## What Lobster must not do

- 不自己猜 cohort SQL
- 不自己发明第二套任务 schema
- 不把 planning-only 结果说成真实复现
- 不绕过 `TaskContract` 直接让多个子技能各自理解论文
- 不把同目录旧论文的 markdown 当作当前论文的事实来源
