# paper-repro-agent 项目与报告整理总览（2026-03-28）

## 1. 项目定位（当前共识）

本项目当前定位已收敛为:

- `MIMIC-only` 复现引擎
- 当前执行口径为 `MIMIC-IV`
- 采用 `LLM 控制面 + 本地确定性执行面` 架构
- 对不满足条件的任务返回 `planning/blocked`，不伪造“已复现”

对应口径文件:

- `README.md`
- `docs/architecture/architecture.md`
- `openclaw/AGENTS.md`
- `openclaw/SOUL.MD`

## 2. 目录级整理结果

### 2.1 文档层（docs）

当前核心文档可分为 4 组:

- 架构与能力边界:
  - `docs/architecture/architecture.md`
  - `docs/architecture/clinical-analysis-capability-map.md`
- 复现对齐报告:
  - `docs/reports/s12890-025-04067-0-reproduction-alignment-2026-03-27.md`
  - `docs/reports/s12890-025-04067-0-reproduction-report-vs-paper-2026-03-27.md`
- OpenClaw 自动化模板:
  - `docs/operations/openclaw-mimic-automation-templates-2026-03-28.md`
- Skills 与补充映射:
  - `docs/operations/skills-catalog.md`
  - `docs/operations/skills-smoke-tests.md`
  - `docs/operations/skills-validation-log-2026-03-27.md`
  - `docs/architecture/supplemental-codex-skill-map.md`

### 2.2 复现产物层（shared/results）

当前可确认的稳定 run 产物主目录:

- `shared/runs/mimic_tyg_stroke_nondiabetic/`
- `results/runs/mimic_tyg_stroke_nondiabetic/`

当前可确认的关键 session 目录:

- `shared/sessions/session-e9edb53ab5b9/`（成功）
- `shared/sessions/session-lobster-improve-20260327/`（blocked，历史命名样本）
- `shared/sessions/session-lobster-smoke-20260327/`（blocked，历史命名样本）
- `shared/sessions/session-openclaw-plan-demo/`（plan-only）

## 3. 复现状态与误差边际（当前可量化）

### 3.1 ARF nomogram 路线（session-e9edb53ab5b9）

已实现:

- 自动化 agentic 执行成功
- 输出 binary-outcome 产物链（logistic、cox、ROC、calibration、DCA、nomogram）

当前对齐边际:

- 样本量: 论文 `559` vs 复现 `739`（+180）
- 验证集 AUC: 论文 `0.790` vs 复现 `0.715739`（-0.074261）
- 验证集 C-index: 论文 `0.749` vs 复现 `0.677468`（-0.071532）

结论:

- 已具备“可自动执行并产出完整报告”的能力
- 尚未达到“论文数值完全对齐”

### 3.2 TyG stroke 路线（mimic_tyg_stroke_nondiabetic）

已实现:

- 多终点生存分析工件完整输出（baseline/Cox/KM/RCS/subgroup/report）

当前对齐边际:

- 样本量: 论文 `1073` vs 复现 `1071`（-2，接近对齐）

已知方法缺口:

- fasting 语义近似
- MICE/PSM 未补齐
- 部分干预变量为编码近似

## 4. OpenClaw 自动化配置现状

当前已经具备的配置面:

- 宪法与行为边界:
  - `openclaw/SOUL.MD`
  - `openclaw/AGENTS.md`
- 技能阶段契约:
  - `openclaw/skills/skills_manifest.yaml`
- 运行配置:
  - `configs/openclaw.agentic.yaml`
  - `configs/openclaw.mimic-real-run.yaml`
  - `configs/openclaw.agentic.relaxed-cohort.yaml`
- 请求模板:
  - `configs/openclaw.request.plan-only.example.json`
  - `configs/openclaw.request.agentic-repro.example.json`
  - `configs/openclaw.request.follow-up.example.json`
  - `configs/openclaw.request.stroke-tyg-agentic.example.json`
  - `configs/openclaw.request.arf-nomogram-agentic.example.json`
  - `configs/openclaw.request.trajectory-plan.example.json`

## 5. 当前阶段建议（以“整理后可执行”为目标）

优先级 1:

- 把 ARF 路线先收敛队列口径到论文 N（优先于调模型）

优先级 2:

- 给 TyG stroke 路线补齐 MICE/PSM 敏感性分析，缩小方法缺口

优先级 3:

- 保持 MIMIC-only 边界，不新增非 MIMIC 运行路径

优先级 4:

- 将 blocked 的历史 session 保留为诊断样本，不作为“成功复现”证据

## 6. 本次整理验证

测试结果:

- `python3 -m pytest -q /home/bingkun_zhao/projects/paper-repro-agent/tests`
- `44 passed`

结论:

- 本次文档与配置整理未破坏现有测试基线
