# OpenClaw MIMIC 自动化复现模板清单（2026-03-28）

## 0. 结论先行

本项目当前能力可以定义为:

- 支持 `MIMIC-only` 的自动化论文复现编排
- 对已支持路径可实现 `plan -> run -> artifacts` 自动闭环
- 对未补齐方法细节的论文会给出 `planning/blocked` 诊断，而不是伪造复现成功

换句话说，系统已经具备“自动化复现已有论文研究”的能力，但属于“条件化自动化”而非“任意论文一键全自动”。

## 1. 历史误差边际（当前可量化样本）

| 论文/会话 | 关键指标 | 论文目标 | 当前复现 | 绝对差值 | 相对差值 | 状态 |
|---|---|---:|---:|---:|---:|---|
| ARF nomogram (`session-e9edb53ab5b9`) | 样本量 N | 559 | 739 | +180 | +32.20% | 未对齐 |
| ARF nomogram (`session-e9edb53ab5b9`) | 验证集 AUC | 0.790 | 0.715739 | -0.074261 | -9.40% | 未对齐 |
| ARF nomogram (`session-e9edb53ab5b9`) | 验证集 C-index | 0.749 | 0.677468 | -0.071532 | -9.55% | 未对齐 |
| TyG stroke (`mimic_tyg_stroke_nondiabetic`) | 样本量 N | 1073 | 1071 | -2 | -0.186% | 接近对齐 |

证据来源:

- `docs/reports/s12890-025-04067-0-reproduction-alignment-2026-03-27.md`
- `shared/sessions/session-e9edb53ab5b9/binary_outcome/stats_summary.json`
- `shared/runs/mimic_tyg_stroke_nondiabetic/cohort_alignment.json`

## 2. 自动化复现最小配置面（OpenClaw）

## 2.1 SOUL.MD 需要固定的内容

建议将 `openclaw/SOUL.MD` 固定为以下语义边界:

- 身份: 医学论文复现引擎，不是通用聊天机器人
- 数据边界: 仅 `MIMIC-IV`
- 行为边界: LLM 只做控制面，SQL/统计/作图必须确定性执行
- 真实性边界: 不允许“未计算先报告”
- 结果边界: 部分复现必须显式写 `method gap` 或 `fidelity gap`

## 2.2 Skills 需要固定的阶段契约

`openclaw/skills/skills_manifest.yaml` 中建议维持 7 段契约化技能:

1. `paper_intake_and_contract`
2. `mimic_cohort_execution`
3. `analysis_dataset_expansion`
4. `longitudinal_trajectory_execution`
5. `survival_stats_execution`
6. `result_figure_generation`
7. `paper_alignment_verification`

每个技能必须写清楚:

- `inputs`
- `outputs`
- `guardrails`
- `fails_when`

这样 OpenClaw 才能做稳定路由，而不是依赖临场提示词。

## 2.3 模型选择（推荐）

现有配置已经是正确形态:

- `paper_parser_agent` / `study_design_agent` / `verify_agent` / `report_agent`: 使用推理型 LLM
- `cohort_agent` / `feature_agent` / `stats_agent` / `figure_agent`: 使用 `deterministic_only`

参考配置:

- `configs/openclaw.agentic.yaml`
- `configs/openclaw.mimic-real-run.yaml`

推荐策略:

- 温度固定 `0.0`
- 高风险阶段（paper intake / verify）优先稳定模型
- 不在统计计算阶段使用 LLM 直接产数值

## 2.4 必要环境变量

- `MIMIC_PG_HOST`
- `MIMIC_PG_PORT`
- `MIMIC_PG_DB`
- `MIMIC_PG_USER`
- `MIMIC_PG_PASSWORD`
- `MIMIC_PG_SSLMODE`
- `SILICONFLOW_API_KEY`（或你当前配置中的 LLM key 环境变量）

## 3. 按论文类型的运行模板

| 论文类型 | 推荐后端 | 推荐 run_mode | 推荐配置 | 质量门槛建议 | 期望输出 |
|---|---|---|---|---|---|
| 已有 preset（如 TyG sepsis） | `deterministic_bridge` | `preset_real_run` | `configs/openclaw.mimic-real-run.yaml` | `expected_cohort_size` 写论文 N，`cohort_tolerance_percent` 5-10 | cohort、dataset、stats、figure、verify、report |
| 多终点生存（如 TyG stroke） | `profile_survival_bridge` | `agentic_repro` | `configs/openclaw.agentic.relaxed-cohort.yaml` | `expected_cohort_size=1073`，`cohort_tolerance_percent=5` | baseline、Cox、KM、RCS、subgroup、alignment report |
| ARF nomogram / 二分类预测 | `hybrid_binary_runner` | `agentic_repro` | `configs/openclaw.agentic.relaxed-cohort.yaml` | 初期允许更宽容差，先收敛 N 再收敛 AUC/C-index | logistic、cox、ROC、calibration、DCA、nomogram |
| 轨迹生存（LGMM 类） | `trajectory_python_bridge` | 先 `plan_only` 再 `agentic_repro` | `configs/openclaw.agentic.yaml` | 必须先满足 repeated panel 字段完整 | trajectory table/figure + KM/Cox + fidelity gap |

## 4. OpenClaw 请求模板（可直接跑）

建议优先使用 `paper-repro openclaw-request --request-file ...`

新增模板文件:

- `configs/openclaw.request.stroke-tyg-agentic.example.json`
- `configs/openclaw.request.arf-nomogram-agentic.example.json`
- `configs/openclaw.request.trajectory-plan.example.json`

## 5. 自动化验收标准（建议固定）

- 执行状态:
  - `session_state.status = success` 才计为“执行成功”
- 工件完整性:
  - 必须存在 `task_contract.json`、`agent_decision.json`、`stats_summary.json`、`reproduction_report.md`
- 对齐诊断:
  - 必须产出 cohort 与核心指标偏差说明（不得只给最终图）
- 边界一致性:
  - 非 MIMIC 请求必须返回 unsupported 诊断

## 6. 下一步建议（按优先级）

1. 将 ARF 路线的队列口径先收敛到论文 N（再谈 AUC/C-index 收敛）
2. 将 TyG stroke 路线补齐 MICE/PSM 敏感性分析
3. 在 CI 中增加最小 OpenClaw 回归测试:
   - 一个 `plan_only`
   - 一个 `agentic_repro`
   - 一个 `preset_real_run`
