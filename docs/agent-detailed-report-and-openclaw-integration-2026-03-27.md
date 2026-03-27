# paper-repro-scientist 详细报告与OpenClaw接入说明（2026-03-27）

## 1. 报告范围

本报告覆盖两部分内容：

1. 当前 `paper-repro-agent` 的 Agent 架构、能力边界与运行现状
2. 后续接入OpenClaw（OpenClaw 外部编排）时的推荐调用方式与落地步骤

核心定位来自：

- `openclaw/SOUL.MD`
- `openclaw/AGENTS.md`
- `openclaw/skills/skills_manifest.yaml`
- `src/repro_agent/integrations/openclaw.py`

## 2. Agent 总体结论

当前 Agent（`paper-repro-scientist`）已经具备可执行的医学论文复现主链路，整体属于：

- 架构形态：`hybrid_llm_plus_deterministic`
- 当前成熟度：`partial_real_execution`
- 已具备的核心接口：`plan_task`、`continue_session`、`run_task`、`export_contract`、`run_preset_pipeline`、`extract_analysis_dataset`
- 运行模式：`plan_only`、`agentic_repro`、`preset_real_run`

简化判断：

1. 能做：论文解析、任务契约结构化、支持路径的真实执行、工件落盘、对齐报告
2. 不能夸大：任意论文的完全自动 SQL 编译与全方法族无差别执行
3. 必须坚持：`TaskContract` 单一主 schema，数值结果必须来自本地确定性执行

## 3. 架构分层（代码对齐）

### 3.1 论文理解与契约归一层

- 入口模块：`src/repro_agent/paper/*`
- 核心动作：
  - 读论文（PDF/MD/TXT）
  - 抽取 `paper_evidence`
  - 归一为 `TaskContract`
  - 识别 preset / profile / study template

### 3.2 Agent 控制层

- 入口模块：`src/repro_agent/agentic/*`
- 核心动作：
  - 生成 `agent_decision`
  - 输出 `analysis_family_route`
  - 决定 `ready / blocked / planning_ready`
  - 组织多阶段执行顺序（paper parser -> study design -> cohort -> feature -> stats -> figure -> verify -> report）

### 3.3 确定性执行层

- 入口模块：`src/repro_agent/sql/*`、`src/repro_agent/analysis/*`
- 核心动作：
  - SQL 队列提取
  - 分析数据集构建
  - 统计建模与图形输出
  - 工件落盘

### 3.4 集成与对外桥接层

- 入口模块：`src/repro_agent/integrations/openclaw.py`
- 对外职责：
  - 暴露标准化接口给上层编排器（OpenClaw）
  - 固定工件路径约定
  - 提供可读的系统集成描述（`describe_openclaw_integration`）

## 4. 官方技能面（项目内）

当前项目内 OpenClaw 主技能共 7 个：

1. `paper_intake_and_contract`
2. `mimic_cohort_execution`
3. `analysis_dataset_expansion`
4. `longitudinal_trajectory_execution`
5. `survival_stats_execution`
6. `result_figure_generation`
7. `paper_alignment_verification`

说明：

- `.codex/skills` 是补充能力池（reference layer），不应直接当作“原生运行能力”对外承诺。
- 只有被吸收到项目确定性代码或官方 OpenClaw 技能合同中的能力，才算生产可执行能力。

## 5. 决策状态语义（对接必须理解）

`agent_decision` 里的关键状态：

- `status=ready`：可执行（通常可进入 `run_task`）
- `status=blocked`：高影响字段缺失或能力不支持，需补问题或改路径
- `status=planning_ready`：可规划但不可真实执行

`mode` 语义：

- `deterministic_preset_run`：命中 preset，走最稳定真实执行
- `agentic_execution`：可执行但非 preset 固定桥
- `needs_contract_completion`：先补字段
- `planning_only`：只给计划与 scaffold
- `unsupported`：直接返回不支持诊断

## 6. OpenClaw接入：推荐调用协议

### 6.1 推荐时序（官方建议）

1. OpenClaw发送 `paper_path` 或 `paper_content` + `instructions`
2. 先调 `plan_task`
3. 若返回 `follow_up_questions`，调 `continue_session` 补结构化答案
4. 若 `execution_supported=true` 且字段齐全，调 `run_task`
5. 从 `artifacts` 与 `shared/sessions/<session_id>/` 读取最终结果

如果希望OpenClaw侧只维护一个调用动作，也可以直接调用项目新增的单入口：

- `handle_openclaw_request(project_root, request)`
- 它会按 `run_mode` 自动路由 `plan_task -> continue_session -> run_task`

### 6.2 关键输入字段

`plan_task`：

- `paper_path` 或 `paper_content`（二选一）
- `instructions`
- `config_path`
- 可选：`session_id`、`use_llm`

`continue_session`：

- `session_id`
- `answers`（字典）
- 可选：`instructions`、`run_if_ready`、`dry_run`

`run_task`：

- `session_id`
- `config_path`
- 可选：`dry_run`

### 6.3 关键输出字段

OpenClaw至少应消费这些字段：

- `session_id`
- `status`
- `execution_supported`
- `execution_backend`
- `missing_high_impact_fields`
- `follow_up_questions`
- `recommended_run_profile`
- `agent_decision`
- `analysis_family_route`
- `task_contract_path`
- `artifacts`（run 后）

## 7. OpenClaw落地方式（两种）

### 7.1 方式 A：CLI 子进程调用（最简单）

前提：命令行可调用（建议先 `pip install -e .`）。

示例：

```bash
paper-repro plan-task \
  --project-root /home/bingkun_zhao/projects/paper-repro-agent \
  --config configs/openclaw.agentic.yaml \
  --paper-path papers/s12890-025-04067-0.pdf \
  --instructions "请按论文方法复现并返回关键对齐指标。"
```

如果当前环境还没安装入口脚本，可先用模块方式：

```bash
PYTHONPATH=src python3 -m repro_agent.cli plan-task \
  --project-root /home/bingkun_zhao/projects/paper-repro-agent \
  --config configs/openclaw.agentic.yaml \
  --paper-path papers/s12890-025-04067-0.pdf \
  --instructions "请按论文方法复现并返回关键对齐指标。"
```

单入口 CLI（推荐给OpenClaw侧集成）：

```bash
PYTHONPATH=src python3 -m repro_agent.cli openclaw-request \
  --project-root /home/bingkun_zhao/projects/paper-repro-agent \
  --request-file path/to/openclaw_request.json
```

快速拿模板（避免手写 JSON）：

```bash
PYTHONPATH=src python3 -m repro_agent.cli openclaw-request --template agentic_repro
```

仓库内置了 3 份可直接改的请求示例：

- `configs/openclaw.request.plan-only.example.json`
- `configs/openclaw.request.agentic-repro.example.json`
- `configs/openclaw.request.follow-up.example.json`

示例 `openclaw_request.json`：

```json
{
  "paper_path": "papers/s12890-025-04067-0.pdf",
  "instructions": "请提取论文证据并在可执行时自动运行复现。",
  "session_id": "session-openclaw-demo",
  "run_mode": "agentic_repro",
  "config_path": "configs/openclaw.agentic.yaml",
  "use_llm": true,
  "dry_run": false
}
```

### 7.2 方式 B：Python 直接调用（推荐给编排器）

```python
from pathlib import Path
from repro_agent.openclaw_bridge import plan_task, continue_session, run_task

project_root = Path("/home/bingkun_zhao/projects/paper-repro-agent")
config_path = project_root / "configs" / "openclaw.agentic.yaml"

# 1) 先规划
plan = plan_task(
    project_root=project_root,
    config_path=config_path,
    paper_path="papers/s12890-025-04067-0.pdf",
    instructions="请提取论文证据并构建可执行 TaskContract。",
    use_llm=True,
)

sid = plan["session_id"]

# 2) 若有缺口，补答案
if plan.get("follow_up_questions"):
    payload = continue_session(
        project_root=project_root,
        config_path=config_path,
        session_id=sid,
        answers={
            "outcome_variables": "28-day mortality",
            "models": "Cox regression, ROC, calibration, DCA, nomogram"
        },
        run_if_ready=False,
    )
else:
    payload = plan

# 3) 执行
if payload.get("execution_supported") and not payload.get("missing_high_impact_fields"):
    run = run_task(
        project_root=project_root,
        config_path=config_path,
        session_id=sid,
        dry_run=False,
    )
    print(run["status"])
    print(run["artifacts"])
```

## 8. OpenClaw接入时的运行策略

### 8.1 run profile 选择

- `plan_only`：只结构化契约，不做真实执行
- `agentic_repro`：默认推荐，适用于大多数非固定 preset 论文
- `preset_real_run`：命中预设论文时优先，真实执行稳定性最高

### 8.2 错误与阻塞处理建议

1. `status=blocked` 且有 `follow_up_questions`：
   - OpenClaw应转化为结构化追问，不要让模型自由猜字段
2. 请求里若出现未知字段：
   - 返回体会带 `request_warnings`，便于OpenClaw侧日志告警与字段清洗
3. 布尔字段（`use_llm`、`dry_run`）建议只传 `true/false`：
   - 非法值会直接报错，避免静默回退导致行为不一致
4. `execution_supported=false`：
   - 返回规划产物，不要伪造“已复现”
5. 数据库或依赖缺失：
   - 透传依赖诊断，标记受影响阶段
6. 部分成功：
   - 保留已生成工件，明确剩余阻塞点

## 9. 工件消费约定（OpenClaw读哪些文件）

优先读取：

1. `shared/sessions/<session_id>/task_contract.json`
2. `shared/sessions/<session_id>/session_state.json`
3. `shared/sessions/<session_id>/*_spec*.json`
4. `shared/sessions/<session_id>/**/stats_summary.json`
5. `results/sessions/<session_id>/**/*.png`
6. `shared/sessions/<session_id>/**/reproduction_report.md`

## 10. 上线前检查清单

1. 环境变量：`MIMIC_PG_*`、`SILICONFLOW_API_KEY`（或你选定 LLM key）
2. 连接检查：`validate-env`、`probe-db`、`probe-llm`
3. 路由检查：`describe-openclaw` 返回 `interfaces` 与 `run_profiles`
4. 小样本冒烟：先跑 `plan_task -> continue_session -> run_task`
5. 观测留痕：确保 `shared/sessions/<session_id>/` 与 `results/sessions/<session_id>/` 工件完整

## 11. 当前建议

如果你下一步要正式接入OpenClaw，建议采用：

1. 第一阶段：先用 `Python 直接调用`（方式 B）跑通端到端
2. 第二阶段：再封装成你OpenClaw侧的统一任务节点（含重试与状态机）
3. 第三阶段：最后加上质量门控（cohort、AUC/C-index、工件完整性）作为自动判定条件

这样可以在不改动当前核心框架的前提下，最快实现稳定联调。
