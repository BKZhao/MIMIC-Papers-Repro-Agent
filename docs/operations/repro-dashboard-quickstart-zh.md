# Repro Dashboard 快速使用（中文）

## 1. 目标

`Repro Dashboard` 用于可视化查看论文复现 session 的执行与门禁状态，重点覆盖：

- `reproducibility_verdict`
- 阶段门禁（cohort/feature/stats/figure/verify）
- 对齐迭代日志（`alignment_iteration_log`）
- Token 用量汇总
- 交付产物完整性（artifact existence）

它直接读取仓库现有工件，不引入新数据库。

## 2. 安装

在仓库根目录执行：

```bash
pip install -e ".[dashboard]"
```

## 3. 启动

推荐命令：

```bash
paper-repro-dashboard
```

如果你希望指定端口，可直接透传 Streamlit 参数：

```bash
paper-repro-dashboard --server.port 8510
```

## 4. 页面说明（异步任务模式）

### 4.1 Run New Paper · Job Center

页面升级为“任务中心”，分三块：

1. `Create Job`
- 上传论文 PDF 或填写仓库内 `paper_path`
- 配置 `instructions / run_mode / config_path`
- 点击“创建任务并开始”后立即返回 `job_id`（不阻塞页面）

2. `Job List`
- 展示最近任务：`job_id / status / progress_stage / session_id / run_mode / execution_status / elapsed`
- 默认仅显示“当前浏览器用户”创建的任务，避免混入他人任务
- 支持状态筛选、手动刷新
- 运行中可自动刷新（2-3 秒）

3. `Job Detail`
- 显示任务状态、会话 ID、执行状态、耗时、裁决对象
- 展示 artifacts 表（含 required 与存在性）
- 预览 `workflow_stage_report.md`
- 常见工件提供下载按钮（`md/csv/json/zip/png/pdf/tex`）

### 4.1.1 Create Job 字段速查

| 字段 | 含义 | 何时使用 |
|---|---|---|
| 上传论文 PDF | 直接上传论文文件，系统自动保存并转成 `paper_path` | 论文文件在本机但不在仓库时 |
| paper_path | 已在仓库内的论文路径 | 已提前放到 `papers/` 目录时 |
| instructions | 对本次运行的额外要求 | 例如要求先输出纳排、再做统计 |
| run_mode | 执行深度选择 | 常规用 `agentic_repro`，仅规划用 `plan_only` |
| config_path | 运行配置文件路径 | 默认不改；仅在切配置策略时修改 |
| session_id | 会话 ID | 新任务留空；续跑已有任务时填写 |
| use_llm | 是否启用 LLM 读论文/抽证据 | 默认开启，除非做无 LLM 对照 |
| dry_run | 仅校验流程，不做完整执行 | 首次调试参数时建议开启 |

### 4.2 Follow-up 页面内补答

当任务返回 `task_not_ready + follow_up_questions` 时，任务会进入：

- `waiting_user_input`

此时 `Job Detail` 会自动渲染动态表单。用户补答后点击提交，系统会：

- 复用原 `job_id + session_id`
- 将答案写入任务历史
- 自动重新入队并继续执行

### 4.3 Session Explorer（历史审计视图）

保留原有 Session Explorer，继续用于审计和回看：
- 默认仅显示当前浏览器用户相关 session
- 如需查看全量 session，可勾选 `显示全部 session（管理员视角）`

1. `Session 总览`  
显示每个 session 的状态、裁决、执行路线、迭代次数、误差与 token。

2. `Stage Gates`  
显示每个阶段的阈值、实际误差、是否通过、原因说明。

3. `Steps`  
显示每轮 step 的执行状态与消息，便于定位卡点。

4. `Artifacts`  
显示产物清单并校验 required artifact 是否缺失。

5. `Verdict & Tokens`  
展示统一裁决对象和 token summary 原始结构。

6. `Workflow Report`  
直接渲染 `workflow_stage_report.md`。

## 5. 默认数据根目录

Dashboard 默认读取当前仓库根目录，核心路径：

- `shared/sessions/*`
- `results/sessions/*`
- `shared/web_jobs/*.json`（异步任务状态持久化）

可在左侧 `Project Root` 输入框切换到其他仓库副本。

## 6. 任务生命周期（简版）

```text
queued
  -> running
  -> waiting_user_input (需要 follow-up 补答)
      -> queued -> running
  -> completed
  -> failed
  -> cancelled (预留)
```

## 7. 常见问题

1. 报错 `Streamlit is not installed`  
请执行 `pip install -e ".[dashboard]"`。

2. 页面无 session  
检查 `shared/sessions/` 是否存在，或 `Project Root` 是否指向正确仓库。

3. Token 显示为空  
确认是否生成 `results/sessions/<session_id>/llm_token_usage_summary.json`。

4. 任务长时间停在 `queued`  
确认页面右上方 worker 状态是否为 `running/idle`；若长期 idle，点击“手动刷新”触发下一次调度。

5. 某个 job JSON 损坏或手工改坏  
删除 `shared/web_jobs/<job_id>.json` 后重新创建任务。

6. 工件显示缺失  
检查 `job detail` 里的 `rel_path` 是否存在；若是 required artifact 缺失，通常意味着该任务执行中断或处于早期阶段。
