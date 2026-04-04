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

## 4. 页面说明

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

可在左侧 `Project Root` 输入框切换到其他仓库副本。

## 6. 常见问题

1. 报错 `Streamlit is not installed`  
请执行 `pip install -e ".[dashboard]"`。

2. 页面无 session  
检查 `shared/sessions/` 是否存在，或 `Project Root` 是否指向正确仓库。

3. Token 显示为空  
确认是否生成 `results/sessions/<session_id>/llm_token_usage_summary.json`。
