<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# 我们可以创建多个agent实现

完全可以，而且这是 Codex 最强的特性之一。Codex 原生支持主 Agent 派生子 Agent 并行运行，每个 Agent 在独立的 worktree 中工作，互不干扰 。以下是专为论文复现设计的完整多 Agent 架构：[^1]

***

## 整体架构：1 + 4 模式

```
                    ┌─────────────────────────┐
                    │   ORCHESTRATOR AGENT     │
                    │   (主控 / 协调员)         │
                    │   读论文 → 拆任务 → 汇总  │
                    └────────────┬────────────┘
                                 │ 派生 4 个子 Agent（并行）
           ┌─────────────────────┼──────────────────────┐
           ▼                     ▼                      ▼                    ▼
  ┌──────────────┐    ┌──────────────────┐   ┌──────────────────┐  ┌──────────────────┐
  │  SQL AGENT   │    │   STATS AGENT    │   │  VERIFY AGENT    │  │  REPORT AGENT    │
  │  数据提取员   │    │   统计分析员      │   │  数值校验员       │  │  报告撰写员       │
  │  MIMIC-IV SQL│    │   R统计脚本       │   │  对比论文数值     │  │  输出复现报告     │
  └──────────────┘    └──────────────────┘   └──────────────────┘  └──────────────────┘
```

每个 Agent 专注单一职责，主控 Agent 不做具体工作只负责编排 ——**主 Agent 不应该审查自己的代码**，这是 Codex 多 Agent 设计的核心原则 。[^2]

***

## 项目结构

```
mimic-repro/
├── AGENTS.md                          # 主控 Agent 规则
│
├── agents/
│   ├── sql_agent/
│   │   └── AGENTS.md                  # SQL Agent 专属规则
│   ├── stats_agent/
│   │   └── AGENTS.md                  # Stats Agent 专属规则
│   ├── verify_agent/
│   │   └── AGENTS.md                  # Verify Agent 专属规则
│   └── report_agent/
│       └── AGENTS.md                  # Report Agent 专属规则
│
├── shared/                            # 各 Agent 共享的中间文件
│   ├── methods.json                   # Orchestrator 解析论文后写入
│   ├── cohort.csv                     # SQL Agent 输出
│   ├── results_table.csv             # Stats Agent 输出
│   └── deviation_table.json          # Verify Agent 输出
│
├── .codex/
│   └── mcp.json                       # M3 数据库连接
│
├── papers/
│   └── tyg_sepsis.pdf
│
└── results/
    └── reproduction_report.md         # Report Agent 最终输出
```


***

## 五个 AGENTS.md 文件

### 根目录 `AGENTS.md`（主控 Orchestrator）

```markdown
# ORCHESTRATOR AGENT — 论文复现主控

## 角色
你是论文复现的总指挥，负责解析论文、拆解任务、
派生子 Agent 并行执行、汇总结果。
你不直接写 SQL，不直接跑 R，只负责编排和质检。

## 你管理的子 Agent
| Agent | 职责 | 触发条件 |
|-------|------|---------|
| sql_agent | MIMIC-IV 数据提取 | methods.json 就绪后 |
| stats_agent | R 统计分析 | cohort.csv 就绪后 |
| verify_agent | 数值对比校验 | results_table.csv 就绪后 |
| report_agent | 生成复现报告 | deviation_table.json 就绪后 |

## 启动流程
1. 读取 papers/tyg_sepsis.pdf
2. 提取结构化方法学信息 → 写入 shared/methods.json
3. 用 `codex exec` 并行派生 sql_agent 和 stats_agent（stats_agent 等待 sql_agent 完成）
4. sql_agent 完成后，触发 stats_agent
5. stats_agent 完成后，并行触发 verify_agent 和 report_agent
6. 收集所有子 Agent 结果，输出最终状态

## 派生子 Agent 的标准命令
codex exec --agent sql_agent \
  --context shared/methods.json \
  --output shared/cohort.csv \
  "按 agents/sql_agent/AGENTS.md 规则提取 MIMIC-IV 队列"

## 质控门槛（不达标则暂停，向用户报告）
- cohort.csv 样本量：目标 1742，允许 ±5%（1655–1829）
- results_table.csv 必须包含全部 3 个 Cox 模型结果
- deviation_table.json 中 ❌ 指标数量 ≤ 2 才允许生成报告

## 当前目标论文
DOI: 10.1038/s41598-024-75050-8
数据库: MIMIC-IV v2.2
```


***

### `agents/sql_agent/AGENTS.md`（数据提取员）

```markdown
# SQL AGENT — MIMIC-IV 数据提取专员

## 角色
你只负责一件事：从 MIMIC-IV 数据库提取符合论文标准的队列数据。
不做统计，不写报告。

## 输入
shared/methods.json（由 Orchestrator 提供）

## 输出
shared/cohort.csv，包含以下字段：
subject_id, hadm_id, stay_id, age, gender, weight, height,
race, insurance, marital_status,
[所有检验指标...],
tyg_index,           ← 计算列：ln(TG_mg_dL × Glucose_mg/dL ÷ 2)
tyg_quartile,        ← Q1/Q2/Q3/Q4 分组
hospital_mortality,  ← 结局变量
icu_mortality,
hospital_los,        ← 生存时间
icu_los,
[所有评分和合并症...]

## 执行规则
1. 调用 mcp__m3__get_table_info 确认表结构再写 SQL
2. Sepsis-3.0 定义优先引用：
   https://github.com/MIT-LCP/mimic-code/blob/main/mimic-iv/concepts/sepsis/sepsis3.sql
3. 每段 SQL 执行前打印语句，等待 Orchestrator 确认
4. 输出样本量核对：
   - 原始池：来自 MIMIC 的 sepsis 患者总数
   - 逐步排除：< 18岁 / ICU < 48h / 非首次入院 / 缺失 TG 或 Glucose
   - 最终纳入：目标 1742

## MIMIC-IV v2.2 关键表
- mimiciv_derived.sepsis3          ← Sepsis-3.0 官方衍生表（优先用！）
- mimiciv_hosp.admissions
- mimiciv_icu.icustays
- mimiciv_hosp.labevents
- mimiciv_hosp.diagnoses_icd
- mimiciv_hosp.patients
```


***

### `agents/stats_agent/AGENTS.md`（统计分析员）

```markdown
# STATS AGENT — R 统计分析专员

## 角色
你只负责统计分析：读入队列数据，执行全套统计流程，输出结果表格。
不提取数据，不写最终报告。

## 输入
shared/cohort.csv（由 SQL Agent 提供）

## 输出
shared/results_table.csv，包含：
- 所有 Cox 模型的 HR、95%CI、P值
- RCS 分析结果（整体P、非线性P、拐点）
- 亚组分析结果

同时输出图表到 results/figures/：
- km_hospital.png / km_icu.png
- rcs_hospital.png / rcs_icu.png
- forest_subgroup.png

## R 统计流程
### Step 1 缺失值处理
library(mice)
imp <- mice(cohort, method='rf', m=5, seed=2024)
cohort_imp <- complete(imp, action='long', include=TRUE)

### Step 2 Cox 回归（三模型）
library(survival)
# Model 1: 单变量
# Model 2: + 年龄/性别/身高/体重/种族
# Model 3: + 全部 23 个协变量（见 shared/methods.json）
# TyG 连续变量 + 分位数变量各跑一遍

### Step 3 RCS 非线性分析
library(rms)
# knots=4, 参考组 Q1
# 分别对院内死亡和 ICU 死亡建模
# 输出整体P、非线性P、拐点坐标

### Step 4 亚组分析
# 8个分层变量，每个输出 HR+CI+interaction P

## 随机种子
set.seed(2024)  ← 固定种子，保证每次 mice 结果一致
```


***

### `agents/verify_agent/AGENTS.md`（数值校验员）

```markdown
# VERIFY AGENT — 复现结果校验专员

## 角色
你只负责一件事：严格对比复现数值与论文原始数值，输出偏差报告。
不解释原因，不提建议，只核对数字。

## 输入
- shared/results_table.csv（Stats Agent 的输出）
- 论文目标数值（硬编码如下，以论文为准）

## 论文目标数值（Table 2）
| 指标 | 目标 HR | 目标 CI下 | 目标 CI上 | 目标 P |
|------|--------|---------|---------|------|
| 院内死亡_连续_M1 | 1.19 | 1.05 | 1.35 | <0.05 |
| 院内死亡_Q4_M1 | 1.63 | 1.22 | 2.18 | <0.01 |
| ICU死亡_连续_M1 | 1.26 | 1.10 | 1.45 | <0.001 |
| ICU死亡_Q4_M1 | 1.79 | 1.28 | 2.51 | <0.001 |
| ICU死亡_Q2_M3 | 1.33 | 1.20 | 1.53 | <0.001 |
| ICU死亡_Q3_M3 | 1.75 | 1.16 | 2.63 | <0.007 |
| ICU死亡_Q4_M3 | 3.40 | 2.24 | 5.16 | <0.001 |
| RCS拐点 | 8.9 | — | — | — |

## 评级规则
- 偏差 ≤ 5%：✅ 精确复现
- 偏差 5–10%：⚠️ 基本复现
- 偏差 > 10%：❌ 显著偏差

## 输出
shared/deviation_table.json，格式：
{
  "summary": {"total": 8, "pass": ?, "warn": ?, "fail": ?},
  "score": 0-100,
  "details": [{"metric": ..., "target": ..., "actual": ..., "deviation": ..., "status": ...}]
}
```


***

### `agents/report_agent/AGENTS.md`（报告撰写员）

```markdown
# REPORT AGENT — 复现报告生成专员

## 角色
你只负责把所有结果整合成一份清晰的复现报告。
不跑代码，不做计算，只写报告。

## 输入
- shared/methods.json
- shared/deviation_table.json
- results/figures/*.png

## 输出
results/reproduction_report.md

## 报告模板
# Reproduction Report
**论文**: TyG index and mortality in critically ill sepsis patients
**DOI**: 10.1038/s41598-024-75050-8
**复现日期**: {今天日期}
**可复现性评分**: {score}/100

## 1. 队列一致性
| 项目 | 论文 | 复现 | 状态 |
| 样本量 | 1,742 | ? | ✅/❌ |
| 院内死亡率 | 19.75% | ?% | ✅/❌ |
| ICU死亡率 | 14.75% | ?% | ✅/❌ |

## 2. 主要结果对比
{从 deviation_table.json 自动生成}

## 3. 生存曲线
![KM曲线](figures/km_hospital.png)

## 4. RCS 非线性分析
![RCS](figures/rcs_icu.png)

## 5. 偏差原因分析
{对所有 ❌ 指标逐一分析可能原因}

## 6. 结论
{根据 score 给出总体评价}
```


***

## 执行命令

```bash
# 一键启动全流程（主控 Agent 自动派生其余四个）
codex "按 AGENTS.md 启动论文复现流程，目标论文 papers/tyg_sepsis.pdf"

# 查看各 Agent 实时状态
codex status --all-agents

# 单独重跑某个失败的 Agent
codex exec --agent stats_agent \
  "重新执行，上次 mice 插补种子未固定导致偏差，这次加 set.seed(2024)"
```


***

## 执行时序图

```
时间轴 ──────────────────────────────────────────────────────▶

Orchestrator  [解析PDF → methods.json]──────────────────────────┐
                                                                  │
SQL Agent                             [提取队列 → cohort.csv]──┐  │
                                                                │  │
Stats Agent                                                [R分析]─┤
                                                                │  │
Verify Agent                                                   [校验]─┐
                                                                      │
Report Agent                                                        [报告]
```

SQL Agent 和 Stats Agent 之间有依赖（Stats 等 cohort.csv 完成），Verify 和 Report 并行运行 ——这个 Fan-Out/Fan-In 模式是 Codex 多 Agent 最常用的标准模式。[^3]
<span style="display:none">[^10][^11][^12][^13][^14][^15][^4][^5][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://developers.openai.com/codex/concepts/subagents/

[^2]: https://www.linkedin.com/posts/ravicaw_codex-aiagents-aicoding-activity-7416329555788869632--pbm

[^3]: https://atalupadhyay.wordpress.com/2026/03/17/codex-subagents-parallel-ai-coding-at-scale/

[^4]: https://morphllm.com/comparisons/codex-vs-claude-code

[^5]: https://github.com/awslabs/cli-agent-orchestrator/blob/main/docs/codex-cli.md

[^6]: https://tembo.io/blog/coding-cli-tools-comparison

[^7]: https://www.reddit.com/r/codex/comments/1s0cytp/multiagent_orchestration_memory/

[^8]: https://code.visualstudio.com/blogs/2026/02/05/multi-agent-development

[^9]: https://www.leanware.co/insights/codex-vs-claude-code

[^10]: https://morphllm.com/comparisons/opencode-vs-codex

[^11]: https://intuitionlabs.ai/articles/openai-codex-app-ai-coding-agents

[^12]: https://www.verdent.ai/guides/codex-app-first-impressions-2026

[^13]: https://openai.com/index/unrolling-the-codex-agent-loop/

[^14]: https://developers.openai.com/codex/concepts/multi-agents/

[^15]: https://developers.openai.com/codex/subagents/

