<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# 现在给我重新更新这个MD文档，我给它读一下

```markdown
# MIMIC-IV 论文自动复现框架
## 基于 Codex CLI + M3 + claude-scientific-skills

**目标论文**：TyG index and mortality in critically ill sepsis patients  
**DOI**：10.1038/s41598-024-75050-8  
**数据库**：MIMIC-IV v2.2  
**目标样本量**：n = 1,742

---

## 一、工具栈（均已核实真实存在）

| 工具 | 用途 | 来源 |
|------|------|------|
| **Codex CLI** | 主 Agent 运行环境，支持多 Agent 并行 | OpenAI 官方 |
| **M3**（rafiattrach/m3） | MIMIC-IV ↔ MCP 桥接，自然语言查询数据库 | GitHub，MIT/Harvard BIDMC 团队，70 stars |
| **claude-scientific-skills**（K-Dense-AI） | 170+ 预制科研 Skill 模板库 | GitHub，16k stars，持续更新 |

---

## 二、环境安装

### 2.1 Codex CLI
```bash
npm install -g @openai/codex-cli
codex login
```


### 2.2 M3（MIMIC-IV 数据库接入）

M3 支持三种后端，根据你们服务器实际情况选择：


| 后端 | 适用场景 | 命令 |
| :-- | :-- | :-- |
| DuckDB Demo | 调试/测试，100患者 | `m3 init mimic-iv-demo` |
| DuckDB Full | **你们的情况：服务器有CSV文件** | `m3 init mimic-iv-full` |
| BigQuery | GCP 云端数据 | `m3 config --backend bigquery` |

```bash
pip install m3-mcp

# 指向你们服务器上的 MIMIC-IV CSV 文件夹（约30分钟转换）
m3 init mimic-iv-full --source-dir /path/to/mimic-iv-2.2/

# 生成 MCP 配置（自动写入 .codex/mcp.json）
m3 config --quick
```


### 2.3 安装科研 Skills

```bash
git clone https://github.com/K-Dense-AI/claude-scientific-skills

# 全局安装（所有项目共用）
cp -r claude-scientific-skills/scientific-skills/* ~/.codex/skills/

# 或项目级安装（只在本项目用）
mkdir -p .codex/skills/
cp -r claude-scientific-skills/scientific-skills/* .codex/skills/
```

你们复现这篇论文需要的 Skills（均在该库中真实存在）：


| Skill | 用途 |
| :-- | :-- |
| `Statistical Analysis` | 描述统计、Table 1 基线特征 |
| `Clinical Reports` | 生成 PDF/Markdown 复现报告 |
| `ClinicalTrials.gov` | 核查论文注册信息 |
| `PyHealth` | 临床数据预处理 |

> ⚠️ **注意**：`survival-analysis`（KM曲线/Cox回归/RCS）在该库中
> 未单独列出，统计分析部分需要在 R 脚本中自行实现，
> 或在 `Statistical Analysis` Skill 基础上扩展。

---

## 三、项目结构

```
mimic-repro/
├── AGENTS.md                    # 主控 Orchestrator 规则
│
├── agents/
│   ├── sql_agent/
│   │   └── AGENTS.md            # SQL Agent 专属规则
│   ├── stats_agent/
│   │   └── AGENTS.md            # 统计分析 Agent 专属规则
│   ├── verify_agent/
│   │   └── AGENTS.md            # 数值校验 Agent 专属规则
│   └── report_agent/
│       └── AGENTS.md            # 报告生成 Agent 专属规则
│
├── .codex/
│   ├── mcp.json                 # M3 自动生成，Claude/Codex 连接 MIMIC-IV
│   └── skills/                  # Skills 目录（从 claude-scientific-skills 复制）
│       ├── Statistical Analysis/
│       ├── Clinical Reports/
│       └── mimic-connector/     # 自己写，10行
│
├── shared/                      # Agent 间传递的中间文件
│   ├── methods.json             # Orchestrator 解析论文后输出
│   ├── cohort.csv               # SQL Agent 输出
│   ├── results_table.csv        # Stats Agent 输出
│   └── deviation_table.json     # Verify Agent 输出
│
├── papers/
│   └── tyg_sepsis.pdf
│
├── R/
│   ├── 01_imputation.R
│   ├── 02_cox_regression.R
│   ├── 03_rcs_analysis.R
│   └── 04_subgroup.R
│
└── results/
    ├── reproduction_report.md
    └── figures/
        ├── km_hospital.png
        ├── km_icu.png
        ├── rcs_hospital.png
        ├── rcs_icu.png
        └── forest_subgroup.png
```


---

## 四、AGENTS.md 文件（分层加载，子目录覆盖父目录）

### 根目录 AGENTS.md（主控 Orchestrator）

```markdown
# ORCHESTRATOR — 论文复现主控

## 角色
总指挥。负责解析论文、拆解任务、派生子 Agent、汇总结果。
不直接写 SQL，不直接跑 R。

## 当前目标论文
DOI: 10.1038/s41598-024-75050-8
数据库: MIMIC-IV v2.2
目标样本量: n = 1,742

## 论文关键数值（Table 2，用于最终校验）
| 指标 | 目标 HR | 95% CI |
|------|--------|--------|
| Q4 院内死亡 Model 1 | 1.63 | 1.22–2.18 |
| Q4 ICU死亡 Model 1 | 1.79 | 1.28–2.51 |
| Q2 ICU死亡 Model 3 | 1.33 | 1.20–1.53 |
| Q3 ICU死亡 Model 3 | 1.75 | 1.16–2.63 |
| Q4 ICU死亡 Model 3 | 3.40 | 2.24–5.16 |
| RCS 拐点 | 8.9 | — |

## 标准执行流程
1. 解析 papers/tyg_sepsis.pdf → 写入 shared/methods.json
2. 派生 sql_agent → 输出 shared/cohort.csv
3. cohort.csv 就绪后，派生 stats_agent → 输出 shared/results_table.csv
4. results_table.csv 就绪后，并行派生 verify_agent + report_agent
5. 汇总两者输出，返回最终复现报告

## 质控门槛（不达标则暂停，向用户报告）
- 样本量偏差 ≤ 5%（目标 1742，允许范围 1655–1829）
- ❌ 指标数量 ≤ 2，才允许生成最终报告

## 全局规则
- 数据库所有操作必须是 SELECT，禁止写入
- SQL 执行前打印预览，等待确认
- 所有数值保留两位小数
- 与用户中文沟通，代码和报告用英文
```


---

### agents/sql_agent/AGENTS.md

```markdown
# SQL AGENT — MIMIC-IV 数据提取

## 角色
只负责数据提取，不做统计。

## 输入
shared/methods.json

## 输出
shared/cohort.csv

## 必须包含的字段
subject_id, hadm_id, stay_id, age, gender, weight, height, race,
insurance, marital_status, [全部检验指标],
tyg_index,        ← ln(TG_mg/dL × Glucose_mg/dL ÷ 2)
tyg_quartile,     ← Q1(<8.56) / Q2(8.56-9.03) / Q3(9.03-9.56) / Q4(≥9.56)
hospital_mortality, icu_mortality, hospital_los, icu_los,
[全部评分和合并症字段]

## 纳入/排除标准
纳入：
- Sepsis-3.0（优先使用 mimiciv_derived.sepsis3 官方衍生表）
- 年龄 ≥ 18 岁
- 首次 ICU 入院
- ICU 住院时长 ≥ 48 小时
- 有甘油三酯 + 空腹血糖记录

排除：
- 年龄 < 18 岁
- ICU 多次入院（保留第一次）
- 关键检验数据缺失

## MIMIC-IV v2.2 关键表
- mimiciv_derived.sepsis3        ← Sepsis-3.0 官方衍生表（优先用）
- mimiciv_hosp.admissions
- mimiciv_icu.icustays
- mimiciv_hosp.labevents
- mimiciv_hosp.diagnoses_icd
- mimiciv_hosp.patients

## 执行规则
- 通过 M3 MCP 工具执行所有查询
- 每段 SQL 执行前打印语句，等待确认
- 输出样本量逐步筛选漏斗（原始数 → 逐步排除 → 最终 n）
```


---

### agents/stats_agent/AGENTS.md

```markdown
# STATS AGENT — R 统计分析

## 角色
只负责统计分析，不提取数据，不写报告。

## 输入
shared/cohort.csv

## 输出
shared/results_table.csv（全部 HR/CI/P 值）
results/figures/（KM曲线、RCS图、森林图）

## R 统计流程

### Step 1 缺失值处理
library(mice)
set.seed(2024)   ← 必须固定，保证可重复
imp <- mice(cohort, method = 'rf', m = 5)
# 缺失率 > 30% 的变量先删除，再做插补

### Step 2 Cox 回归（三模型）
library(survival)
# TyG 连续变量 + 四分位分组各跑一次

# Model 1：单变量
# Model 2：+ 年龄/性别/身高/体重/种族
# Model 3：+ 以下全部协变量：
#   保险状态、婚姻状态、WBC、RBC、RDW、白蛋白、氯化物、
#   ALT、AST、SOFA、APSIII、SAPSII、OASIS、Charlson、GCS、
#   高血压、2型糖尿病、心力衰竭、心肌梗死、恶性肿瘤、
#   CKD、ARF、卒中、高脂血症、COPD

### Step 3 RCS 非线性分析
library(rms)
# knots = 4，参考组 = Q1（TyG < 8.56）
# 院内死亡 + ICU死亡各建一个模型
# 输出：整体效应 P值 + 非线性 P值 + 拐点

### Step 4 亚组分析
# 8个分层变量：
# 年龄(70岁) / 性别 / BMI(27.4/31.2) /
# 高血压 / 2型糖尿病 / 心衰 / CRRT / 机械通气
# 每组输出 HR + CI + P for interaction
```


---

### agents/verify_agent/AGENTS.md

```markdown
# VERIFY AGENT — 数值校验

## 角色
只做一件事：严格对比数字，不解释，不建议。

## 输入
shared/results_table.csv + 论文目标数值（见下表）

## 论文目标数值
| 指标 | 目标 HR | CI 下限 | CI 上限 |
|------|--------|--------|--------|
| 院内死亡_连续_M1 | 1.19 | 1.05 | 1.35 |
| 院内死亡_Q4_M1 | 1.63 | 1.22 | 2.18 |
| ICU死亡_连续_M1 | 1.26 | 1.10 | 1.45 |
| ICU死亡_Q4_M1 | 1.79 | 1.28 | 2.51 |
| ICU死亡_Q2_M3 | 1.33 | 1.20 | 1.53 |
| ICU死亡_Q3_M3 | 1.75 | 1.16 | 2.63 |
| ICU死亡_Q4_M3 | 3.40 | 2.24 | 5.16 |
| RCS 拐点 | 8.9 | — | — |

## 评级规则
- 偏差 ≤ 5%：✅ 精确复现
- 偏差 5–10%：⚠️ 基本复现
- 偏差 > 10%：❌ 显著偏差

## 输出
shared/deviation_table.json
{
  "summary": {"total": 8, "pass": ?, "warn": ?, "fail": ?},
  "score": 0–100,
  "details": [{"metric": ..., "target": ..., "actual": ..., "pct": ..., "status": ...}]
}
```


---

### agents/report_agent/AGENTS.md

```markdown
# REPORT AGENT — 复现报告生成

## 角色
只负责整合所有结果，写成清晰报告。不跑代码，不做计算。

## 输入
shared/deviation_table.json + results/figures/*.png

## 输出
results/reproduction_report.md

## 报告结构
1. 封面：论文标题 / DOI / 复现日期 / 可复现性评分
2. 队列一致性：样本量 / 院内死亡率 / ICU死亡率
3. 主要结果对比表（从 deviation_table.json 生成）
4. 生存曲线图（KM curves）
5. RCS 非线性分析图
6. 偏差分析：对所有 ❌ 指标说明可能原因
7. 总结评价
```


---

## 五、自己写的唯一一个 Skill

其余 Skills 从 `claude-scientific-skills` 直接复制，
只需自己写这一个 MIMIC 专属接入 Skill：

**`.codex/skills/mimic-connector/SKILL.md`**：

```markdown
***
name: mimic-connector
description: Query MIMIC-IV clinical database via M3 MCP.
  Use for all MIMIC-IV data extraction tasks.
***

Use M3 MCP tool for all database queries.
SELECT only — no INSERT, UPDATE, or DELETE.
Always print SQL before executing and wait for confirmation.
Database: mimic4
Schemas: mimiciv_hosp / mimiciv_icu / mimiciv_derived
```


---

## 六、执行命令

```bash
cd mimic-repro

# 逐步调试模式（推荐第一次使用）
codex "按照 AGENTS.md 的流程，第一步：解析 papers/tyg_sepsis.pdf"
codex "第二步：提取 MIMIC-IV 队列，先展示 SQL 给我看"
codex "确认 SQL 没问题，执行提取"
codex "第三步：运行 R 统计分析"
codex "第四步：对比论文数值，生成复现报告"

# 全自动模式（调试完成后）
codex "按 AGENTS.md 完整复现论文，偏差 >10% 时暂停报告"
```


---

## 七、执行时序

```
Orchestrator  [解析PDF → methods.json]
                        ↓
SQL Agent               [MIMIC-IV提取 → cohort.csv]
                                ↓
Stats Agent                     [R统计分析 → results_table.csv]
                                        ↓              ↓（并行）
Verify Agent                          [数值校验]   Report Agent [写报告]
                                              ↓
                                    reproduction_report.md
```


---

## 八、可能遇到的最大障碍

| 问题 | 原因 | 解决方案 |
| :-- | :-- | :-- |
| 样本量与 1742 偏差大 | Sepsis-3.0 SQL 实现不同 | 使用 MIMIC 官方 `mimiciv_derived.sepsis3` 表，而非自己写 SQL |
| mice 插补结果每次不同 | 未固定随机种子 | 必须加 `set.seed(2024)` |
| M3 连接失败 | CSV 转 DuckDB 未完成 | 先跑 `m3 init mimic-iv-full`，等待30分钟 |
| Cox Model 3 偏差大 | 协变量列表遗漏 | 核查 AGENTS.md 中 Model 3 的 25 个协变量是否完整 |

```

***

以上全部内容均基于已核实的真实工具。需要我把哪一部分单独拆出来细化，或者调整格式方便打印/发给团队？```

