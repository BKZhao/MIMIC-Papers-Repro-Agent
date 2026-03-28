# s12890-025-04067-0 论文复现对比报告（MD）

- 日期: `2026-03-27`
- 论文: `papers/s12890-025-04067-0.pdf`
- 复现会话: `session-e9edb53ab5b9`
- 执行模式: `hybrid_llm_assisted`（LLM 抽取论文证据 + 本地确定性统计执行）

## 1. 报告目的

本报告用于并排比较该论文的目标结果与当前复现结果，重点覆盖:

1. 样本规模与关键性能指标
2. 论文要求图表与当前生成图表的一致性
3. 当前未对齐原因与下一步收敛方向

## 2. 证据文件

- 论文目标抽取:
  - `shared/sessions/session-e9edb53ab5b9/paper_evidence.json`
- 当前复现统计:
  - `shared/sessions/session-e9edb53ab5b9/binary_outcome/stats_summary.json`
  - `shared/sessions/session-e9edb53ab5b9/binary_outcome/train_validation_summary.json`
  - `shared/sessions/session-e9edb53ab5b9/binary_outcome/roc_summary.json`
- 当前复现报告:
  - `shared/sessions/session-e9edb53ab5b9/binary_outcome/reproduction_report.md`
- 当前复现图形:
  - `results/sessions/session-e9edb53ab5b9/binary_outcome/roc.png`
  - `results/sessions/session-e9edb53ab5b9/binary_outcome/nomogram.png`
  - `results/sessions/session-e9edb53ab5b9/binary_outcome/calibration_curve.png`
  - `results/sessions/session-e9edb53ab5b9/binary_outcome/decision_curve.png`

## 3. 关键指标并排对比

| 指标 | 论文目标 | 当前复现 | 差值（复现-论文） | 对齐状态 |
|---|---:|---:|---:|---|
| 总样本量 N | 559 | 739 | +180 | 未对齐 |
| 28 天死亡事件数 | 论文未明确给出 | 225 | - | 信息补充 |
| AUC（训练集） | 0.811 | 0.762816 | -0.048184 | 未对齐 |
| AUC（验证集） | 0.790 | 0.715739 | -0.074261 | 未对齐 |
| C-index（训练集） | 0.782 | 0.702077 | -0.079923 | 未对齐 |
| C-index（验证集） | 0.749 | 0.677468 | -0.071532 | 未对齐 |
| AUC（全样本，补充） | 论文未直接给出 | 0.755677 | - | 参考值 |
| C-index（全样本，补充） | 论文未直接给出 | 0.696315 | - | 参考值 |
| Brier score（全样本，补充） | 论文未直接给出 | 0.176217 | - | 参考值 |

补充说明:

- 训练/验证拆分: `train=518`, `validation=221`
- 缺失值预处理:
  - 缺失阈值 `0.20`
  - `>20%` 缺失变量剔除: 无
  - 剩余缺失插补: 数值型中位数（如 `wbc=12.4`, `glucose=157.0`, `temperature=36.9069`）

## 4. 图表并排对比

### 4.1 论文要求图意图（来自 paper_evidence）

- `Nomogram`
- `ROC curves`
- `Calibration curve`
- `Decision curve analysis plot`
- 表格类: `Baseline characteristics table`, `Cox regression results table`

### 4.2 当前复现图表产物与对齐判断

| 论文图意图 | 当前产物文件 | 是否生成 | 对齐判断 |
|---|---|---|---|
| Nomogram | `results/sessions/session-e9edb53ab5b9/binary_outcome/nomogram.png` | 是 | 语义对齐；为 Cox 系数驱动近似实现，非论文原工具链逐像素复刻 |
| ROC curves | `results/sessions/session-e9edb53ab5b9/binary_outcome/roc.png` | 是 | 语义对齐；含主模型与临床评分对照（APSIII/SAPSII/OASIS） |
| Calibration curve | `results/sessions/session-e9edb53ab5b9/binary_outcome/calibration_curve.png` | 是 | 已生成；优先主模型 `model_1` |
| Decision curve analysis | `results/sessions/session-e9edb53ab5b9/binary_outcome/decision_curve.png` | 是 | 已生成；优先主模型 `model_1` |

图像规格（当前文件属性）:

- `roc.png`: `1770 x 1466`
- `nomogram.png`: `2435 x 1774`
- `calibration_curve.png`: `1590 x 1286`
- `decision_curve.png`: `1650 x 1286`

### 4.3 ROC 对照结果（当前复现）

- `model_1`: AUC `0.755677`
- `APSIII`: AUC `0.639840`
- `SAPSII`: AUC `0.647903`
- `OASIS`: AUC `0.613904`

## 5. 当前结论

1. 图表层面已从“生存分析固定模板”升级为更贴近论文需求的 `nomogram + ROC + calibration + DCA` 组合，图意图基本对齐。
2. 数值层面尚未对齐，核心差异仍集中在样本口径和验证集判别性能。
3. 当前版本可以作为“可运行、可追踪、可复查”的阶段性复现基线，但还不是最终论文级一致版本。

## 6. 建议下一步（按优先级）

1. 优先收敛队列口径（目标先逼近 `N=559`）:
   - 细化 ARF ICD 代码集合
   - 复核主诊断判定规则
   - 复核 first ICU stay 与 LOS 组合排除策略
2. 在固定 split 种子的前提下收敛性能:
   - 先做变量候选池与 LASSO 路径对齐，再看训练/验证 AUC 与 C-index
3. 补齐论文方法细节:
   - 多重插补（MICE）
   - bootstrap 校准（500 次）

---

如果后续需要投稿版输出，可在此 MD 基础上再导出为 PDF（当前环境缺少 `pandoc/wkhtmltopdf/soffice`，需先安装其中一个工具）。
