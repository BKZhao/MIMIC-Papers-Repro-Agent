# Research Skills Smoke Tests

- Updated: `2026-03-27`
- Result labels: `pass`, `fail`, `blocked-not-failed`
- Required restart: restart Codex before running any real trigger test so the vendored project skills and updated descriptions are rediscovered
- Recommended run order: base checks -> category core tests -> extended implicit suite -> credential-gated tests

## Base Checks

Run these once after restart:

1. Confirm `.codex/skills` contains the vendored project-owned skills.
2. Confirm high-priority skills such as `scientific-writing`, `statsmodels`, `scanpy`, `scientific-visualization`, and `codex-autoresearch` are present under `.codex/skills`.
3. Record credential status before live API tests.

Suggested shell checks:

```bash
ls -1 .codex/skills | sort
for s in scientific-writing statsmodels scanpy scientific-visualization codex-autoresearch; do
  test -d ".codex/skills/$s" && echo "$s present" || echo "$s missing"
done
for key in PARALLEL_API_KEY OPENROUTER_API_KEY FRED_API_KEY EDGAR_IDENTITY; do
  if [ -n "${!key}" ]; then echo "$key=present"; else echo "$key=missing"; fi
done
```

## Category Core Tests

| Category | Implicit prompt | Expected skills | Explicit prompt | Expected skills | Pass if | Blocked / skip rule |
| --- | --- | --- | --- | --- | --- | --- |
| A. 论文写作、文献检索、审稿与引用 | `帮我把这篇回顾性医学论文按 IMRAD 结构重写引言，并补一版 2023–2026 的文献脉络和 Vancouver 引用。` | `scientific-writing`, `literature-review`, `citation-management`; optional `research-lookup` | `Use $scientific-writing to rewrite the introduction in IMRAD style, and use $citation-management to normalize Vancouver references.` | `scientific-writing`, `citation-management` | The response follows IMRAD writing flow, uses prose instead of bullets for the manuscript text, and routes references/citations to the right skills | If `research-lookup` is unavailable because keys are missing, keep result as `pass` if the core writing skills still activate |
| B. 医学统计、科研复现、因果推断、计量经济学 | `给这个临床队列研究设计统计分析方案：Table 1、组间比较、Kaplan-Meier、Cox、多因素和亚组分析，最后按论文结果段格式写出来。` | `statistical-analysis`, `statsmodels`, `scikit-survival`, `clinical-reports` | `Use $statsmodels to build the regression backbone, and route survival pieces to $scikit-survival.` | `statsmodels`, `scikit-survival` | The response clearly separates test-selection guidance from model implementation and recognizes survival analysis as a separate branch | If only `statistical-analysis` appears and model-specific routing never happens, mark `fail` |
| C. 生物信息学与多组学 | `我有一个 h5ad 文件，想做单细胞 QC、UMAP、聚类、marker gene 和 batch correction。` | `anndata`, `scanpy`, `scvi-tools` | `Use $scanpy to process this h5ad file for QC, UMAP, clustering, and marker genes.` | `scanpy` | The response recognizes the h5ad / single-cell workflow and distinguishes analysis from data-structure handling | If the request is routed to generic plotting or generic Python help only, mark `fail` |
| D. 科研画图与可视化 | `把这个结果整理成期刊投稿用的多面板图，要求 PDF/SVG 输出、色盲友好、图例统一。` | `scientific-visualization`, `matplotlib`, `seaborn` | `Use $scientific-visualization to turn these results into a journal-ready multi-panel figure with PDF and SVG export.` | `scientific-visualization` | The response frames the task as publication-ready figure assembly rather than ad hoc plotting | If the response defaults to only `plotly` or generic plotting help, mark `fail` |
| E. GitHub 与发布工作流 | `把这个仓库当前修改按标准流程提交并推送到远程，先检查 SSH、branch 和 status。` | `git-github-update` | `Use $git-github-update to publish this repo with a conventional commit message.` | `git-github-update` | The response performs preflight git checks and routes publishing through the standardized workflow | If it proposes force-push or skips preflight checks, mark `fail` |

## Required Additional Explicit Checks

These are separate from the category matrix because the plan requires them explicitly.

| Required check | Prompt | Expected skills | Pass if |
| --- | --- | --- | --- |
| Bulk RNA-seq explicit check | `Use $pydeseq2 to analyze this bulk RNA-seq count matrix, return DE genes, and outline volcano and MA plots.` | `pydeseq2` | The response frames the task as DESeq2-style differential expression from count data |
| Single-cell explicit check | `Use $scanpy to analyze this h5ad dataset and produce QC, UMAP, clustering, and marker genes.` | `scanpy` | The response frames the task as a standard scanpy single-cell pipeline |
| Writing explicit check | `Use $scientific-writing to draft the introduction and discussion for this retrospective cohort paper.` | `scientific-writing` | The response uses full manuscript prose conventions |
| Statistics explicit check | `Use $statsmodels to fit the DID model, report robust standard errors, and produce a publication-ready coefficient table.` | `statsmodels` | The response routes to econometric modeling rather than generic stats help |
| Figure explicit check | `Use $scientific-visualization to prepare a multi-panel journal figure from these analysis outputs.` | `scientific-visualization` | The response routes to publication-quality figure assembly |

## Extended Implicit Suite

Run this suite after the core category tests. These are the exact prompts we want to keep stable across future regressions.

| ID | Prompt | Expected skills |
| --- | --- | --- |
| 1 | `帮我把这篇回顾性医学论文按 IMRAD 结构重写引言，并补一版 2023–2026 的文献脉络和 Vancouver 引用。` | `scientific-writing`, `literature-review`, `citation-management`; optional `research-lookup` |
| 2 | `给这个临床队列研究设计统计分析方案：Table 1、组间比较、Kaplan-Meier、Cox、多因素和亚组分析，最后按论文结果段格式写出来。` | `statistical-analysis`, `statsmodels`, `scikit-survival`, `clinical-reports` |
| 3 | `我有一份 RNA-seq count matrix，帮我做差异表达、火山图、热图和 KEGG/Reactome 富集。` | `pydeseq2`, `scientific-visualization`, `kegg-database`, `reactome-database` |
| 4 | `我有一个 h5ad 文件，想做单细胞 QC、UMAP、聚类、marker gene 和 batch correction。` | `anndata`, `scanpy`, `scvi-tools` |
| 5 | `我想做一个双重差分和事件研究，分析政策出台前后结果变量变化，并给稳健标准误和可发表图。` | `statsmodels`, `pymc`, `scientific-visualization` |
| 6 | `帮我拉美国通胀、失业率和国债数据，做一个计量经济学风格的政策分析草案。` | `fred-economic-data`, `usfiscaldata`, `datacommons-client`, `statsmodels` |
| 7 | `把这个结果整理成期刊投稿用的多面板图，要求 PDF/SVG 输出、色盲友好、图例统一。` | `scientific-visualization`, `matplotlib`, `seaborn` |

## Credential-Gated Tests

| Skill | Current status | Prerequisite | Test prompt | Result rule |
| --- | --- | --- | --- | --- |
| `research-lookup` | `blocked-not-failed` | `PARALLEL_API_KEY` and `OPENROUTER_API_KEY` | `查一下 2025–2026 年关于 sepsis phenotyping 的最新研究，并给出可核对的论文来源。` | If either key is missing, log `blocked-not-failed` and do not count as failure |
| `fred-economic-data` | `blocked-not-failed` | `FRED_API_KEY` | `拉取美国 CPI、失业率和联邦基金利率，并输出 2008–2026 的月度数据。` | If key is missing, log `blocked-not-failed` |
| `edgartools` | `blocked-not-failed` | `EDGAR_IDENTITY` | `抓取一家美国上市公司最近 3 份 10-K/10-Q，并提取收入、利润和现金流。` | If identity is missing, log `blocked-not-failed` |
| `scientific-schematics` | `blocked-not-failed` | `OPENROUTER_API_KEY` | `生成一个用于论文的 sepsis 患者纳排流程图和分析流程示意图。` | If OpenRouter key is missing, log `blocked-not-failed` |
| `timesfm-forecasting` | `deferred` | install skill first, then run its system checker | `Use $timesfm-forecasting to run the mandatory system checker before any forecasting.` | Not part of this round because the skill is intentionally not installed |

## Pass Criteria

- At least one implicit prompt per category activates the correct primary skill group.
- The explicit checks for `scientific-writing`, `statsmodels`, `scanpy`, `pydeseq2`, and `scientific-visualization` must all pass.
- The project-owned skill set under `.codex/skills` remains self-contained and usable without requiring `~/.codex/skills`.
- Any missing-key live test is recorded as `blocked-not-failed`, not `fail`.

## Additional Workflow Skill Check

Use this after restart if you want to validate the newly added autonomous loop skill.

| Prompt | Expected skill | Pass if | Caution |
| --- | --- | --- | --- |
| `Use $codex-autoresearch to reduce the number of failing tests in this repo, propose the metric and guard commands, then stop before execution and wait for my go signal.` | `codex-autoresearch` | The response switches into measurable improve-verify-loop planning, proposes a baseline metric plus guard, and explicitly waits for confirmation before launching | This skill is designed for stateful commit/rollback loops; do not start unattended execution on a repo unless you are comfortable with autonomous git activity |

## Logging Template

Use this flat template for each run:

```text
date:
session:
test_id:
prompt:
expected_skills:
observed_skills:
result:
notes:
```
