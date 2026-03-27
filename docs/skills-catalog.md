# Research Skills Catalog

- Updated: `2026-03-27`
- GitHub auth baseline: `SSH` verified with `ssh -T git@github.com`
- Primary skill root for this repo: `.codex/skills`
- Optional global fallback root: `~/.codex/skills`
- Project-owned copy is authoritative for this repo; user-home global skills are no longer required
- Agent bridge artifact: `openclaw/skills/codex_skill_bridge.yaml`
- Rows marked `installed` below refer to skills now vendored into `.codex/skills`
- Deferred second-layer installs: `pyopenms`, `timesfm-forecasting`

## A. 论文写作、文献检索、审稿与引用

| Skill | Status | Source path | Install location | API / service dependency | 推荐触发中文短语 | 分工边界 |
| --- | --- | --- | --- | --- | --- | --- |
| `clinical-reports` | `project-local` | `projects/paper-repro-agent/.codex/skills/clinical-reports` | `.codex/skills/clinical-reports` | none | 临床论文写作；病例报告；临床研究结果写作；医学报告 | 偏临床文档和病例/试验报告，不替代通用论文写作 |
| `scientific-writing` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/scientific-writing` | `.codex/skills/scientific-writing` | none | 科研论文写作；论文改写；IMRAD；论文润色 | 偏成稿写作和段落化表达，不单独承担数据库检索 |
| `literature-review` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/literature-review` | `.codex/skills/literature-review` | none | 文献综述；系统综述；研究背景检索；研究现状梳理 | 偏检索后综述与证据综合，不负责精细引文修正 |
| `citation-management` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/citation-management` | `.codex/skills/citation-management` | none | 引文管理；参考文献校对；DOI 转 BibTeX；Vancouver 引用 | 偏元数据、BibTeX、参考文献规范，不负责全文写作 |
| `peer-review` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/peer-review` | `.codex/skills/peer-review` | none | 审稿意见；论文评审；grant review；方法学评估 | 偏评审和挑错，不替代作者视角的改写 |
| `pubmed-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/pubmed-database` | `.codex/skills/pubmed-database` | none | PubMed 检索；医学文献查询；PMID 查询 | 偏生物医学正式文献库，不覆盖 CS/econ 预印本 |
| `arxiv-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/arxiv-database` | `.codex/skills/arxiv-database` | none | arXiv 检索；统计学预印本；经济学预印本 | 偏 arXiv 预印本，不替代 PubMed 或 bioRxiv |
| `biorxiv-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/biorxiv-database` | `.codex/skills/biorxiv-database` | none | bioRxiv 检索；生命科学预印本 | 偏生命科学预印本，不替代正式数据库检索 |
| `research-lookup` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/research-lookup` | `.codex/skills/research-lookup` | `PARALLEL_API_KEY`, `OPENROUTER_API_KEY` | 最新研究进展；当前研究信息；研究事实核对 | 偏实时研究信息和联网查询，缺 key 时应标记为 `blocked-not-failed` |

## B. 医学统计、科研复现、因果推断、计量经济学

| Skill | Status | Source path | Install location | API / service dependency | 推荐触发中文短语 | 分工边界 |
| --- | --- | --- | --- | --- | --- | --- |
| `statistical-analysis` | `project-local` | `projects/paper-repro-agent/.codex/skills/statistical-analysis` | `.codex/skills/statistical-analysis` | none | 医学统计；统计分析方案；Table 1；描述统计；假设检验 | 偏检验选择、假设检查和论文式结果汇报，不负责复杂模型实现 |
| `pyhealth` | `project-local` | `projects/paper-repro-agent/.codex/skills/pyhealth` | `.codex/skills/pyhealth` | Python package install at use time | 电子病历分析；医疗 AI；MIMIC 论文复现；临床预测建模 | 偏临床机器学习和 EHR 处理，不替代传统统计建模 |
| `clinicaltrials-database` | `project-local` | `projects/paper-repro-agent/.codex/skills/clinicaltrials-database` | `.codex/skills/clinicaltrials-database` | none | ClinicalTrials 查询；NCT 检索；试验注册信息 | 偏试验元数据检索，对回顾性数据库论文是辅助而不是主线 |
| `statsmodels` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/statsmodels` | `.codex/skills/statsmodels` | Python package install at use time | 计量经济学；因果推断；双重差分；事件研究；稳健标准误 | 偏回归、时间序列、因果和系数表，不负责检验流程引导 |
| `pymc` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/pymc` | `.codex/skills/pymc` | Python package install at use time | 贝叶斯建模；分层模型；贝叶斯因果推断；不确定性量化 | 偏贝叶斯与概率建模，不替代频率学派标准流程 |
| `scikit-survival` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/scikit-survival` | `.codex/skills/scikit-survival` | Python package install at use time | 生存分析；Kaplan-Meier；Cox 回归；删失数据 | 偏 time-to-event 分析，不负责一般 GLM/面板模型 |
| `shap` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/shap` | `.codex/skills/shap` | Python package install at use time | SHAP 解释；特征重要性；模型解释 | 偏模型可解释性，不负责训练主模型 |
| `fred-economic-data` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/fred-economic-data` | `.codex/skills/fred-economic-data` | `FRED_API_KEY` | FRED 数据；美国通胀；失业率；宏观经济指标 | 偏 FRED 指标获取，缺 key 时应标记为 `blocked-not-failed` |
| `usfiscaldata` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/usfiscaldata` | `.codex/skills/usfiscaldata` | none | 国债数据；Treasury Fiscal Data；财政数据 | 偏美国财政部公开数据，不替代 FRED 宏观指标库 |
| `datacommons-client` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/datacommons-client` | `.codex/skills/datacommons-client` | none | 公共统计数据；人口统计；地区经济数据 | 偏多来源公共统计，不替代回归建模技能 |
| `edgartools` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/edgartools` | `.codex/skills/edgartools` | `EDGAR_IDENTITY` | SEC 文件；10-K；10-Q；上市公司财报 | 偏 SEC filings 和结构化财报，缺 identity 时应标记为 `blocked-not-failed` |
| `timesfm-forecasting` | `planned-second-layer` | `K-Dense-AI/claude-scientific-skills/scientific-skills/timesfm-forecasting` | `not installed` | model download and system-check required | 时间序列预测；zero-shot forecasting | 第二层可选，不纳入本轮默认安装 |

## C. 生物信息学与多组学

| Skill | Status | Source path | Install location | API / service dependency | 推荐触发中文短语 | 分工边界 |
| --- | --- | --- | --- | --- | --- | --- |
| `biopython` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/biopython` | `.codex/skills/biopython` | Python package install at use time | 序列处理；FASTA；GenBank；NCBI Entrez | 偏通用生信脚本和格式处理，不替代单细胞或 DE 主流程 |
| `scikit-bio` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/scikit-bio` | `.codex/skills/scikit-bio` | Python package install at use time | 微生物组；系统发育；alpha/beta diversity | 偏生态/系统发育和多样性分析 |
| `gget` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/gget` | `.codex/skills/gget` | internet access | 基因快速查询；gene info；BLAST；enrichment | 偏轻量级快速联查，不替代严谨批量处理 |
| `pysam` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/pysam` | `.codex/skills/pysam` | Python package install at use time | BAM/CRAM；VCF；变异文件处理；coverage | 偏 NGS 文件与变异/比对操作 |
| `pydeseq2` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/pydeseq2` | `.codex/skills/pydeseq2` | Python package install at use time | 差异表达；bulk RNA-seq；count matrix；火山图 | 偏 bulk RNA-seq DE，不替代单细胞 pipeline |
| `geo-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/geo-database` | `.codex/skills/geo-database` | none | GEO 检索；GSE 查询；表达数据下载 | 偏公共转录组数据集获取 |
| `gwas-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/gwas-database` | `.codex/skills/gwas-database` | none | GWAS Catalog；rsID 查询；遗传流行病学 | 偏 SNP-trait 关联与目录检索 |
| `gtex-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/gtex-database` | `.codex/skills/gtex-database` | none | GTEx；组织特异表达；eQTL；sQTL | 偏组织表达与调控解释 |
| `ensembl-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/ensembl-database` | `.codex/skills/ensembl-database` | none | Ensembl；基因注释；VEP；ortholog | 偏基因组注释和 REST 查询 |
| `clinvar-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/clinvar-database` | `.codex/skills/clinvar-database` | none | ClinVar；致病性注释；VUS | 偏临床变异解释 |
| `gene-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/gene-database` | `.codex/skills/gene-database` | none | NCBI Gene；基因信息；gene symbol 查询 | 偏基因元数据和 cross-reference |
| `uniprot-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/uniprot-database` | `.codex/skills/uniprot-database` | none | UniProt；蛋白注释；ID mapping | 偏蛋白注释与 ID 映射 |
| `scanpy` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/scanpy` | `.codex/skills/scanpy` | Python package install at use time | 单细胞分析；h5ad；QC；UMAP；聚类；marker gene | 偏标准单细胞探索性 pipeline，不替代深度概率模型 |
| `scvi-tools` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/scvi-tools` | `.codex/skills/scvi-tools` | Python package install at use time | scVI；batch correction；多模态单细胞 | 偏高级单细胞生成模型和 batch correction |
| `anndata` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/anndata` | `.codex/skills/anndata` | Python package install at use time | h5ad 读写；AnnData；obs/var 管理 | 偏数据结构本身，不替代分析流程 |
| `cellxgene-census` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/cellxgene-census` | `.codex/skills/cellxgene-census` | internet access | CELLxGENE Census；参考图谱；大规模单细胞查询 | 偏公共单细胞 atlas 查询 |
| `kegg-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/kegg-database` | `.codex/skills/kegg-database` | academic-use license condition | KEGG 富集；pathway mapping；代谢通路 | 偏 KEGG 通路和 ID 转换 |
| `reactome-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/reactome-database` | `.codex/skills/reactome-database` | none | Reactome 富集；通路分析 | 偏 Reactome pathway 和富集 |
| `string-database` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/string-database` | `.codex/skills/string-database` | none | STRING；蛋白互作；PPI 网络 | 偏蛋白互作网络 |
| `pyopenms` | `planned-second-layer` | `K-Dense-AI/claude-scientific-skills/scientific-skills/pyopenms` | `not installed` | Python package install at use time | 质谱；proteomics；LC-MS/MS | 第二层可选，不纳入本轮默认安装 |

## D. 科研画图与可视化

| Skill | Status | Source path | Install location | API / service dependency | 推荐触发中文短语 | 分工边界 |
| --- | --- | --- | --- | --- | --- | --- |
| `scientific-visualization` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/scientific-visualization` | `.codex/skills/scientific-visualization` | Python package install at use time | 科研作图；论文图；多面板图；期刊投稿图 | 偏期刊级静态图整合，不是最快速探索方式 |
| `scientific-schematics` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/scientific-schematics` | `.codex/skills/scientific-schematics` | `OPENROUTER_API_KEY` | 机制图；流程图；示意图；CONSORT 图 | 偏 AI 生成示意图，缺 key 时应标记为 `blocked-not-failed` |
| `matplotlib` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/matplotlib` | `.codex/skills/matplotlib` | Python package install at use time | 自定义静态图；publication plot | 偏底层可控绘图 |
| `seaborn` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/seaborn` | `.codex/skills/seaborn` | Python package install at use time | 箱线图；小提琴图；热图；快速统计图 | 偏快速统计可视化 |
| `plotly` | `installed` | `K-Dense-AI/claude-scientific-skills/scientific-skills/plotly` | `.codex/skills/plotly` | Python package install at use time | 交互图；hover；dashboard | 偏交互与展示，不是默认投稿静态图方案 |

## E. GitHub 与发布工作流

| Skill | Status | Source path | Install location | API / service dependency | 推荐触发中文短语 | 分工边界 |
| --- | --- | --- | --- | --- | --- | --- |
| `git-github-update` | `project-local` | `projects/paper-repro-agent/.codex/skills/git-github-update` | `.codex/skills/git-github-update` | `SSH` verified; `gh` not required | 提交并推送；同步远程；检查 SSH；发布更新 | 偏标准 git 发布流程，不负责 GitHub API 自动化 |

## F. 自主迭代与夜间运行

| Skill | Status | Source path | Install location | API / service dependency | 推荐触发中文短语 | 分工边界 |
| --- | --- | --- | --- | --- | --- | --- |
| `codex-autoresearch` | `installed` | `leo-lilinxiao/codex-autoresearch` | `.codex/skills/codex-autoresearch` | none for install; runtime depends on the target repo's toolchain | 夜间自动迭代；无人值守修复；持续改进；improve-verify loop；背景运行 | 偏自动化 modify-verify-keep/discard 循环，会涉及 commit/rollback，适合可机械验证的目标，不适合普通一次性问答 |

## Current Credential Snapshot

| Key / requirement | Current status | Impact |
| --- | --- | --- |
| `PARALLEL_API_KEY` | `missing` | `research-lookup` should be treated as `blocked-not-failed` for live backend tests |
| `OPENROUTER_API_KEY` | `missing` | blocks `research-lookup` academic backend and `scientific-schematics` |
| `FRED_API_KEY` | `missing` | blocks live `fred-economic-data` API tests |
| `EDGAR_IDENTITY` | `missing` | blocks live `edgartools` API tests |

## Notes

- This round intentionally keeps `clinical-reports`, `clinicaltrials-database`, `pyhealth`, `statistical-analysis`, and `git-github-update` in the project-local tree.
- `codex-autoresearch` is now also vendored into this repo so the project remains self-contained.
- The OpenClaw-facing bridge for these project-owned skills now lives at `openclaw/skills/codex_skill_bridge.yaml`.
- If a future trigger is still too strict, keep widening `description` coverage before creating a new wrapper skill.
- Any actual auto-trigger validation must be rerun in a fresh Codex session after restart so the vendored project skills and updated descriptions are rediscovered.
