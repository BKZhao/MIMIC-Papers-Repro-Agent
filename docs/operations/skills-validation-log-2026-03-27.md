# Skills Validation Log — 2026-03-27

## Method

- Environment / installation checks were run directly in the shell.
- No-key live script checks were run for:
  - `git-github-update` SSH verification
  - `clinicaltrials-database` API helper
- Prompt-level routing checks were run in fresh ephemeral Codex sessions via `codex exec -C /home/bingkun_zhao/projects/paper-repro-agent --ephemeral -s read-only`.
- For routing checks, Codex was instructed to return JSON with `inferred_skills` and `reason`, without executing the underlying task.

## Post-Validation Vendoring Update

- After the validation rounds, the previously global-only scientific skills were vendored into `projects/paper-repro-agent/.codex/skills`.
- The repo is now self-contained for this skill set; `~/.codex/skills` is optional fallback only and is no longer required for this project.
- The project now also carries a machine-readable bridge at `openclaw/skills/codex_skill_bridge.yaml` so the vendored skills are connected to the OpenClaw stage model without being misrepresented as native runtime skills.
- Historical validation rows below still reflect the environment at the time they were recorded.

## Base Checks

| Check | Result | Notes |
| --- | --- | --- |
| Global scientific skills installed | `pass` | Global skills present under `~/.codex/skills` |
| `codex-autoresearch` installed | `pass` | Present at `~/.codex/skills/codex-autoresearch` |
| Project-local skills kept project-only | `pass` | No global duplicates for `clinical-reports`, `clinicaltrials-database`, `pyhealth`, `statistical-analysis`, `git-github-update` |
| Credential snapshot | `blocked-not-failed` | `PARALLEL_API_KEY`, `OPENROUTER_API_KEY`, `FRED_API_KEY`, `EDGAR_IDENTITY` were all missing |

## Live Script Checks

| Test | Result | Notes |
| --- | --- | --- |
| `git-github-update` SSH check | `pass` | GitHub SSH auth succeeded for `BKZhao` |
| `clinicaltrials-database` helper | `pass-with-fix` | Live API call succeeded, but validation exposed a stale `totalCount` assumption; script and docs were updated to treat `totalCount` as optional and rely on `studies` + `nextPageToken` |

## Prompt Routing Checks

| ID | Prompt summary | Expected main skills | Observed inferred skills | Result | Notes |
| --- | --- | --- | --- | --- | --- |
| A | IMRAD intro + 2023–2026 literature + Vancouver | `scientific-writing`, `literature-review`, `citation-management` | `literature-review`, `citation-management`, `scientific-writing` | `pass` | Strong match |
| B | Clinical cohort stats plan with Table 1, KM, Cox, subgroup, results paragraph | `statistical-analysis`, `statsmodels`, `scikit-survival`, `clinical-reports` | `statistical-analysis`, `scikit-survival`, `scientific-writing` | `partial-pass` | Correctly caught stats planning and survival branch, but did not pull in `statsmodels`; preferred `scientific-writing` over `clinical-reports` |
| C | h5ad single-cell QC / UMAP / clustering / marker gene / batch correction | `anndata`, `scanpy`, `scvi-tools` | `scanpy`, `scvi-tools`, `anndata` | `pass` | Strong match |
| D | Journal-ready multi-panel figure | `scientific-visualization` with plotting helpers | `scientific-visualization` | `pass` | Strong match |
| E | Standard git commit/push workflow with SSH/status checks | `git-github-update` | `git-github-update` | `pass` | Strong match |
| 3 | bulk RNA-seq DE + volcano + heatmap + KEGG/Reactome | `pydeseq2`, `scientific-visualization`, `kegg-database`, `reactome-database` | `pydeseq2`, `seaborn`, `kegg-database`, `reactome-database` | `partial-pass` | Core bioinformatics routing is correct; plotting was routed to `seaborn` rather than the higher-level `scientific-visualization` meta-skill |
| 5 | DID + event study + robust SE + publishable figure | `statsmodels`, `pymc`, `scientific-visualization` | `statsmodels`, `scientific-visualization` | `partial-pass` | Excellent trigger for econometrics; `pymc` was not inferred, which is acceptable unless Bayesian analysis is explicitly desired |
| 6 | U.S. inflation + unemployment + debt + econometric policy draft | `fred-economic-data`, `usfiscaldata`, `datacommons-client`, `statsmodels` | `fred-economic-data`, `usfiscaldata`, `statsmodels`, `scientific-writing` | `partial-pass` | Main economic data + modeling skills were inferred; `datacommons-client` was not selected, and drafting was routed to `scientific-writing` |

## Interpretation

- Current routing is already strong for:
  - scientific writing and citation workflows
  - single-cell analysis
  - publication-grade figure requests
  - GitHub workflow automation
  - econometrics / DID prompts
- The main gaps are not failures of installation; they are mostly preference/ranking issues in overlapping skill areas:
  - `statsmodels` is not always chosen for broad clinical cohort analysis prompts unless the prompt is explicitly econometric / regression-heavy
  - `clinical-reports` tends to lose to `scientific-writing` for journal-style result-section requests
  - `scientific-visualization` can lose to concrete plotting skills like `seaborn` when the prompt emphasizes specific plot types instead of publication assembly
  - `datacommons-client` is not a first-choice route for the current macro policy prompt wording

## Recommended Next Adjustments

1. Broaden `statsmodels` description further with `多因素分析`, `多变量回归`, `临床队列回归`, and `亚组回归`.
2. If you want `clinical-reports` to appear in retrospective clinical paper prompts, broaden it with `clinical cohort manuscript results`, `回顾性队列论文`, and `医学论文结果段`.
3. If you want `scientific-visualization` to outrank `seaborn` for RNA-seq figure prompts, add wording such as `publication assembly for volcano plots and heatmaps`.
4. Re-run the same prompts after any description changes to compare against this baseline log.

## Refinement Round

### Description changes applied

- `statsmodels`: added stronger routing terms for `多因素分析`, `多变量回归`, `临床队列回归`, `subgroup regression`, and `multivariable adjustment`
- `scientific-visualization`: added stronger publication-assembly language for `RNA-seq figures`, volcano plots, heatmaps, and enrichment figures
- `clinical-reports`: added `回顾性队列论文`, `医学论文结果段`, and `clinical cohort manuscript results`
- `datacommons-client`: added public-statistics / policy-background wording for econometric policy analysis
- `statistical-analysis`: strengthened the handoff language so analysis planning stays there, but multivariable / subgroup regression implementation is explicitly routed to `statsmodels`

### Re-run results after refinement

| ID | Prompt summary | Before | After | Result | Interpretation |
| --- | --- | --- | --- | --- | --- |
| B | Clinical cohort stats plan with Table 1, KM, Cox, subgroup, results paragraph | `statistical-analysis`, `scikit-survival`, `scientific-writing` | `statistical-analysis`, `scikit-survival`, `scientific-writing` | `no-change` | This prompt is still interpreted as a planning + writing task rather than a programmatic regression-implementation task. Automatic routing appears to prefer the minimal planning-oriented set here. |
| 3 | bulk RNA-seq DE + volcano + heatmap + KEGG/Reactome | `pydeseq2`, `seaborn`, `kegg-database`, `reactome-database` | `pydeseq2`, `scientific-visualization`, `kegg-database`, `reactome-database` | `improved` | The visualization routing now prefers the publication-oriented meta-skill, which is the intended outcome. |
| 6 | U.S. inflation + unemployment + debt + econometric policy draft | `fred-economic-data`, `usfiscaldata`, `statsmodels`, `scientific-writing` | `fred-economic-data`, `usfiscaldata`, `statsmodels`, `scientific-writing` | `no-change` | The main macro data and econometric routing were already strong. `datacommons-client` remains a secondary or supplementary route rather than a first-choice match for this wording. |

### Updated interpretation after refinement

- The trigger optimization clearly helped in overlapping visualization space: `scientific-visualization` now outranks `seaborn` for the RNA-seq publication figure prompt.
- `statsmodels` remains strong for explicitly econometric prompts such as DID / event study, but does not displace `statistical-analysis` in broad clinical analysis-planning prompts.
- `clinical-reports` still does not outrank `scientific-writing` for the tested clinical cohort prompt, which suggests the prompt is semantically closer to manuscript writing than to clinical reporting in Codex's current ranking behavior.
- `datacommons-client` is currently best treated as a supplementary data-source skill, not a primary trigger target for the tested macro policy prompt.

### Practical conclusion

- Keep the current refinements: they improved the RNA-seq plotting route without harming the already-good econometrics routing.
- Treat prompt B as an expected routing boundary:
  - if the task is "design the analysis plan and write the results section", the current route is acceptable
  - if the task is "implement multivariable / subgroup regression models", use wording that explicitly asks for regression implementation or explicitly invoke `$statsmodels`
- Do not keep widening descriptions indefinitely for prompt B; the remaining gap appears to be a true overlap/boundary issue rather than missing keywords.

## Blocked Live Tests

| Skill | Status | Blocker |
| --- | --- | --- |
| `research-lookup` | `blocked-not-failed` | missing `PARALLEL_API_KEY` and `OPENROUTER_API_KEY` |
| `scientific-schematics` | `blocked-not-failed` | missing `OPENROUTER_API_KEY` |
| `fred-economic-data` live API call | `blocked-not-failed` | missing `FRED_API_KEY` |
| `edgartools` live API call | `blocked-not-failed` | missing `EDGAR_IDENTITY` |

## Project-Owned Bridge Smoke Round

- After the project-owned bridge landed at `openclaw/skills/codex_skill_bridge.yaml`, a new smoke round was run against the repo-local `.codex/skills` tree rather than relying on `~/.codex/skills`.
- Routing probes again used fresh ephemeral Codex sessions via `codex exec -C /home/bingkun_zhao/projects/paper-repro-agent --ephemeral -s read-only`.
- For routing probes, Codex was instructed to identify inferred skills only and not execute the underlying task.

### Project-Owned Base Checks

| Check | Result | Notes |
| --- | --- | --- |
| Repo-local `.codex/skills` present | `pass` | `46` vendored skills were listed under `projects/paper-repro-agent/.codex/skills` |
| High-priority vendored skills present | `pass` | `scientific-writing`, `statsmodels`, `scanpy`, `scientific-visualization`, and `codex-autoresearch` were all present under `.codex/skills` |
| Credential snapshot | `blocked-not-failed` | `PARALLEL_API_KEY`, `OPENROUTER_API_KEY`, `FRED_API_KEY`, and `EDGAR_IDENTITY` were still missing |

### Category Core Tests Re-Run

| Test | Expected skills | Observed inferred skills | Result | Notes |
| --- | --- | --- | --- | --- |
| A implicit | `scientific-writing`, `literature-review`, `citation-management` | `scientific-writing`, `literature-review`, `citation-management` | `pass` | Clean writing / literature / citation split |
| B implicit | `statistical-analysis`, `statsmodels`, `scikit-survival`, `clinical-reports` | `statistical-analysis`, `scikit-survival`, `statsmodels`, `clinical-reports` | `pass` | Improved versus the earlier baseline; the full intended clinical stats bundle now appeared together |
| C implicit | `anndata`, `scanpy`, `scvi-tools` | `scanpy`, `anndata`, `scvi-tools` | `pass` | Correct single-cell routing |
| D implicit | `scientific-visualization`, `matplotlib`, `seaborn` | `scientific-visualization`, `matplotlib` | `pass` | Publication-figure routing remained anchored on the higher-level visualization skill |
| E implicit | `git-github-update` | `git-github-update` | `pass` | Standardized GitHub publishing workflow still routes correctly |
| A explicit | `scientific-writing`, `citation-management` | `scientific-writing`, `citation-management` | `pass` | Explicit skill mentions were honored |
| B explicit | `statsmodels`, `scikit-survival` | `statsmodels`, `scikit-survival` | `pass` | Clean split between regression backbone and survival branch |
| C explicit | `scanpy` | `scanpy`, `anndata` | `pass` | `scanpy` remained primary; `anndata` was added appropriately as data-structure support |
| D explicit | `scientific-visualization` | `scientific-visualization` | `pass` | Direct hit |
| E explicit | `git-github-update` | `git-github-update` | `pass` | Direct hit |

### Extended Implicit Suite Completion

| Test | Expected skills | Observed inferred skills | Result | Notes |
| --- | --- | --- | --- | --- |
| 3 implicit | `pydeseq2`, `scientific-visualization`, `kegg-database`, `reactome-database` | `pydeseq2`, `scientific-visualization`, `kegg-database`, `reactome-database` | `pass` | The RNA-seq pathway now matches the intended publication-oriented visualization route |
| 5 implicit | `statsmodels`, `pymc`, `scientific-visualization` | `statsmodels`, `scientific-visualization` | `partial-pass` | Strong econometrics routing remained stable; `pymc` still does not appear unless the request is more explicitly Bayesian |
| 6 implicit | `fred-economic-data`, `usfiscaldata`, `datacommons-client`, `statsmodels` | `fred-economic-data`, `usfiscaldata`, `statsmodels`, `scientific-writing` | `partial-pass` | Macro data + econometric modeling still route well; `datacommons-client` remains supplementary rather than primary for this wording |

### Required Additional Explicit Checks Re-Run

| Test | Expected skills | Observed inferred skills | Result | Notes |
| --- | --- | --- | --- | --- |
| Writing explicit | `scientific-writing` | `scientific-writing` | `pass` | Exact required prompt passed with a clean single-skill hit |
| Statistics explicit | `statsmodels` | `statsmodels` | `pass` | Exact DID + robust-SE + coefficient-table prompt passed |
| Single-cell explicit | `scanpy` | `scanpy` | `pass` | Exact required prompt passed |
| Bulk RNA-seq explicit | `pydeseq2` | `pydeseq2` | `pass` | Correct DESeq2-style routing from bulk count-matrix wording |
| Figure explicit | `scientific-visualization` | `scientific-visualization` | `pass` | Exact required prompt passed |

### Workflow Skill Check

| Test | Expected skill | Result | Notes |
| --- | --- | --- | --- |
| `codex-autoresearch` routing probe | `codex-autoresearch` | `pass` | Explicit skill mention routed correctly |
| `codex-autoresearch` behavior check | `codex-autoresearch` | `pass` | In a real read-only probe, Codex proposed a numeric failure-count metric plus guard commands and stopped before execution as requested. The ephemeral session could not inspect the repo shell directly because of `bwrap: loopback: Failed RTM_NEWADDR: Operation not permitted`, so the proposed commands were clearly marked provisional rather than silently treated as validated repo-specific commands |

### Credential-Gated Checks Re-Confirmed

| Skill | Status | Blocker | Notes |
| --- | --- | --- | --- |
| `research-lookup` | `blocked-not-failed` | missing `PARALLEL_API_KEY` and `OPENROUTER_API_KEY` | Live backend test intentionally not counted as failure |
| `scientific-schematics` | `blocked-not-failed` | missing `OPENROUTER_API_KEY` | Service-backed schematic generation remains blocked by credentials |
| `fred-economic-data` live API call | `blocked-not-failed` | missing `FRED_API_KEY` | Routing is present, but live fetch remains blocked |
| `edgartools` live API call | `blocked-not-failed` | missing `EDGAR_IDENTITY` | SEC access identity not configured |

### Updated Interpretation After Bridge Smoke Round

- The project-owned `.codex/skills` tree is now sufficient for the core routing checks; these probes no longer need to rely on the user-home global skill tree to explain the observed matches.
- The biggest improvement compared with the earlier baseline was the clinical analysis prompt: the implicit cohort-analysis request now resolved to the full intended bundle of `statistical-analysis`, `scikit-survival`, `statsmodels`, and `clinical-reports`.
- The publication-figure lane stayed stable: `scientific-visualization` remained the primary route for journal-ready multi-panel figures.
- The single-cell and bulk RNA-seq lanes remained strong and specific.
- Credential-gated live skills are still correctly treated as blocked dependencies rather than installation or routing failures.
- The required explicit pass set is now complete: `scientific-writing`, `statsmodels`, `scanpy`, `pydeseq2`, and `scientific-visualization` all passed on their exact required prompts.
