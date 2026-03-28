# Supplemental Codex Skill Map

This document records which developer-local Codex skills are worth borrowing
from while we continue to expand the medical paper reproduction agent.

It is intentionally **not** part of the default runtime contract.

The runtime contract remains:

- `TaskContract`
- `openclaw/skills/*`
- deterministic SQL / stats / figure execution for supported paths

This document is a developer-facing reference layer for capability expansion.
The machine-readable companion for that layer lives in
[`../../openclaw/skills/codex_skill_bridge.yaml`](../../openclaw/skills/codex_skill_bridge.yaml),
which is now the project-owned bridge between vendored `.codex/skills` content
and the OpenClaw stage model.

## Adoption Rules

When referencing a local Codex skill, keep the following boundaries:

1. Use external or developer-local skills to improve paper interpretation,
   implementation planning, visualization quality, or method design.
2. Do not make `~/.codex/skills` a required runtime dependency of the product; prefer vendored project-owned copies under `.codex/skills`.
3. Do not let a skill fabricate statistics, coefficients, or figure values.
4. If a skill suggests a method, convert that suggestion into local executable
   code or a session-scoped scaffold artifact before calling the task complete.
5. Keep the hybrid contract explicit:
   LLM interprets; local code executes.

## Priority Skill Groups

### 1. Core clinical statistics and table logic

These are the highest-value references for expanding beyond the current
survival-first feature set.

- `.codex/skills/statistical-analysis`
  Use for test-selection logic, assumption checks, effect-size reporting, and
  manuscript-style statistical narration.
- `.codex/skills/statsmodels`
  Use for logistic regression, GLM families, mixed effects, regression tables,
  diagnostics, robust standard errors, and inference-heavy workflows.
- `.codex/skills/seaborn`
  Use for fast statistical plotting patterns before final polishing.
- `.codex/skills/matplotlib`
  Use for final plot control, layout, and export.

Target analysis families this should accelerate:

- `descriptive_statistics`
- `hypothesis_testing`
- `logistic_regression`
- `mixed_effects`
- `baseline_table`

### 2. Publication-ready figure polishing

These references are useful once a family is already numerically executable and
we need paper-grade output quality.

- `.codex/skills/scientific-visualization`
  Use for multi-panel layout, publication styling, color-safe palettes, and
  journal-style export conventions.
- `.codex/skills/clinical-reports`
  Use for report structure, study-flow narration, and timeline-style visual
  expectations, but not for generating study numbers.

Target figure families this should accelerate:

- `kaplan_meier`
- `subgroup_forest`
- `restricted_cubic_spline`
- `roc_analysis`
- `calibration_curve`
- `decision_curve_analysis`
- figure sections inside `reproduction_report.md`

### 3. Clinical prediction and model explainability

These references help us move prediction papers from scaffold-only to real
execution.

- `.codex/skills/pyhealth`
  Use for healthcare prediction task conventions, EHR feature handling, and
  future model-evaluation patterns.
- `.codex/skills/shap`
  Use for SHAP computation strategy and standard explanation plots.
- `.codex/skills/scikit-survival`
  Use for time-to-event ML and competing-risk-adjacent survival modeling.
- `.codex/skills/pymc`
  Use for Bayesian survival and uncertainty-aware prediction extensions.

Target analysis families this should accelerate:

- `machine_learning_prediction`
- `shap_explainability`
- `deep_survival_prediction`
- `competing_risk`
- `bayesian_survival`
- `nri_idi_comparison`

### 4. Clinical protocol and external evidence alignment

These references are not part of the core MIMIC execution lane, but they can
strengthen paper understanding and verification.

- `.codex/skills/clinicaltrials-database`
  Useful when a paper references a trial registry or protocol identifier and we
  need to cross-check design intent.
- `.codex/skills/research-lookup`
  Useful for controlled literature lookup during paper interpretation.
- `.codex/skills/citation-management`
  Useful for bibliography handling when producing richer reports.
- `.codex/skills/pubmed-database`
  Useful for linked-paper verification and citation grounding.

These are best treated as verification-side helpers rather than execution
dependencies.

### 5. Future bioinformatics extension lane

These are valuable, but they belong to the explicit future extension lane rather
than the current MIMIC-first clinical scope.

- `.codex/skills/scanpy`
- `.codex/skills/anndata`
- `.codex/skills/pydeseq2`
- `.codex/skills/biopython`
- `.codex/skills/scikit-bio`
- `.codex/skills/pysam`
- `.codex/skills/gget`
- `.codex/skills/cellxgene-census`
- omics and database lookup skills under `.codex/skills/*-database`

These should map to the `bioinformatics_extension` analysis family and remain
planning-reference only until there is a real execution surface in this repo.

### 6. Developer operations

- `.codex/skills/git-github-update`

This can be useful for maintainer workflows, but it should not leak into the
medical reproduction runtime contract.

## Recommended Adoption Order

The next practical adoption order should be:

1. `statsmodels` + `statistical-analysis`
   To move `logistic_regression`, `descriptive_statistics`, and
   `hypothesis_testing` from scaffold-heavy to runnable.
2. `scientific-visualization` + `matplotlib` + `seaborn`
   To raise figure quality for KM, ROC, forest, calibration, and DCA outputs.
3. `pyhealth` + `shap`
   To support clinical prediction papers and explainability figures.
4. `scikit-survival` + `pymc`
   To expand into competing risk, ML survival, and Bayesian lanes.
5. `scanpy` / omics stack
   Only after the MIMIC clinical core is stable.

## What To Carry Back Into The Repo

When a local skill proves useful, carry it back into the repo in one of these
forms:

- a new `ClinicalAnalysisFamily`
- a deterministic runner implementation
- a better `analysis_spec` / `figure_spec` scaffold
- a project-owned OpenClaw skill under `openclaw/skills`
- a docs update in `clinical-analysis-capability-map.md`

That keeps the official project surface clean while still letting us learn from
the larger Codex skill library.
