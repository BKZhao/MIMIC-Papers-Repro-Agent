# Clinical Analysis Capability Map

This document records how the agent should think about statistical and figure
generation skills in the new hybrid architecture.

The key rule is simple:

- use deterministic local code whenever the execution path is already known
- use LLM reasoning to interpret the paper, fill semantic gaps, and draft
  executable specs when the method is underspecified or not yet natively wired
- never let the LLM fabricate numeric results
- let the LLM drive paper figure intent and artifact semantics first; use rule
  normalization only as a bridge into local executors and skill families

## Architecture Lanes

### `native_supported`

These families should stay deterministic-first.

- `baseline_table`
  Preferred libraries: `pandas`, `tableone`, `statsmodels`
- `kaplan_meier`
  Preferred libraries: `lifelines`, `matplotlib`, `seaborn`
- `cox_regression`
  Preferred libraries: `lifelines`, `statsmodels`
- `subgroup_forest`
  Preferred libraries: `matplotlib`, `forestplot`
- `restricted_cubic_spline`
  Preferred libraries: `statsmodels`, `patsy`, `matplotlib`
- `roc_analysis`
  Preferred libraries: `scikit-learn`, `matplotlib`
- `missingness_report`
  Preferred libraries: `pandas`, `numpy`
- `trajectory_survival`
  Preferred libraries: `pandas`, `numpy`, `scikit-learn`, `lifelines`, `matplotlib`
  Current status: method-aligned experimental backend, not paper-identical LGMM

### `llm_compiled_then_execute`

These families should use the hybrid pattern:

1. LLM reads the paper and drafts an executable analysis spec
2. local code executes the actual statistics and figures
3. report layer marks any fidelity gaps explicitly

Priority clinical families:

- `descriptive_statistics`
  Libraries: `pandas`, `pingouin`, `scipy.stats`, `statsmodels`, `tableone`
- `hypothesis_testing`
  Libraries: `pingouin`, `scipy.stats`, `statsmodels`, `scikit-posthocs`
- `logistic_regression`
  Libraries: `statsmodels`, `scikit-learn`
- `lasso_feature_selection`
  Libraries: `scikit-learn`, `statsmodels`
- `multiple_imputation`
  Libraries: `statsmodels`, `scikit-learn`, `pandas`
- `calibration_curve`
  Libraries: `scikit-learn`, `matplotlib`, `statsmodels`
- `decision_curve_analysis`
  Libraries: `matplotlib`, `pandas`, `scikit-learn`
- `nomogram_prediction`
  Libraries: `statsmodels`, `matplotlib`, `rpy2`
- `nri_idi_comparison`
  Libraries: `numpy`, `pandas`, `scikit-learn`
- `machine_learning_prediction`
  Libraries: `scikit-learn`, `imbalanced-learn`, `matplotlib`
- `shap_explainability`
  Libraries: `shap`, `matplotlib`, `scikit-learn`
- `distribution_comparison`
  Libraries: `seaborn`, `matplotlib`, `plotly`
- `heatmap_visualization`
  Libraries: `seaborn`, `matplotlib`, `plotly`
- `propensity_score_matching`
  Libraries: `statsmodels`, `scikit-learn`, `matplotlib`
- `iptw_weighting`
  Libraries: `statsmodels`, `scikit-learn`, `pandas`
- `competing_risk`
  Libraries: `lifelines`, `scikit-survival`, `matplotlib`
- `mixed_effects`
  Libraries: `statsmodels`, `pingouin`
- `deep_survival_prediction`
  Libraries: `pycox`, `scikit-survival`, `matplotlib`
- `bayesian_survival`
  Libraries: `pymc`, `arviz`, `matplotlib`

### `planning_reference`

These are documented extension lanes and should not be presented as fully
supported clinical execution today.

- `bioinformatics_extension`
  Libraries: `scanpy`, `anndata`, `biopython`, `pydeseq2`, `gseapy`, `pysam`
  Intended use: future omics and bioinformatics papers beyond the current
  MIMIC-first scope

## Default Decision Rule

When a paper requests a method:

1. Check whether it maps to a registered family.
2. If the family is `native_supported`, execute locally.
3. If the family is `llm_compiled_then_execute`, ask the LLM for a structured
   executable spec and then run local code.
4. If the family is only `planning_reference`, return a planning artifact and
   do not claim execution support.

## Paper-Driven Figure Routing

The current figure policy is:

1. LLM paper intake extracts the paper's actual requested figures and tables.
2. The builder preserves those figure labels instead of collapsing them into a
   tiny fixed chart bundle.
3. The analysis-family router normalizes those labels into execution families,
   preferred libraries, supplemental Codex skills, and style hints.
4. Local code still owns numeric execution and final artifact generation.

This keeps the system agentic without letting the LLM hallucinate results.

## High-Priority Figure Skills

The most reusable figure families for the current project are:

- Kaplan-Meier curve with number-at-risk table
- ROC curve
- forest plot
- calibration curve
- SHAP summary figure
- nomogram figure
- decision-curve plot
- love plot for causal balance diagnostics
- clinical distribution figures such as box/violin/strip plots
- annotated heatmaps and correlation matrices

These should remain the first-class visualization targets for OpenClaw and
OpenClaw integration.

## Supplemental Skill Pool

We also maintain a larger project-owned Codex skill pool under `.codex/skills`
outside the official runtime surface. Those skills are useful as reference
implementations and method design aids, but they must still be absorbed back
into deterministic repo code before we claim execution support.

High-value references:

- `.codex/skills/statistical-analysis`
  Best reference for test selection, assumption checks, effect-size reporting,
  and manuscript-ready summaries.
- `.codex/skills/statsmodels`
  Best reference for logistic regression, GLM, mixed effects, robust inference,
  and regression diagnostics.
- `.codex/skills/scientific-visualization`
  Best reference for publication-grade multi-panel figures and journal-style
  export.
- `.codex/skills/pyhealth`
  Best reference for future clinical prediction and healthcare-ML tasks.
- `.codex/skills/shap`
  Best reference for explainability plots and SHAP execution patterns.
- `.codex/skills/scikit-survival`
  Best reference for ML survival and competing-risk-adjacent extensions.
- `.codex/skills/pymc`
  Best reference for Bayesian survival and uncertainty-aware modeling.

For the grouped adoption roadmap, see
[`supplemental-codex-skill-map.md`](supplemental-codex-skill-map.md).
The machine-readable bridge for these supplemental skills now lives in
[`../openclaw/skills/codex_skill_bridge.yaml`](../openclaw/skills/codex_skill_bridge.yaml).
