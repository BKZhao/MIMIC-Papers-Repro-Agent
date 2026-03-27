from __future__ import annotations

from dataclasses import dataclass

from ..config import PipelineConfig


@dataclass(frozen=True)
class SkillDefinition:
    name: str
    source: str
    location: str


@dataclass(frozen=True)
class ClinicalAnalysisFamily:
    key: str
    support_level: str
    maturity: str
    execution_mode: str
    description: str
    required_skills: tuple[str, ...] = ()
    primary_outputs: tuple[str, ...] = ()
    scaffold_outputs: tuple[str, ...] = ()
    llm_role: str = ""
    notes: tuple[str, ...] = ()
    preferred_libraries: tuple[str, ...] = ()
    paper_signals: tuple[str, ...] = ()
    supplemental_codex_skills: tuple[str, ...] = ()
    figure_style_hints: tuple[str, ...] = ()
    domain_scope: str = "clinical_mimic"

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "support_level": self.support_level,
            "maturity": self.maturity,
            "execution_mode": self.execution_mode,
            "description": self.description,
            "required_skills": list(self.required_skills),
            "primary_outputs": list(self.primary_outputs),
            "scaffold_outputs": list(self.scaffold_outputs),
            "llm_role": self.llm_role,
            "notes": list(self.notes),
            "preferred_libraries": list(self.preferred_libraries),
            "paper_signals": list(self.paper_signals),
            "supplemental_codex_skills": list(self.supplemental_codex_skills),
            "figure_style_hints": list(self.figure_style_hints),
            "domain_scope": self.domain_scope,
        }


DEFAULT_EXTERNAL_SKILLS: dict[str, str] = {}

DEFAULT_LOCAL_SKILLS: dict[str, str] = {
    "paper_intake_and_contract": "openclaw/skills/paper_intake_and_contract/SKILL.md",
    "mimic_cohort_execution": "openclaw/skills/mimic_cohort_execution/SKILL.md",
    "analysis_dataset_expansion": "openclaw/skills/analysis_dataset_expansion/SKILL.md",
    "longitudinal_trajectory_execution": "openclaw/skills/longitudinal_trajectory_execution/SKILL.md",
    "survival_stats_execution": "openclaw/skills/survival_stats_execution/SKILL.md",
    "result_figure_generation": "openclaw/skills/result_figure_generation/SKILL.md",
    "paper_alignment_verification": "openclaw/skills/paper_alignment_verification/SKILL.md",
}

DEFAULT_AGENT_SKILL_MAP: dict[str, list[str]] = {
    "paper_parser_agent": ["paper_intake_and_contract"],
    "study_design_agent": ["paper_intake_and_contract", "paper_alignment_verification"],
    "cohort_agent": ["mimic_cohort_execution"],
    "feature_agent": ["analysis_dataset_expansion", "longitudinal_trajectory_execution"],
    "stats_agent": ["survival_stats_execution", "longitudinal_trajectory_execution"],
    "figure_agent": ["result_figure_generation"],
    "verify_agent": ["paper_alignment_verification"],
    "report_agent": ["paper_alignment_verification", "result_figure_generation"],
}


CORE_CLINICAL_ANALYSIS_FAMILIES: tuple[ClinicalAnalysisFamily, ...] = (
    ClinicalAnalysisFamily(
        key="descriptive_statistics",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="hybrid_statistical_runner",
        description=(
            "Generate descriptive summaries for continuous and categorical variables, including distribution-aware summary styles "
            "that commonly appear in Table 1 and supplementary methods."
        ),
        required_skills=("analysis_dataset_expansion", "survival_stats_execution"),
        primary_outputs=("descriptive_summary.csv", "descriptive_summary.md"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py"),
        llm_role="identify the paper's summary rules and variable groupings, then hand a deterministic summary spec to local code",
        notes=(
            "Preferred architecture is hybrid: LLM interprets the paper; local code computes the actual statistics.",
        ),
        preferred_libraries=("pandas", "pingouin", "scipy.stats", "statsmodels", "tableone"),
        paper_signals=("table 1", "baseline characteristics", "descriptive statistics", "median (IQR)", "mean ± sd"),
        supplemental_codex_skills=("statsmodels", "scientific-writing"),
    ),
    ClinicalAnalysisFamily(
        key="hypothesis_testing",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="hybrid_statistical_runner",
        description=(
            "Run paper-aligned univariate statistical testing, including parametric, non-parametric, chi-square, and post-hoc comparisons."
        ),
        required_skills=("analysis_dataset_expansion", "survival_stats_execution"),
        primary_outputs=("hypothesis_tests.csv", "hypothesis_tests.md"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py"),
        llm_role="determine which test family the paper intends and compile the comparison plan without inventing p-values",
        notes=(
            "Supports the common clinical-paper pattern of summary table plus per-variable significance testing.",
        ),
        preferred_libraries=("pingouin", "scipy.stats", "statsmodels", "scikit-posthocs"),
        paper_signals=("t test", "anova", "kruskal", "mann-whitney", "chi-square", "post hoc", "dunn"),
        supplemental_codex_skills=("statsmodels",),
    ),
    ClinicalAnalysisFamily(
        key="baseline_table",
        support_level="native_supported",
        maturity="stable",
        execution_mode="deterministic_profile_runner",
        description="Generate paper-like baseline characteristic tables from the profile-driven analysis dataset.",
        required_skills=("analysis_dataset_expansion", "survival_stats_execution"),
        primary_outputs=("baseline_table.csv", "baseline_table.md"),
        llm_role="compile table schema and variable grouping from the paper into a deterministic table plan",
        preferred_libraries=("pandas", "tableone", "statsmodels"),
        paper_signals=("baseline table", "table 1", "characteristics"),
        supplemental_codex_skills=("statsmodels", "scientific-writing"),
    ),
    ClinicalAnalysisFamily(
        key="cohort_flowchart",
        support_level="native_supported",
        maturity="stable",
        execution_mode="deterministic_profile_runner",
        description="Render participant-selection flowcharts from cohort funnel counts for paper-aligned figure panels.",
        required_skills=("mimic_cohort_execution", "result_figure_generation"),
        primary_outputs=("cohort_flowchart.png",),
        llm_role="preserve the paper's participant-selection semantics while leaving counts and layout rendering deterministic",
        preferred_libraries=("matplotlib", "pandas"),
        paper_signals=("flowchart", "participant selection", "patient selection", "study flow"),
        supplemental_codex_skills=("scientific-visualization", "matplotlib"),
        figure_style_hints=("participant_flowchart", "sequential_exclusions", "count_boxes"),
    ),
    ClinicalAnalysisFamily(
        key="kaplan_meier",
        support_level="native_supported",
        maturity="stable",
        execution_mode="deterministic_profile_runner",
        description="Fit Kaplan-Meier survival curves and render publication-style survival figures with at-risk counts.",
        required_skills=("survival_stats_execution", "result_figure_generation"),
        primary_outputs=("km_summary.json", "km.png"),
        llm_role="compile time horizon, grouping logic, and figure annotations into a deterministic plotting spec",
        preferred_libraries=("lifelines", "matplotlib", "seaborn"),
        paper_signals=("kaplan-meier", "log-rank", "survival curve", "number at risk"),
        supplemental_codex_skills=("scientific-visualization", "matplotlib", "seaborn", "scikit-survival"),
        figure_style_hints=("survival_curve", "number_at_risk", "paper_aligned_legend", "multi_panel_if_needed"),
    ),
    ClinicalAnalysisFamily(
        key="cox_regression",
        support_level="native_supported",
        maturity="stable",
        execution_mode="deterministic_profile_runner",
        description="Run Cox proportional hazards models with paper-aligned adjustment sets and table exports.",
        required_skills=("survival_stats_execution",),
        primary_outputs=("cox_models.csv", "cox_models.md"),
        llm_role="compile covariate sets, reference groups, and model ordering into a deterministic execution plan",
        preferred_libraries=("lifelines", "statsmodels", "pandas"),
        paper_signals=("cox", "hazard ratio", "proportional hazards", "multivariable cox"),
        supplemental_codex_skills=("statsmodels", "scikit-survival"),
    ),
    ClinicalAnalysisFamily(
        key="logistic_regression",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="llm_scaffold_then_local_execute",
        description="Reserved core family for binary-outcome logistic regression workflows driven by paper contracts.",
        required_skills=("analysis_dataset_expansion", "survival_stats_execution"),
        primary_outputs=("logistic_models.csv", "logistic_models.md"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py", "figure_spec.json"),
        llm_role="generate an executable regression scaffold and plotting spec, then hand off to local code execution",
        notes=(
            "Recognized in the planning layer and variable parser.",
            "LLM must not fabricate coefficients or performance metrics; local execution remains required.",
        ),
        preferred_libraries=("statsmodels", "scikit-learn", "pandas"),
        paper_signals=("logistic regression", "odds ratio", "binary outcome"),
        supplemental_codex_skills=("statsmodels", "scientific-visualization"),
    ),
    ClinicalAnalysisFamily(
        key="lasso_feature_selection",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="hybrid_statistical_runner",
        description="Run LASSO-style feature screening before multivariable modeling for prediction and risk-score papers.",
        required_skills=("analysis_dataset_expansion", "survival_stats_execution"),
        primary_outputs=("lasso_features.csv", "lasso_trace.png"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py", "figure_spec.json"),
        llm_role="extract the feature-screening intent and compile an executable local LASSO plan",
        notes=(
            "Useful for papers that perform variable shrinking before Cox or logistic regression.",
        ),
        preferred_libraries=("scikit-learn", "statsmodels", "matplotlib"),
        paper_signals=("lasso", "least absolute shrinkage", "feature selection"),
        supplemental_codex_skills=("statsmodels", "matplotlib"),
        figure_style_hints=("regularization_path", "cross_validation_trace"),
    ),
    ClinicalAnalysisFamily(
        key="subgroup_forest",
        support_level="native_supported",
        maturity="stable",
        execution_mode="deterministic_profile_runner",
        description="Run subgroup interaction summaries and render forest plots for supported survival papers.",
        required_skills=("survival_stats_execution", "result_figure_generation"),
        primary_outputs=("subgroup_analysis.csv", "subgroup_forest.png"),
        llm_role="compile subgroup definitions, reference levels, and forest-plot labels into a deterministic run plan",
        preferred_libraries=("matplotlib", "forestplot", "statsmodels", "pandas"),
        paper_signals=("subgroup analysis", "forest plot", "interaction", "heterogeneity"),
        supplemental_codex_skills=("scientific-visualization", "matplotlib", "statsmodels"),
        figure_style_hints=("horizontal_ci_plot", "reference_line_at_1", "subgroup_label_blocks"),
    ),
    ClinicalAnalysisFamily(
        key="restricted_cubic_spline",
        support_level="native_supported",
        maturity="stable",
        execution_mode="deterministic_profile_runner",
        description="Fit restricted cubic spline analyses and export spline summaries and figures.",
        required_skills=("survival_stats_execution", "result_figure_generation"),
        primary_outputs=("rcs_summary.json", "rcs.png"),
        llm_role="compile spline knots, reference values, and figure semantics into a deterministic execution plan",
        preferred_libraries=("statsmodels", "patsy", "matplotlib"),
        paper_signals=("restricted cubic spline", "rcs", "nonlinear relationship", "spline"),
        supplemental_codex_skills=("scientific-visualization", "matplotlib", "statsmodels"),
        figure_style_hints=("smooth_effect_curve", "confidence_band", "reference_line"),
    ),
    ClinicalAnalysisFamily(
        key="trajectory_survival",
        support_level="native_supported",
        maturity="experimental",
        execution_mode="trajectory_python_bridge",
        description="Derive longitudinal trajectory classes from repeated ICU measurements, then run downstream KM and Cox analysis.",
        required_skills=("longitudinal_trajectory_execution", "survival_stats_execution", "result_figure_generation"),
        primary_outputs=(
            "trajectory_assignments.csv",
            "trajectory_table.csv",
            "trajectory.png",
            "km.png",
            "reproduction_report.md",
        ),
        llm_role="compile repeated-measure extraction rules, class semantics, and figure/report framing while leaving the fitting local",
        notes=(
            "Paper-required LGMM is not implemented; the current backend is method-aligned only.",
            "The current runnable profile is the heart-rate trajectory sepsis paper.",
        ),
        preferred_libraries=("pandas", "numpy", "scikit-learn", "lifelines", "matplotlib"),
        paper_signals=("trajectory", "latent growth mixture", "lgmm", "repeated measures", "hourly heart rate"),
        supplemental_codex_skills=("scientific-visualization", "matplotlib", "seaborn"),
        figure_style_hints=("trajectory_panel", "class_palette", "paired_survival_outputs"),
    ),
    ClinicalAnalysisFamily(
        key="missingness_report",
        support_level="native_supported",
        maturity="stable",
        execution_mode="deterministic_profile_runner",
        description="Generate block-aware missingness diagnostics and dataset source-strategy reports.",
        required_skills=("analysis_dataset_expansion", "paper_alignment_verification"),
        primary_outputs=("analysis_missingness.json", "reproduction_report.md"),
        llm_role="compile which missingness diagnostics matter for paper alignment and how to narrate their downstream impact",
        preferred_libraries=("pandas", "numpy"),
        paper_signals=("missing data", "missingness", "complete case", "imputation"),
        supplemental_codex_skills=("scientific-writing",),
    ),
    ClinicalAnalysisFamily(
        key="roc_analysis",
        support_level="native_supported",
        maturity="stable",
        execution_mode="deterministic_profile_runner",
        description="Generate ROC summaries and figures for supported prediction-oriented clinical papers.",
        required_skills=("survival_stats_execution", "result_figure_generation"),
        primary_outputs=("roc_summary.json", "roc.png"),
        llm_role="compile target outcome, score source, and figure annotations into a deterministic ROC analysis plan",
        preferred_libraries=("scikit-learn", "matplotlib"),
        paper_signals=("roc", "auc", "discrimination", "receiver operating characteristic"),
        supplemental_codex_skills=("scientific-visualization", "matplotlib", "plotly"),
        figure_style_hints=("roc_curve", "diagonal_reference_line", "auc_annotation"),
    ),
    ClinicalAnalysisFamily(
        key="machine_learning_prediction",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="hybrid_statistical_runner",
        description="Train and evaluate classical clinical prediction models such as random forest, gradient boosting, SVM, or XGBoost-style workflows.",
        required_skills=("analysis_dataset_expansion", "survival_stats_execution", "result_figure_generation"),
        primary_outputs=("ml_metrics.json", "roc.png", "feature_importance.csv"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py", "figure_spec.json"),
        llm_role="map paper-stated model families and evaluation protocol onto executable local ML scaffolds",
        notes=(
            "Local execution remains authoritative for metrics, thresholds, and plots.",
        ),
        preferred_libraries=("scikit-learn", "imbalanced-learn", "pandas", "matplotlib"),
        paper_signals=("random forest", "xgboost", "svm", "machine learning", "cross-validation"),
        supplemental_codex_skills=("shap", "scientific-visualization", "matplotlib"),
    ),
    ClinicalAnalysisFamily(
        key="shap_explainability",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="hybrid_statistical_runner",
        description="Produce SHAP-based explainability outputs for clinical ML models, including beeswarm, bar, and dependence-style figures.",
        required_skills=("result_figure_generation",),
        primary_outputs=("shap_values.parquet", "shap_summary.png"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py", "figure_spec.json"),
        llm_role="interpret how the paper uses explainability and compile the exact SHAP views needed for local generation",
        preferred_libraries=("shap", "scikit-learn", "matplotlib"),
        paper_signals=("shap", "shapley", "explainability", "feature importance"),
        supplemental_codex_skills=("shap", "scientific-visualization", "matplotlib"),
        figure_style_hints=("beeswarm_or_bar_summary", "feature_rank_ordering"),
    ),
    ClinicalAnalysisFamily(
        key="multiple_imputation",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="llm_scaffold_then_local_execute",
        description="Impute missing values with paper-aligned strategies before downstream modeling.",
        required_skills=("analysis_dataset_expansion", "paper_alignment_verification"),
        primary_outputs=("imputation_summary.json", "analysis_dataset_imputed.csv"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py"),
        llm_role="translate the paper's imputation method into an executable local imputation scaffold",
        notes=(
            "Do not claim paper-identical imputation until the exact method is executed locally.",
        ),
        preferred_libraries=("statsmodels", "scikit-learn", "pandas"),
        paper_signals=("multiple imputation", "mice", "missing values"),
        supplemental_codex_skills=("statsmodels",),
    ),
    ClinicalAnalysisFamily(
        key="propensity_score_matching",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="llm_scaffold_then_local_execute",
        description="Construct matched cohorts for treatment-effect style observational studies.",
        required_skills=("analysis_dataset_expansion", "paper_alignment_verification"),
        primary_outputs=("psm_summary.json", "matched_dataset.csv", "love_plot.png"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py", "figure_spec.json"),
        llm_role="compile treatment definition, matching ratio, caliper, and diagnostics into an executable matching scaffold",
        notes=(
            "LLM prepares the scaffold; balance diagnostics and matched estimates must come from local execution.",
        ),
        preferred_libraries=("statsmodels", "scikit-learn", "pandas", "matplotlib"),
        paper_signals=("propensity score matching", "psm", "matching"),
        supplemental_codex_skills=("scientific-visualization", "matplotlib", "statsmodels"),
        figure_style_hints=("love_plot", "pre_post_balance_diagnostics"),
    ),
    ClinicalAnalysisFamily(
        key="iptw_weighting",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="llm_scaffold_then_local_execute",
        description="Construct inverse-probability weighted analyses for causal-style observational studies.",
        required_skills=("analysis_dataset_expansion", "paper_alignment_verification"),
        primary_outputs=("iptw_summary.json", "weighted_dataset.csv", "balance_table.csv"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py"),
        llm_role="compile weighting estimand, covariate set, and diagnostics into an executable local scaffold",
        preferred_libraries=("statsmodels", "scikit-learn", "pandas"),
        paper_signals=("iptw", "inverse probability weighting", "stabilized weights"),
        supplemental_codex_skills=("statsmodels",),
    ),
    ClinicalAnalysisFamily(
        key="calibration_curve",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="llm_scaffold_then_local_execute",
        description="Evaluate model calibration and export calibration summaries and plots.",
        required_skills=("survival_stats_execution", "result_figure_generation"),
        primary_outputs=("calibration_summary.json", "calibration_curve.png"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py", "figure_spec.json"),
        llm_role="compile calibration metric choices, bootstrap settings, and figure semantics into an executable scaffold",
        preferred_libraries=("scikit-learn", "matplotlib", "statsmodels"),
        paper_signals=("calibration", "calibration curve", "brier score"),
        supplemental_codex_skills=("scientific-visualization", "matplotlib", "statsmodels"),
        figure_style_hints=("reference_diagonal", "calibration_bins_or_smoother"),
    ),
    ClinicalAnalysisFamily(
        key="decision_curve_analysis",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="llm_scaffold_then_local_execute",
        description="Run decision-curve analysis for prediction models and produce net-benefit plots.",
        required_skills=("survival_stats_execution", "result_figure_generation"),
        primary_outputs=("dca_summary.json", "decision_curve.png"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py", "figure_spec.json"),
        llm_role="compile threshold ranges, comparators, and plot semantics into an executable DCA scaffold",
        preferred_libraries=("matplotlib", "scikit-learn", "pandas"),
        paper_signals=("decision curve", "dca", "net benefit"),
        supplemental_codex_skills=("scientific-visualization", "matplotlib"),
        figure_style_hints=("net_benefit_threshold_curve", "treat_all_vs_none_reference"),
    ),
    ClinicalAnalysisFamily(
        key="nomogram_prediction",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="hybrid_statistical_runner",
        description="Build nomogram-style prediction outputs for regression-based clinical prediction studies.",
        required_skills=("survival_stats_execution", "result_figure_generation"),
        primary_outputs=("nomogram_scores.csv", "nomogram.png"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py", "figure_spec.json"),
        llm_role="identify the paper's nomogram inputs and scoring semantics, then compile a reproducible local plotting plan",
        notes=(
            "Some papers may still need an R-backed bridge for publication-identical nomograms.",
        ),
        preferred_libraries=("statsmodels", "matplotlib", "rpy2"),
        paper_signals=("nomogram", "risk score", "prediction model"),
        supplemental_codex_skills=("scientific-visualization", "matplotlib", "statsmodels"),
        figure_style_hints=("point_scale_layout", "linear_predictor_to_probability"),
    ),
    ClinicalAnalysisFamily(
        key="nri_idi_comparison",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="hybrid_statistical_runner",
        description="Compare prediction models with net reclassification improvement and integrated discrimination improvement metrics.",
        required_skills=("paper_alignment_verification",),
        primary_outputs=("nri_idi_summary.json", "model_comparison.md"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py"),
        llm_role="compile the model-comparison design and exact NRI/IDI definitions before local metric execution",
        preferred_libraries=("numpy", "pandas", "scikit-learn"),
        paper_signals=("nri", "idi", "reclassification improvement", "integrated discrimination improvement"),
        supplemental_codex_skills=("statsmodels",),
    ),
    ClinicalAnalysisFamily(
        key="competing_risk",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="llm_scaffold_then_local_execute",
        description="Run competing-risk analyses such as Fine-Gray style models for time-to-event outcomes with competing events.",
        required_skills=("survival_stats_execution", "paper_alignment_verification"),
        primary_outputs=("competing_risk_models.csv", "cif_summary.json", "cif_plot.png"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py", "figure_spec.json"),
        llm_role="compile event definitions, subdistribution-hazard setup, and plotting semantics into an executable scaffold",
        preferred_libraries=("lifelines", "scikit-survival", "matplotlib"),
        paper_signals=("competing risk", "fine-gray", "cumulative incidence"),
        supplemental_codex_skills=("scientific-visualization", "matplotlib", "scikit-survival"),
        figure_style_hints=("cumulative_incidence_curve", "competing_event_legend"),
    ),
    ClinicalAnalysisFamily(
        key="mixed_effects",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="llm_scaffold_then_local_execute",
        description="Fit repeated-measure or hierarchical mixed-effects models for longitudinal clinical studies.",
        required_skills=("analysis_dataset_expansion", "longitudinal_trajectory_execution"),
        primary_outputs=("mixed_effects_models.csv", "random_effects_summary.json"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py"),
        llm_role="compile random-effects structure, correlation assumptions, and covariate formulas into an executable scaffold",
        preferred_libraries=("statsmodels", "pingouin", "pandas"),
        paper_signals=("mixed effects", "hierarchical model", "random effect", "repeated measures"),
        supplemental_codex_skills=("statsmodels",),
    ),
    ClinicalAnalysisFamily(
        key="deep_survival_prediction",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="hybrid_statistical_runner",
        description="Support deep-learning survival workflows such as DeepSurv-style modeling when papers move beyond classical Cox analysis.",
        required_skills=("survival_stats_execution", "result_figure_generation"),
        primary_outputs=("deep_survival_metrics.json", "time_auc.png"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py", "figure_spec.json"),
        llm_role="translate deep-survival method descriptions into executable training and evaluation scaffolds without fabricating outcomes",
        preferred_libraries=("pycox", "scikit-survival", "matplotlib"),
        paper_signals=("deepsurv", "pycox", "deep survival", "time-dependent auc"),
        supplemental_codex_skills=("scientific-visualization", "matplotlib", "scikit-survival"),
        figure_style_hints=("time_dependent_auc_curve", "model_comparison_legend"),
    ),
    ClinicalAnalysisFamily(
        key="bayesian_survival",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="hybrid_statistical_runner",
        description="Support Bayesian survival variants for papers that require probabilistic time-to-event modeling.",
        required_skills=("survival_stats_execution", "paper_alignment_verification"),
        primary_outputs=("bayesian_survival_summary.json", "posterior_survival.png"),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py", "figure_spec.json"),
        llm_role="extract the Bayesian survival formulation from the paper and compile a local execution scaffold",
        preferred_libraries=("pymc", "arviz", "matplotlib"),
        paper_signals=("bayesian survival", "weibull aft", "frailty", "posterior"),
        supplemental_codex_skills=("pymc", "scientific-visualization", "matplotlib"),
        figure_style_hints=("posterior_band", "credible_interval"),
    ),
    ClinicalAnalysisFamily(
        key="distribution_comparison",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="llm_scaffold_then_local_execute",
        description="Render paper-aligned distribution comparison figures such as boxplots, violin plots, strip plots, histograms, and paired comparisons.",
        required_skills=("result_figure_generation",),
        primary_outputs=("distribution_summary.png",),
        scaffold_outputs=("figure_spec.json", "executor_scaffold.py"),
        llm_role="interpret the paper's requested comparison plot semantics and compile a plotting spec for local rendering",
        preferred_libraries=("seaborn", "matplotlib", "plotly"),
        paper_signals=("boxplot", "violin plot", "strip plot", "swarm plot", "histogram", "density plot", "paired plot"),
        supplemental_codex_skills=("scientific-visualization", "seaborn", "matplotlib", "plotly"),
        figure_style_hints=("box_violin_strip_combo", "groupwise_color_palette", "paired_or_unpaired_labels"),
    ),
    ClinicalAnalysisFamily(
        key="heatmap_visualization",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="llm_scaffold_then_local_execute",
        description="Render paper-aligned heatmaps, annotated matrices, and correlation-style matrix figures once the numeric matrix has been computed locally.",
        required_skills=("result_figure_generation",),
        primary_outputs=("heatmap.png",),
        scaffold_outputs=("figure_spec.json", "executor_scaffold.py"),
        llm_role="identify the matrix semantics, color encoding, and annotation rules, then compile a local plotting spec",
        preferred_libraries=("seaborn", "matplotlib", "plotly"),
        paper_signals=("heatmap", "correlation matrix", "clustermap", "annotated matrix"),
        supplemental_codex_skills=("scientific-visualization", "seaborn", "matplotlib", "plotly"),
        figure_style_hints=("annotated_matrix", "colorbar", "cluster_or_sorted_axes"),
    ),
    ClinicalAnalysisFamily(
        key="bioinformatics_extension",
        support_level="llm_compiled_then_execute",
        maturity="planned",
        execution_mode="planning_and_scaffold_only",
        description="Reference family for future omics and bioinformatics papers that fall outside the current MIMIC-first clinical scope.",
        required_skills=("paper_alignment_verification",),
        primary_outputs=("bioinformatics_plan.md",),
        scaffold_outputs=("analysis_spec.json", "executor_scaffold.py"),
        llm_role="route non-clinical papers toward the right external ecosystem without pretending they are already executable in this repo",
        notes=(
            "This is a future-extension lane, not a claim of current end-to-end support.",
        ),
        preferred_libraries=("scanpy", "anndata", "biopython", "pydeseq2", "gseapy", "pysam"),
        paper_signals=("single-cell", "scrna", "gsea", "deseq2", "bam", "genome"),
        supplemental_codex_skills=("scanpy", "anndata", "pydeseq2", "scientific-visualization"),
        domain_scope="bioinformatics_extension",
    ),
)


def build_skill_registry(config: PipelineConfig) -> dict[str, SkillDefinition]:
    registry: dict[str, SkillDefinition] = {}
    external = dict(DEFAULT_EXTERNAL_SKILLS)
    external.update(config.skill_registry.external)
    local = dict(DEFAULT_LOCAL_SKILLS)
    local.update(config.skill_registry.local)
    for name, location in external.items():
        registry[name] = SkillDefinition(name=name, source="external", location=location)
    for name, location in local.items():
        registry[name] = SkillDefinition(name=name, source="local", location=location)
    return registry


def resolve_agent_skills(config: PipelineConfig, agent_name: str) -> list[str]:
    route = config.agent_routes.get(agent_name)
    if route and route.allowed_skills:
        return list(route.allowed_skills)

    merged = dict(DEFAULT_AGENT_SKILL_MAP)
    merged.update(config.skill_registry.agent_skill_map)
    return list(merged.get(agent_name, []))


def list_core_clinical_analysis_families() -> list[ClinicalAnalysisFamily]:
    return list(CORE_CLINICAL_ANALYSIS_FAMILIES)


def get_core_clinical_analysis_family(key: str | None) -> ClinicalAnalysisFamily | None:
    normalized = (key or "").strip().lower()
    if not normalized:
        return None
    for family in CORE_CLINICAL_ANALYSIS_FAMILIES:
        if family.key == normalized:
            return family
    return None
