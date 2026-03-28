from __future__ import annotations

"""Legacy deterministic pipeline compatibility implementation.

This module preserves the original preset-style pipeline so deprecated CLI
commands and bridge adapters can continue to work. New execution work should
prefer the paper/agentic/profile-first stack instead of growing this module.
"""

import csv
import io
import json
import math
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..config import PipelineConfig
from ..contracts import RunSummary, StepResult, StepStatus
from ..paper.contract import build_paper_alignment_contract
from ..runtime import LocalRuntime
from ..analysis.stats import run_stats_analysis


class LegacyPaperReproPipeline:
    def __init__(self, project_root: Path, config: PipelineConfig):
        self.project_root = project_root
        self.config = config
        self.runtime = LocalRuntime(project_root=project_root)

    def run(self, dry_run: bool | None = None) -> RunSummary:
        effective_dry_run = self.config.run.dry_run if dry_run is None else dry_run
        run_id = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
        self.runtime.ensure_layout()
        self.runtime.emit_event("orchestrator", "start", "Pipeline started", {"run_id": run_id, "dry_run": effective_dry_run})

        results: list[StepResult] = []
        for step_name in ("paper_parser", "cohort_agent", "stats_agent", "verify_agent", "report_agent"):
            if not self._is_enabled(step_name):
                results.append(StepResult(step=step_name, status=StepStatus.SKIPPED, message="Step disabled by config"))
                continue

            handler = getattr(self, f"_run_{step_name}")
            step_result = handler(dry_run=effective_dry_run)
            results.append(step_result)
            self.runtime.emit_event(step_name, step_result.status.value, step_result.message, step_result.meta)

            if step_result.status in {StepStatus.BLOCKED, StepStatus.FAILED}:
                self.runtime.emit_event("orchestrator", "stop", "Pipeline stopped due to blocking/failure")
                return RunSummary(run_id=run_id, status=step_result.status, step_results=results)

        self.runtime.emit_event("orchestrator", "done", "Pipeline completed")
        return RunSummary(run_id=run_id, status=StepStatus.SUCCESS, step_results=results)

    def _is_enabled(self, step_name: str) -> bool:
        step_cfg = self.config.agents.get(step_name, {})
        if isinstance(step_cfg, dict):
            return bool(step_cfg.get("enabled", True))
        return True

    def _run_paper_parser(self, dry_run: bool) -> StepResult:
        rel_paper_path = self.config.run.paper_path
        paper_path = self.project_root / rel_paper_path
        if not paper_path.exists():
            return StepResult(
                step="paper_parser",
                status=StepStatus.BLOCKED,
                message=f"Paper file not found: {rel_paper_path}",
            )

        paper_text = paper_path.read_text(encoding="utf-8", errors="ignore")
        title = _extract_title(paper_text) or "unknown"
        contract = build_paper_alignment_contract()
        methods = {
            "paper_title": title,
            "doi": self.config.run.doi,
            "dataset": self.config.run.dataset,
            "inclusion_criteria": [
                "Sepsis-3 patients aged >=18 years",
                "first ICU stay only",
                "TG and glucose available in the baseline lab window",
            ],
            "exclusion_criteria": [
                "ICU stay < 48 hours",
                "multiple ICU admissions due to sepsis",
                "insufficient data (for example, missing triglycerides or fasting blood glucose)",
            ],
            "primary_outcomes": ["in_hospital_mortality", "icu_mortality"],
            "covariates": [
                "age",
                "gender",
                "height_cm",
                "weight_kg",
                "race",
                "insurance",
                "marital_status",
                "white_blood_cell_count",
                "red_blood_cell_count",
                "hemoglobin_count",
                "rdw",
                "albumin",
                "chloride",
                "alanine_aminotransferase",
                "aspartate_aminotransferase",
                "sofa_score",
                "apache_iii_score",
                "saps_ii_score",
                "oasis_score",
                "charlson_score",
                "gcs_score",
                "hypertension",
                "type2_diabetes",
                "heart_failure",
                "myocardial_infarction",
                "malignant_tumor",
                "chronic_renal_failure",
                "acute_renal_failure",
                "stroke",
                "hyperlipidemia",
                "copd",
            ],
            "target_metrics": self.config.targets or contract.get("metric_targets", []),
            "paper_alignment_contract": contract,
            "notes": [
                "Auto-generated methods contract from papers/MIMIC.md plus paper alignment defaults.",
                "Use the paper alignment contract as the primary structured target for cohort, KM, Cox, and RCS diagnostics.",
            ],
        }
        out = self.runtime.write_json("shared/methods.json", methods)
        contract_out = self.runtime.write_json("shared/paper_alignment_contract.json", contract)
        paper_targets_out = self.runtime.write_json(
            "shared/paper_material_targets.json",
            {
                "source_files": list(contract.get("source_files", [])),
                "cohort_targets": dict(contract.get("cohort_targets", {})),
                "baseline_targets": dict(contract.get("baseline_targets", {})),
                "supplement_baseline_targets": dict(contract.get("supplement_baseline_targets", {})),
                "cox_table_targets": list(contract.get("cox_table_targets", [])),
                "km_targets": dict(contract.get("km_targets", {})),
                "rcs_targets": dict(contract.get("rcs_targets", {})),
                "parsed_target_counts": dict(contract.get("parsed_target_counts", {})),
                "notes": list(contract.get("notes", [])),
            },
        )
        return StepResult(
            step="paper_parser",
            status=StepStatus.SUCCESS,
            message="Generated methods contract",
            outputs=[out, contract_out, paper_targets_out],
            meta={"dry_run": dry_run, "paper_path": rel_paper_path},
        )

    def _run_cohort_agent(self, dry_run: bool) -> StepResult:
        _ = self.runtime.read_json("shared/methods.json")
        expected = self.config.quality_gates.expected_cohort_size
        out = "shared/cohort.csv"
        extra_outputs: list[str] = []
        if dry_run:
            row_count = max(expected, 10)
            rows = _make_cohort_rows(row_count)
            out = self.runtime.write_csv(
                out,
                rows=rows,
                fieldnames=[
                    "subject_id",
                    "hadm_id",
                    "stay_id",
                    "age",
                    "sex",
                    "tyg_index",
                    "tyg_quartile",
                    "hospital_mortality",
                    "icu_mortality",
                ],
            )
        else:
            try:
                row_count = _extract_real_cohort_csv(self.project_root, self.project_root / out)
            except RuntimeError as exc:
                return StepResult(
                    step="cohort_agent",
                    status=StepStatus.FAILED,
                    message="Cohort extraction failed",
                    outputs=[],
                    meta={"error": str(exc)},
                )
            for rel_path in ("shared/cohort_funnel.json", "shared/cohort_alignment.json"):
                if (self.project_root / rel_path).exists():
                    extra_outputs.append(rel_path)

        gate_ok, lower, upper = _check_cohort_gate(
            actual=row_count,
            expected=expected,
            tolerance_percent=self.config.quality_gates.cohort_tolerance_percent,
        )
        if not gate_ok:
            meta = {
                "actual": row_count,
                "expected": expected,
                "allowed_range": [lower, upper],
                "quality_gate_passed": False,
                "block_on_cohort_mismatch": self.config.quality_gates.block_on_cohort_mismatch,
            }
            if not self.config.quality_gates.block_on_cohort_mismatch:
                return StepResult(
                    step="cohort_agent",
                    status=StepStatus.SUCCESS,
                    message="Cohort quality gate mismatch noted; continuing with downstream analysis",
                    outputs=[out, *extra_outputs],
                    meta=meta,
                )
            return StepResult(
                step="cohort_agent",
                status=StepStatus.BLOCKED,
                message="Cohort quality gate failed",
                outputs=[out, *extra_outputs],
                meta=meta,
            )

        return StepResult(
            step="cohort_agent",
            status=StepStatus.SUCCESS,
            message="Cohort artifact generated",
            outputs=[out, *extra_outputs],
            meta={
                "actual": row_count,
                "expected": expected,
                "allowed_range": [lower, upper],
                "quality_gate_passed": True,
                "block_on_cohort_mismatch": self.config.quality_gates.block_on_cohort_mismatch,
            },
        )

    def _run_stats_agent(self, dry_run: bool) -> StepResult:
        cohort = self.runtime.read_csv("shared/cohort.csv")
        rows = []
        metric_values: dict[str, float | None] = {}
        prep_outputs: list[str] = []
        if not dry_run:
            try:
                prep_outputs = _extract_real_analysis_dataset(self.project_root)
                stats_run = run_stats_analysis(
                    project_root=self.project_root,
                    cohort_rel="shared/cohort.csv",
                    targets=self.config.targets,
                )
            except Exception as exc:
                return StepResult(
                    step="stats_agent",
                    status=StepStatus.FAILED,
                    message="Stats analysis failed",
                    outputs=[],
                    meta={"error": str(exc)},
                )
            metric_values = stats_run.metrics
 
        for item in self.config.targets:
            metric = str(item.get("metric", "unknown_metric"))
            target = _to_float(item.get("target"), default=0.0)
            if dry_run:
                reproduced = target
                notes = "dry_run placeholder"
            else:
                reproduced = metric_values.get(metric)
                notes = "wide_dataset_stats" if reproduced is not None else "metric_not_implemented"
            rows.append(
                {
                    "metric": metric,
                    "target": f"{target:.6f}",
                    "reproduced": "" if reproduced is None else f"{reproduced:.6f}",
                    "model": "stub_model" if dry_run else "stats_workflow",
                    "notes": notes,
                }
            )

        out = self.runtime.write_csv(
            "shared/results_table.csv",
            rows=rows,
            fieldnames=["metric", "target", "reproduced", "model", "notes"],
        )
        output_paths = [out]
        meta: dict[str, Any] = {"metrics": len(rows), "cohort_n": len(cohort)}
        if not dry_run:
            output_paths = list(dict.fromkeys([*prep_outputs, *stats_run.outputs, out]))
            meta["analysis_mode"] = stats_run.analysis_mode
        return StepResult(
            step="stats_agent",
            status=StepStatus.SUCCESS,
            message="Stats artifact generated",
            outputs=output_paths,
            meta=meta,
        )

    def _run_verify_agent(self, dry_run: bool) -> StepResult:
        rows = self.runtime.read_csv("shared/results_table.csv")
        metric_details = _build_metric_verification_details(rows)
        sections: dict[str, dict[str, Any]] = {
            "metric_alignment": {
                "summary": _summarize_status_rows(metric_details),
                "details": metric_details,
            }
        }

        diagnostics_path = self.project_root / "shared" / "paper_alignment_diagnostics.json"
        if diagnostics_path.exists():
            diagnostics = json.loads(diagnostics_path.read_text(encoding="utf-8"))
            for section_name in _ordered_alignment_sections(diagnostics):
                payload = diagnostics.get(section_name)
                if not isinstance(payload, dict):
                    continue
                details = list(payload.get("rows", []))
                if not details:
                    continue
                mapped_name = f"paper_{section_name}"
                if section_name == "metric_alignment":
                    mapped_name = "paper_metric_alignment"
                sections[mapped_name] = {
                    "summary": dict(payload.get("summary", {})),
                    "details": details,
                }

        summary = _summarize_section_summaries(sections)
        fail_count = int(summary["fail"])
        warn_count = int(summary["warn"])
        score = int(max(0, 100 - fail_count * 20 - warn_count * 5))
        payload = {
            "summary": summary,
            "score": score,
            "details": metric_details,
            "sections": sections,
        }
        out = self.runtime.write_json("shared/deviation_table.json", payload)

        if fail_count > self.config.quality_gates.max_fail_metrics:
            return StepResult(
                step="verify_agent",
                status=StepStatus.BLOCKED,
                message="Verification quality gate failed",
                outputs=[out],
                meta={"fail_count": fail_count, "max_fail_metrics": self.config.quality_gates.max_fail_metrics},
            )
        return StepResult(
            step="verify_agent",
            status=StepStatus.SUCCESS,
            message="Verification artifact generated",
            outputs=[out],
            meta={"fail_count": fail_count, "score": score},
        )

    def _run_report_agent(self, dry_run: bool) -> StepResult:
        methods = self.runtime.read_json("shared/methods.json")
        deviation = self.runtime.read_json("shared/deviation_table.json")
        results = self.runtime.read_csv("shared/results_table.csv")
        stats_summary = _read_json_if_exists(self.project_root / "shared" / "stats_summary.json")
        diagnostics = _read_json_if_exists(self.project_root / "shared" / "paper_alignment_diagnostics.json")

        status = "reproduced"
        if deviation["summary"]["fail"] > 0:
            status = "partially_reproduced"
        if deviation["summary"]["fail"] > self.config.quality_gates.max_fail_metrics:
            status = "not_reproduced"

        lines = [
            "# Reproduction Report",
            "",
            f"- Run mode: {'dry-run' if dry_run else 'production'}",
            f"- Paper: {methods.get('paper_title', 'unknown')}",
            f"- DOI: {methods.get('doi', 'unknown')}",
            f"- Dataset: {methods.get('dataset', 'unknown')}",
            f"- Reproducibility status: **{status}**",
            "",
            "## Verification Summary",
            f"- Total metrics: {deviation['summary']['total']}",
            f"- Pass: {deviation['summary']['pass']}",
            f"- Warn: {deviation['summary']['warn']}",
            f"- Fail: {deviation['summary']['fail']}",
            f"- Score: {deviation['score']}",
            "",
            "## Alignment Diagnostics",
        ]
        for section_name in _ordered_report_alignment_sections(dict(deviation.get("sections", {}))):
            section = dict(deviation.get("sections", {})).get(section_name)
            if not section:
                continue
            section_summary = dict(section.get("summary", {}))
            lines.extend(
                [
                    f"### {section_name.replace('_', ' ').title()}",
                    f"- Pass: {section_summary.get('pass', 0)}",
                    f"- Warn: {section_summary.get('warn', 0)}",
                    f"- Fail: {section_summary.get('fail', 0)}",
                ]
            )
            for item in _top_misaligned_rows(list(section.get("details", []))):
                lines.append(
                    f"- {item.get('metric')}: target={_format_report_value(item.get('target'))}, "
                    f"actual={_format_report_value(item.get('actual'))}, status={item.get('status')}"
                )

        if diagnostics:
            lines.extend(
                [
                    "",
                    "## Diagnostic Summary",
                    f"- Baseline mean percent deviation: {_format_report_value(diagnostics.get('summary', {}).get('baseline_mean_percent_deviation'))}",
                    f"- Supplement baseline mean percent deviation: {_format_report_value(diagnostics.get('summary', {}).get('supplement_baseline_mean_percent_deviation'))}",
                    f"- Cox table mean percent deviation: {_format_report_value(diagnostics.get('summary', {}).get('cox_table_mean_percent_deviation'))}",
                    f"- Metric mean percent deviation: {_format_report_value(diagnostics.get('summary', {}).get('metric_mean_percent_deviation'))}",
                ]
            )

        if stats_summary:
            lines.extend(
                [
                    "",
                    "## Stats Summary",
                    f"- Analysis mode: {stats_summary.get('analysis_mode', 'unknown')}",
                    f"- Cohort N used in stats: {stats_summary.get('cohort_n', 'unknown')}",
                ]
            )

        lines.extend(
            [
                "",
            "## Metric Table",
            "| metric | target | reproduced |",
            "|---|---:|---:|",
            ]
        )
        for row in results:
            lines.append(f"| {row['metric']} | {row['target']} | {row['reproduced']} |")

        report_rel = "results/reproduction_report.md"
        (self.project_root / report_rel).write_text("\n".join(lines) + "\n", encoding="utf-8")
        return StepResult(
            step="report_agent",
            status=StepStatus.SUCCESS,
            message="Final report generated",
            outputs=[report_rel],
            meta={"status": status},
        )


def _extract_title(paper_text: str) -> str:
    for line in paper_text.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        if candidate.startswith("#"):
            return candidate.lstrip("#").strip()
    return paper_text.splitlines()[0].strip() if paper_text.splitlines() else ""


def _make_cohort_rows(n: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for i in range(n):
        q = (i % 4) + 1
        rows.append(
            {
                "subject_id": 100000 + i,
                "hadm_id": 200000 + i,
                "stay_id": 300000 + i,
                "age": 65 + (i % 20),
                "sex": "M" if i % 2 == 0 else "F",
                "tyg_index": round(8.0 + (i % 12) * 0.15, 4),
                "tyg_quartile": f"Q{q}",
                "hospital_mortality": 1 if i % 7 == 0 else 0,
                "icu_mortality": 1 if i % 9 == 0 else 0,
            }
        )
    return rows


def _extract_real_cohort_csv(project_root: Path, output_path: Path) -> int:
    script_path = project_root / "scripts" / "profiles" / "build_profile_cohort.py"
    cmd = [
        "python3",
        str(script_path),
        "--project-root",
        str(project_root),
        "--profile",
        "mimic_tyg_sepsis",
        "--output",
        str(output_path.relative_to(project_root)),
        "--funnel-output",
        "shared/cohort_funnel.json",
        "--alignment-output",
        "shared/cohort_alignment.json",
    ]
    env = os.environ.copy()
    completed = subprocess.run(cmd, env=env, text=True, capture_output=True)
    if completed.returncode != 0:
        error_text = completed.stderr.strip() or completed.stdout.strip() or "cohort script failed"
        raise RuntimeError(error_text)

    if not output_path.exists():
        raise RuntimeError(f"Cohort output not found after script run: {output_path}")
    return len(_read_csv_rows(output_path))


def _extract_real_analysis_dataset(project_root: Path) -> list[str]:
    script_path = project_root / "scripts" / "profiles" / "build_profile_analysis_dataset.py"
    output_rel = "shared/analysis_dataset.csv"
    missingness_rel = "shared/analysis_missingness.json"
    cmd = [
        "python3",
        str(script_path),
        "--project-root",
        str(project_root),
        "--profile",
        "mimic_tyg_sepsis",
        "--output",
        output_rel,
        "--missingness-output",
        missingness_rel,
    ]
    env = os.environ.copy()
    completed = subprocess.run(cmd, env=env, text=True, capture_output=True)
    if completed.returncode != 0:
        error_text = completed.stderr.strip() or completed.stdout.strip() or "analysis dataset script failed"
        raise RuntimeError(error_text)

    for rel in (output_rel, missingness_rel):
        if not (project_root / rel).exists():
            raise RuntimeError(f"Expected analysis artifact missing after script run: {rel}")
    return [output_rel, missingness_rel]


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _check_cohort_gate(actual: int, expected: int, tolerance_percent: float) -> tuple[bool, int, int]:
    margin = int(round(expected * (tolerance_percent / 100.0)))
    lower = expected - margin
    upper = expected + margin
    return (lower <= actual <= upper), lower, upper


def _ordered_alignment_sections(diagnostics: dict[str, Any]) -> list[str]:
    preferred = [
        "cohort_alignment",
        "baseline_alignment",
        "supplement_baseline_alignment",
        "cox_table_alignment",
        "metric_alignment",
        "km_alignment",
        "rcs_alignment",
    ]
    available = [
        name
        for name, payload in diagnostics.items()
        if name.endswith("_alignment") and isinstance(payload, dict)
    ]
    ordered = [name for name in preferred if name in available]
    ordered.extend(sorted(name for name in available if name not in ordered))
    return ordered


def _ordered_report_alignment_sections(sections: dict[str, Any]) -> list[str]:
    preferred = [
        "paper_cohort_alignment",
        "paper_baseline_alignment",
        "paper_supplement_baseline_alignment",
        "paper_cox_table_alignment",
        "paper_km_alignment",
        "paper_rcs_alignment",
        "paper_metric_alignment",
    ]
    available = [
        name
        for name, payload in sections.items()
        if name.startswith("paper_") and name.endswith("_alignment") and isinstance(payload, dict)
    ]
    ordered = [name for name in preferred if name in available]
    ordered.extend(sorted(name for name in available if name not in ordered))
    return ordered


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        num = float(text)
        if math.isnan(num) or math.isinf(num):
            return None
        return num
    except (TypeError, ValueError):
        return None


def _risk_ratio_q4_vs_q1(rows: list[dict[str, str]], outcome_key: str) -> float | None:
    q_stats: dict[str, dict[str, float]] = {
        "Q1": {"n": 0.0, "events": 0.0},
        "Q2": {"n": 0.0, "events": 0.0},
        "Q3": {"n": 0.0, "events": 0.0},
        "Q4": {"n": 0.0, "events": 0.0},
    }
    for row in rows:
        q = _normalize_quartile(row.get("tyg_quartile", ""))
        if q not in q_stats:
            continue
        q_stats[q]["n"] += 1
        q_stats[q]["events"] += _to_float(row.get(outcome_key), default=0.0)

    n1 = q_stats["Q1"]["n"]
    n4 = q_stats["Q4"]["n"]
    if n1 == 0 or n4 == 0:
        return None
    r1 = q_stats["Q1"]["events"] / n1
    r4 = q_stats["Q4"]["events"] / n4
    if r1 <= 0:
        return None
    return r4 / r1


def _normalize_quartile(value: Any) -> str:
    text = str(value).strip().upper()
    if text in {"1", "2", "3", "4"}:
        return f"Q{text}"
    return text


def _percent_deviation(target: float, actual: float) -> float:
    if target == 0:
        return 0.0 if actual == 0 else 100.0
    return abs(actual - target) / abs(target) * 100.0


def _grade_deviation(deviation_percent: float) -> str:
    if deviation_percent <= 5:
        return "pass"
    if deviation_percent <= 10:
        return "warn"
    return "fail"


def _build_metric_verification_details(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for row in rows:
        metric = row["metric"]
        target = _to_float(row["target"], default=0.0)
        reproduced = _to_optional_float(row.get("reproduced"))
        if reproduced is None:
            details.append(
                {
                    "metric": metric,
                    "target": target,
                    "actual": None,
                    "deviation_percent": None,
                    "status": "missing",
                }
            )
            continue
        deviation = _percent_deviation(target, reproduced)
        details.append(
            {
                "metric": metric,
                "target": target,
                "actual": reproduced,
                "deviation_percent": round(deviation, 4),
                "status": _grade_deviation(deviation),
            }
        )
    return details


def _summarize_status_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "total": len(rows),
        "pass": sum(1 for row in rows if row.get("status") == "pass"),
        "warn": sum(1 for row in rows if row.get("status") == "warn"),
        "fail": sum(1 for row in rows if row.get("status") == "fail"),
        "missing": sum(1 for row in rows if row.get("status") == "missing"),
    }


def _summarize_section_summaries(sections: dict[str, dict[str, Any]]) -> dict[str, int]:
    summary = {"total": 0, "pass": 0, "warn": 0, "fail": 0, "missing": 0}
    for payload in sections.values():
        section_summary = dict(payload.get("summary", {}))
        summary["total"] += int(section_summary.get("total", 0))
        summary["pass"] += int(section_summary.get("pass", 0))
        summary["warn"] += int(section_summary.get("warn", 0))
        summary["fail"] += int(section_summary.get("fail", 0))
        summary["missing"] += int(section_summary.get("missing", 0))
    return summary


def _read_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _top_misaligned_rows(rows: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    filtered = [row for row in rows if row.get("status") in {"fail", "warn", "missing"}]
    filtered.sort(
        key=lambda row: (
            {"fail": 0, "warn": 1, "missing": 2}.get(str(row.get("status")), 3),
            -(float(row.get("deviation_percent")) if row.get("deviation_percent") is not None else -1.0),
        )
    )
    return filtered[:limit]


def _format_report_value(value: Any) -> str:
    num = _to_optional_float(value)
    if num is None:
        return "NA"
    if abs(num) >= 1000 or abs(num - round(num)) < 1e-9:
        return f"{num:.0f}"
    return f"{num:.4f}".rstrip("0").rstrip(".")


# Backward-compatible alias for imports that still refer to the historical name.
PaperReproPipeline = LegacyPaperReproPipeline
