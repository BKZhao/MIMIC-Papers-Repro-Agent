from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET
from zipfile import ZipFile


TABLE1_BASELINE_MAP: tuple[tuple[str, str, str, str], ...] = (
    ("年龄", "age", "continuous_mean", "years"),
    ("体重", "weight_kg", "continuous_mean", "kg"),
    ("身高", "height_cm", "continuous_mean", "cm"),
    ("男性", "gender_male_pct", "categorical_pct", "%"),
    ("wbc", "white_blood_cell_count", "continuous_mean", "10^9/L"),
    ("白蛋白", "albumin", "continuous_mean", "g/L"),
    ("血糖", "blood_glucose", "continuous_mean", "mg/dL"),
    ("hba1c", "hba1c", "continuous_mean", "%"),
    ("甘油三酯", "triglycerides", "continuous_mean", "mg/dL"),
    ("总胆固醇", "total_cholesterol", "continuous_mean", "mg/dL"),
    ("2型糖尿病", "type2_diabetes_pct", "categorical_pct", "%"),
    ("高血压", "hypertension_pct", "categorical_pct", "%"),
    ("心力衰竭", "heart_failure_pct", "categorical_pct", "%"),
    ("sofa", "sofa_score", "continuous_mean", "score"),
    ("apsiii", "apache_iii_score", "continuous_mean", "score"),
    ("sapsii", "saps_ii_score", "continuous_mean", "score"),
)

SUPPLEMENT_CONTINUOUS_MAP: tuple[tuple[str, str, str, str], ...] = (
    ("hemoglobin", "hemoglobin_count", "continuous_mean", "g/L"),
    ("rdw", "rdw", "continuous_mean", "%"),
    ("hematocrit", "hematocrit", "continuous_mean", "%"),
    ("globulin", "globulin", "continuous_mean", "g/L"),
    ("totalprotein", "total_protein", "continuous_mean", "g/L"),
    ("sodium", "sodium", "continuous_mean", "mmol/L"),
    ("potassium", "potassium", "continuous_mean", "mmol/L"),
    ("totalcalcium", "calcium", "continuous_mean", "mmol/L"),
    ("chlorine", "chloride", "continuous_mean", "mmol/L"),
    ("aniongap", "anion_gap", "continuous_mean", "mmol/L"),
    ("ph", "blood_ph", "continuous_mean", ""),
    ("pco2", "arterial_pco2", "continuous_mean", "mmHg"),
    ("po2", "arterial_po2", "continuous_mean", "mmHg"),
    ("lactate", "lactate", "continuous_mean", "mmol/L"),
    ("totalco2", "total_carbon_dioxide", "continuous_mean", "mmol/L"),
    ("fibrinogen", "fibrinogen", "continuous_mean", "mg/dL"),
    ("ptt", "partial_thromboplastin_time", "continuous_mean", "s"),
    ("inr", "international_normalized_ratio", "continuous_mean", ""),
    ("ddimer", "d_dimer", "continuous_mean", "ug/mL"),
    ("hdlc", "high_density_lipoprotein", "continuous_mean", "mg/dL"),
    ("ldlc", "low_density_lipoprotein", "continuous_mean", "mg/dL"),
    ("totalbilirubin", "total_bilirubin", "continuous_mean", "umol/L"),
    ("directbilirubin", "direct_bilirubin", "continuous_mean", "umol/L"),
    ("indirectbilirubin", "indirect_bilirubin", "continuous_mean", "umol/L"),
    ("alt", "alanine_aminotransferase", "continuous_mean", "U/L"),
    ("ast", "aspartate_aminotransferase", "continuous_mean", "U/L"),
    ("bun", "urea_nitrogen", "continuous_mean", "mmol/L"),
    ("creatinine", "creatinine", "continuous_mean", "g/L"),
    ("troponint", "troponin_t", "continuous_mean", "ng/mL"),
    ("ntprobnp", "ntprobnp", "continuous_mean", "pg/mL"),
    ("urineglucose", "urinary_sugar", "continuous_mean", "mmol/L"),
    ("urinealbumin", "urinary_albumin", "continuous_mean", "mg/dL"),
    ("oasisscore", "oasis_score", "continuous_mean", "score"),
    ("gcsscore", "gcs_score", "continuous_mean", "score"),
    ("charlsonscore", "charlson_score", "continuous_mean", "score"),
    ("sirsscore", "sirs_score", "continuous_mean", "score"),
)

SUPPLEMENT_BINARY_MAP: tuple[tuple[str, str], ...] = (
    ("myocardialinfarction", "myocardial_infarction_pct"),
    ("malignanttumor", "malignant_tumor_pct"),
    ("ckd", "chronic_renal_failure_pct"),
    ("arf", "acute_renal_failure_pct"),
    ("cirrhosis", "cirrhosis_pct"),
    ("hepatitis", "hepatitis_pct"),
    ("tb", "tuberculosis_pct"),
    ("pneumonia", "pneumonia_pct"),
    ("stroke", "stroke_pct"),
    ("hyperlipidemia", "hyperlipidemia_pct"),
    ("copd", "copd_pct"),
    ("crrt", "continuous_renal_replacement_therapy_pct"),
)

TABLE2_MODEL_MAP: dict[int, tuple[str, str]] = {
    1: ("cox_m1_quartile_unadjusted", "cox_m1_continuous_unadjusted"),
    2: ("cox_m2_quartile_adjusted", "cox_m2_continuous_adjusted"),
    3: ("cox_m3_quartile_adjusted", "cox_m3_continuous_adjusted"),
}


def collect_paper_materials(paper_path: Path) -> dict[str, str]:
    materials: dict[str, str] = {}
    if paper_path.exists():
        suffix = paper_path.suffix.lower()
        if suffix == ".pdf":
            materials[paper_path.name] = extract_pdf_text(paper_path)
        else:
            materials[paper_path.name] = paper_path.read_text(encoding="utf-8", errors="ignore")

    for candidate in _companion_material_paths(paper_path):
        if not candidate.exists() or candidate == paper_path:
            continue
        if candidate.suffix.lower() == ".md":
            materials[candidate.name] = candidate.read_text(encoding="utf-8", errors="ignore")
        elif candidate.suffix.lower() == ".docx":
            materials[candidate.name] = extract_docx_text(candidate)
    return materials


def _companion_material_paths(paper_path: Path) -> list[Path]:
    parent = paper_path.parent
    candidates: list[Path] = []
    normalized_stem = paper_path.stem.strip().lower()

    # Prefer same-stem companions to avoid cross-paper contamination when multiple papers share one directory.
    for suffix in (".md", ".docx"):
        candidate = parent / f"{paper_path.stem}{suffix}"
        if candidate != paper_path:
            candidates.append(candidate)

    # Generic sidecars only attach to the legacy single-paper layout `paper.*`.
    if normalized_stem == "paper":
        for sibling_name in ("table.md", "si.docx"):
            candidate = parent / sibling_name
            if candidate != paper_path:
                candidates.append(candidate)

    deduped: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


def extract_pdf_text(path: Path) -> str:
    extractors = (
        _extract_pdf_text_with_pymupdf,
        _extract_pdf_text_with_pdfplumber,
        _extract_pdf_text_with_pdftotext,
        _extract_pdf_text_with_ghostscript,
    )
    errors: list[str] = []
    for extractor in extractors:
        extractor_name = getattr(extractor, "__name__", extractor.__class__.__name__)
        try:
            text = extractor(path)
        except Exception as exc:
            errors.append(f"{extractor_name}: {exc}")
            continue
        cleaned = _normalize_pdf_text(text)
        if cleaned:
            return cleaned
        errors.append(f"{extractor_name}: empty text")
    joined = "; ".join(errors) if errors else "no available extractor"
    raise RuntimeError(f"Unable to extract text from PDF {path}: {joined}")


def _extract_pdf_text_with_pymupdf(path: Path) -> str:
    import fitz  # type: ignore

    fragments: list[str] = []
    with fitz.open(path) as document:
        for page in document:
            text = page.get_text("text")
            if text:
                fragments.append(text)
    return "\n".join(fragments)


def _extract_pdf_text_with_pdfplumber(path: Path) -> str:
    import pdfplumber  # type: ignore

    fragments: list[str] = []
    with pdfplumber.open(path) as document:
        for page in document.pages:
            text = page.extract_text() or ""
            if text:
                fragments.append(text)
    return "\n".join(fragments)


def _extract_pdf_text_with_pdftotext(path: Path) -> str:
    completed = subprocess.run(
        ["pdftotext", "-layout", str(path), "-"],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or "pdftotext extraction failed")
    return completed.stdout


def _extract_pdf_text_with_ghostscript(path: Path) -> str:
    completed = subprocess.run(
        [
            "gs",
            "-q",
            "-dNOPAUSE",
            "-dBATCH",
            "-sDEVICE=txtwrite",
            "-sOutputFile=-",
            str(path),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or "ghostscript extraction failed")
    return completed.stdout


def _normalize_pdf_text(text: str) -> str:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.replace("\x0c", " ").strip()
        line = re.sub(r"\s+", " ", line)
        if line:
            lines.append(line)
    return "\n".join(lines).strip()


def extract_docx_text(path: Path) -> str:
    with ZipFile(path) as archive:
        xml = archive.read("word/document.xml")
    root = ET.fromstring(xml)
    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    fragments: list[str] = []
    for node in root.findall(".//w:t", namespace):
        text = node.text or ""
        if text:
            fragments.append(text)
    return " ".join(fragments)


def extract_docx_tables(path: Path) -> list[list[list[str]]]:
    with ZipFile(path) as archive:
        xml = archive.read("word/document.xml")
    root = ET.fromstring(xml)
    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    tables: list[list[list[str]]] = []
    for tbl in root.findall(".//w:tbl", namespace):
        rows: list[list[str]] = []
        for tr in tbl.findall("./w:tr", namespace):
            cells: list[str] = []
            for tc in tr.findall("./w:tc", namespace):
                fragments = [(node.text or "") for node in tc.findall(".//w:t", namespace)]
                cell_text = " ".join(text for text in fragments if text).strip()
                cells.append(cell_text)
            rows.append(cells)
        tables.append(rows)
    return tables


def parse_structured_paper_targets(paper_path: Path) -> dict[str, Any]:
    targets: dict[str, Any] = {
        "source_files": [],
        "cohort_targets": {},
        "baseline_targets": {},
        "supplement_baseline_targets": {},
        "cox_table_targets": [],
        "km_targets": {},
        "rcs_targets": {},
        "parsed_target_counts": {},
        "notes": [],
    }
    parent = paper_path.parent

    table_path = parent / "table.md"
    if table_path.exists():
        targets["source_files"].append("papers/table.md")
        table_targets = _parse_table_markdown_targets(table_path.read_text(encoding="utf-8", errors="ignore"))
        _merge_target_payload(targets, table_targets)

    supplement_path = parent / "si.docx"
    if supplement_path.exists():
        targets["source_files"].append("papers/si.docx")
        supplement_targets = _parse_supplement_docx_targets(supplement_path)
        _merge_target_payload(targets, supplement_targets)

    targets["parsed_target_counts"] = {
        "baseline_targets": len(targets.get("baseline_targets", {})),
        "supplement_baseline_targets": len(targets.get("supplement_baseline_targets", {})),
        "cox_table_targets": len(targets.get("cox_table_targets", [])),
        "km_targets": len(targets.get("km_targets", {})),
        "rcs_targets": len(targets.get("rcs_targets", {})),
    }
    return targets


def _merge_target_payload(base: dict[str, Any], update: dict[str, Any]) -> None:
    for key, value in update.items():
        if key in {"baseline_targets", "supplement_baseline_targets", "cohort_targets", "km_targets", "rcs_targets"}:
            base.setdefault(key, {})
            base[key].update(value)
        elif key == "cox_table_targets":
            base.setdefault(key, [])
            base[key].extend(value)
        elif key == "notes":
            base.setdefault(key, [])
            base[key].extend(value)


def _parse_table_markdown_targets(text: str) -> dict[str, Any]:
    lines = text.splitlines()
    current_heading = ""
    table1_lines: list[str] = []
    table2_lines: list[str] = []
    for raw_line in lines:
        line = raw_line.strip()
        if line.startswith("### "):
            current_heading = line
            continue
        if not line.startswith("|"):
            continue
        if "Table 1" in current_heading:
            table1_lines.append(line)
        elif "Table 2" in current_heading:
            table2_lines.append(line)

    baseline_targets: dict[str, dict[str, Any]] = {}
    cohort_targets: dict[str, Any] = {}
    cox_table_targets: list[dict[str, Any]] = []
    km_targets: dict[str, float] = {}
    rcs_targets: dict[str, dict[str, float]] = {}

    if table1_lines:
        header = _split_markdown_row(table1_lines[0])
        cohort_targets.update(_parse_table1_header_targets(header))
        for raw_line in table1_lines[2:]:
            row = _split_markdown_row(raw_line)
            if len(row) < 7:
                continue
            label = _strip_markdown_formatting(row[0])
            if not label or label.startswith("**"):
                continue
            matched = _lookup_label_mapping(label, TABLE1_BASELINE_MAP)
            if matched is None:
                continue
            metric, kind, unit = matched
            target_value = _extract_primary_numeric(row[1], percent=(kind == "categorical_pct"))
            if target_value is None:
                continue
            baseline_targets[metric] = {
                "kind": kind,
                "target": target_value,
                "unit": unit,
                "source_file": "papers/table.md",
                "source_label": label,
                "source_section": "Table 1",
            }

    if table2_lines:
        endpoint = ""
        for raw_line in table2_lines[2:]:
            row = _split_markdown_row(raw_line)
            if len(row) < 4:
                continue
            label = _strip_markdown_formatting(row[0])
            if "院内死亡" in label:
                endpoint = "hospital"
                continue
            if "ICU 死亡" in label:
                endpoint = "icu"
                continue
            if label.startswith("Q1"):
                continue
            row_targets = _parse_table2_row(endpoint=endpoint, label=label, cells=row[1:4])
            cox_table_targets.extend(row_targets)

    km_match_hospital = re.search(r"Fig 2A.*?Log-rank P\s*=\s*\**([<>]?[0-9.]+)", text, flags=re.IGNORECASE | re.DOTALL)
    km_match_icu = re.search(r"Fig 2B.*?Log-rank P\s*=\s*\**([<>]?[0-9.]+)", text, flags=re.IGNORECASE | re.DOTALL)
    if km_match_hospital:
        parsed = _parse_p_value(km_match_hospital.group(1))
        if parsed is not None:
            km_targets["hospital_logrank_p_value"] = parsed
    if km_match_icu:
        parsed = _parse_p_value(km_match_icu.group(1))
        if parsed is not None:
            km_targets["icu_logrank_p_value"] = parsed

    for key, panel in (
        ("hospital_unadjusted", "Fig 3a"),
        ("hospital_adjusted", "Fig 3b"),
        ("icu_unadjusted", "Fig 3c"),
        ("icu_adjusted", "Fig 3d"),
    ):
        match = re.search(
            rf"{panel}.*?整体P\s*=?\s*([<>]?[0-9.]+).*?非线性P\s*=?\s*([<>]?[0-9.]+)(?:.*?拐点\s*=\s*([0-9.]+))?",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not match:
            continue
        payload: dict[str, float] = {}
        overall = _parse_p_value(match.group(1))
        nonlinearity = _parse_p_value(match.group(2))
        inflection = _to_float(match.group(3))
        if overall is not None:
            payload["overall_p_value"] = overall
        if nonlinearity is not None:
            payload["nonlinearity_p_value"] = nonlinearity
        if inflection is not None:
            payload["inflection"] = inflection
        if payload:
            rcs_targets[key] = payload

    return {
        "cohort_targets": cohort_targets,
        "baseline_targets": baseline_targets,
        "cox_table_targets": cox_table_targets,
        "km_targets": km_targets,
        "rcs_targets": rcs_targets,
        "notes": [
            "Parsed structured targets from papers/table.md.",
        ],
    }


def _parse_supplement_docx_targets(path: Path) -> dict[str, Any]:
    tables = extract_docx_tables(path)
    if not tables:
        return {}
    rows = tables[0]
    supplement_targets: dict[str, dict[str, Any]] = {}
    pending_binary_metric = ""
    for row in rows:
        if len(row) < 2:
            continue
        label = row[0].strip()
        overall = row[1].strip() if len(row) > 1 else ""
        if not label:
            continue
        normalized_label = _normalize_label(label)

        if normalized_label in {"laboratoryparameters", "comorbidity", "scoringsystems"}:
            pending_binary_metric = ""
            continue

        matched_continuous = _lookup_label_mapping(label, SUPPLEMENT_CONTINUOUS_MAP)
        if matched_continuous and overall:
            metric, kind, unit = matched_continuous
            target_value = _extract_primary_numeric(overall)
            if target_value is not None:
                supplement_targets[metric] = {
                    "kind": kind,
                    "target": target_value,
                    "unit": unit,
                    "source_file": "papers/si.docx",
                    "source_label": label,
                    "source_section": "Supplementary Table 1",
                }
            pending_binary_metric = ""
            continue

        matched_binary = _lookup_binary_metric(label)
        if matched_binary:
            pending_binary_metric = matched_binary
            continue

        if normalized_label == "yes" and pending_binary_metric and overall:
            target_value = _extract_percent_from_count_cell(overall)
            if target_value is not None:
                supplement_targets[pending_binary_metric] = {
                    "kind": "categorical_pct",
                    "target": target_value,
                    "unit": "%",
                    "source_file": "papers/si.docx",
                    "source_label": pending_binary_metric,
                    "source_section": "Supplementary Table 1",
                }
            pending_binary_metric = ""

    return {
        "supplement_baseline_targets": supplement_targets,
        "notes": [
            "Parsed supplementary baseline targets from papers/si.docx.",
        ],
    }


def _parse_table1_header_targets(header_cells: list[str]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if len(header_cells) < 6:
        return payload
    overall_match = re.search(r"n\s*=\s*([\d,\s]+)", header_cells[1], flags=re.IGNORECASE)
    if overall_match:
        payload["final_n"] = int(re.sub(r"[^\d]", "", overall_match.group(1)))

    quartile_counts: dict[str, int] = {}
    quartile_bounds: dict[str, float] = {}
    for quartile, cell in zip(("Q1", "Q2", "Q3", "Q4"), header_cells[2:6], strict=False):
        count_match = re.search(r"n\s*=\s*([\d,\s]+)", cell, flags=re.IGNORECASE)
        if count_match:
            quartile_counts[quartile] = int(re.sub(r"[^\d]", "", count_match.group(1)))
        values = [float(match) for match in re.findall(r"(\d+\.\d+)", cell)]
        if quartile == "Q1" and values:
            quartile_bounds["q1_max"] = values[0]
        elif quartile == "Q2" and len(values) >= 2:
            quartile_bounds["q2_max"] = values[1]
        elif quartile == "Q3" and len(values) >= 2:
            quartile_bounds["q3_max"] = values[1]
    if quartile_counts:
        payload["tyg_quartile_target_counts"] = quartile_counts
    if quartile_bounds:
        payload["tyg_quartile_bounds"] = quartile_bounds
    return payload


def _parse_table2_row(endpoint: str, label: str, cells: list[str]) -> list[dict[str, Any]]:
    if endpoint not in {"hospital", "icu"}:
        return []
    normalized = _normalize_label(label)
    targets: list[dict[str, Any]] = []
    for idx, cell in enumerate(cells, start=1):
        parsed = _parse_hr_cell(cell)
        if not parsed:
            continue
        if normalized.startswith("tyg"):
            model = TABLE2_MODEL_MAP[idx][1]
            term = "tyg_index"
        elif normalized in {"q2", "q3", "q4"}:
            model = TABLE2_MODEL_MAP[idx][0]
            term = f"{normalized.upper()}_vs_Q1"
        else:
            continue
        targets.append(
            {
                "endpoint": endpoint,
                "model": model,
                "term": term,
                "hazard_ratio": parsed["hazard_ratio"],
                "ci_lower_95": parsed["ci_lower_95"],
                "ci_upper_95": parsed["ci_upper_95"],
                "p_value": parsed["p_value"],
                "source_file": "papers/table.md",
                "source_section": "Table 2",
                "source_label": label,
            }
        )
    return targets


def _split_markdown_row(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _strip_markdown_formatting(text: str) -> str:
    return text.replace("**", "").replace("`", "").strip()


def _lookup_label_mapping(label: str, mapping: tuple[tuple[str, str, str, str], ...]) -> tuple[str, str, str] | None:
    normalized = _normalize_label(label)
    for fragment, metric, kind, unit in mapping:
        if fragment in normalized:
            return metric, kind, unit
    return None


def _lookup_binary_metric(label: str) -> str:
    normalized = _normalize_label(label)
    for fragment, metric in SUPPLEMENT_BINARY_MAP:
        if fragment in normalized:
            return metric
    return ""


def _normalize_label(text: str) -> str:
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", text.lower())


def _extract_primary_numeric(text: str, percent: bool = False) -> float | None:
    cleaned = text.replace(",", "").replace(" ", "")
    if percent:
        match = re.search(r"([0-9.]+)%", cleaned)
        if match:
            return float(match.group(1))
    match = re.search(r"([0-9.]+)", cleaned)
    if not match:
        return None
    return float(match.group(1))


def _extract_percent_from_count_cell(text: str) -> float | None:
    cleaned = text.replace(",", "").replace(" ", "")
    match = re.search(r"\(([0-9.]+)\)", cleaned)
    if not match:
        return None
    return float(match.group(1))


def _parse_hr_cell(text: str) -> dict[str, float] | None:
    cleaned = _strip_markdown_formatting(text).replace("–", "-").replace("—", "-").strip()
    match = re.search(r"([0-9.]+)\s*\(([0-9.]+)-([0-9.]+)\)\s*([<>]?[0-9.]+)", cleaned)
    if not match:
        return None
    p_value = _parse_p_value(match.group(4))
    if p_value is None:
        return None
    return {
        "hazard_ratio": float(match.group(1)),
        "ci_lower_95": float(match.group(2)),
        "ci_upper_95": float(match.group(3)),
        "p_value": p_value,
    }


def _parse_p_value(text: str) -> float | None:
    cleaned = text.replace("**", "").strip()
    if not cleaned:
        return None
    if cleaned.startswith("<"):
        value = _to_float(cleaned[1:])
        return value
    return _to_float(cleaned)


def _to_float(text: str | None) -> float | None:
    if text is None:
        return None
    try:
        return float(str(text).strip())
    except ValueError:
        return None
