from __future__ import annotations

from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile


def collect_paper_materials(paper_path: Path) -> dict[str, str]:
    materials: dict[str, str] = {}
    if paper_path.exists():
        materials[paper_path.name] = paper_path.read_text(encoding="utf-8", errors="ignore")

    parent = paper_path.parent
    for sibling_name in ("table.md", "si.docx", "paper.md", "MIMIC.md"):
        candidate = parent / sibling_name
        if not candidate.exists() or candidate == paper_path:
            continue
        if candidate.suffix.lower() == ".md":
            materials[candidate.name] = candidate.read_text(encoding="utf-8", errors="ignore")
        elif candidate.suffix.lower() == ".docx":
            materials[candidate.name] = extract_docx_text(candidate)
    return materials


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
