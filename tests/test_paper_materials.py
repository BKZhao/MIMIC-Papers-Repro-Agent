from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from repro_agent.paper_materials import collect_paper_materials, extract_pdf_text  # noqa: E402


class PaperMaterialsTests(unittest.TestCase):
    def test_collect_paper_materials_extracts_pdf_and_related_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            pdf_path = project_root / "paper.pdf"
            pdf_path.write_bytes(b"%PDF-1.4\n")
            markdown_path = project_root / "paper.md"
            markdown_path.write_text("Structured companion markdown", encoding="utf-8")

            with patch("repro_agent.paper_materials.extract_pdf_text", return_value="PDF body text") as extractor:
                materials = collect_paper_materials(pdf_path)

            extractor.assert_called_once_with(pdf_path)
            self.assertEqual(materials["paper.pdf"], "PDF body text")
            self.assertEqual(materials["paper.md"], "Structured companion markdown")

    def test_collect_paper_materials_ignores_unrelated_markdown_from_same_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            pdf_path = project_root / "nlr-study.pdf"
            pdf_path.write_bytes(b"%PDF-1.4\n")
            (project_root / "MIMIC.md").write_text("Old TyG design that should not leak in", encoding="utf-8")
            (project_root / "table.md").write_text("Old TyG table that should not leak in", encoding="utf-8")

            with patch("repro_agent.paper_materials.extract_pdf_text", return_value="NLR paper text"):
                materials = collect_paper_materials(pdf_path)

            self.assertEqual(materials, {"nlr-study.pdf": "NLR paper text"})

    def test_extract_pdf_text_falls_back_between_extractors(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_path = Path(tmpdir) / "paper.pdf"
            pdf_path.write_bytes(b"%PDF-1.4\n")

            with patch(
                "repro_agent.paper_materials._extract_pdf_text_with_pymupdf",
                side_effect=RuntimeError("pymupdf unavailable"),
            ), patch(
                "repro_agent.paper_materials._extract_pdf_text_with_pdfplumber",
                side_effect=RuntimeError("pdfplumber unavailable"),
            ), patch(
                "repro_agent.paper_materials._extract_pdf_text_with_ghostscript",
                return_value="  Title line \n\n  Abstract line  ",
            ):
                text = extract_pdf_text(pdf_path)

            self.assertEqual(text, "Title line\nAbstract line")


if __name__ == "__main__":
    unittest.main()
