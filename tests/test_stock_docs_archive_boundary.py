from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "AIRIVO_STOCK_DOCS_ARCHIVE_MANIFEST_20260514.md"
STOCK_DOCS = ROOT / "stock_ultimate_system" / "docs"
ARCHIVE_DIR = STOCK_DOCS / "archive" / "2026-05-14"

ACTIVE_KEEP_DOCS = {
    "stock_ultimate_system/docs/API_REFERENCE.md",
    "stock_ultimate_system/docs/ARTIFACT_POLLUTION_CLEANUP_INVENTORY.md",
    "stock_ultimate_system/docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md",
    "stock_ultimate_system/docs/FORMAL_RUNTIME_CONVERGENCE_FINDINGS_2026-04-30.md",
    "stock_ultimate_system/docs/MAIN_CHAIN_AUTHENTICITY_CHECKLIST.md",
    "stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md",
    "stock_ultimate_system/docs/OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md",
    "stock_ultimate_system/docs/PRIMARY_RESULT_EVIDENCE_STOPLOSS.md",
    "stock_ultimate_system/docs/PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md",
    "stock_ultimate_system/docs/PRIMARY_RESULT_LATEST_REBUILD_RUNBOOK.md",
    "stock_ultimate_system/docs/PRIMARY_RESULT_REVIEW_RUNBOOK.md",
    "stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md",
}


def _section(text: str, heading: str) -> str:
    marker = f"## {heading}"
    start = text.find(marker)
    assert start >= 0, f"missing section: {heading}"
    next_heading = text.find("\n## ", start + len(marker))
    if next_heading < 0:
        return text[start:]
    return text[start:next_heading]


def _manifest_doc_refs(text: str) -> set[str]:
    refs = set(re.findall(r"`(stock_ultimate_system/docs/[^`]+\.md)`", text))
    refs.discard("stock_ultimate_system/docs/*.md")
    return refs


def test_stock_docs_manifest_covers_active_and_archived_docs():
    text = MANIFEST.read_text(encoding="utf-8")
    active_docs = {path.relative_to(ROOT).as_posix() for path in STOCK_DOCS.glob("*.md")}
    archived_docs = {
        f"stock_ultimate_system/docs/{path.name}"
        for path in ARCHIVE_DIR.glob("*.md")
    }

    assert active_docs == ACTIVE_KEEP_DOCS
    assert len(archived_docs) == 43
    assert _manifest_doc_refs(text) == active_docs | archived_docs


def test_stock_docs_active_keep_allowlist_is_small_and_explicit():
    text = MANIFEST.read_text(encoding="utf-8")
    keep_section = _section(text, "Keep: Current Operating Documents")
    keep_refs = _manifest_doc_refs(keep_section)

    assert keep_refs == ACTIVE_KEEP_DOCS
    assert 10 <= len(keep_refs) <= 15


def test_stock_docs_archive_manifest_has_execution_guardrails():
    text = MANIFEST.read_text(encoding="utf-8")

    assert "stock_ultimate_system/docs/archive/2026-05-14/" in text
    assert "43 documents were moved" in text
    assert "Do not rewrite historical docs just to modernize language." in text
